import asyncio
import hashlib
import json
import logging
import os
import traceback
from concurrent.futures import ThreadPoolExecutor

import couchdb
from redis import RedisCluster, RedisError
from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.handler import TransactionHandler

from scheduler import create_scheduler

# CouchDB configuration
COUCHDB_URL = f"http://{os.getenv('COUCHDB_USER')}:{os.getenv('COUCHDB_PASSWORD')}@{os.getenv('COUCHDB_HOST', 'couch-db-0:5984')}"
COUCHDB_DB = 'resource_registry'
COUCHDB_SCHEDULE_DB = 'schedules'
COUCHDB_DATA_DB = 'task_data'

# Redis configuration
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis-cluster:6379')

# Sawtooth configuration
FAMILY_NAME = 'iot-schedule'
FAMILY_VERSION = '1.0'

WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
DOCKER_IMAGE_NAMESPACE = hashlib.sha512('docker-image'.encode()).hexdigest()[:6]
SCHEDULE_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

logger = logging.getLogger(__name__)


class IoTScheduleTransactionHandler(TransactionHandler):
    def __init__(self):
        self.scheduler = None
        self.couch = couchdb.Server(COUCHDB_URL)
        self.schedule_db = self.couch[COUCHDB_SCHEDULE_DB]
        self.data_db = self.couch[COUCHDB_DATA_DB]
        self.redis = RedisCluster.from_url(REDIS_URL, decode_responses=True)
        self.loop = asyncio.new_event_loop()
        self.executor = ThreadPoolExecutor(max_workers=3)

    @property
    def family_name(self):
        return FAMILY_NAME

    @property
    def family_versions(self):
        return [FAMILY_VERSION]

    @property
    def namespaces(self):
        return [SCHEDULE_NAMESPACE, WORKFLOW_NAMESPACE, DOCKER_IMAGE_NAMESPACE]

    def apply(self, transaction, context):
        try:
            payload = json.loads(transaction.payload.decode())
            workflow_id = payload['workflow_id']
            schedule_id = payload['schedule_id']
            source_url = payload['source_url']
            source_public_key = payload['source_public_key']
            timestamp = payload['timestamp']

            logger.info(f"Processing schedule for workflow ID: {workflow_id}, schedule ID: {schedule_id}")

            if not self._validate_workflow_id(context, workflow_id):
                raise InvalidTransaction(f"Invalid workflow ID: {workflow_id}")

            # Run async operations in a separate thread
            future = self.executor.submit(self._run_async_operations, schedule_id, workflow_id, source_url,
                                          source_public_key, timestamp, context)
            result = future.result()  # This will raise any exceptions that occurred in the async operations

            schedule_address = self._make_schedule_address(schedule_id)
            schedule_state_data = json.dumps(result).encode()

            logger.info(f"Writing schedule status to blockchain for schedule ID: {schedule_id}")
            context.set_state({schedule_address: schedule_state_data})
            logger.info(f"Successfully wrote schedule status to blockchain for schedule ID: {schedule_id}")

        except json.JSONDecodeError as _:
            raise InvalidTransaction("Invalid payload: not a valid JSON")
        except KeyError as e:
            raise InvalidTransaction(f"Invalid payload: missing {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in apply method: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(str(e))

    def _run_async_operations(self, schedule_id, workflow_id, source_url, source_public_key, timestamp, context):
        return self.loop.run_until_complete(self._async_operations(schedule_id, workflow_id, source_url,
                                                                   source_public_key, timestamp, context))

    async def _async_operations(self, schedule_id, workflow_id, source_url, source_public_key, timestamp, context):
        if await self._check_schedule_in_redis(schedule_id):
            logger.info(f"Schedule {schedule_id} already exists in Redis. Proceeding with blockchain update.")
        else:
            scheduler = self._initialize_scheduler(context, workflow_id)
            schedule_result = scheduler.schedule()

            if await self._check_schedule_in_redis(schedule_id):
                logger.info(f"Schedule {schedule_id} generated but won't be saved as record already exists. "
                            f"Proceeding with blockchain update.")
            else:
                # Store in Redis and CouchDB
                await asyncio.gather(
                    self._store_schedule_in_redis(schedule_id, schedule_result, workflow_id,
                                                  source_url, source_public_key, timestamp),
                    self._store_schedule_in_couchdb(schedule_id, schedule_result, workflow_id,
                                                    source_url, source_public_key, timestamp)
                )

        schedule_doc = await self._fetch_data_with_retry(schedule_id)
        return {
            'schedule_id': schedule_id,
            'workflow_id': workflow_id,
            'timestamp': timestamp,
            'source_url': source_url,
            'source_public_key': source_public_key,
            'schedule': schedule_doc['schedule']
        }

    async def _store_schedule_in_couchdb(self, schedule_id, schedule_result, workflow_id, source_url,
                                         source_public_key, timestamp):
        try:
            document = {
                '_id': schedule_id,
                'schedule': schedule_result,
                'workflow_id': workflow_id,
                'source_url': source_url,
                'source_public_key': source_public_key,
                'timestamp': timestamp,
                'status': 'ACTIVE'
            }
            self.schedule_db.save(document)
            logger.info(f"Successfully stored schedule in CouchDB for schedule ID: {schedule_id}")
        except Exception as e:
            logger.error(f"Failed to store schedule in CouchDB: {str(e)}")
            raise

    async def _store_schedule_in_redis(self, schedule_id, schedule_result, workflow_id, source_url,
                                       source_public_key, timestamp):
        try:
            schedule_data = {
                'schedule_id': schedule_id,
                'schedule': schedule_result,
                'workflow_id': workflow_id,
                'source_url': source_url,
                'source_public_key': source_public_key,
                'timestamp': timestamp,
                'status': 'ACTIVE'
            }
            schedule_json = json.dumps(schedule_data)
            key = f"schedule_{schedule_id}"

            # Use a pipeline to perform both operations atomically
            pipeline = self.redis.pipeline()

            # Set the schedule data in Redis
            pipeline.set(key, schedule_json)

            # Publish the schedule data to the single "schedule" channel
            channel = "schedule"
            pipeline.publish(channel, schedule_json)

            # Execute the pipeline
            results = pipeline.execute()

            if all(results):
                logger.info(f"Successfully stored and published schedule in Redis for schedule ID: {schedule_id}")
            else:
                raise Exception("Redis set or publish operation failed")
        except RedisError as e:
            logger.error(f"Failed to store and publish schedule in Redis: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in _store_schedule_in_redis: {str(e)}")
            raise

    async def _check_schedule_in_redis(self, schedule_id):
        try:
            key = f"schedule_{schedule_id}"
            schedule_data = self.redis.get(key)
            return schedule_data is not None
        except RedisError as e:
            logger.error(f"Error checking schedule in Redis: {str(e)}")
            return False

    async def _fetch_data_with_retry(self, schedule_id, max_retries=3, retry_delay=1):
        for attempt in range(max_retries):
            try:
                # Try Redis first
                redis_data = self.redis.get(f"schedule_{schedule_id}")
                if redis_data:
                    return json.loads(redis_data)

                # If not in Redis, try CouchDB
                couchdb_data = self.schedule_db.get(schedule_id)
                if couchdb_data:
                    return couchdb_data

                raise Exception("Data not found in Redis or CouchDB")
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch data after {max_retries} attempts: {str(e)}")
                    raise
                await asyncio.sleep(retry_delay)

    def _initialize_scheduler(self, context, workflow_id):
        try:
            dependency_graph = self._get_dependency_graph(context, workflow_id)
            app_requirements = {}
            for app_id in dependency_graph['nodes']:
                app_requirements[app_id] = self._get_app_requirements(context, app_id)

            db_config = {
                "url": COUCHDB_URL,
                "name": COUCHDB_DB
            }

            redis_config = {
                "url": REDIS_URL
            }

            return create_scheduler("lcdwrr", dependency_graph, app_requirements, db_config, redis_config)
        except Exception as e:
            logger.error(f"Failed to initialize scheduler: {str(e)}")
            raise

    def _validate_workflow_id(self, context, workflow_id):
        address = self._make_workflow_address(workflow_id)
        state_entries = context.get_state([address])
        return len(state_entries) > 0

    def _get_dependency_graph(self, context, workflow_id):
        address = self._make_workflow_address(workflow_id)
        state_entries = context.get_state([address])
        if state_entries:
            try:
                workflow_data = json.loads(state_entries[0].data.decode())
                if 'dependency_graph' not in workflow_data:
                    raise KeyError("'dependency_graph' not found in workflow data")
                dependency_graph = workflow_data['dependency_graph']
                if 'nodes' not in dependency_graph:
                    raise KeyError("'nodes' not found in dependency graph")
                return dependency_graph
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Error parsing workflow data: {e}")
                raise InvalidTransaction(f"Invalid workflow data for workflow ID: {workflow_id}")
        else:
            raise InvalidTransaction(f"No workflow data found for workflow ID: {workflow_id}")

    def _get_app_requirements(self, context, app_id):
        address = self._make_docker_image_address(app_id)
        state_entries = context.get_state([address])
        if state_entries:
            app_data = json.loads(state_entries[0].data.decode())
            return {
                "memory": app_data["resource_requirements"]["memory"],
                "cpu": app_data["resource_requirements"]["cpu"],
                "disk": app_data["resource_requirements"]["disk"]
            }
        else:
            raise InvalidTransaction(f"No requirements found for app ID: {app_id}")

    @staticmethod
    def _make_workflow_address(workflow_id):
        return WORKFLOW_NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]

    @staticmethod
    def _make_docker_image_address(app_id):
        return DOCKER_IMAGE_NAMESPACE + hashlib.sha512(app_id.encode()).hexdigest()[:64]

    @staticmethod
    def _make_schedule_address(schedule_id):
        return SCHEDULE_NAMESPACE + hashlib.sha512(schedule_id.encode()).hexdigest()[:64]


def main():
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = IoTScheduleTransactionHandler()
    processor.add_handler(handler)
    processor.start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
