import asyncio
import hashlib
import json
import logging
import os
import ssl
import tempfile
import traceback
from concurrent.futures import ThreadPoolExecutor

from coredis import RedisCluster
from coredis.exceptions import RedisError
from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.handler import TransactionHandler

from scheduler import create_scheduler

# Redis configuration
REDIS_CLUSTER_URL = os.getenv('REDIS_CLUSTER_URL', 'rediss://redis-cluster:6379')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_SSL_CERT = os.getenv('REDIS_SSL_CERT')
REDIS_SSL_KEY = os.getenv('REDIS_SSL_KEY')
REDIS_SSL_CA = os.getenv('REDIS_SSL_CA')

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
        self.redis = None
        self.loop = asyncio.get_event_loop()
        self.thread_pool = ThreadPoolExecutor()
        self._initialize_redis()

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
            future = self.thread_pool.submit(self._run_async_operations, schedule_id, workflow_id, source_url,
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

    def _initialize_redis(self):
        logger.info("Starting Redis initialization")
        temp_files = []
        try:
            ssl_context = ssl.create_default_context()

            if REDIS_SSL_CA:
                ca_file = tempfile.NamedTemporaryFile(delete=False, mode='w+')
                ca_file.write(REDIS_SSL_CA)
                ca_file.flush()
                temp_files.append(ca_file.name)
                ssl_context.load_verify_locations(cafile=ca_file.name)
                logger.debug(f"CA certificate loaded from temp file: {ca_file.name}")
            else:
                logger.warning("REDIS_SSL_CA is empty or not set")

            if REDIS_SSL_CERT and REDIS_SSL_KEY:
                cert_file = tempfile.NamedTemporaryFile(delete=False, mode='w+')
                key_file = tempfile.NamedTemporaryFile(delete=False, mode='w+')
                cert_file.write(REDIS_SSL_CERT)
                key_file.write(REDIS_SSL_KEY)
                cert_file.flush()
                key_file.flush()
                temp_files.extend([cert_file.name, key_file.name])
                ssl_context.load_cert_chain(
                    certfile=cert_file.name,
                    keyfile=key_file.name
                )
                logger.debug(f"Client certificate loaded from temp file: {cert_file.name}")
                logger.debug(f"Client key loaded from temp file: {key_file.name}")
            else:
                logger.warning("REDIS_SSL_CERT or REDIS_SSL_KEY is empty or not set")

            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            logger.debug("SSL context configured: check_hostname=False, verify_mode=CERT_NONE")

            logger.info(f"Attempting to connect to Redis cluster at {REDIS_CLUSTER_URL}")
            self.redis = RedisCluster.from_url(
                REDIS_CLUSTER_URL,
                password=REDIS_PASSWORD,
                ssl=True,
                ssl_context=ssl_context,
                decode_responses=True
            )
            logger.info("Connected to Redis cluster successfully")

        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        finally:
            # Clean up temporary files
            for file_path in temp_files:
                try:
                    os.unlink(file_path)
                    logger.debug(f"Temporary file deleted: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")

    def _run_async_operations(self, schedule_id, workflow_id, source_url, source_public_key, timestamp, context):
        return self.loop.run_until_complete(self._async_operations(schedule_id, workflow_id, source_url,
                                                                   source_public_key, timestamp, context))

    async def _async_operations(self, schedule_id, workflow_id, source_url, source_public_key, timestamp, context):

        if await self._check_schedule_in_redis(schedule_id):
            logger.info(f"Schedule {schedule_id} already exists in Redis. Proceeding with blockchain update.")
        else:
            scheduler = self._initialize_scheduler(context, workflow_id)
            schedule_result = await scheduler.schedule()  # Await the coroutine here

            if await self._check_schedule_in_redis(schedule_id):
                logger.info(f"Schedule {schedule_id} generated but won't be saved as record already exists. "
                            f"Proceeding with blockchain update.")
            else:
                # Store in Redis
                await self._store_schedule_in_redis(schedule_id, schedule_result, workflow_id,
                                                    source_url, source_public_key, timestamp)

        schedule_doc = await self._fetch_data_with_retry(schedule_id)
        return {
            'schedule_id': schedule_id,
            'workflow_id': workflow_id,
            'timestamp': timestamp,
            'source_url': source_url,
            'source_public_key': source_public_key,
            'schedule': schedule_doc['schedule']
        }

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

            # Set the schedule data in Redis
            await self.redis.set(key, schedule_json)

            # Publish the schedule data to the single "schedule" channel
            channel = "schedule"
            publish_result = await self.redis.publish(channel, schedule_json)

            if publish_result == 0:
                logger.warning(f"No clients received the published message for schedule ID: {schedule_id}")

            logger.info(f"Successfully stored and published schedule in Redis for schedule ID: {schedule_id}")
        except RedisError as e:
            logger.error(f"Failed to store and publish schedule in Redis: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in _store_schedule_in_redis: {str(e)}")
            raise

    async def _check_schedule_in_redis(self, schedule_id):
        try:
            key = f"schedule_{schedule_id}"
            schedule_data = await self.redis.get(key)
            return schedule_data is not None
        except RedisError as e:
            logger.error(f"Error checking schedule in Redis: {str(e)}")
            return False

    async def _fetch_data_with_retry(self, schedule_id, max_retries=3, retry_delay=1):
        for attempt in range(max_retries):
            try:
                # Try Redis first
                redis_data = await self.redis.get(f"schedule_{schedule_id}")
                if redis_data:
                    return json.loads(redis_data)

                raise Exception("Data not found in Redis")
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

            redis_config = {
                "redis-client": self.redis
            }

            return create_scheduler("lcdwrr", dependency_graph, app_requirements, redis_config)
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
