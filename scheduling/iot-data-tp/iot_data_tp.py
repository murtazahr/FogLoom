import asyncio
import hashlib
import json
import logging
import os
import traceback
from concurrent.futures import ThreadPoolExecutor

import couchdb
from coredis import RedisCluster
from coredis.exceptions import RedisError
from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.handler import TransactionHandler

# CouchDB configuration
COUCHDB_URL = f"http://{os.getenv('COUCHDB_USER')}:{os.getenv('COUCHDB_PASSWORD')}@{os.getenv('COUCHDB_HOST', 'couch-db-0:5984')}"
COUCHDB_DATA_DB = 'task_data'

# Redis configuration
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis-cluster:6379')

# Sawtooth configuration
FAMILY_NAME = 'iot-data'
FAMILY_VERSION = '1.0'

WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
IOT_DATA_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

logger = logging.getLogger(__name__)


class IoTDataTransactionHandler(TransactionHandler):
    def __init__(self):
        self.couch = couchdb.Server(COUCHDB_URL)
        self.data_db = self.couch[COUCHDB_DATA_DB]
        self.redis = None
        self.loop = asyncio.get_event_loop()
        self.executor = ThreadPoolExecutor()

    async def initialize_redis(self):
        self.redis = await RedisCluster.from_url(REDIS_URL, decode_responses=True)

    @property
    def family_name(self):
        return FAMILY_NAME

    @property
    def family_versions(self):
        return [FAMILY_VERSION]

    @property
    def namespaces(self):
        return [IOT_DATA_NAMESPACE, WORKFLOW_NAMESPACE]

    def apply(self, transaction, context):
        try:
            payload = json.loads(transaction.payload.decode())
            iot_data = payload['iot_data']
            workflow_id = payload['workflow_id']
            schedule_id = payload['schedule_id']
            persist_data = payload.get('persist_data', False)  # New line

            logger.info(f"Processing IoT data for workflow ID: {workflow_id}, schedule ID: {schedule_id}")

            if not isinstance(iot_data, list):
                raise InvalidTransaction("Invalid payload: iot_data must be a list")

            if not self._validate_workflow_id(context, workflow_id):
                raise InvalidTransaction(f"Invalid workflow ID: {workflow_id}")

            iot_data_hash = self._calculate_hash(iot_data)

            # Run async operations in a separate thread
            future = self.executor.submit(self._run_async_operations, context, workflow_id, schedule_id, iot_data,
                                          iot_data_hash, persist_data)  # Modified line
            future.result()  # This will raise any exceptions that occurred in the async operations

            if persist_data:
                iot_data_address = self._make_iot_data_address(schedule_id)
                iot_data_state = json.dumps({
                    'schedule_id': schedule_id,
                    'workflow_id': workflow_id,
                    'iot_data_hash': iot_data_hash
                }).encode()

                logger.info(f"Writing IoT data hash to blockchain for schedule ID: {schedule_id}")
                context.set_state({iot_data_address: iot_data_state})
                logger.info(f"Successfully wrote IoT data hash to blockchain for schedule ID: {schedule_id}")

        except json.JSONDecodeError as _:
            raise InvalidTransaction("Invalid payload: not a valid JSON")
        except KeyError as e:
            raise InvalidTransaction(f"Invalid payload: missing {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in apply method: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(str(e))

    def _run_async_operations(self, context, workflow_id, schedule_id, iot_data, iot_data_hash, persist_data):
        return self.loop.run_until_complete(
            self._async_operations(context, workflow_id, schedule_id, iot_data, iot_data_hash, persist_data))

    async def _async_operations(self, context, workflow_id, schedule_id, iot_data, iot_data_hash, persist_data):
        if not self.redis:
            await self.initialize_redis()

        dependency_graph = self._get_dependency_graph(context, workflow_id)
        data_id = self._generate_data_id(workflow_id, schedule_id, dependency_graph["start"], 'input')

        # Always store in Redis, but conditionally store in CouchDB
        tasks = [self._store_data_in_redis(data_id, iot_data, iot_data_hash, workflow_id, schedule_id, persist_data)]
        if persist_data:
            tasks.append(self._store_data_in_couchdb(data_id, iot_data, iot_data_hash, workflow_id, schedule_id))

        await asyncio.gather(*tasks)

    async def _store_data_in_redis(self, data_id, data, data_hash, workflow_id, schedule_id, persist_data):
        try:
            data_json = json.dumps({
                'data': data,
                'workflow_id': workflow_id,
                'schedule_id': schedule_id,
                'persist_data': persist_data
            })
            result = await self.redis.set(f"iot_data_{data_id}", data_json)
            if result:
                logger.info(f"Successfully stored IoT data in Redis for ID: {data_id}")
            else:
                raise Exception("Redis set operation failed")
        except RedisError as e:
            logger.error(f"Failed to store IoT data in Redis: {str(e)}")
            raise

    async def _store_data_in_couchdb(self, data_id, data, data_hash, workflow_id, schedule_id):
        try:
            self.data_db.save({
                '_id': data_id,
                'data': data,
                'data_hash': data_hash,
                'workflow_id': workflow_id,
                'schedule_id': schedule_id
            })
            logger.info(f"Successfully stored IoT data in CouchDB for ID: {data_id}")
        except couchdb.http.ResourceConflict:
            logger.info(f"Data for ID: {data_id} already exists in CouchDB. Skipping storage.")
        except Exception as e:
            logger.error(f"Unexpected error while storing data in CouchDB for ID {data_id}: {str(e)}")
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

    @staticmethod
    def _generate_data_id(workflow_id, schedule_id, app_id, data_type):
        return f"{workflow_id}_{schedule_id}_{app_id}_{data_type}"

    @staticmethod
    def _calculate_hash(data):
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

    @staticmethod
    def _make_iot_data_address(schedule_id):
        return IOT_DATA_NAMESPACE + hashlib.sha512(schedule_id.encode()).hexdigest()[:64]

    @staticmethod
    def _make_workflow_address(workflow_id):
        return WORKFLOW_NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]


def main():
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = IoTDataTransactionHandler()
    processor.add_handler(handler)
    processor.start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
