import asyncio
import hashlib
import json
import logging
import os
import ssl
import tempfile
import traceback
from concurrent.futures import ThreadPoolExecutor

from ibmcloudant.cloudant_v1 import CloudantV1
from ibm_cloud_sdk_core.authenticators import BasicAuthenticator
from coredis import RedisCluster
from coredis.exceptions import RedisError
from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.handler import TransactionHandler

# Cloudant configuration
CLOUDANT_URL = f"https://{os.getenv('COUCHDB_HOST', 'cloudant-host:6984')}"
CLOUDANT_USERNAME = os.getenv('COUCHDB_USER')
CLOUDANT_PASSWORD = os.getenv('COUCHDB_PASSWORD')
CLOUDANT_DB = 'task_data'
CLOUDANT_SSL_CA = os.getenv('COUCHDB_SSL_CA')
CLOUDANT_SSL_CERT = os.getenv('COUCHDB_SSL_CERT')
CLOUDANT_SSL_KEY = os.getenv('COUCHDB_SSL_KEY')

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_SSL_CERT = os.getenv('REDIS_SSL_CERT')
REDIS_SSL_KEY = os.getenv('REDIS_SSL_KEY')
REDIS_SSL_CA = os.getenv('REDIS_SSL_CA')

# Sawtooth configuration
FAMILY_NAME = 'iot-data'
FAMILY_VERSION = '1.0'

WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
IOT_DATA_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

logger = logging.getLogger(__name__)


class IoTDataTransactionHandler(TransactionHandler):
    def __init__(self):
        self.cloudant_client = None
        self.redis = None
        self.loop = asyncio.get_event_loop()
        self.executor = ThreadPoolExecutor()
        self.cloudant_certs = []
        self._initialize_cloudant()
        self._initialize_redis()

    def _initialize_cloudant(self):
        logger.info("Starting Cloudant initialization")
        try:
            authenticator = BasicAuthenticator(CLOUDANT_USERNAME, CLOUDANT_PASSWORD)
            self.cloudant_client = CloudantV1(authenticator=authenticator)
            self.cloudant_client.set_service_url(CLOUDANT_URL)

            cert_files = None

            if CLOUDANT_SSL_CA:
                ca_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.crt', delete=False)
                ca_file.write(CLOUDANT_SSL_CA)
                ca_file.flush()
                self.cloudant_certs.append(ca_file)
                ssl_verify = ca_file.name
            else:
                logger.warning("CLOUDANT_SSL_CA is empty or not set")
                ssl_verify = False

            if CLOUDANT_SSL_CERT and CLOUDANT_SSL_KEY:
                cert_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.crt', delete=False)
                key_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.key', delete=False)
                cert_file.write(CLOUDANT_SSL_CERT)
                key_file.write(CLOUDANT_SSL_KEY)
                cert_file.flush()
                key_file.flush()
                self.cloudant_certs.extend([cert_file, key_file])
                cert_files = (cert_file.name, key_file.name)
            else:
                logger.warning("CLOUDANT_SSL_CERT or CLOUDANT_SSL_KEY is empty or not set")

            # Set the SSL configuration for the client
            self.cloudant_client.set_http_config({
                'verify': ssl_verify,
                'cert': cert_files
            })

            self.cloudant_client.get_database_information(db=CLOUDANT_DB).get_result()
            logger.info(f"Successfully connected to database '{CLOUDANT_DB}'.")

        except Exception as e:
            logger.error(f"Failed to initialize Cloudant connection: {str(e)}")
            self.cleanup_temp_files()
            raise

    def _initialize_redis(self):
        logger.info("Starting Redis initialization")
        temp_files = []
        try:
            ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
            ssl_context.maximum_version = ssl.TLSVersion.TLSv1_3

            if REDIS_SSL_CA:
                ca_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.crt')
                ca_file.write(REDIS_SSL_CA)
                ca_file.flush()
                temp_files.append(ca_file.name)
                ssl_context.load_verify_locations(cafile=ca_file.name)
            else:
                logger.warning("REDIS_SSL_CA is empty or not set")

            if REDIS_SSL_CERT and REDIS_SSL_KEY:
                cert_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.crt')
                key_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.key')
                cert_file.write(REDIS_SSL_CERT)
                key_file.write(REDIS_SSL_KEY)
                cert_file.flush()
                key_file.flush()
                temp_files.extend([cert_file.name, key_file.name])
                ssl_context.load_cert_chain(
                    certfile=cert_file.name,
                    keyfile=key_file.name
                )
            else:
                logger.warning("REDIS_SSL_CERT or REDIS_SSL_KEY is empty or not set")

            logger.info(f"Attempting to connect to Redis cluster at {REDIS_HOST}:{REDIS_PORT}")
            self.redis = RedisCluster(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                ssl=True,
                ssl_context=ssl_context,
                decode_responses=True
            )
            logger.info("Connected to Redis cluster successfully")

        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
        finally:
            for file_path in temp_files:
                try:
                    os.unlink(file_path)
                    logger.debug(f"Temporary file deleted: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")

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
            persist_data = payload.get('persist_data', False)

            logger.info(f"Processing IoT data for workflow ID: {workflow_id}, schedule ID: {schedule_id}")

            if not isinstance(iot_data, list):
                raise InvalidTransaction("Invalid payload: iot_data must be a list")

            if not self._validate_workflow_id(context, workflow_id):
                raise InvalidTransaction(f"Invalid workflow ID: {workflow_id}")

            iot_data_hash = self._calculate_hash(iot_data)

            future = self.executor.submit(self._run_async_operations, context, workflow_id, schedule_id, iot_data,
                                          iot_data_hash, persist_data)
            future.result()

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
        dependency_graph = self._get_dependency_graph(context, workflow_id)
        data_id = self._generate_data_id(workflow_id, schedule_id, dependency_graph["start"], 'input')

        tasks = [self._store_data_in_redis(data_id, iot_data, workflow_id, schedule_id, persist_data)]
        if persist_data:
            tasks.append(self._store_data_in_cloudant(data_id, iot_data, iot_data_hash, workflow_id, schedule_id))

        await asyncio.gather(*tasks)

    async def _store_data_in_redis(self, data_id, data, workflow_id, schedule_id, persist_data):
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

    async def _store_data_in_cloudant(self, data_id, data, data_hash, workflow_id, schedule_id):
        try:
            doc = {
                '_id': data_id,
                'data': data,
                'data_hash': data_hash,
                'workflow_id': workflow_id,
                'schedule_id': schedule_id
            }
            self.cloudant_client.post_document(db=CLOUDANT_DB, document=doc).get_result()
            logger.info(f"Successfully stored IoT data in Cloudant for ID: {data_id}")
        except Exception as e:
            logger.error(f"Unexpected error while storing data in Cloudant for ID {data_id}: {str(e)}")
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

    def cleanup_temp_files(self):
        for file in self.cloudant_certs:
            file.close()
            os.unlink(file.name)
        self.cloudant_certs = []
        logger.info("Temporary SSL files cleaned up")

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
    try:
        processor.start()
    finally:
        handler.cleanup_temp_files()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
