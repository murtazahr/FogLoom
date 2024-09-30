import hashlib
import json
import logging
import os
import traceback
from time import sleep

import couchdb
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

            if self._check_schedule_in_couchdb(schedule_id):
                logger.info(f"Schedule {schedule_id} already exists in CouchDB. Proceeding with blockchain update.")
            else:
                self.scheduler = self._initialize_scheduler(context, workflow_id)
                schedule_result = self.scheduler.schedule()

                if self._check_schedule_in_couchdb(schedule_id):
                    logger.info(f"Schedule {schedule_id} generated but won't be saved as record already exists in "
                                f"CouchDB. Proceeding with blockchain update.")
                else:
                    self._store_schedule_in_couchdb(schedule_id, schedule_result, workflow_id, source_url,
                                                    source_public_key, timestamp)

            schedule_doc = self.fetch_data_with_retry(self.schedule_db, schedule_id)
            schedule_address = self._make_schedule_address(schedule_id)
            schedule_state_data = json.dumps({
                'schedule_id': schedule_id,
                'workflow_id': workflow_id,
                'timestamp': timestamp,
                'source_url': source_url,
                'source_public_key': source_public_key,
                'schedule': schedule_doc['schedule']
            }).encode()

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

    @staticmethod
    def fetch_data_with_retry(db, key, max_retries=5, initial_delay=0.1):
        delay = initial_delay
        for attempt in range(max_retries):
            try:
                doc = db.get(key)
                if doc is None:
                    raise couchdb.http.ResourceNotFound
                return doc
            except couchdb.http.ResourceNotFound:
                if attempt == max_retries - 1:
                    logger.error(f"Data not found for key {key} after {max_retries} attempts")
                    raise
                logger.warning(
                    f"Data not found for key {key}, retrying in {delay:.2f} seconds (attempt {attempt + 1}/{max_retries})")
                sleep(delay)
                delay *= 2  # Exponential backoff
            except Exception as e:
                logger.error(f"Error fetching data for key {key}: {str(e)}", exc_info=True)
                raise

    def _check_schedule_in_couchdb(self, schedule_id):
        try:
            _ = self.schedule_db[schedule_id]
            return True
        except couchdb.http.ResourceNotFound:
            return False

    def _store_schedule_in_couchdb(self, schedule_id, schedule_result, workflow_id, source_url,
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
            raise InvalidTransaction(f"Failed to store schedule off-chain for schedule ID: {schedule_id}")

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

    def _make_workflow_address(self, workflow_id):
        return WORKFLOW_NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]

    def _make_docker_image_address(self, app_id):
        return DOCKER_IMAGE_NAMESPACE + hashlib.sha512(app_id.encode()).hexdigest()[:64]

    def _make_schedule_address(self, schedule_id):
        return SCHEDULE_NAMESPACE + hashlib.sha512(schedule_id.encode()).hexdigest()[:64]


def main():
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = IoTScheduleTransactionHandler()
    processor.add_handler(handler)
    processor.start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
