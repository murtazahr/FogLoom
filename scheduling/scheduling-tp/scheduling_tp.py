import hashlib
import json
import logging
import os
import traceback

from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.handler import TransactionHandler

# Import the LCDWRRScheduler
from scheduler import create_scheduler

COUCHDB_URL = f"http://{os.getenv('COUCHDB_USER')}:{os.getenv('COUCHDB_PASSWORD')}@{os.getenv('COUCHDB_HOST', 'couch-db-0:5984')}"
COUCHDB_DB = 'resource_registry'

FAMILY_NAME = 'iot-schedule'
FAMILY_VERSION = '1.0'

WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
DOCKER_IMAGE_NAMESPACE = hashlib.sha512('docker-image'.encode()).hexdigest()[:6]
SCHEDULE_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

logger = logging.getLogger(__name__)


class IoTScheduleTransactionHandler(TransactionHandler):
    def __init__(self):
        self.scheduler = None

    @property
    def family_name(self):
        return FAMILY_NAME

    @property
    def family_versions(self):
        return [FAMILY_VERSION]

    @property
    def namespaces(self):
        return [SCHEDULE_NAMESPACE, WORKFLOW_NAMESPACE, DOCKER_IMAGE_NAMESPACE]

    def _initialize_scheduler(self, context, workflow_id):
        try:
            # Fetch dependency graph from blockchain
            dependency_graph = self._get_dependency_graph(context, workflow_id)

            # Fetch app requirements for each app in the dependency graph
            app_requirements = {}
            for app_id in dependency_graph['nodes']:
                app_requirements[app_id] = self._get_app_requirements(context, app_id)

            db_config = {
                "url": COUCHDB_URL,
                "name": COUCHDB_DB
            }

            return create_scheduler("lcdwrr", dependency_graph, app_requirements, db_config)
        except Exception as e:
            logger.error(f"Failed to initialize scheduler: {str(e)}")
            raise

    def apply(self, transaction, context):
        try:
            payload = json.loads(transaction.payload.decode())
            iot_data = payload['iot_data']
            workflow_id = payload['workflow_app_id']
            timestamp = payload['timestamp']

            logger.info(f"Processing IoT data for workflow ID: {workflow_id}")

            # Validate workflow_id
            if not self._validate_workflow_id(context, workflow_id):
                raise InvalidTransaction(f"Invalid workflow ID: {workflow_id}")

            # Initialize scheduler with blockchain data
            self.scheduler = self._initialize_scheduler(context, workflow_id)

            # Schedule data processing
            schedule_result = self._schedule_data_processing(iot_data, workflow_id)

            # Store the schedule result in state
            schedule_address = self._make_schedule_address(workflow_id)
            schedule_state_data = json.dumps({
                'schedule': schedule_result,
                'timestamp': timestamp
            }).encode()
            context.set_state({schedule_address: schedule_state_data})

            logger.info(f"Scheduling completed for workflow ID: {workflow_id}")

        except json.JSONDecodeError as e:
            raise InvalidTransaction("Invalid payload: not a valid JSON")
        except KeyError as e:
            raise InvalidTransaction(f"Invalid payload: missing {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in apply method: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(str(e))

    def _validate_workflow_id(self, context, workflow_id):
        address = self._make_workflow_address(workflow_id)
        state_entries = context.get_state([address])
        return len(state_entries) > 0

    def _get_dependency_graph(self, context, workflow_id):
        address = self._make_workflow_address(workflow_id)
        state_entries = context.get_state([address])
        if state_entries:
            return json.loads(state_entries[0].data.decode())
        else:
            raise InvalidTransaction(f"No dependency graph found for workflow ID: {workflow_id}")

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

    def _schedule_data_processing(self, iot_data, workflow_id):
        try:
            logger.info(f"Scheduling data processing for workflow ID: {workflow_id}")
            schedule_result = self.scheduler.schedule(iot_data)
            logger.info(f"Scheduling result: {schedule_result}")
            return schedule_result
        except Exception as e:
            logger.error(f"Error in scheduling: {str(e)}")
            raise

    def _make_workflow_address(self, workflow_id):
        return WORKFLOW_NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]

    def _make_docker_image_address(self, app_id):
        return DOCKER_IMAGE_NAMESPACE + hashlib.sha512(app_id.encode()).hexdigest()[:64]

    def _make_schedule_address(self, workflow_id):
        return SCHEDULE_NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]


def main():
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = IoTScheduleTransactionHandler()
    processor.add_handler(handler)
    processor.start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
