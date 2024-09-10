import hashlib
import json
import logging
import os
import traceback

import couchdb
from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.handler import TransactionHandler

from scheduler import create_scheduler

COUCHDB_URL = f"http://{os.getenv('COUCHDB_USER')}:{os.getenv('COUCHDB_PASSWORD')}@{os.getenv('COUCHDB_HOST', 'couch-db-0:5984')}"
COUCHDB_DB = 'resource_registry'
COUCHDB_SCHEDULE_DB = 'schedules'

FAMILY_NAME = 'iot-schedule'
FAMILY_VERSION = '1.0'

WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
DOCKER_IMAGE_NAMESPACE = hashlib.sha512('docker-image'.encode()).hexdigest()[:6]
SCHEDULE_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

logger = logging.getLogger(__name__)


class IoTScheduleTransactionHandler(TransactionHandler):
    def __init__(self):
        self.scheduler = None
        # CouchDB connection
        self.couch = couchdb.Server(COUCHDB_URL)
        self.db = self.couch[COUCHDB_SCHEDULE_DB]

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
            workflow_id = payload['workflow_id']
            schedule_id = payload['schedule_id']
            timestamp = payload['timestamp']

            logger.info(f"Processing IoT data for workflow ID: {workflow_id}, schedule ID: {schedule_id}")

            if not self._validate_workflow_id(context, workflow_id):
                raise InvalidTransaction(f"Invalid workflow ID: {workflow_id}")

            self.scheduler = self._initialize_scheduler(context, workflow_id)

            schedule_result = self._schedule_data_processing(iot_data, workflow_id)

            # Store the full schedule result in CouchDB
            self._store_schedule_in_couchdb(schedule_id, schedule_result, workflow_id, timestamp)

            # Store minimal information in blockchain
            schedule_address = self._make_schedule_address(schedule_id)
            schedule_state_data = json.dumps({
                'schedule_id': schedule_id,
                'workflow_id': workflow_id,
                'timestamp': timestamp,
                'status': 'COMPLETED'  # You might want to add more detailed status handling
            }).encode()

            logger.info(f"Writing schedule status to blockchain for schedule ID: {schedule_id}")
            context.set_state({schedule_address: schedule_state_data})
            logger.info(f"Successfully wrote schedule status to blockchain for schedule ID: {schedule_id}")

            logger.info(f"Scheduling completed for workflow ID: {workflow_id}, schedule ID: {schedule_id}")

        except json.JSONDecodeError as e:
            raise InvalidTransaction("Invalid payload: not a valid JSON")
        except KeyError as e:
            raise InvalidTransaction(f"Invalid payload: missing {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in apply method: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(str(e))

    def _store_schedule_in_couchdb(self, schedule_id, schedule_result, workflow_id, timestamp):
        try:
            document = {
                '_id': schedule_id,
                'schedule': schedule_result,
                'workflow_id': workflow_id,
                'timestamp': timestamp
            }
            self.db.save(document)
            logger.info(f"Successfully stored schedule in CouchDB for schedule ID: {schedule_id}")
        except Exception as e:
            logger.error(f"Failed to store schedule in CouchDB: {str(e)}")
            raise InvalidTransaction(f"Failed to store schedule off-chain for schedule ID: {schedule_id}")

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
                logger.debug(f"Retrieved workflow data: {workflow_data}")

                if 'dependency_graph' not in workflow_data:
                    raise KeyError("'dependency_graph' not found in workflow data")

                dependency_graph = workflow_data['dependency_graph']
                logger.debug(f"Extracted dependency graph: {dependency_graph}")

                if 'nodes' not in dependency_graph:
                    raise KeyError("'nodes' not found in dependency graph")

                return dependency_graph

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in workflow data: {e}")
                raise InvalidTransaction(f"Invalid workflow data for workflow ID: {workflow_id}")
            except KeyError as e:
                logger.error(f"Missing key in workflow data: {e}")
                raise InvalidTransaction(f"Invalid workflow data structure for workflow ID: {workflow_id}")
            except Exception as e:
                logger.error(f"Unexpected error parsing workflow data: {e}")
                raise InvalidTransaction(f"Error processing workflow data for workflow ID: {workflow_id}")
        else:
            logger.error(f"No workflow data found for workflow ID: {workflow_id}")
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

    def _schedule_data_processing(self, iot_data, workflow_id):
        try:
            logger.info(f"Starting scheduling process for workflow ID: {workflow_id}")

            # Log the input data (be cautious with sensitive data)
            logger.debug(f"IoT data for workflow {workflow_id}: {iot_data}")

            # Perform the scheduling
            schedule_result = self.scheduler.schedule(iot_data)

            # Log the scheduling result
            logger.info(f"Scheduling completed for workflow ID: {workflow_id}")
            logger.debug(f"Scheduling result for workflow {workflow_id}: {schedule_result}")

            # Validate the scheduling result
            if not schedule_result or not isinstance(schedule_result, dict):
                raise ValueError(f"Invalid scheduling result for workflow {workflow_id}")

            return schedule_result

        except Exception as e:
            logger.error(f"Error in scheduling for workflow ID {workflow_id}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise InvalidTransaction(f"Scheduling failed for workflow ID {workflow_id}: {str(e)}")

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
