import hashlib
import json
import logging
import os
import traceback

import couchdb
from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.handler import TransactionHandler

# CouchDB configuration
COUCHDB_URL = f"http://{os.getenv('COUCHDB_USER')}:{os.getenv('COUCHDB_PASSWORD')}@{os.getenv('COUCHDB_HOST', 'couch-db-0:5984')}"
COUCHDB_DATA_DB = 'task_data'

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

            logger.info(f"Processing IoT data for workflow ID: {workflow_id}, schedule ID: {schedule_id}")

            if not isinstance(iot_data, list):
                raise InvalidTransaction("Invalid payload: iot_data must be a list")

            if not self._validate_workflow_id(context, workflow_id):
                raise InvalidTransaction(f"Invalid workflow ID: {workflow_id}")

            iot_data_hash = self._calculate_hash(iot_data)

            self._store_initial_input_data(context, workflow_id, schedule_id, iot_data)

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

    def _store_initial_input_data(self, context, workflow_id, schedule_id, iot_data):
        try:
            dependency_graph = self._get_dependency_graph(context, workflow_id)

            data_id = self._generate_data_id(workflow_id, schedule_id, dependency_graph["start"], 'input')
            iot_data_hash = self._calculate_hash(iot_data)
            self._safe_store_data(data_id, iot_data, iot_data_hash, workflow_id, schedule_id)

            logger.info(f"Successfully processed initial input data for workflow ID: {workflow_id}, "
                        f"schedule ID: {schedule_id}")
        except KeyError as e:
            logger.error(f"Error processing initial input data: {str(e)}")
            raise InvalidTransaction(f"Failed to process initial input data for workflow ID: {workflow_id}, "
                                     f"schedule ID: {schedule_id}. Error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error processing initial input data: {str(e)}")
            raise InvalidTransaction(f"Failed to process initial input data for workflow ID: {workflow_id}, "
                                     f"schedule ID: {schedule_id}")

    def _safe_store_data(self, data_id, data, data_hash, workflow_id, schedule_id):
        try:
            self.data_db.save({
                '_id': data_id,
                'data': data,
                'data_hash': data_hash,
                'workflow_id': workflow_id,
                'schedule_id': schedule_id
            })
            logger.info(f"Successfully stored data for ID: {data_id}")
        except couchdb.http.ResourceConflict:
            logger.info(f"Data for ID: {data_id} already exists. Skipping storage.")
        except Exception as e:
            logger.error(f"Unexpected error while storing data for ID {data_id}: {str(e)}")
            raise

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
