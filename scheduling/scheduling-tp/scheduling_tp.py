import hashlib
import json
import logging
import os
import traceback

from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.core import TransactionProcessor

FAMILY_NAME = 'iot-data'
FAMILY_VERSION = '1.0'
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
DOCKER_IMAGE_NAMESPACE = hashlib.sha512('docker-image'.encode()).hexdigest()[:6]

logger = logging.getLogger(__name__)


class IoTDataTransactionHandler(TransactionHandler):
    @property
    def family_name(self):
        return FAMILY_NAME

    @property
    def family_versions(self):
        return [FAMILY_VERSION]

    @property
    def namespaces(self):
        return [NAMESPACE]

    def apply(self, transaction, context):
        try:
            payload = json.loads(transaction.payload.decode())
            iot_data = payload['iot_data']
            workflow_app_id = payload['workflow_app_id']
            timestamp = payload['timestamp']

            logger.info(f"Processing IoT data for workflow/app ID: {workflow_app_id}")

            # Validate workflow_app_id
            if not self._validate_workflow_app_id(context, workflow_app_id):
                raise InvalidTransaction(f"Invalid workflow/app ID: {workflow_app_id}")

            # Pass data to scheduler (skeleton function for now)
            self._schedule_data_processing(iot_data, workflow_app_id)

            # Store the transaction in state
            state_data = json.dumps({
                'iot_data': iot_data,
                'workflow_app_id': workflow_app_id,
                'timestamp': timestamp,
                'status': 'scheduled'
            }).encode()
            state_address = self._make_iot_data_address(workflow_app_id)
            context.set_state({state_address: state_data})

            logger.info(f"IoT data processed and scheduled for workflow/app ID: {workflow_app_id}")

        except json.JSONDecodeError as e:
            raise InvalidTransaction("Invalid payload: not a valid JSON")
        except KeyError as e:
            raise InvalidTransaction(f"Invalid payload: missing {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in apply method: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(str(e))

    def _validate_workflow_app_id(self, context, workflow_app_id):
        if workflow_app_id.startswith('workflow_'):
            address = self._make_workflow_address(workflow_app_id)
            family_namespace = WORKFLOW_NAMESPACE
        else:
            address = self._make_docker_image_address(workflow_app_id)
            family_namespace = DOCKER_IMAGE_NAMESPACE

        state_entries = context.get_state([address])

        if state_entries:
            try:
                state_data = json.loads(state_entries[0].data.decode())
                logger.info(f"Found state data for {workflow_app_id}: {state_data}")
                return True
            except json.JSONDecodeError:
                logger.error(f"Invalid state data for {workflow_app_id}")
                return False
        else:
            logger.warning(f"No state data found for {workflow_app_id} in {family_namespace} namespace")
            return False

    def _schedule_data_processing(self, iot_data, workflow_app_id):
        # This is a skeleton function for now
        # In the future, this will implement the actual scheduling logic
        logger.info(f"Scheduling data processing for workflow/app ID: {workflow_app_id}")
        # TODO: Implement actual scheduling logic

    def _make_iot_data_address(self, workflow_app_id):
        return NAMESPACE + hashlib.sha512(workflow_app_id.encode()).hexdigest()[:64]

    def _make_workflow_address(self, workflow_id):
        return WORKFLOW_NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]

    def _make_docker_image_address(self, app_id):
        return DOCKER_IMAGE_NAMESPACE + hashlib.sha512(app_id.encode()).hexdigest()[:64]


def main():
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = IoTDataTransactionHandler()
    processor.add_handler(handler)
    processor.start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
