import hashlib
import json
import logging
import os
import traceback

from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.handler import TransactionHandler

# Sawtooth configuration
FAMILY_NAME = 'iot-schedule'
FAMILY_VERSION = '1.0'

SCHEDULE_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

logger = logging.getLogger(__name__)


class IoTScheduleTransactionHandler(TransactionHandler):
    @property
    def family_name(self):
        return FAMILY_NAME

    @property
    def family_versions(self):
        return [FAMILY_VERSION]

    @property
    def namespaces(self):
        return [SCHEDULE_NAMESPACE]

    def apply(self, transaction, context):
        try:
            payload = json.loads(transaction.payload.decode())
            workflow_id = payload['workflow_id']
            schedule_id = payload['schedule_id']
            status = payload['status']
            timestamp = payload['timestamp']

            logger.info(f"Processing status update for workflow ID: {workflow_id}, schedule ID: {schedule_id}")

            schedule_address = self._make_schedule_address(schedule_id)
            schedule_state_data = json.dumps({
                'schedule_id': schedule_id,
                'workflow_id': workflow_id,
                'timestamp': timestamp,
                'status': status
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
