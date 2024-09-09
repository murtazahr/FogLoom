import hashlib
import json
import logging
import os

from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.core import TransactionProcessor

logger = logging.getLogger(__name__)

FAMILY_NAME = 'docker-image'
FAMILY_VERSION = '1.0'
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]


class DockerImageTransactionHandler(TransactionHandler):
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
        logger.info('Applying Docker Image Transaction')
        try:
            payload = json.loads(transaction.payload.decode())
            logger.debug(f"Received action: {payload.get('action')} for image: {payload.get('image_name')}, "
                         f"hash: {payload.get('image_hash')}, app_id: {payload.get('app_id')}, "
                         f"resource_requirements: {json.dumps(payload.get('resource_requirements')) if payload.get('resource_requirements') else None}")

            # Validate resource_requirements
            self.validate_resource_requirements(payload.get('resource_requirements'))

            # Store the image information in the blockchain state
            state_key = NAMESPACE + hashlib.sha512(payload.get('app_id').encode()).hexdigest()[:64]
            state_value = json.dumps(payload).encode()
            context.set_state({state_key: state_value})
            logger.info(f"Stored image info in state: {state_key}")

            # Emit an event
            context.add_event(
                event_type="docker-image-action",
                attributes=[("image_hash", payload.get('image_hash')),
                            ("image_name", payload.get('image_name')),
                            ("app_id", payload.get('app_id')),
                            ("action", payload.get('action')),]
            )
            logger.info(f"Emitted event for action: {payload.get('action')}, image: {payload.get('image_name')}, "
                        f"hash: {payload.get('image_hash')}, app_id: {payload.get('app_id')}")

        except ValueError as e:
            raise InvalidTransaction("Invalid payload format") from e
        except Exception as e:
            logger.error(f"Error processing transaction: {str(e)}")
            raise InvalidTransaction(str(e))


    def validate_resource_requirements(self, resource_requirements):
        if resource_requirements is None:
            return

        if not isinstance(resource_requirements, dict):
            raise InvalidTransaction("resource_requirements must be a dictionary")

        required_fields = ['memory', 'cpu', 'disk']

        # Check if only the required fields are present
        if set(resource_requirements.keys()) != set(required_fields):
            raise InvalidTransaction(f"resource_requirements must contain only these fields: {', '.join(required_fields)}")

        # Validate each field
        for field in required_fields:
            value = resource_requirements.get(field)
            if not isinstance(value, (int, float)):
                raise InvalidTransaction(f"{field} in resource_requirements must be a number")


def main():
    logger.info("Starting Docker Image Transaction Processor")
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = DockerImageTransactionHandler()
    processor.add_handler(handler)
    processor.start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
