import hashlib
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
            image_hash, image_name, app_id, action = transaction.payload.decode().split(',')
            logger.debug(f"Received action: {action} for image: {image_name}, "
                         f"hash: {image_hash}, app_id: {app_id}")

            # Store the image information in the blockchain state
            state_key = NAMESPACE + hashlib.sha512(app_id.encode()).hexdigest()[:64]
            state_value = f"{image_hash},{image_name},{app_id},{action}".encode()
            context.set_state({state_key: state_value})
            logger.info(f"Stored image info in state: {state_key}")

            # Emit an event
            context.add_event(
                event_type="docker-image-action",
                attributes=[("image_hash", image_hash),
                            ("image_name", image_name),
                            ("app_id", app_id),
                            ("action", action)]
            )
            logger.info(f"Emitted event for action: {action}, image: {image_name}, "
                        f"hash: {image_hash}, app_id: {app_id}")

        except ValueError as e:
            raise InvalidTransaction("Invalid payload format") from e
        except Exception as e:
            logger.error(f"Error processing transaction: {str(e)}")
            raise InvalidTransaction(str(e))


def main():
    logger.info("Starting Docker Image Transaction Processor")
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = DockerImageTransactionHandler()
    processor.add_handler(handler)
    processor.start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
