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

        # The header is already a TransactionHeader object, no need to parse
        header = transaction.header

        try:
            image_hash, image_name = transaction.payload.decode().split(',')
            logger.debug(f"Received image hash: {image_hash} for image: {image_name}")

            # Store the image hash in the blockchain state
            state_key = NAMESPACE + hashlib.sha512(image_name.encode()).hexdigest()[:64]
            state_value = f"{image_hash},{image_name}".encode()
            context.set_state({state_key: state_value})
            logger.info(f"Stored image hash in state: {state_key}")

            # Emit an event
            context.add_event(
                event_type="docker-image-added",
                attributes=[("image_hash", image_hash), ("image_name", image_name)]
            )
            logger.info(f"Emitted event for image: {image_name} with hash: {image_hash}")

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
