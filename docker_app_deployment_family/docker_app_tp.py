import hashlib
import logging

import cbor
from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader

# Set up logging
logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

FAMILY_NAME = "docker_app"
FAMILY_VERSION = "1.0"
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode('utf-8')).hexdigest()[:6]

LOGGER.info(f"Transaction Family: {FAMILY_NAME}")
LOGGER.info(f"Version: {FAMILY_VERSION}")
LOGGER.info(f"Namespace: {NAMESPACE}")


class DockerAppTransactionHandler(TransactionHandler):
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
        LOGGER.debug("Entering apply method")
        try:
            header = TransactionHeader()
            header.ParseFromString(transaction.header)
            signer = header.signer_public_key
            LOGGER.info(f"Transaction signer: {signer}")

            payload = cbor.loads(transaction.payload)
            LOGGER.info(f"Decoded payload: {payload}")

            app_name = payload['name']
            app_version = payload['version']
            app_data = payload['data']

            LOGGER.info(f"Processing Docker App: {app_name} v{app_version}")

            state_key = NAMESPACE + hashlib.sha512(app_name.encode('utf-8')).hexdigest()[-64:]
            LOGGER.debug(f"Generated state key: {state_key}")

            state_value = cbor.dumps({
                'name': app_name,
                'version': app_version,
                'data': app_data,
                'owner': signer
            })
            LOGGER.debug(f"State value to be set: {state_value}")

            context.set_state({state_key: state_value})
            LOGGER.info(f"Successfully stored Docker App: {app_name} v{app_version}")

        except Exception as e:
            LOGGER.error(f"Error in apply method: {str(e)}")
            raise

        LOGGER.debug("Exiting apply method")


def main():
    LOGGER.info("Starting Docker App Transaction Processor")
    try:
        processor = TransactionProcessor(url='tcp://validator:4004')

        handler = DockerAppTransactionHandler()
        processor.add_handler(handler)
        LOGGER.info("Added Docker App Transaction Handler")

        LOGGER.info("Starting processor")
        processor.start()
    except KeyboardInterrupt:
        LOGGER.info("Stopped by user")
    except Exception as e:
        LOGGER.error(f"Error occurred: {str(e)}")
    finally:
        LOGGER.info("Stopping processor")


if __name__ == '__main__':
    main()
