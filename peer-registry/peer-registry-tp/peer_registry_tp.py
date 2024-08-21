import hashlib
import logging
import os
import json

from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.core import TransactionProcessor

logger = logging.getLogger(__name__)

FAMILY_NAME = 'peer-registry'
FAMILY_VERSION = '1.0'
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]


class PeerRegistryTransactionHandler(TransactionHandler):
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
        logger.info('Applying Peer Registry Transaction')

        try:
            payload = json.loads(transaction.payload.decode())
            node_id = payload['node_id']
            resource_data = payload['resource_data']

            logger.debug(f"Received resource data for node {node_id}: {json.dumps(resource_data, indent=2)}")

            # Store the resource data in the blockchain state
            state_key = NAMESPACE + hashlib.sha512(node_id.encode()).hexdigest()[:64]
            state_value = json.dumps(resource_data).encode()
            context.set_state({state_key: state_value})
            logger.info(f"Stored resource data in state: {state_key}")

        except json.JSONDecodeError as e:
            raise InvalidTransaction("Invalid payload format: not valid JSON") from e
        except KeyError as e:
            raise InvalidTransaction(f"Invalid payload format: missing key {str(e)}") from e
        except Exception as e:
            logger.error(f"Error processing transaction: {str(e)}")
            raise InvalidTransaction(str(e))


def main():
    logger.info("Starting Resource Transaction Processor")
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = PeerRegistryTransactionHandler()
    processor.add_handler(handler)
    processor.start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
