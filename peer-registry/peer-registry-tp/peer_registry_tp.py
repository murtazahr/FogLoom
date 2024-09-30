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
            updates = payload['updates']

            logger.debug(f"Processing {len(updates)} resource updates for node {node_id}")

            # Store the resource data in the blockchain state
            state_key = NAMESPACE + hashlib.sha512(node_id.encode()).hexdigest()[:64]
            state_entries = context.get_state([state_key])

            if state_entries:
                current_state = json.loads(state_entries[0].data.decode())
                current_state['updates'].extend(updates)
            else:
                current_state = {'node_id': node_id, 'updates': updates}

            # Limit the number of stored updates to prevent unbounded growth
            max_updates = int(os.getenv('MAX_UPDATES_PER_NODE', 100))
            if len(current_state['updates']) > max_updates:
                current_state['updates'] = current_state['updates'][-max_updates:]

            # Update the state
            state_data = json.dumps(current_state).encode()
            context.set_state({ state_key: state_data })

            logger.info(f"Updated state for node {node_id} with {len(updates)} new resource updates")

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
