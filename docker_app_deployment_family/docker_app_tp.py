import hashlib
import logging

import cbor
from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader

LOGGER = logging.getLogger(__name__)

FAMILY_NAME = "docker_app"
FAMILY_VERSION = "1.0"
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode('utf-8')).hexdigest()[:6]


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
        header = TransactionHeader()
        header.ParseFromString(transaction.header)
        signer = header.signer_public_key

        payload = cbor.loads(transaction.payload)
        LOGGER.info(f"Docker App payload: {payload}")

        # Implement your transaction logic here
        # For example:
        app_name = payload['name']
        app_version = payload['version']
        app_data = payload['data']

        # Store the app data in state
        state_key = NAMESPACE + hashlib.sha512(app_name.encode('utf-8')).hexdigest()[-64:]
        state_value = cbor.dumps({
            'name': app_name,
            'version': app_version,
            'data': app_data,
            'owner': signer
        })
        context.set_state({state_key: state_value})
        LOGGER.info(f"Stored Docker App: {app_name} v{app_version}")


def main():
    processor = TransactionProcessor(url='tcp://validator:4004')
    handler = DockerAppTransactionHandler()
    processor.add_handler(handler)
    processor.start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
