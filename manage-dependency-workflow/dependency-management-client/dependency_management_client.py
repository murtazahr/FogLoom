import hashlib
import json
import logging
import os
import sys
import uuid

from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_signing import create_context, CryptoFactory, secp256k1
from sawtooth_sdk.messaging.stream import Stream

logger = logging.getLogger(__name__)

FAMILY_NAME = 'workflow-dependency'
FAMILY_VERSION = '1.0'
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

# Path to the private key file
PRIVATE_KEY_FILE = os.getenv('SAWTOOTH_PRIVATE_KEY', '/root/.sawtooth/keys/root.priv')


def load_private_key(key_file):
    try:
        with open(key_file, 'r') as key_reader:
            private_key_str = key_reader.read().strip()
            return secp256k1.Secp256k1PrivateKey.from_hex(private_key_str)
    except IOError as e:
        raise IOError(f"Failed to load private key from {key_file}: {str(e)}") from e


def create_workflow(dependency_graph):
    workflow_id = str(uuid.uuid4())
    return _send_workflow_transaction(workflow_id, dependency_graph, "create")


def get_workflow(workflow_id):
    return _send_workflow_transaction(workflow_id, None, "get")


def _send_workflow_transaction(workflow_id, dependency_graph, action):
    try:
        private_key = load_private_key(PRIVATE_KEY_FILE)
    except IOError as e:
        logger.error(str(e))
        sys.exit(1)

    context = create_context('secp256k1')
    signer = CryptoFactory(context).new_signer(private_key)

    payload = {
        "action": action,
        "workflow_id": workflow_id
    }
    if dependency_graph:
        payload["dependency_graph"] = dependency_graph

    transaction = _create_transaction(payload, signer)
    batch = _create_batch([transaction], signer)
    batch_list = BatchList(batches=[batch])

    result = _send_request(batch_list)

    return workflow_id, result


def _create_transaction(payload, signer):
    payload_bytes = json.dumps(payload).encode()

    txn_header = TransactionHeader(
        family_name=FAMILY_NAME,
        family_version=FAMILY_VERSION,
        inputs=[NAMESPACE],
        outputs=[NAMESPACE],
        signer_public_key=signer.get_public_key().as_hex(),
        batcher_public_key=signer.get_public_key().as_hex(),
        dependencies=[],
        payload_sha512=hashlib.sha512(payload_bytes).hexdigest()
    ).SerializeToString()

    signature = signer.sign(txn_header)

    txn = Transaction(
        header=txn_header,
        header_signature=signature,
        payload=payload_bytes
    )

    return txn


def _create_batch(transactions, signer):
    batch_header = BatchHeader(
        signer_public_key=signer.get_public_key().as_hex(),
        transaction_ids=[txn.header_signature for txn in transactions],
    ).SerializeToString()

    signature = signer.sign(batch_header)

    batch = Batch(
        header=batch_header,
        header_signature=signature,
        transactions=transactions
    )

    return batch


def _send_request(batch_list):
    validator_url = os.getenv('VALIDATOR_URL', 'tcp://localhost:4004')
    stream = Stream(validator_url)
    future = stream.send(
        message_type='CLIENT_BATCH_SUBMIT_REQUEST',
        content=batch_list.SerializeToString()
    )
    result = future.result()
    return result


def main():
    if len(sys.argv) < 2:
        print("Usage: python workflow_client.py <create|get> [<dependency_graph_file>|<workflow_id>]")
        sys.exit(1)

    action = sys.argv[1]

    if action == "create":
        if len(sys.argv) != 3:
            print("Usage: python workflow_client.py create <dependency_graph_file>")
            sys.exit(1)
        with open(sys.argv[2], 'r') as f:
            dependency_graph = json.load(f)
        workflow_id, result = create_workflow(dependency_graph)
        print(f"Workflow created with ID: {workflow_id}")
        print(f"Result: {result}")
    elif action == "get":
        if len(sys.argv) != 3:
            print("Usage: python workflow_client.py get <workflow_id>")
            sys.exit(1)
        workflow_id = sys.argv[2]
        _, result = get_workflow(workflow_id)
        print(f"Workflow data: {result}")
    else:
        print(f"Unknown action: {action}")
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
