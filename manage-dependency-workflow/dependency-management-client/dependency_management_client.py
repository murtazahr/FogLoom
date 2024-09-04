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
WORKFLOW_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

DOCKER_IMAGE_FAMILY = 'docker-image'
DOCKER_IMAGE_NAMESPACE = hashlib.sha512(DOCKER_IMAGE_FAMILY.encode()).hexdigest()[:6]

# Path to the private key file
PRIVATE_KEY_FILE = os.getenv('SAWTOOTH_PRIVATE_KEY', '/root/.sawtooth/keys/root.priv')


def load_private_key(key_file):
    logger.debug(f"Attempting to load private key from {key_file}")
    try:
        with open(key_file, 'r') as key_reader:
            private_key_str = key_reader.read().strip()
            private_key = secp256k1.Secp256k1PrivateKey.from_hex(private_key_str)
            logger.info(f"Successfully loaded private key from {key_file}")
            return private_key
    except IOError as e:
        logger.error(f"Failed to load private key from {key_file}: {str(e)}")
        raise IOError(f"Failed to load private key from {key_file}: {str(e)}") from e


def create_workflow(dependency_graph):
    logger.info("Creating new workflow")
    workflow_id = str(uuid.uuid4())
    logger.debug(f"Generated workflow ID: {workflow_id}")
    return _send_workflow_transaction(workflow_id, dependency_graph, "create")


def get_workflow(workflow_id):
    logger.info(f"Retrieving workflow with ID: {workflow_id}")
    return _send_workflow_transaction(workflow_id, None, "get")


def _send_workflow_transaction(workflow_id, dependency_graph, action):
    logger.debug(f"Preparing to send workflow transaction. Action: {action}, Workflow ID: {workflow_id}")
    try:
        private_key = load_private_key(PRIVATE_KEY_FILE)
    except IOError as e:
        logger.error(str(e))
        sys.exit(1)

    context = create_context('secp256k1')
    signer = CryptoFactory(context).new_signer(private_key)
    logger.debug(f"Created signer with public key: {signer.get_public_key().as_hex()}")

    payload = {
        "action": action,
        "workflow_id": workflow_id
    }
    if dependency_graph:
        payload["dependency_graph"] = dependency_graph
    logger.debug(f"Prepared payload: {json.dumps(payload)}")

    transaction = _create_transaction(payload, signer)
    logger.debug(f"Created transaction with header signature: {transaction.header_signature}")

    batch = _create_batch([transaction], signer)
    logger.debug(f"Created batch with header signature: {batch.header_signature}")

    batch_list = BatchList(batches=[batch])

    result = _send_request(batch_list)
    logger.info(f"Sent request to validator. Result: {result}")

    return workflow_id, result


def _create_transaction(payload, signer):
    logger.debug("Creating transaction")
    payload_bytes = json.dumps(payload).encode()

    # Include both namespaces in inputs and outputs
    inputs = [WORKFLOW_NAMESPACE, DOCKER_IMAGE_NAMESPACE]
    outputs = [WORKFLOW_NAMESPACE]

    txn_header = TransactionHeader(
        family_name=FAMILY_NAME,
        family_version=FAMILY_VERSION,
        inputs=inputs,
        outputs=outputs,
        signer_public_key=signer.get_public_key().as_hex(),
        batcher_public_key=signer.get_public_key().as_hex(),
        dependencies=[],
        payload_sha512=hashlib.sha512(payload_bytes).hexdigest()
    ).SerializeToString()

    signature = signer.sign(txn_header)
    logger.debug(f"Transaction header signed with signature: {signature}")

    txn = Transaction(
        header=txn_header,
        header_signature=signature,
        payload=payload_bytes
    )

    logger.debug(f"Transaction created with header signature: {txn.header_signature}")
    return txn


def _create_batch(transactions, signer):
    logger.debug("Creating batch")
    batch_header = BatchHeader(
        signer_public_key=signer.get_public_key().as_hex(),
        transaction_ids=[txn.header_signature for txn in transactions],
    ).SerializeToString()

    signature = signer.sign(batch_header)
    logger.debug(f"Batch header signed with signature: {signature}")

    batch = Batch(
        header=batch_header,
        header_signature=signature,
        transactions=transactions
    )

    logger.debug(f"Batch created with header signature: {batch.header_signature}")
    return batch


def _send_request(batch_list):
    validator_url = os.getenv('VALIDATOR_URL', 'tcp://localhost:4004')
    logger.info(f"Sending request to validator at {validator_url}")
    stream = Stream(validator_url)
    future = stream.send(
        message_type='CLIENT_BATCH_SUBMIT_REQUEST',
        content=batch_list.SerializeToString()
    )
    result = future.result()
    logger.debug(f"Received result from validator: {result}")
    return result


def main():
    logger.info("Workflow client started")
    if len(sys.argv) < 2:
        logger.error("Insufficient arguments provided")
        print("Usage: python workflow_client.py <create|get> [<dependency_graph_file>|<workflow_id>]")
        sys.exit(1)

    action = sys.argv[1]
    logger.info(f"Action requested: {action}")

    if action == "create":
        if len(sys.argv) != 3:
            logger.error("Incorrect number of arguments for create action")
            print("Usage: python workflow_client.py create <dependency_graph_file>")
            sys.exit(1)

        dependency_graph_file = sys.argv[2]
        logger.info(f"Reading dependency graph from file: {dependency_graph_file}")
        try:
            with open(dependency_graph_file, 'r') as f:
                dependency_graph = json.load(f)
            logger.debug(f"Dependency graph loaded: {json.dumps(dependency_graph)}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse dependency graph file: {str(e)}")
            sys.exit(1)
        except IOError as e:
            logger.error(f"Failed to read dependency graph file: {str(e)}")
            sys.exit(1)

        workflow_id, result = create_workflow(dependency_graph)
        logger.info(f"Workflow created with ID: {workflow_id}")
        print(f"Workflow created with ID: {workflow_id}")
        print(f"Result: {result}")

    elif action == "get":
        if len(sys.argv) != 3:
            logger.error("Incorrect number of arguments for get action")
            print("Usage: python workflow_client.py get <workflow_id>")
            sys.exit(1)

        workflow_id = sys.argv[2]
        logger.info(f"Retrieving workflow with ID: {workflow_id}")
        _, result = get_workflow(workflow_id)
        print(f"Workflow data: {result}")

    else:
        logger.error(f"Unknown action: {action}")
        print(f"Unknown action: {action}")
        sys.exit(1)

    logger.info("Workflow client finished")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    )
    main()
