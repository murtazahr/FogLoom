import uuid
import hashlib
import json
import logging
import os
import time
import argparse

from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_signing import create_context, CryptoFactory, secp256k1
from sawtooth_sdk.messaging.stream import Stream

logger = logging.getLogger(__name__)

FAMILY_NAME = 'iot-schedule'
FAMILY_VERSION = '1.0'

WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
DOCKER_IMAGE_NAMESPACE = hashlib.sha512('docker-image'.encode()).hexdigest()[:6]
SCHEDULE_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

PRIVATE_KEY_FILE = os.getenv('SAWTOOTH_PRIVATE_KEY', '/root/.sawtooth/keys/client.priv')
VALIDATOR_URL = os.getenv('VALIDATOR_URL', 'tcp://validator:4004')


def load_private_key(key_file):
    try:
        with open(key_file, 'r') as key_reader:
            private_key_str = key_reader.read().strip()
            return secp256k1.Secp256k1PrivateKey.from_hex(private_key_str)
    except IOError as ex:
        raise IOError(f"Failed to load private key from {key_file}: {str(ex)}") from ex


def create_iot_schedule_transaction(signer, iot_data, workflow_id):
    schedule_id = str(uuid.uuid4())  # Generate a unique schedule_id
    payload = {
        "iot_data": iot_data,
        "workflow_id": workflow_id,
        "schedule_id": schedule_id,
        "timestamp": int(time.time())
    }
    payload_bytes = json.dumps(payload).encode()

    # Use all namespaces for input (for reading/validation)
    inputs = [SCHEDULE_NAMESPACE, WORKFLOW_NAMESPACE, DOCKER_IMAGE_NAMESPACE]
    # Use only Schedule namespace for output
    outputs = [SCHEDULE_NAMESPACE]

    txn_header = TransactionHeader(
        family_name=FAMILY_NAME,
        family_version=FAMILY_VERSION,
        inputs=inputs,
        outputs=outputs,
        signer_public_key=signer.get_public_key().as_hex(),
        batcher_public_key=signer.get_public_key().as_hex(),
        dependencies=[],
        payload_sha512=hashlib.sha512(payload_bytes).hexdigest(),
        nonce=hex(int(time.time()))
    ).SerializeToString()

    signature = signer.sign(txn_header)

    txn = Transaction(
        header=txn_header,
        header_signature=signature,
        payload=payload_bytes
    )

    return txn


def create_batch(transactions, signer):
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


def submit_batch(batch):
    stream = Stream(VALIDATOR_URL)
    future = stream.send(
        message_type='CLIENT_BATCH_SUBMIT_REQUEST',
        content=BatchList(batches=[batch]).SerializeToString()
    )
    result = future.result()
    return result


def submit_iot_data_from_file(json_file):
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)

        if not data or 'iot_data' not in data or 'workflow_id' not in data:
            raise ValueError("Missing iot_data or workflow_id in the JSON file")

        iot_data = data['iot_data']
        workflow_id = data['workflow_id']

        transaction = create_iot_schedule_transaction(signer, iot_data, workflow_id)
        batch = create_batch([transaction], signer)
        result = submit_batch(batch)

        print({
            "message": "Data submitted successfully",
            "result": str(result),
            "schedule_id": json.loads(transaction.payload.decode())['schedule_id']
        })
    except Exception as ex:
        logger.error(f"Error processing file: {str(ex)}")
        raise


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    # Argument parsing
    parser = argparse.ArgumentParser(description='Submit IoT data to Sawtooth blockchain.')
    parser.add_argument('json_file', help='Path to the JSON file containing IoT data and workflow_id')
    args = parser.parse_args()

    # Load private key and signer
    try:
        private_key = load_private_key(PRIVATE_KEY_FILE)
        context = create_context('secp256k1')
        signer = CryptoFactory(context).new_signer(private_key)
    except IOError as e:
        logger.error(f"Failed to load private key: {str(e)}")
        raise

    # Submit IoT data from the provided JSON file
    submit_iot_data_from_file(args.json_file)
