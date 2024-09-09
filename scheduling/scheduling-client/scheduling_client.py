import hashlib
import json
import logging
import os
import time

from flask import Flask, request, jsonify
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_signing import create_context, CryptoFactory, secp256k1
from sawtooth_sdk.messaging.stream import Stream

app = Flask(__name__)

logger = logging.getLogger(__name__)

FAMILY_NAME = 'iot-data'
FAMILY_VERSION = '1.0'
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
DOCKER_IMAGE_NAMESPACE = hashlib.sha512('docker-image'.encode()).hexdigest()[:6]

PRIVATE_KEY_FILE = os.getenv('SAWTOOTH_PRIVATE_KEY', '/root/.sawtooth/keys/client.priv')
VALIDATOR_URL = os.getenv('VALIDATOR_URL', 'tcp://validator:4004')


def load_private_key(key_file):
    try:
        with open(key_file, 'r') as key_reader:
            private_key_str = key_reader.read().strip()
            return secp256k1.Secp256k1PrivateKey.from_hex(private_key_str)
    except IOError as ex:
        raise IOError(f"Failed to load private key from {key_file}: {str(ex)}") from ex


def create_iot_data_transaction(signer, iot_data, workflow_app_id):
    payload = {
        "iot_data": iot_data,
        "workflow_app_id": workflow_app_id,
        "timestamp": int(time.time())
    }
    payload_bytes = json.dumps(payload).encode()

    # Use all namespaces for input (for reading/validation)
    inputs = [NAMESPACE, WORKFLOW_NAMESPACE, DOCKER_IMAGE_NAMESPACE]
    # Use only IoT data namespace for output
    outputs = [NAMESPACE]

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


# Load the private key and create a signer when the server starts
try:
    private_key = load_private_key(PRIVATE_KEY_FILE)
    context = create_context('secp256k1')
    signer = CryptoFactory(context).new_signer(private_key)
except IOError as e:
    logger.error(f"Failed to load private key: {str(e)}")
    raise


@app.route('/submit_iot_data', methods=['POST'])
def submit_iot_data():
    try:
        data = request.json
        if not data or 'iot_data' not in data or 'workflow_app_id' not in data:
            return jsonify({"error": "Missing iot_data or workflow_app_id in request"}), 400

        iot_data = data['iot_data']
        workflow_app_id = data['workflow_app_id']

        transaction = create_iot_data_transaction(signer, iot_data, workflow_app_id)
        batch = create_batch([transaction], signer)
        result = submit_batch(batch)

        return jsonify({"message": "Data submitted successfully", "result": str(result)}), 200
    except Exception as ex:
        logger.error(f"Error processing request: {str(ex)}")
        return jsonify({"error": str(ex)}), 500


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    app.run(host='0.0.0.0', port=8080)
