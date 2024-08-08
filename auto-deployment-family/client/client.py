import base64
import hashlib
import os
import sys
import time

import requests
import yaml

from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList

# Transaction Family Name
FAMILY_NAME = 'auto-deployment-docker'
FAMILY_VERSION = '1.0'

# REST API URL
REST_API_URL = 'http://sawtooth-rest-api-default-0:8008'


def _hash(data):
    return hashlib.sha512(data).hexdigest()


def load_docker_image(file_path):
    print(f"Attempting to load Docker image from: {file_path}")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    if os.path.isdir(file_path):
        raise IsADirectoryError(f"Expected a file, but {file_path} is a directory")

    try:
        with open(file_path, 'rb') as file:
            return file.read()
    except IOError as e:
        print(f"Error reading file: {e}")
        raise


def create_transaction(signer, payload, inputs, outputs):
    txn_header_bytes = TransactionHeader(
        family_name=FAMILY_NAME,
        family_version=FAMILY_VERSION,
        inputs=inputs,
        outputs=outputs,
        signer_public_key=signer.get_public_key().as_hex(),
        batcher_public_key=signer.get_public_key().as_hex(),
        dependencies=[],
        payload_sha512=_hash(payload),
    ).SerializeToString()

    signature = signer.sign(txn_header_bytes)

    txn = Transaction(
        header=txn_header_bytes,
        header_signature=signature,
        payload=payload
    )

    return txn


def create_batch(transactions, signer):
    batch_header_bytes = BatchHeader(
        signer_public_key=signer.get_public_key().as_hex(),
        transaction_ids=[txn.header_signature for txn in transactions]
    ).SerializeToString()

    signature = signer.sign(batch_header_bytes)

    batch = Batch(
        header=batch_header_bytes,
        header_signature=signature,
        transactions=transactions
    )

    return batch


def submit_batch(batch):
    batch_list_bytes = BatchList(batches=[batch]).SerializeToString()
    response = requests.post(
        f'{REST_API_URL}/batches',
        headers={'Content-Type': 'application/octet-stream'},
        data=batch_list_bytes
    )
    return response


def main():

    # Wait for the REST API to be available
    while True:
        try:
            response = requests.get(f'{REST_API_URL}/blocks')
            if response.status_code == 200:
                print("REST API is available. Proceeding with transaction submission.")
                break
        except requests.exceptions.RequestException:
            print("Waiting for REST API to be available...")
            time.sleep(1)

    if len(sys.argv) != 2:
        print("Usage: python client.py <docker_image_file>")
        sys.exit(1)

    docker_image_file = sys.argv[1]
    print(f"Docker image file path: {docker_image_file}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Directory contents:")
    for item in os.listdir(os.path.dirname(docker_image_file)):
        print(f" - {item}")

    try:
        image_data = load_docker_image(docker_image_file)
        image_name = os.path.basename(docker_image_file).split('.')[0]
    except Exception as e:
        print(f"Failed to load Docker image: {e}")
        sys.exit(1)

    # Prepare the payload
    payload = {
        'image_data': base64.b64encode(image_data).decode('utf-8'),
        'image_name': image_name,
        'image_tag': 'latest'
    }
    payload_bytes = yaml.dump(payload).encode('utf-8')

    # Create a signer
    context = create_context('secp256k1')
    private_key = context.new_random_private_key()
    signer = CryptoFactory(context).new_signer(private_key)

    # Define inputs and outputs
    address_prefix = _hash(FAMILY_NAME.encode('utf-8'))[0:6]
    image_address = address_prefix + _hash(image_name.encode('utf-8'))[0:64]
    inputs = [image_address]
    outputs = [image_address]

    # Create a transaction
    txn = create_transaction(signer, payload_bytes, inputs, outputs)

    # Create a batch
    batch = create_batch([txn], signer)

    # Submit the batch
    response = submit_batch(batch)

    print(f"Transaction submitted: {response.status_code}")
    print(f"Response: {response.text}")


if __name__ == '__main__':
    main()
