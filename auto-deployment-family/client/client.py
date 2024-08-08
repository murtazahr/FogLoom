import argparse
import base64
import hashlib
import os
import sys

import requests
import yaml

from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey

# Transaction Family Name
FAMILY_NAME = 'auto-deployment-docker'
FAMILY_VERSION = '1.0'

# REST API URL
REST_API_URL = 'http://sawtooth-rest-api-default-0:8008'


def load_private_key(key_file):
    try:
        with open(key_file, 'r') as key_file:
            private_key_str = key_file.read().strip()
            return Secp256k1PrivateKey.from_hex(private_key_str)
    except IOError as e:
        raise Exception(f"Failed to load private key: {str(e)}")


def create_signer(private_key):
    context = create_context('secp256k1')
    return CryptoFactory(context).new_signer(private_key)


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


def submit_batch(batch, url):
    batch_list_bytes = BatchList(batches=[batch]).SerializeToString()
    response = requests.post(
        f'{url}/batches',
        headers={'Content-Type': 'application/octet-stream'},
        data=batch_list_bytes
    )
    return response


def main():
    parser = argparse.ArgumentParser(description='Sawtooth Docker Deployment Client')
    parser.add_argument('key_file', help='Path to the private key file')
    parser.add_argument('docker_image', help='Path to the Docker image tar file')
    parser.add_argument('--url', default=REST_API_URL, help='URL of the REST API')
    args = parser.parse_args()

    try:
        private_key = load_private_key(args.key_file)
        signer = create_signer(private_key)
    except Exception as e:
        print(f"Error loading private key: {e}")
        sys.exit(1)

    try:
        image_data = load_docker_image(args.docker_image)
        image_name = os.path.basename(args.docker_image).split('.')[0]
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
    try:
        response = submit_batch(batch, args.url)
        print(f"Transaction submitted: {response.status_code}")
        print(f"Response: {response.text}")
    except Exception as e:
        print(f"Failed to submit transaction: {e}")


if __name__ == '__main__':
    main()
