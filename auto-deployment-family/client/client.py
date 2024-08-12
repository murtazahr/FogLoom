import argparse
import base64
import hashlib
import os
import sys

import requests
import yaml
import docker
from docker import errors
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey

FAMILY_NAME = 'auto-deployment-docker'
FAMILY_VERSION = '1.0'
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


def generate_image_key_pair():
    return rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )


def calculate_image_signature(image_path, private_key):
    with open(image_path, 'rb') as f:
        image_hash = hashlib.sha256()
        for chunk in iter(lambda: f.read(4096), b""):
            image_hash.update(chunk)

    signature = private_key.sign(
        image_hash.digest(),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    return base64.b64encode(signature).decode()


def _hash(data):
    return hashlib.sha512(data).hexdigest()


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
    print(f"Submitting batch to {url}/batches")
    try:
        response = requests.post(
            f'{url}/batches',
            headers={'Content-Type': 'application/octet-stream'},
            data=batch_list_bytes,
            timeout=30
        )
        print(f"Received response with status code: {response.status_code}")
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error submitting batch: {e}")
        raise


def push_image_to_registry(image_path, docker_hub_username):
    client = docker.from_env()

    # Load the image
    with open(image_path, 'rb') as f:
        image = client.images.load(f)[0]

    # Tag the image for Docker Hub
    image_name = os.path.basename(image_path).split('.')[0]
    new_tag = f"{docker_hub_username}/{image_name}:latest"
    image.tag(new_tag)

    # Push the image
    print(f"Pushing image to Docker Hub: {new_tag}")
    try:
        push_output = client.images.push(new_tag)
        print(f"Push output: {push_output}")
    except docker.errors.APIError as e:
        if 'unauthorized' in str(e).lower():
            print("Authentication required. Please run 'docker login' and try again.")
            return None
        else:
            raise

    return new_tag


def main():
    parser = argparse.ArgumentParser(description='Sawtooth Docker Deployment Client')
    parser.add_argument('key_file', help='Path to the private key file for transaction signing')
    parser.add_argument('docker_image', help='Path to the Docker image tar file')
    parser.add_argument('docker_hub_username', help='Your Docker Hub username')
    parser.add_argument('--url', default=REST_API_URL, help='URL of the REST API')
    args = parser.parse_args()

    # Load the private key for transaction signing
    try:
        private_key = load_private_key(args.key_file)
        signer = create_signer(private_key)
        print("Successfully loaded private key and created signer for transactions")
    except Exception as e:
        print(f"Error loading private key: {e}")
        sys.exit(1)

    image_name = os.path.basename(args.docker_image).split('.')[0]
    print(f"Processing Docker image: {image_name}")

    # Push image to registry
    repository = push_image_to_registry(args.docker_image, args.docker_hub_username)
    if repository is None:
        print("Failed to push image. Exiting.")
        return
    image_name, image_tag = repository.split('/')[-1].split(':')

    # Generate a new key pair for image signing
    image_private_key = generate_image_key_pair()
    image_public_key = image_private_key.public_key()

    # Calculate image signature
    signature = calculate_image_signature(args.docker_image, image_private_key)

    print("Preparing payload...")
    payload = {
        'image_name': image_name,
        'image_tag': image_tag,
        'registry_url': args.registry_url,
        'signature': signature,
        'public_key': image_public_key.public_bytes(
            encoding=Encoding.PEM,
            format=PublicFormat.SubjectPublicKeyInfo
        ).decode()
    }

    print("Creating YAML payload...")
    payload_bytes = yaml.dump(payload).encode('utf-8')
    print(f"Payload prepared. Size: {len(payload_bytes)} bytes")

    print("Defining inputs and outputs...")
    address_prefix = hashlib.sha512(FAMILY_NAME.encode('utf-8')).hexdigest()[:6]
    image_address = address_prefix + hashlib.sha512(image_name.encode('utf-8')).hexdigest()[:64]
    inputs = [image_address]
    outputs = [image_address]

    print("Creating transaction...")
    txn = create_transaction(signer, payload_bytes, inputs, outputs)

    print("Creating batch...")
    batch = create_batch([txn], signer)

    print("Submitting batch...")
    try:
        response = submit_batch(batch, args.url)
        print(f"Transaction submitted: {response.status_code}")
        print(f"Response: {response.text}")
    except Exception as e:
        print(f"Failed to submit transaction: {e}")


if __name__ == '__main__':
    main()
