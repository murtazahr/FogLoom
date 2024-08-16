import hashlib
import json
import logging
import os
import socket
import sys
import docker
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from docker import errors
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_signing import create_context, CryptoFactory, secp256k1
from sawtooth_sdk.messaging.stream import Stream

logger = logging.getLogger(__name__)

FAMILY_NAME = 'docker-image'
FAMILY_VERSION = '1.0'
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]
REGISTRY_URL = os.getenv('REGISTRY_URL', 'http://sawtooth-registry:5000')

# Path to the private key file
PRIVATE_KEY_FILE = os.getenv('SAWTOOTH_PRIVATE_KEY', '/root/.sawtooth/keys/root.priv')


def load_private_key(key_file):
    try:
        with open(key_file, 'r') as key_reader:
            private_key_str = key_reader.read().strip()
            return secp256k1.Secp256k1PrivateKey.from_hex(private_key_str)
    except IOError as e:
        raise IOError(f"Failed to load private key from {key_file}: {str(e)}") from e


def debug_dns(hostname):
    try:
        ip = socket.gethostbyname(hostname)
        logger.debug(f"DNS resolution for {hostname}: {ip}")
    except socket.gaierror as e:
        logger.error(f"DNS resolution failed for {hostname}: {e}")


def create_docker_client():
    # Create a custom session with retry logic
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))

    # Create a Docker client that uses the custom session
    return docker.DockerClient(base_url='unix://var/run/docker.sock', use_ssh_client=True, session=session)


def verify_image_in_registry(image_name):
    # Extract repository and tag
    repo, tag = image_name.split('/')[-1].split(':')

    # Construct the URL to check the image manifest
    url = f"{REGISTRY_URL}/v2/{repo}/manifests/{tag}"

    try:
        debug_dns('sawtooth-registry')
        response = requests.head(url, timeout=10)
        logger.debug(f"Registry response headers: {response.headers}")
        if response.status_code == 200:
            logger.info(f"Image {image_name} verified in registry")
            return True
        else:
            logger.error(f"Image {image_name} not found in registry. Status code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Error verifying image in registry: {e}")
        return False


def hash_and_push_docker_image(tar_path):
    logger.info(f"Processing Docker image from tar: {tar_path}")
    client = create_docker_client()

    # Calculate hash
    sha256_hash = hashlib.sha256()
    with open(tar_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            sha256_hash.update(chunk)
    image_hash = sha256_hash.hexdigest()
    logger.info(f"Calculated image hash: {image_hash}")

    # Load image from tar
    with open(tar_path, 'rb') as f:
        image = client.images.load(f.read())[0]

    # Tag and push to local registry
    image_name = image.tags[0] if image.tags else f"image-{image_hash[:12]}"
    registry_image_name = f"{REGISTRY_URL.split('://')[-1]}/{image_name}"
    image.tag(registry_image_name)
    logger.info(f"Pushing Docker image to local registry: {registry_image_name}")

    try:
        debug_dns('sawtooth-registry')
        push_result = client.images.push(registry_image_name, stream=True, decode=True)
        push_success = False
        for line in push_result:
            logger.debug(json.dumps(line))
            if 'error' in line:
                logger.error(f"Error during push: {line['error']}")
                raise Exception(f"Push error: {line['error']}")
            elif 'status' in line and 'Pushed' in line['status']:
                push_success = True

        if push_success:
            logger.info("Image push completed successfully")
        else:
            logger.error("Image push did not complete successfully")
            raise Exception("Image push failed")

        # Verify the image is in the registry
        if verify_image_in_registry(registry_image_name):
            logger.info("Image verified in registry")
        else:
            logger.error("Failed to verify image in registry")
            raise Exception("Image verification failed")
    except docker.errors.APIError as e:
        logger.error(f"Failed to push image: {e}")
        raise

    return image_hash, registry_image_name


def create_transaction(image_hash, image_name, signer):
    logger.info(f"Creating transaction for image: {image_name} with hash: {image_hash}")
    payload = f"{image_hash},{image_name}".encode()

    header = TransactionHeader(
        family_name=FAMILY_NAME,
        family_version=FAMILY_VERSION,
        inputs=[NAMESPACE],
        outputs=[NAMESPACE],
        signer_public_key=signer.get_public_key().as_hex(),
        batcher_public_key=signer.get_public_key().as_hex(),
        dependencies=[],
        payload_sha512=hashlib.sha512(payload).hexdigest(),
    ).SerializeToString()

    signature = signer.sign(header)

    transaction = Transaction(
        header=header,
        payload=payload,
        header_signature=signature
    )

    logger.info(f"Transaction created with signature: {signature}")
    return transaction


def create_batch(transactions, signer):
    logger.info(f"Creating batch for transactions: {transactions}")
    batch_header = BatchHeader(
        signer_public_key=signer.get_public_key().as_hex(),
        transaction_ids=[t.header_signature for t in transactions],
    ).SerializeToString()

    signature = signer.sign(batch_header)

    batch = Batch(
        header=batch_header,
        transactions=transactions,
        header_signature=signature,
    )

    logger.info(f"Batch created with signature: {signature}")
    return batch


def submit_batch(batch):
    logger.info("Submitting batch to validator")
    stream = Stream(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))

    batch_list = BatchList(batches=[batch])
    future = stream.send(
        message_type='CLIENT_BATCH_SUBMIT_REQUEST',
        content=batch_list.SerializeToString()
    )

    result = future.result()
    logger.info(f"Submitted batch to validator: {result}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python docker_image_client.py <path_to_docker_image.tar>")
        sys.exit(1)

    tar_path = sys.argv[1]

    if not os.path.exists(tar_path):
        print(f"Error: File {tar_path} does not exist")
        sys.exit(1)

    try:
        private_key = load_private_key(PRIVATE_KEY_FILE)
    except IOError as e:
        logger.error(str(e))
        sys.exit(1)

    context = create_context('secp256k1')
    signer = CryptoFactory(context).new_signer(private_key)

    try:
        image_hash, registry_image_name = hash_and_push_docker_image(tar_path)
        transaction = create_transaction(image_hash, registry_image_name, signer)
        batch = create_batch([transaction], signer)
        submit_batch(batch)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
