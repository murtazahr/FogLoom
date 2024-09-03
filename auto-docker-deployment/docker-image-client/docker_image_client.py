import hashlib
import json
import logging
import os
import sys
import time
import uuid

import docker
from docker import errors
from sawtooth_sdk.protobuf.client_batch_submit_pb2 import ClientBatchSubmitResponse
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


def process_future_result(future_result):
    try:
        result = future_result.result()
        response = ClientBatchSubmitResponse()
        response.ParseFromString(result.content)

        if response.status == ClientBatchSubmitResponse.OK:
            return {
                "status": "SUCCESS",
                "message": "Batch submitted successfully"
            }
        elif response.status == ClientBatchSubmitResponse.INVALID_BATCH:
            return {
                "status": "FAILURE",
                "message": "Invalid batch submitted",
                "error_details": response.error_message
            }
        else:
            return {
                "status": "FAILURE",
                "message": f"Batch submission failed with status: {response.status}",
                "error_details": response.error_message
            }
    except Exception as e:
        return {
            "status": "ERROR",
            "message": "An error occurred while processing the submission result",
            "error_details": str(e)
        }


def hash_and_push_docker_image(tar_path):
    logger.info(f"Processing Docker image from tar: {tar_path}")
    client = docker.from_env()

    # Load image from tar
    with open(tar_path, 'rb') as f:
        image = client.images.load(f.read())[0]

    # Tag and push to local registry
    image_name = image.tags[0] if image.tags else f"image-{image.id[:12]}"
    registry_image_name = f"{REGISTRY_URL.split('://')[-1]}/{image_name}"
    image.tag(registry_image_name)
    logger.info(f"Pushing Docker image to local registry: {registry_image_name}")

    try:
        push_result = client.images.push(registry_image_name, stream=True, decode=True)
        content_digest = None
        for line in push_result:
            logger.debug(json.dumps(line))
            if 'error' in line:
                logger.error(f"Error during push: {line['error']}")
                raise Exception(f"Error during image push: {line['error']}")
            elif 'aux' in line and 'Digest' in line['aux']:
                content_digest = line['aux']['Digest']
                logger.info(f"Image push completed successfully. Content digest: {content_digest}")
                break

        if not content_digest:
            logger.error("Image push completed but digest not found")
            raise Exception("Image push completed but digest not found")

    except docker.errors.APIError as e:
        logger.error(f"Failed to push image: {e}")
        raise

    return content_digest, registry_image_name


def create_transaction(image_hash, image_name, app_id, action, signer):
    logger.info(
        f"Creating transaction for image: {image_name} with hash: {image_hash}, app_id: {app_id}, action: {action}")
    payload = f"{image_hash},{image_name},{app_id},{action}".encode()

    header = TransactionHeader(
        family_name=FAMILY_NAME,
        family_version=FAMILY_VERSION,
        inputs=[NAMESPACE],
        outputs=[NAMESPACE],
        signer_public_key=signer.get_public_key().as_hex(),
        batcher_public_key=signer.get_public_key().as_hex(),
        dependencies=[],
        nonce=hex(int(time.time())),
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

    result = process_future_result(future)
    logger.info(f"Submitted batch to validator: {result}")
    return result


def process_action(action, tar_path=None, image_name=None, app_id=None):
    try:
        private_key = load_private_key(PRIVATE_KEY_FILE)
    except IOError as e:
        logger.error(str(e))
        sys.exit(1)

    context = create_context('secp256k1')
    signer = CryptoFactory(context).new_signer(private_key)

    if action == "deploy_image":
        if not tar_path:
            logger.error("Tar path is required for deploy_image action")
            sys.exit(1)
        image_hash, registry_image_name = hash_and_push_docker_image(tar_path)
        app_id = str(uuid.uuid4())  # Generate a unique application ID
    elif action in ["deploy_container", "remove_container", "remove_image"]:
        if not image_name or not app_id:
            logger.error("Image name and app_id are required for this action")
            sys.exit(1)
        image_hash = "N/A"  # Not needed for these actions
        registry_image_name = image_name
    else:
        logger.error(f"Unknown action: {action}")
        sys.exit(1)

    transaction = create_transaction(image_hash, registry_image_name, app_id, action, signer)
    batch = create_batch([transaction], signer)
    result = submit_batch(batch)

    return app_id, result


def main():
    if len(sys.argv) < 2:
        print("Usage: python docker_image_client.py <action> [<args>]")
        print("Actions:")
        print("  deploy_image <path_to_docker_image.tar>")
        print("  deploy_container <image_name> <app_id>")
        print("  remove_container <image_name> <app_id>")
        print("  remove_image <image_name> <app_id>")
        sys.exit(1)

    action = sys.argv[1]

    if action == "deploy_image":
        if len(sys.argv) != 3:
            print("Usage: python docker_image_client.py deploy_image <path_to_docker_image.tar>")
            sys.exit(1)
        tar_path = sys.argv[2]
        if not os.path.exists(tar_path):
            print(f"Error: File {tar_path} does not exist")
            sys.exit(1)
        app_id, result = process_action(action, tar_path=tar_path)
        print(f"Deployment submitted. Application ID: {app_id}")
    elif action in ["deploy_container", "remove_container", "remove_image"]:
        if len(sys.argv) != 4:
            print(f"Usage: python docker_image_client.py {action} <image_name> <app_id>")
            sys.exit(1)
        image_name = sys.argv[2]
        app_id = sys.argv[3]
        _, result = process_action(action, image_name=image_name, app_id=app_id)
    else:
        print(f"Unknown action: {action}")
        sys.exit(1)

    print(f"Submission result:")
    print(f"  Status: {result['status']}")
    print(f"  Message: {result['message']}")
    if 'error_details' in result:
        print(f"  Error Details: {result['error_details']}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
