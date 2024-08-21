import hashlib
import logging
import os
import sys
import time
import json
import psutil
from sawtooth_sdk.messaging.stream import Stream

from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_signing import create_context, CryptoFactory, secp256k1

logger = logging.getLogger(__name__)

FAMILY_NAME = 'resource'
FAMILY_VERSION = '1.0'
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

# Path to the private key file
PRIVATE_KEY_FILE = os.getenv('SAWTOOTH_PRIVATE_KEY', '/root/.sawtooth/keys/root.priv')


def get_resource_data():
    try:
        # CPU information
        cpu_count = psutil.cpu_count()
        cpu_percent = psutil.cpu_percent(interval=1)

        # Memory information
        memory = psutil.virtual_memory()
        memory_total = memory.total
        memory_used = memory.used
        memory_percent = memory.percent

        # Disk information
        disk = psutil.disk_usage('/')
        disk_total = disk.total
        disk_used = disk.used
        disk_percent = disk.percent

        return {
            'cpu': {
                'total': cpu_count,
                'used_percent': cpu_percent
            },
            'memory': {
                'total': memory_total,
                'used': memory_used,
                'used_percent': memory_percent
            },
            'disk': {
                'total': disk_total,
                'used': disk_used,
                'used_percent': disk_percent
            }
        }
    except Exception as e:
        logger.error(f"Error getting resource data: {str(e)}")
        return None


def load_private_key(key_file):
    try:
        with open(key_file, 'r') as key_reader:
            private_key_str = key_reader.read().strip()
            return secp256k1.Secp256k1PrivateKey.from_hex(private_key_str)
    except IOError as e:
        raise IOError(f"Failed to load private key from {key_file}: {str(e)}") from e


def create_transaction(node_id, resource_data, signer):
    logger.info(f"Creating peer registry transaction for node ID: {node_id}")
    payload = json.dumps({
        'node_id': node_id,
        'resource_data': resource_data
    }).encode()

    header = TransactionHeader(
        family_name=FAMILY_NAME,
        family_version=FAMILY_VERSION,
        inputs=[NAMESPACE],
        outputs=[NAMESPACE],
        signer_public_key=signer.get_public_key().as_hex(),
        batcher_public_key=signer.get_public_key().as_hex(),
        dependencies=[],
        payload_sha512=hashlib.sha512(payload).hexdigest(),
        nonce=hex(int(time.time()))
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
    logger.info("Starting Resource Registration Client")
    node_id = os.getenv('NODE_ID', 'node1')

    try:
        private_key = load_private_key(PRIVATE_KEY_FILE)
    except IOError as e:
        logger.error(str(e))
        sys.exit(1)

    context = create_context('secp256k1')
    signer = CryptoFactory(context).new_signer(private_key)

    while True:
        try:
            resource_data = get_resource_data()
            if resource_data:
                logger.debug(f"Resource data: {json.dumps(resource_data, indent=2)}")
                transaction = create_transaction(node_id, resource_data, signer)
                batch = create_batch([transaction], signer)
                submit_batch(batch)
            else:
                logger.warning("Failed to get resource data")
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
        time.sleep(60)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
