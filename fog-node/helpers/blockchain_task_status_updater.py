import json
import time
import hashlib
import logging
import os
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_signing import create_context, CryptoFactory, secp256k1
from sawtooth_sdk.messaging.stream import Stream

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
FAMILY_NAME = 'iot-schedule'
FAMILY_VERSION = '1.0'
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


def create_schedule_status_update_transaction(signer, workflow_id, schedule_id, status):
    payload = {
        "workflow_id": workflow_id,
        "schedule_id": schedule_id,
        "status": status,
        "timestamp": int(time.time())
    }
    payload_bytes = json.dumps(payload).encode()

    txn_header = TransactionHeader(
        family_name=FAMILY_NAME,
        family_version=FAMILY_VERSION,
        inputs=[SCHEDULE_NAMESPACE],
        outputs=[SCHEDULE_NAMESPACE],
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


class StatusUpdateTransactor:
    def __init__(self):
        private_key = load_private_key(PRIVATE_KEY_FILE)
        context = create_context('secp256k1')
        self.signer = CryptoFactory(context).new_signer(private_key)

    def create_and_send_transaction(self, workflow_id, schedule_id, status):
        try:
            transaction = create_schedule_status_update_transaction(self.signer, workflow_id, schedule_id, status)
            batch = create_batch([transaction], self.signer)
            result = submit_batch(batch)

            logger.info({
                "message": "Status update submitted successfully",
                "result": str(result),
                "schedule_id": json.loads(transaction.payload.decode())['schedule_id']
            })

            return json.loads(transaction.payload.decode())['schedule_id']

        except Exception as ex:
            logger.error(f"Error creating and sending transaction: {str(ex)}")
            raise


# This can be imported and used by various data sources
status_update_transactor = StatusUpdateTransactor()
