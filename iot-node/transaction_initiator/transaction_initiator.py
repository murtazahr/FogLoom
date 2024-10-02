import json
import uuid
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
SCHEDULE_FAMILY_NAME = 'iot-schedule'
SCHEDULE_FAMILY_VERSION = '1.0'
STATUS_FAMILY_NAME = 'schedule-status'
STATUS_FAMILY_VERSION = '1.0'
IOT_DATA_FAMILY_NAME = 'iot-data'
IOT_DATA_FAMILY_VERSION = '1.0'
IOT_DATA_NAMESPACE = hashlib.sha512(IOT_DATA_FAMILY_NAME.encode()).hexdigest()[:6]
SCHEDULE_NAMESPACE = hashlib.sha512(SCHEDULE_FAMILY_NAME.encode()).hexdigest()[:6]
STATUS_NAMESPACE = hashlib.sha512(STATUS_FAMILY_NAME.encode()).hexdigest()[:6]
WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
DOCKER_IMAGE_NAMESPACE = hashlib.sha512('docker-image'.encode()).hexdigest()[:6]

PRIVATE_KEY_FILE = os.getenv('SAWTOOTH_PRIVATE_KEY', '/root/.sawtooth/keys/client.priv')
VALIDATOR_URL = os.getenv('VALIDATOR_URL', 'tcp://validator:4004')
IOT_URL = os.getenv('IOT_URL', 'tcp://iot')


def load_private_key(key_file):
    try:
        with open(key_file, 'r') as key_reader:
            private_key_str = key_reader.read().strip()
            return secp256k1.Secp256k1PrivateKey.from_hex(private_key_str)
    except IOError as ex:
        raise IOError(f"Failed to load private key from {key_file}: {str(ex)}") from ex


def create_transaction(signer, family_name, family_version, payload, inputs, outputs):
    payload_bytes = json.dumps(payload).encode()

    txn_header = TransactionHeader(
        family_name=family_name,
        family_version=family_version,
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


class TransactionCreator:
    def __init__(self):
        private_key = load_private_key(PRIVATE_KEY_FILE)
        context = create_context('secp256k1')
        self.signer = CryptoFactory(context).new_signer(private_key)

    def create_and_send_transactions(self, iot_data, workflow_id, iot_port, iot_public_key):
        try:
            schedule_id = str(uuid.uuid4())
            timestamp = int(time.time())

            # Create iot-schedule transaction
            schedule_payload = {
                "workflow_id": workflow_id,
                "schedule_id": schedule_id,
                "source_url": f"{IOT_URL}:{iot_port}",
                "source_public_key": iot_public_key,
                "timestamp": timestamp
            }
            schedule_inputs = [SCHEDULE_NAMESPACE, WORKFLOW_NAMESPACE, DOCKER_IMAGE_NAMESPACE]
            schedule_outputs = [SCHEDULE_NAMESPACE]
            schedule_txn = create_transaction(self.signer, SCHEDULE_FAMILY_NAME, SCHEDULE_FAMILY_VERSION,
                                              schedule_payload, schedule_inputs, schedule_outputs)

            # Create schedule-status transaction
            status_payload = {
                "schedule_id": schedule_id,
                "workflow_id": workflow_id,
                "timestamp": timestamp,
                "status": "ACTIVE"
            }
            status_inputs = [STATUS_NAMESPACE]
            status_outputs = [STATUS_NAMESPACE]
            status_txn = create_transaction(self.signer, STATUS_FAMILY_NAME, STATUS_FAMILY_VERSION,
                                            status_payload, status_inputs, status_outputs)

            # Create iot-data transaction
            iot_data_payload = {
                "iot_data": iot_data,
                "workflow_id": workflow_id,
                "schedule_id": schedule_id
            }
            iot_data_inputs = [IOT_DATA_NAMESPACE, WORKFLOW_NAMESPACE]
            iot_data_outputs = [IOT_DATA_NAMESPACE]
            iot_data_txn = create_transaction(self.signer, IOT_DATA_FAMILY_NAME, IOT_DATA_FAMILY_VERSION,
                                              iot_data_payload, iot_data_inputs, iot_data_outputs)

            # Create and submit batch with all three transactions
            batch = create_batch([schedule_txn, status_txn, iot_data_txn], self.signer)
            result = submit_batch(batch)

            logger.info({
                "message": "Data submitted successfully",
                "result": str(result),
                "schedule_id": schedule_id
            })

            return schedule_id

        except Exception as ex:
            logger.error(f"Error creating and sending transactions: {str(ex)}")
            raise


# This can be imported and used by various data sources
transaction_creator = TransactionCreator()
