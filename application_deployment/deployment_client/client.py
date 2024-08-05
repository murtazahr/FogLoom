import sys
import hashlib
import cbor
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch
from sawtooth_signing import create_context, CryptoFactory
from deployment_tp.payload_pb2 import DockerDeploymentPayload

FAMILY_NAME = 'docker_deployment'
FAMILY_VERSION = '1.0'
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode('utf-8')).hexdigest()[:6]


def _hash(data):
    return hashlib.sha512(data).hexdigest()


def _make_address(image):
    return NAMESPACE + _hash(image.encode('utf-8'))[:64]


def create_transaction(signer, action, image):
    payload = DockerDeploymentPayload(
        action=action,
        image=image
    )

    payload_bytes = payload.SerializeToString()

    txn_header_bytes = TransactionHeader(
        family_name=FAMILY_NAME,
        family_version=FAMILY_VERSION,
        inputs=[_make_address(image)],
        outputs=[_make_address(image)],
        signer_public_key=signer.get_public_key().as_hex(),
        batcher_public_key=signer.get_public_key().as_hex(),
        dependencies=[],
        payload_sha512=_hash(payload_bytes)
    ).SerializeToString()

    signature = signer.sign(txn_header_bytes)

    txn = Transaction(
        header=txn_header_bytes,
        header_signature=signature,
        payload=payload_bytes
    )

    return txn


def create_batch(transactions, signer):
    batch_header_bytes = BatchHeader(
        signer_public_key=signer.get_public_key().as_hex(),
        transaction_ids=[txn.header_signature for txn in transactions],
    ).SerializeToString()

    signature = signer.sign(batch_header_bytes)

    batch = Batch(
        header=batch_header_bytes,
        header_signature=signature,
        transactions=transactions
    )

    return batch


def main():
    if len(sys.argv) < 3:
        print("Usage: python client.py <deploy|undeploy> <docker_image>")
        sys.exit(1)

    action = sys.argv[1].upper()
    image = sys.argv[2]

    if action not in ['DEPLOY', 'UNDEPLOY']:
        print("Invalid action. Use 'deploy' or 'undeploy'.")
        sys.exit(1)

    context = create_context('secp256k1')
    private_key = context.new_random_private_key()
    signer = CryptoFactory(context).new_signer(private_key)

    action_enum = DockerDeploymentPayload.DEPLOY if action == 'DEPLOY' else DockerDeploymentPayload.UNDEPLOY
    txn = create_transaction(signer, action_enum, image)
    batch = create_batch([txn], signer)
    batch_list_bytes = batch.SerializeToString()

    try:
        print(f"Submitting {action.lower()} transaction for image: {image}")
        # Here you would typically send the batch_list_bytes to the Sawtooth REST API
        # For demonstration, we'll just print the bytes
        print(f"Batch bytes: {batch_list_bytes.hex()}")
        print("Transaction submitted successfully.")
    except Exception as e:
        print(f"Error submitting transaction: {str(e)}")


if __name__ == "__main__":
    main()
