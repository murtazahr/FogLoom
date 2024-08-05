import hashlib
import requests
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory
from deployment_tp.payload_pb2 import DockerDeploymentPayload


class DockerDeploymentClient:
    def __init__(self, url):
        self._url = url
        context = create_context('secp256k1')
        private_key = context.new_random_private_key()
        self._signer = CryptoFactory(context).new_signer(private_key)

    def deploy_docker_image(self, image):
        payload = DockerDeploymentPayload(
            action=DockerDeploymentPayload.DEPLOY,
            image=image
        ).SerializeToString()
        return self._send_transaction(payload)

    def undeploy_docker_image(self, image):
        payload = DockerDeploymentPayload(
            action=DockerDeploymentPayload.UNDEPLOY,
            image=image
        ).SerializeToString()
        return self._send_transaction(payload)

    def _send_transaction(self, payload):
        header = TransactionHeader(
            signer_public_key=self._signer.get_public_key().as_hex(),
            family_name="docker_deployment",
            family_version="1.0",
            inputs=[self._get_address(payload)],
            outputs=[self._get_address(payload)],
            dependencies=[],
            payload_sha512=hashlib.sha512(payload).hexdigest(),
            batcher_public_key=self._signer.get_public_key().as_hex(),
            nonce=hashlib.sha512(payload).hexdigest()
        ).SerializeToString()

        signature = self._signer.sign(header)

        transaction = Transaction(
            header=header,
            payload=payload,
            header_signature=signature
        )

        batch_header = BatchHeader(
            signer_public_key=self._signer.get_public_key().as_hex(),
            transaction_ids=[transaction.header_signature],
        ).SerializeToString()

        batch_signature = self._signer.sign(batch_header)

        batch = Batch(
            header=batch_header,
            transactions=[transaction],
            header_signature=batch_signature
        )

        batch_list = BatchList(batches=[batch])
        return self._send_request(
            "batches",
            batch_list.SerializeToString(),
            'application/octet-stream'
        )

    def _get_address(self, payload):
        return hashlib.sha512('docker_deployment'.encode('utf-8')).hexdigest()[:6] + \
            hashlib.sha512(payload).hexdigest()[:64]

    def _send_request(self, suffix, data=None, content_type=None):
        url = "{}/{}".format(self._url, suffix)
        headers = {}

        if content_type is not None:
            headers['Content-Type'] = content_type

        try:
            if data is not None:
                result = requests.post(url, headers=headers, data=data)
            else:
                result = requests.get(url, headers=headers)

            if not result.ok:
                raise Exception("Error {}: {}".format(
                    result.status_code, result.reason))

        except requests.ConnectionError as err:
            raise Exception(
                'Failed to connect to {}: {}'.format(url, str(err)))

        except BaseException as err:
            raise Exception(err)

        return result.text
