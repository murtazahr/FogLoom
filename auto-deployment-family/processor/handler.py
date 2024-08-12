import hashlib
import json
import sys

from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.protobuf.events_pb2 import Event


def _emit_image_stored_event(context, image_id, image_name, image_tag, registry_url):
    event = Event(
        event_type="auto-deployment-docker/stored",
        attributes=[
            ("image_id", image_id),
            ("image_name", image_name),
            ("image_tag", image_tag),
            ("registry_url", registry_url),
        ]
    )
    context.add_event(event)


def _calculate_image_id(image_name, image_tag, registry_url):
    return hashlib.sha512(f"{registry_url}/{image_name}:{image_tag}".encode()).hexdigest()


class AutoDockerDeploymentHandler(TransactionHandler):
    def __init__(self):
        self._namespace_prefix = hashlib.sha512('auto-deployment-docker'.encode('utf8')).hexdigest()[:6]

    @property
    def family_name(self):
        return 'auto-deployment-docker'

    @property
    def family_versions(self):
        return ['1.0']

    @property
    def namespaces(self):
        return [self._namespace_prefix]

    def apply(self, transaction, context):
        try:
            # Decode the payload
            payload = json.loads(transaction.payload.decode())
            image_name = payload['image_name']
            image_tag = payload.get('image_tag', 'latest')
            registry_url = payload['registry_url']
            signature = payload['signature']
            public_key = payload['public_key']

            # Calculate image ID
            image_id = _calculate_image_id(image_name, image_tag, registry_url)

            # Store metadata
            metadata = {
                'id': image_id,
                'name': image_name,
                'tag': image_tag,
                'registry_url': registry_url,
                'signature': signature,
                'public_key': public_key
            }
            metadata_address = self._get_image_metadata_address(image_id)
            context.set_state({metadata_address: json.dumps(metadata).encode()})

            # Emit event
            _emit_image_stored_event(context, image_id, image_name, image_tag, registry_url)

        except Exception as e:
            raise InvalidTransaction(f"Failed to process Docker image transaction: {str(e)}")

    def _get_image_metadata_address(self, image_id):
        return self._namespace_prefix + hashlib.sha512(f"{image_id}_metadata".encode()).hexdigest()[:64]


def main():
    # Use the URL passed as a command-line argument
    url = sys.argv[1] if len(sys.argv) > 1 else 'tcp://localhost:4004'
    processor = TransactionProcessor(url=url)
    handler = AutoDockerDeploymentHandler()
    processor.add_handler(handler)
    processor.start()


if __name__ == '__main__':
    main()
