import base64
import gzip
import hashlib
import json
import sys

from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.protobuf.events_pb2 import Event

MAX_STATE_ENTRY_SIZE = 1000000  # 1MB, adjustable based on network config


def _calculate_image_id(image_data):
    return hashlib.sha512(image_data).hexdigest()


def _emit_image_stored_event(context, image_id, image_name, image_tag):
    event = Event(
        event_type="auto-deployment-docker/stored",
        attributes=[
            ("image_id", image_id),
            ("image_name", image_name),
            ("image_tag", image_tag),
        ]
    )
    context.add_event(event)


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
            image_data = base64.b64decode(payload['image_data'])
            image_name = payload['image_name']
            image_tag = payload.get('image_tag', 'latest')

            # Calculate image ID
            image_id = _calculate_image_id(image_data)

            # Compress the image data
            compressed_data = gzip.compress(image_data)

            # Split compressed data into chunks
            chunks = [compressed_data[i:i + MAX_STATE_ENTRY_SIZE]
                      for i in range(0, len(compressed_data), MAX_STATE_ENTRY_SIZE)]

            # Store chunks in state
            for i, chunk in enumerate(chunks):
                address = self._get_image_chunk_address(image_id, i)
                context.set_state({address: base64.b64encode(chunk)})

            # Store metadata
            metadata = {
                'id': image_id,
                'name': image_name,
                'tag': image_tag,
                'original_size': len(image_data),
                'compressed_size': len(compressed_data),
                'chunk_count': len(chunks),
                'compression': 'gzip'
            }
            metadata_address = self._get_image_metadata_address(image_id)
            context.set_state({metadata_address: json.dumps(metadata).encode()})

            # Emit event
            _emit_image_stored_event(context, image_id, image_name, image_tag)

        except Exception as e:
            raise InvalidTransaction(f"Failed to process Docker image transaction: {str(e)}")

    def _get_image_chunk_address(self, image_id, chunk_index):
        return self._namespace_prefix + hashlib.sha512(f"{image_id}{chunk_index}".encode()).hexdigest()[:64]

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
