import gzip
import base64
import json
import hashlib
import sys

import docker
from docker import errors
from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.protobuf.events_pb2 import EventList
from sawtooth_sdk.protobuf.validator_pb2 import Message


def get_image_metadata_address(image_id):
    namespace_prefix = hashlib.sha512('auto-deployment-docker'.encode('utf8')).hexdigest()[:6]
    return namespace_prefix + hashlib.sha512(f"{image_id}_metadata".encode()).hexdigest()[:64]


def get_image_chunk_address(image_id, chunk_index):
    namespace_prefix = hashlib.sha512('auto-deployment-docker'.encode('utf8')).hexdigest()[:6]
    return namespace_prefix + hashlib.sha512(f"{image_id}{chunk_index}".encode()).hexdigest()[:64]


def get_image_from_state(context, image_id):
    # Fetch metadata
    metadata_address = get_image_metadata_address(image_id)
    metadata_bytes = context.get_state([metadata_address])[metadata_address]
    metadata = json.loads(metadata_bytes.decode())

    # Fetch and reassemble compressed chunks
    compressed_data = b''
    for i in range(metadata['chunk_count']):
        chunk_address = get_image_chunk_address(image_id, i)
        chunk = base64.b64decode(context.get_state([chunk_address])[chunk_address])
        compressed_data += chunk

    # Decompress the data
    image_data = gzip.decompress(compressed_data)

    return image_data


def handle_image_stored_event(event, context):
    image_id = None
    image_name = None
    image_tag = None
    for attr in event.attributes:
        if attr.key == 'image_id':
            image_id = attr.value
        elif attr.key == 'image_name':
            image_name = attr.value
        elif attr.key == 'image_tag':
            image_tag = attr.value

    if not all([image_id, image_name, image_tag]):
        print(f"Incomplete event data: image_id={image_id}, image_name={image_name}, image_tag={image_tag}")
        return

    print(f"Handling event for image: {image_name}:{image_tag} (ID: {image_id})")

    image_data = get_image_from_state(context, image_id)

    # Load image into Docker
    client = docker.from_env()
    try:
        image = client.images.load(image_data)[0]
        print(f"Loaded image: {image.tags}")
    except docker.errors.ImageLoadError as e:
        print(f"Failed to load image: {str(e)}")
        return

    # Run container
    try:
        container = client.containers.run(f"{image_name}:{image_tag}", detach=True, name=f"{image_name}-{image_tag}")
        print(f"Started container: {container.id}")
    except docker.errors.APIError as e:
        print(f"Failed to start container: {str(e)}")


def listen_to_events():
    print("Starting event listener...")
    # Use the URL passed as a command-line argument
    url = sys.argv[1] if len(sys.argv) > 1 else 'tcp://localhost:4004'
    stream = Stream(url=url)

    subscription = EventList(
        events=[
            EventList.Event(event_type="auto-deployment-docker/stored")
        ]
    )

    stream.send(
        message_type=Message.CLIENT_EVENTS_SUBSCRIBE_REQUEST,
        content=subscription.SerializeToString()
    )

    while True:
        msg = stream.receive()
        if msg.message_type == Message.CLIENT_EVENTS:
            event_list = EventList()
            event_list.ParseFromString(msg.content)
            for event in event_list.events:
                if event.event_type == "auto-deployment-docker/stored":
                    handle_image_stored_event(event, stream)


if __name__ == '__main__':
    listen_to_events()
