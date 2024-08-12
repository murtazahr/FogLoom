import base64
import hashlib
import json
import sys
import docker
from docker import errors
from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.protobuf.events_pb2 import EventList, EventSubscription
from sawtooth_sdk.protobuf.validator_pb2 import Message
from sawtooth_sdk.protobuf.client_event_pb2 import ClientEventsSubscribeRequest, ClientEventsSubscribeResponse
from sawtooth_sdk.protobuf.client_state_pb2 import ClientStateGetRequest, ClientStateGetResponse
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from cryptography.exceptions import InvalidSignature


def get_state(context, address):
    request = ClientStateGetRequest(address=address)
    response = context.send(
        Message.CLIENT_STATE_GET_REQUEST,
        request.SerializeToString()
    ).result()

    state_response = ClientStateGetResponse()
    state_response.ParseFromString(response.content)

    if state_response.status == ClientStateGetResponse.OK:
        return state_response.value
    else:
        raise Exception(f"Error getting state: {state_response.status}")


def get_image_metadata_address(image_id):
    namespace_prefix = hashlib.sha512('auto-deployment-docker'.encode('utf8')).hexdigest()[:6]
    return namespace_prefix + hashlib.sha512(f"{image_id}_metadata".encode()).hexdigest()[:64]


def get_image_metadata(context, image_id):
    metadata_address = get_image_metadata_address(image_id)
    metadata_bytes = get_state(context, metadata_address)
    return json.loads(metadata_bytes.decode())


def verify_image_signature(image, signature, public_key_pem):
    # Calculate the image hash
    image_hash = hashlib.sha256()
    for chunk in image.save():
        image_hash.update(chunk)

    # Load the public key
    public_key = load_pem_public_key(public_key_pem.encode())

    # Verify the signature
    try:
        public_key.verify(
            signature,
            image_hash.digest(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return True
    except InvalidSignature:
        return False


def handle_image_stored_event(event, context):
    image_id = None
    image_name = None
    image_tag = None
    registry_url = None

    for attr in event.attributes:
        if attr.key == 'image_id':
            image_id = attr.value
        elif attr.key == 'image_name':
            image_name = attr.value
        elif attr.key == 'image_tag':
            image_tag = attr.value
        elif attr.key == 'registry_url':
            registry_url = attr.value

    if not all([image_id, image_name, image_tag, registry_url]):
        print(
            f"Incomplete event data: image_id={image_id}, image_name={image_name}, "
            f"image_tag={image_tag}, registry_url={registry_url}")
        return

    print(f"Handling event for image: {image_name}:{image_tag} (ID: {image_id}) from registry: {registry_url}")

    # Retrieve the image metadata from the blockchain
    metadata = get_image_metadata(context, image_id)
    signature = base64.b64decode(metadata['signature'])
    public_key_pem = metadata['public_key']

    # Pull image from registry
    client = docker.from_env()
    try:
        image = client.images.pull(f"{registry_url}/{image_name}:{image_tag}")
        print(f"Pulled image: {image.tags}")
    except docker.errors.APIError as e:
        print(f"Failed to pull image: {str(e)}")
        return

    # Verify image signature
    if not verify_image_signature(image, signature, public_key_pem):
        print(f"Image signature verification failed for {image_name}:{image_tag}")
        return

    print(f"Image signature verified successfully for {image_name}:{image_tag}")

    # Run container
    try:
        container = client.containers.run(f"{registry_url}/{image_name}:{image_tag}", detach=True,
                                          name=f"{image_name}-{image_tag}")
        print(f"Started container: {container.id}")
    except docker.errors.APIError as e:
        print(f"Failed to start container: {str(e)}")


def listen_to_events():
    print("Starting event listener...")
    url = sys.argv[1] if len(sys.argv) > 1 else 'tcp://localhost:4004'
    stream = Stream(url=url)

    # Create an event subscription
    subscription = EventSubscription(
        event_type="auto-deployment-docker/stored",
        filters=[]
    )

    # Create the subscription request
    request = ClientEventsSubscribeRequest(
        subscriptions=[subscription]
    )

    # Send the subscription request
    stream.send(
        Message.CLIENT_EVENTS_SUBSCRIBE_REQUEST,
        request.SerializeToString()
    ).result()

    # Process the response
    msg = stream.receive().result()
    response = ClientEventsSubscribeResponse()
    response.ParseFromString(msg.content)

    if response.status != ClientEventsSubscribeResponse.OK:
        print(f"Subscription failed: {response.response_message}")
        return

    # Listen for events
    while True:
        msg = stream.receive().result()
        if msg.message_type == Message.CLIENT_EVENTS:
            event_list = EventList()
            event_list.ParseFromString(msg.content)
            for event in event_list.events:
                if event.event_type == "auto-deployment-docker/stored":
                    handle_image_stored_event(event, stream)


if __name__ == '__main__':
    listen_to_events()
