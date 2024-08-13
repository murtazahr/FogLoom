import hashlib
import logging
import os
import docker
from docker import errors

from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.protobuf.events_pb2 import EventFilter, EventSubscription
from sawtooth_sdk.protobuf.client_event_pb2 import ClientEventsSubscribeRequest, ClientEventsSubscribeResponse
from sawtooth_sdk.protobuf.validator_pb2 import Message

logger = logging.getLogger(__name__)

FAMILY_NAME = "docker-image"
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]
REGISTRY_URL = os.getenv('REGISTRY_URL', 'sawtooth-registry:5000')


def handle_event(event):
    logger.info(f"Handling event: {event.event_type}")
    image_hash = None
    image_name = None
    for attr in event.attributes:
        if attr.key == 'image_hash':
            image_hash = attr.value
        elif attr.key == 'image_name':
            image_name = attr.value

    if image_hash and image_name:
        logger.info(f"Processing image: {image_name} with hash: {image_hash}")
        verify_and_run_container(image_hash, image_name)
    else:
        logger.warning("Incomplete event data")


def hash_docker_image(image):
    logger.info(f"Hashing Docker Image: {image.id}")
    sha256_hash = hashlib.sha256()
    for chunk in image.save():
        sha256_hash.update(chunk)
    image_hash = sha256_hash.hexdigest()
    logger.info(f"Calculated image hash: {image_hash}")
    return image_hash


def verify_and_run_container(stored_hash, image_name):
    client = docker.from_env()
    logger.info(f"Pulling Image: {image_name}")
    try:
        # Use the registry alias in the image name
        image = client.images.pull(image_name)
        logger.info(f"Image pulled successfully: {image.id}")
    except docker.errors.ImageNotFound:
        logger.error(f"Image {image_name} not found")
        return
    except Exception as e:
        logger.error(f"Error pulling image: {str(e)}")
        return

    calculated_hash = hash_docker_image(image)
    if calculated_hash != stored_hash:
        logger.error(f"Image hash mismatch. Expected: {stored_hash} Got: {calculated_hash}")
        return

    logger.info("Image hash verified successfully")

    try:
        container = client.containers.run(image, detach=True)
        logger.info(f"Container started: {container.id}")
    except docker.errors.ContainerError as e:
        logger.error(f"Error starting container: {str(e)}")


def main():
    logger.info(f"Starting Docker Image Event Handler")
    stream = Stream(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))

    event_filter = EventFilter(key="sawtooth/block-commit")
    block_commit_subscription = EventSubscription(event_type="sawtooth/block-commit", filters=[event_filter])

    docker_image_filter = EventFilter(key="docker-image-added")
    docker_image_subscription = EventSubscription(event_type="docker-image-added", filters=[docker_image_filter])

    request = ClientEventsSubscribeRequest(subscriptions=[block_commit_subscription, docker_image_subscription])

    logger.info(f"Subscribing request: {request}")
    response_future = stream.send(
        message_type=Message.CLIENT_EVENTS_SUBSCRIBE_REQUEST,
        content=request.SerializeToString(),
    )
    response = ClientEventsSubscribeResponse()
    response.ParseFromString(response_future.result().content)

    if response.status != ClientEventsSubscribeResponse.OK:
        logger.error(f"Subscription failed: {response.response_message}")
        return

    logger.info("Subscription successful. Listening for events...")

    while True:
        msg = stream.receive()
        if msg.message_type == Message.CLIENT_EVENTS:
            for event in msg.content.events:
                handle_event(event)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
