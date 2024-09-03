import hashlib
import logging
import os
import docker
from docker import errors

from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.protobuf.events_pb2 import EventSubscription, EventList
from sawtooth_sdk.protobuf.client_event_pb2 import ClientEventsSubscribeRequest, ClientEventsSubscribeResponse
from sawtooth_sdk.protobuf.validator_pb2 import Message

logger = logging.getLogger(__name__)

FAMILY_NAME = "docker-image"
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]
REGISTRY_URL = os.getenv('REGISTRY_URL', 'sawtooth-registry:5000')


def handle_event(event):
    logger.info(f"Handling event: {event.event_type}")
    if event.event_type == "docker-image-added":
        image_hash = None
        image_name = None
        for attr in event.attributes:
            if attr.key == "image_hash":
                image_hash = attr.value
            elif attr.key == "image_name":
                image_name = attr.value
        if image_hash and image_name:
            logger.info(f"Processing image: {image_name} with hash: {image_hash}")
            verify_and_run_container(image_hash, image_name)
        else:
            logger.warning("Received docker-image-added event with incomplete data")
    elif event.event_type == "sawtooth/block-commit":
        logger.info("New block committed")
    else:
        logger.info(f"Received unhandled event type: {event.event_type}")


def hash_docker_image(image):
    logger.info(f"Hashing Docker Image: {image.id}")
    sha256_hash = hashlib.sha256()
    for chunk in image.save():
        sha256_hash.update(chunk)
    image_hash = sha256_hash.hexdigest()
    logger.info(f"Calculated image hash: {image_hash}")
    return image_hash


def verify_and_run_container(stored_digest, image_name):
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

    # Get the content digest of the pulled image
    image_inspect = client.api.inspect_image(image.id)
    pulled_digest = image_inspect['RepoDigests'][0].split('@')[1] if image_inspect['RepoDigests'] else None

    if not pulled_digest:
        logger.error("Could not obtain content digest for pulled image")
        return

    if pulled_digest != stored_digest:
        logger.error(f"Image digest mismatch. Expected: {stored_digest} Got: {pulled_digest}")
        return

    logger.info("Image digest verified successfully")

    try:
        container = client.containers.run(image, detach=True)
        logger.info(f"Container started: {container.id}")
    except docker.errors.ContainerError as e:
        logger.error(f"Error starting container: {str(e)}")


def main():
    logger.info("Starting Docker Image Event Handler")
    stream = Stream(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))

    block_commit_subscription = EventSubscription(
        event_type="sawtooth/block-commit"
    )

    docker_image_subscription = EventSubscription(
        event_type="docker-image-added"
    )

    request = ClientEventsSubscribeRequest(
        subscriptions=[block_commit_subscription, docker_image_subscription]
    )

    logger.info(f"Subscribing request: {request}")
    response_future = stream.send(
        message_type=Message.CLIENT_EVENTS_SUBSCRIBE_REQUEST,
        content=request.SerializeToString()
    )
    response = ClientEventsSubscribeResponse()
    response.ParseFromString(response_future.result().content)

    if response.status != ClientEventsSubscribeResponse.OK:
        logger.error(f"Subscription failed: {response.response_message}")
        return

    logger.info("Subscription successful. Listening for events...")

    while True:
        try:
            msg_future = stream.receive()
            msg = msg_future.result()
            if msg.message_type == Message.CLIENT_EVENTS:
                event_list = EventList()
                event_list.ParseFromString(msg.content)
                for event in event_list.events:
                    handle_event(event)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error receiving message: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
