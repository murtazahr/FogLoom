import hashlib
import logging
import os
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.protobuf.events_pb2 import EventSubscription, EventList
from sawtooth_sdk.protobuf.client_event_pb2 import ClientEventsSubscribeRequest, ClientEventsSubscribeResponse
from sawtooth_sdk.protobuf.validator_pb2 import Message

logger = logging.getLogger(__name__)

FAMILY_NAME = "docker-image"
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]
REGISTRY_URL = os.getenv('REGISTRY_URL', 'sawtooth-registry:5000')


class KubernetesEventHandler:
    def __init__(self):
        # Load Kubernetes configuration
        config.load_incluster_config()
        self.v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()

    def handle_event(self, event):
        logger.info(f"Handling event: {event.event_type}")
        if event.event_type == "docker-image-action":
            image_hash = None
            image_name = None
            app_id = None
            action = None
            for attr in event.attributes:
                if attr.key == "image_hash":
                    image_hash = attr.value
                elif attr.key == "image_name":
                    image_name = attr.value
                elif attr.key == "app_id":
                    app_id = attr.value
                elif attr.key == "action":
                    action = attr.value
            if image_name and app_id and action:
                logger.info(
                    f"Processing image: {image_name} with hash: {image_hash}, app_id: {app_id}, action: {action}")
                self.process_image_action(image_hash, image_name, app_id, action)
            else:
                logger.warning("Received docker-image-action event with incomplete data")
        elif event.event_type == "sawtooth/block-commit":
            logger.info("New block committed")
        else:
            logger.info(f"Received unhandled event type: {event.event_type}")

    def process_image_action(self, image_hash, image_name, app_id, action):
        if action == "deploy":
            self.deploy_image(image_name, image_hash, app_id)
        elif action == "remove":
            self.remove_image(app_id)
        else:
            logger.warning(f"Unknown action: {action}")

    def deploy_image(self, image_name, image_hash, app_id):
        daemonset_name = f"sawtooth-{app_id}"

        # Create a DaemonSet
        daemonset = client.V1DaemonSet(
            api_version="apps/v1",
            kind="DaemonSet",
            metadata=client.V1ObjectMeta(name=daemonset_name),
            spec=client.V1DaemonSetSpec(
                selector=client.V1LabelSelector(
                    match_labels={"app": daemonset_name}
                ),
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels={"app": daemonset_name}
                    ),
                    spec=client.V1PodSpec(
                        containers=[
                            client.V1Container(
                                name=daemonset_name,
                                image=f"{REGISTRY_URL}/{image_name}@sha256:{image_hash}",
                                image_pull_policy="Always"
                            )
                        ]
                    )
                )
            )
        )

        # Create DaemonSet
        try:
            self.apps_v1.create_namespaced_daemon_set(
                body=daemonset,
                namespace="default"
            )
            logger.info(f"DaemonSet {daemonset_name} created for app_id: {app_id}")
        except ApiException as e:
            logger.error(f"Exception when calling AppsV1Api->create_namespaced_daemon_set: {e}")

    def remove_image(self, app_id):
        daemonset_name = f"sawtooth-{app_id}"

        # Delete DaemonSet
        try:
            self.apps_v1.delete_namespaced_daemon_set(
                name=daemonset_name,
                namespace="default"
            )
            logger.info(f"DaemonSet {daemonset_name} deleted for app_id: {app_id}")
        except ApiException as e:
            logger.error(f"Exception when calling AppsV1Api->delete_namespaced_daemon_set: {e}")


def main():
    logger.info("Starting Docker Image Event Handler")
    handler = KubernetesEventHandler()
    stream = Stream(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))

    block_commit_subscription = EventSubscription(
        event_type="sawtooth/block-commit"
    )

    docker_image_subscription = EventSubscription(
        event_type="docker-image-action"
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
                    handler.handle_event(event)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error receiving message: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
