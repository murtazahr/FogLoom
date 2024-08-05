import logging
from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from state import DockerState
from payload_pb2 import DockerDeploymentPayload
import docker

LOGGER = logging.getLogger(__name__)


class DockerDeploymentTransactionHandler(TransactionHandler):
    def __init__(self):
        self.client = docker.from_env()

    @property
    def family_name(self):
        return 'docker_deployment'

    @property
    def family_versions(self):
        return ['1.0']

    @property
    def namespaces(self):
        return [DockerState.get_namespace()]

    def _deploy_docker_image(self, image):
        try:
            self.client.images.pull(image)
            self.client.containers.run(image, detach=True)
        except docker.errors.ImageNotFound:
            raise InvalidTransaction(f"Docker image not found: {image}")
        except docker.errors.APIError as e:
            raise InvalidTransaction(f"Docker API error during deployment: {str(e)}")
        except Exception as e:
            raise InvalidTransaction(f"Unexpected error during deployment: {str(e)}")

    def _undeploy_docker_image(self, image):
        try:
            containers = self.client.containers.list(filters={'ancestor': image})
            if not containers:
                raise InvalidTransaction(f"No containers found for image: {image}")
            for container in containers:
                container.stop()
                container.remove()
            self.client.images.remove(image)
        except docker.errors.ImageNotFound:
            raise InvalidTransaction(f"Docker image not found: {image}")
        except docker.errors.APIError as e:
            raise InvalidTransaction(f"Docker API error during undeployment: {str(e)}")
        except Exception as e:
            raise InvalidTransaction(f"Unexpected error during undeployment: {str(e)}")

    def apply(self, transaction, context):
        header = transaction.header
        payload = DockerDeploymentPayload()
        payload.ParseFromString(transaction.payload)
        state = DockerState(context)

        LOGGER.info(f"Docker deployment transaction received. Action: {payload.action}, Image: {payload.image}")

        current_state = state.get_docker_deployment(payload.image)

        if payload.action == DockerDeploymentPayload.DEPLOY:
            if current_state and current_state['action'] == DockerDeploymentPayload.DEPLOY:
                raise InvalidTransaction(f"Image {payload.image} is already deployed")
            LOGGER.info(f"Deploying Docker image: {payload.image}")
            self._deploy_docker_image(payload.image)
            state.set_docker_deployment(payload.image, 'deployed')
            LOGGER.info(f"Successfully deployed Docker image: {payload.image}")
        elif payload.action == DockerDeploymentPayload.UNDEPLOY:
            if not current_state or current_state['action'] != DockerDeploymentPayload.DEPLOY:
                raise InvalidTransaction(f"Image {payload.image} is not currently deployed")
            LOGGER.info(f"Undeploying Docker image: {payload.image}")
            self._undeploy_docker_image(payload.image)
            state.set_docker_deployment(payload.image, 'undeployed')
            LOGGER.info(f"Successfully undeployed Docker image: {payload.image}")
        else:
            raise InvalidTransaction(f'Invalid action: {payload.action}')
