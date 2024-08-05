import hashlib
from google.protobuf import json_format
from payload_pb2 import DockerDeploymentPayload

FAMILY_NAME = 'docker_deployment'
DOCKER_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode('utf-8')).hexdigest()[:6]


def _make_address(image):
    return DOCKER_NAMESPACE + hashlib.sha512(image.encode('utf-8')).hexdigest()[:64]


class DockerState:
    def __init__(self, context):
        self._context = context

    @staticmethod
    def get_namespace():
        return DOCKER_NAMESPACE

    def set_docker_deployment(self, image, status):
        address = _make_address(image)
        state_data = DockerDeploymentPayload(
            image=image,
            action=DockerDeploymentPayload.DEPLOY if status == 'deployed' else DockerDeploymentPayload.UNDEPLOY
        ).SerializeToString()
        self._context.set_state({address: state_data})

    def get_docker_deployment(self, image):
        address = _make_address(image)
        state_entries = self._context.get_state([address])
        if state_entries:
            entry = DockerDeploymentPayload()
            entry.ParseFromString(state_entries[0].data)
            return json_format.MessageToDict(entry)
        return None
