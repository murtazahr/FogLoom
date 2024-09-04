import hashlib
import json
import logging
import os

from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.core import TransactionProcessor

FAMILY_NAME = 'workflow-dependency'
FAMILY_VERSION = '1.0'
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

DOCKER_IMAGE_NAMESPACE = hashlib.sha512('docker-image'.encode()).hexdigest()[:6]

logger = logging.getLogger(__name__)


def _get_all_app_ids(dependency_graph):
    app_ids = set()
    for node in dependency_graph['nodes']:
        app_ids.add(node)
        app_ids.update(dependency_graph['nodes'][node].get('next', []))
    return app_ids


def _make_workflow_address(workflow_id):
    return NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]


def _make_app_address(app_id):
    return DOCKER_IMAGE_NAMESPACE + hashlib.sha512(app_id.encode()).hexdigest()[:64]


def _validate_dependency_graph(context, dependency_graph):
    logger.info("Validating dependency graph")
    app_ids = _get_all_app_ids(dependency_graph)
    for app_id in app_ids:
        app_address = _make_app_address(app_id)
        state_entries = context.get_state([app_address])
        if not state_entries or not state_entries[0].data:
            raise InvalidTransaction(f'Invalid app_id in dependency graph: {app_id}')
    logger.info("Dependency graph validated successfully")


def _get_workflow(context, workflow_id):
    logger.info(f"Retrieving workflow: {workflow_id}")
    state_address = _make_workflow_address(workflow_id)
    state_entries = context.get_state([state_address])

    if state_entries and state_entries[0].data:
        workflow_data = json.loads(state_entries[0].data.decode())
        logger.info(f"Workflow {workflow_id} retrieved successfully")
        return workflow_data
    else:
        raise InvalidTransaction(f'Workflow not found: {workflow_id}')


def _create_workflow(context, workflow_id, dependency_graph):
    logger.info(f"Creating workflow: {workflow_id}")
    _validate_dependency_graph(context, dependency_graph)

    state_data = json.dumps(dependency_graph).encode()
    state_address = _make_workflow_address(workflow_id)
    context.set_state({state_address: state_data})
    logger.info(f"Workflow {workflow_id} created successfully")


class WorkflowTransactionHandler(TransactionHandler):
    @property
    def family_name(self):
        return FAMILY_NAME

    @property
    def family_versions(self):
        return [FAMILY_VERSION]

    @property
    def namespaces(self):
        return [NAMESPACE]

    def apply(self, transaction, context):
        try:
            payload = json.loads(transaction.payload.decode())
            action = payload['action']
            workflow_id = payload['workflow_id']

            if action == 'create':
                dependency_graph = payload['dependency_graph']
                _create_workflow(context, workflow_id, dependency_graph)
            elif action == 'get':
                _get_workflow(context, workflow_id)
            else:
                raise InvalidTransaction(f'Unknown action: {action}')

        except json.decoder.JSONDecodeError:
            raise InvalidTransaction('Invalid payload: not a valid JSON')
        except KeyError as e:
            raise InvalidTransaction(f'Invalid payload: missing {str(e)}')


def main():
    logging.basicConfig(level=logging.DEBUG)
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = WorkflowTransactionHandler()
    processor.add_handler(handler)
    processor.start()


if __name__ == '__main__':
    main()
