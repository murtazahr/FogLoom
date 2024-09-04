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


def _make_app_address(app_id):
    address = DOCKER_IMAGE_NAMESPACE + hashlib.sha512(app_id.encode()).hexdigest()[:64]
    logger.debug(f"Generated app address: {address} for app ID: {app_id}")
    return address


def _make_workflow_address(workflow_id):
    address = NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]
    logger.debug(f"Generated workflow address: {address} for workflow ID: {workflow_id}")
    return address


def _get_all_app_ids(dependency_graph):
    logger.debug("Extracting all app IDs from dependency graph")
    app_ids = set()
    for node in dependency_graph['nodes']:
        app_ids.add(node)
        app_ids.update(dependency_graph['nodes'][node].get('next', []))
    logger.debug(f"Extracted app IDs: {app_ids}")
    return app_ids


def _validate_dependency_graph(context, dependency_graph):
    logger.info("Validating dependency graph")
    try:
        app_ids = _get_all_app_ids(dependency_graph)
        logger.debug(f"App IDs in dependency graph: {app_ids}")
        for app_id in app_ids:
            app_address = _make_app_address(app_id)
            logger.debug(f"Checking app_id {app_id} at address {app_address}")
            state_entries = context.get_state([app_address])
            if not state_entries or not state_entries[0].data:
                logger.error(f"Invalid app_id in dependency graph: {app_id}")
                raise InvalidTransaction(f'Invalid app_id in dependency graph: {app_id}')
        logger.info("Dependency graph validated successfully")
    except InvalidTransaction:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in _validate_dependency_graph: {str(e)}")
        raise InvalidTransaction(str(e))


def _get_workflow(context, workflow_id):
    logger.info(f"Retrieving workflow: {workflow_id}")
    try:
        state_address = _make_workflow_address(workflow_id)
        logger.debug(f"Fetching workflow from address: {state_address}")
        state_entries = context.get_state([state_address])

        if state_entries and state_entries[0].data:
            workflow_data = json.loads(state_entries[0].data.decode())
            logger.info(f"Workflow {workflow_id} retrieved successfully")
            logger.debug(f"Workflow data: {json.dumps(workflow_data)}")
            return workflow_data
        else:
            logger.error(f"Workflow not found: {workflow_id}")
            raise InvalidTransaction(f'Workflow not found: {workflow_id}')
    except Exception as e:
        logger.error(f"Unexpected error in _get_workflow: {str(e)}")
        raise InvalidTransaction(str(e))


def _create_workflow(context, workflow_id, dependency_graph):
    logger.info(f"Creating workflow: {workflow_id}")
    try:
        _validate_dependency_graph(context, dependency_graph)

        state_data = json.dumps(dependency_graph).encode()
        state_address = _make_workflow_address(workflow_id)
        logger.debug(f"Storing workflow at address: {state_address}")
        context.set_state({state_address: state_data})
        logger.info(f"Workflow {workflow_id} created successfully")
    except InvalidTransaction as e:
        logger.error(f"Failed to create workflow {workflow_id}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in _create_workflow: {str(e)}")
        raise InvalidTransaction(str(e))


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
        logger.info(f"Applying transaction: {transaction.header_signature}")
        try:
            payload = json.loads(transaction.payload.decode())
            logger.debug(f"Decoded payload: {json.dumps(payload)}")

            action = payload['action']
            workflow_id = payload['workflow_id']
            logger.info(f"Processing action: {action} for workflow ID: {workflow_id}")

            if action == 'create':
                dependency_graph = payload['dependency_graph']
                logger.debug(f"Dependency graph for workflow {workflow_id}: {json.dumps(dependency_graph)}")
                _create_workflow(context, workflow_id, dependency_graph)
            elif action == 'get':
                _get_workflow(context, workflow_id)
            else:
                logger.error(f"Unknown action: {action}")
                raise InvalidTransaction(f'Unknown action: {action}')

        except json.decoder.JSONDecodeError as e:
            logger.error(f"Invalid payload: not a valid JSON. Error: {str(e)}")
            raise InvalidTransaction('Invalid payload: not a valid JSON')
        except KeyError as e:
            logger.error(f"Invalid payload: missing key {str(e)}")
            raise InvalidTransaction(f'Invalid payload: missing {str(e)}')
        except Exception as e:
            logger.error(f"Unexpected error in apply method: {str(e)}")
            raise


def main():
    logger.info("Starting Workflow Transaction Processor")
    try:
        processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
        handler = WorkflowTransactionHandler()
        processor.add_handler(handler)
        logger.info("Workflow Transaction Handler added to processor")
        processor.start()
    except KeyboardInterrupt:
        logger.info("Workflow Transaction Processor interrupted. Exiting.")
    except Exception as e:
        logger.error(f"Unexpected error in Workflow Transaction Processor: {str(e)}")
    finally:
        logger.info("Workflow Transaction Processor stopped")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    )
    main()
