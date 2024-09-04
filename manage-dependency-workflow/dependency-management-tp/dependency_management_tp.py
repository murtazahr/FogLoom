import hashlib
import json
import logging
import os
import traceback

from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.core import TransactionProcessor

FAMILY_NAME = 'workflow-dependency'
FAMILY_VERSION = '1.0'
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

DOCKER_IMAGE_NAMESPACE = hashlib.sha512('docker-image'.encode()).hexdigest()[:6]

logger = logging.getLogger(__name__)


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
            logger.info("Applying transaction")
            logger.debug(f"Transaction details: {transaction}")

            # Log all attributes of the transaction
            for attr in dir(transaction):
                if not attr.startswith('__'):
                    try:
                        value = getattr(transaction, attr)
                        logger.debug(f"Transaction.{attr} = {value}")
                    except Exception as e:
                        logger.debug(f"Error accessing Transaction.{attr}: {str(e)}")

            # Safely get the header_signature
            header_signature = getattr(transaction, 'header_signature', None)
            logger.info(f"Processing transaction: {header_signature}")

            try:
                payload = json.loads(transaction.payload.decode())
                logger.debug(f"Decoded payload: {json.dumps(payload)}")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid payload: not a valid JSON. Error: {str(e)}")
                raise InvalidTransaction('Invalid payload: not a valid JSON')
            except Exception as e:
                logger.error(f"Error decoding payload: {str(e)}")
                raise InvalidTransaction(f'Error decoding payload: {str(e)}')

            action = payload.get('action')
            workflow_id = payload.get('workflow_id')

            if not action or not workflow_id:
                logger.error("Missing 'action' or 'workflow_id' in payload")
                raise InvalidTransaction("Missing 'action' or 'workflow_id' in payload")

            logger.info(f"Processing action: {action} for workflow ID: {workflow_id}")

            if action == 'create':
                dependency_graph = payload.get('dependency_graph')
                if not dependency_graph:
                    logger.error("Missing 'dependency_graph' in payload for create action")
                    raise InvalidTransaction("Missing 'dependency_graph' in payload for create action")
                logger.debug(f"Dependency graph for workflow {workflow_id}: {json.dumps(dependency_graph)}")
                self._create_workflow(context, workflow_id, dependency_graph)
            elif action == 'get':
                self._get_workflow(context, workflow_id)
            else:
                logger.error(f"Unknown action: {action}")
                raise InvalidTransaction(f'Unknown action: {action}')

        except InvalidTransaction as e:
            logger.error(f"Invalid transaction: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in apply method: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(f"Unexpected error: {str(e)}")

    def _create_workflow(self, context, workflow_id, dependency_graph):
        logger.info(f"Creating workflow: {workflow_id}")
        try:
            self._validate_dependency_graph(context, dependency_graph)

            state_data = json.dumps(dependency_graph).encode()
            state_address = self._make_workflow_address(workflow_id)
            logger.debug(f"Storing workflow at address: {state_address}")
            context.set_state({state_address: state_data})
            logger.info(f"Workflow {workflow_id} created successfully")
        except InvalidTransaction as e:
            logger.error(f"Failed to create workflow {workflow_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in _create_workflow: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(str(e))

    def _get_workflow(self, context, workflow_id):
        logger.info(f"Retrieving workflow: {workflow_id}")
        try:
            state_address = self._make_workflow_address(workflow_id)
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
            logger.error(traceback.format_exc())
            raise InvalidTransaction(str(e))

    def _validate_dependency_graph(self, context, dependency_graph):
        logger.info("Validating dependency graph")
        try:
            app_ids = self._get_all_app_ids(dependency_graph)
            logger.debug(f"App IDs in dependency graph: {app_ids}")
            for app_id in app_ids:
                app_address = self._make_app_address(app_id)
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
            logger.error(traceback.format_exc())
            raise InvalidTransaction(str(e))

    def _get_all_app_ids(self, dependency_graph):
        logger.debug("Extracting all app IDs from dependency graph")
        app_ids = set()
        for node in dependency_graph['nodes']:
            app_ids.add(node)
            app_ids.update(dependency_graph['nodes'][node].get('next', []))
        logger.debug(f"Extracted app IDs: {app_ids}")
        return app_ids

    def _make_workflow_address(self, workflow_id):
        address = NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]
        logger.debug(f"Generated workflow address: {address} for workflow ID: {workflow_id}")
        return address

    def _make_app_address(self, app_id):
        address = DOCKER_IMAGE_NAMESPACE + hashlib.sha512(app_id.encode()).hexdigest()[:64]
        logger.debug(f"Generated app address: {address} for app ID: {app_id}")
        return address


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
        logger.error(traceback.format_exc())
    finally:
        logger.info("Workflow Transaction Processor stopped")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    )
    main()
