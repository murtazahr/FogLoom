import asyncio
import aiohttp
import aiocouch
import json
import logging
import os
from typing import Dict, Any
import docker
from docker import errors

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COUCHDB_URL = f"http://{os.getenv('COUCHDB_USER')}:{os.getenv('COUCHDB_PASSWORD')}@{os.getenv('COUCHDB_HOST', 'couchdb-0:5984')}"
SCHEDULE_DB = 'schedules'
DATA_DB = 'task_data'

NODE_ID = os.getenv('HOSTNAME')


class TaskExecutor:
    def __init__(self):
        self.couch = None
        self.schedule_db = None
        self.data_db = None
        self.docker_client = docker.from_env()

    async def initialize(self):
        self.couch = await aiocouch.CouchDB(COUCHDB_URL)
        self.schedule_db = await self.couch.create_database(SCHEDULE_DB)
        self.data_db = await self.couch.create_database(DATA_DB)
        await self.setup_views()

    async def setup_views(self):
        design_doc = {
            '_id': '_design/tasks',
            'views': {
                'by_node': {
                    'map': '''function(doc) {
                        if (doc.schedule && doc.schedule.level_info) {
                            for (var level in doc.schedule.level_info) {
                                doc.schedule.level_info[level].forEach(function(task) {
                                    if (task.node === "%s") {
                                        emit([doc._id, doc.workflow_id, level, task.app_id], task);
                                    }
                                });
                            }
                        }
                    }''' % NODE_ID
                }
            }
        }
        try:
            await self.schedule_db.create_document(design_doc)
        except aiocouch.exceptions.DocumentConflictError:
            # View already exists, update it
            existing_doc = await self.schedule_db.get_document('_design/tasks')
            existing_doc.update(design_doc)
            await existing_doc.save()

    async def run(self):
        async with aiohttp.ClientSession() as session:
            async for change in self.schedule_db.changes(feed='continuous', heartbeat=1000, include_docs=True):
                if change.get('id'):
                    await self.handle_change(change, session)

    async def handle_change(self, change, session):
        doc = change['doc']
        if doc.get('status') == 'ACTIVE' and 'schedule' in doc:
            workflow_id = doc['workflow_id']
            schedule_id = doc['_id']
            tasks = await self.schedule_db.view('tasks/by_node', key=[schedule_id, workflow_id])
            for row in tasks:
                task = row.value
                if await self.is_task_ready(task, doc):
                    await self.execute_task(workflow_id, schedule_id, task, session)

    async def is_task_ready(self, task: Dict[str, Any], schedule_doc: Dict[str, Any]) -> bool:
        if task.get('status') == 'COMPLETED':
            return False

        level_info = schedule_doc['schedule']['level_info']
        current_level = next((level for level, tasks in level_info.items() if task in tasks), None)
        if current_level is None:
            logger.error(f"Task {task['app_id']} not found in any level")
            return False

        prev_level = int(current_level) - 1
        if prev_level >= 0:
            prev_level_tasks = level_info.get(str(prev_level), [])
            return all(prev_task.get('status') == 'COMPLETED' for prev_task in prev_level_tasks)
        return True

    async def execute_task(self, workflow_id: str, schedule_id: str, task: Dict[str, Any], session):
        app_id = task['app_id']
        input_data = await self.get_input_data(workflow_id, schedule_id, app_id, session)

        container_name = f"sawtooth-{app_id}"
        try:
            container = self.docker_client.containers.get(container_name)
            result = await asyncio.to_thread(container.exec_run, f"python /app/process.py '{json.dumps(input_data)}'")
            output_data = json.loads(result.output.decode())

            await self.store_output_data(workflow_id, schedule_id, app_id, output_data, session)
            await self.update_task_status(workflow_id, schedule_id, app_id, 'COMPLETED', session)
            await self.propagate_data_to_next_tasks(workflow_id, schedule_id, task, output_data, session)
            logger.info(f"Task {app_id} completed successfully for workflow {workflow_id}, schedule {schedule_id}")
        except docker.errors.NotFound:
            logger.error(f"Container {container_name} not found")
        except Exception as e:
            logger.error(f"Error executing task {app_id} for workflow {workflow_id}, schedule {schedule_id}: {str(e)}")
            await self.update_task_status(workflow_id, schedule_id, app_id, 'FAILED', session)

    async def get_input_data(self, workflow_id: str, schedule_id: str, app_id: str, session) -> Dict[str, Any]:
        data_id = self._generate_data_id(workflow_id, schedule_id, app_id, 'input')
        try:
            doc = await self.data_db.get_document(data_id)
            return doc['data']
        except aiocouch.exceptions.DocumentNotFoundError:
            logger.warning(f"Input data not found for task {app_id} in workflow {workflow_id}, schedule {schedule_id}")
            return {}

    async def store_output_data(self, workflow_id: str, schedule_id: str, app_id: str, output_data: Dict[str, Any],
                                session):
        data_id = self._generate_data_id(workflow_id, schedule_id, app_id, 'output')
        document = {
            '_id': data_id,
            'data': output_data,
            'workflow_id': workflow_id,
            'schedule_id': schedule_id
        }
        try:
            await self.data_db.create_document(document)
        except aiocouch.exceptions.DocumentConflictError:
            # Document already exists, update it
            existing_doc = await self.data_db.get_document(data_id)
            existing_doc.update(document)
            await existing_doc.save()

    async def update_task_status(self, workflow_id: str, schedule_id: str, app_id: str, status: str, session):
        try:
            schedule_doc = await self.schedule_db.get_document(schedule_id)
            if 'schedule' in schedule_doc and 'level_info' in schedule_doc['schedule']:
                for level_info in schedule_doc['schedule']['level_info'].values():
                    for task in level_info:
                        if task['app_id'] == app_id:
                            task['status'] = status
                            break
                await schedule_doc.save()
            else:
                logger.error(f"Invalid schedule structure for workflow {workflow_id}, schedule {schedule_id}")
        except aiocouch.exceptions.DocumentNotFoundError:
            logger.error(f"Schedule document not found for workflow {workflow_id}, schedule {schedule_id}")
        except Exception as e:
            logger.error(
                f"Error updating task status for {app_id} in workflow {workflow_id}, schedule {schedule_id}: {str(e)}")

    async def propagate_data_to_next_tasks(self, workflow_id: str, schedule_id: str, task: Dict[str, Any],
                                           output_data: Dict[str, Any], session):
        for next_app_id in task.get('next', []):
            next_input_data_id = self._generate_data_id(workflow_id, schedule_id, next_app_id, 'input')
            try:
                try:
                    existing_doc = await self.data_db.get_document(next_input_data_id)
                    existing_input = existing_doc['data']
                except aiocouch.exceptions.DocumentNotFoundError:
                    existing_input = {}

                existing_input.update(output_data)
                document = {
                    '_id': next_input_data_id,
                    'data': existing_input,
                    'workflow_id': workflow_id,
                    'schedule_id': schedule_id
                }

                if existing_doc:
                    existing_doc.update(document)
                    await existing_doc.save()
                else:
                    await self.data_db.create_document(document)

            except Exception as e:
                logger.error(
                    f"Error propagating data to task {next_app_id} in workflow {workflow_id}, schedule {schedule_id}: {str(e)}")

    def _generate_data_id(self, workflow_id: str, schedule_id: str, app_id: str, data_type: str):
        return f"{workflow_id}_{schedule_id}_{app_id}_{data_type}"


async def main():
    executor = TaskExecutor()
    await executor.initialize()
    await executor.run()


if __name__ == "__main__":
    asyncio.run(main())
