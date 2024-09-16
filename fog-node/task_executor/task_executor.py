import asyncio
import aiohttp
import aiocouch
import json
import logging
import os
from typing import Dict, Any
import docker
import signal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COUCHDB_URL = f"http://{os.getenv('COUCHDB_USER')}:{os.getenv('COUCHDB_PASSWORD')}@{os.getenv('COUCHDB_HOST', 'couchdb-0:5984')}"
SCHEDULE_DB = 'schedules'
DATA_DB = 'task_data'

NODE_ID = os.getenv('NODE_ID')


class TaskExecutor:
    def __init__(self):
        self.couch = None
        self.schedule_db = None
        self.data_db = None
        self.docker_client = None
        self.session = None
        self._shutdown_event = asyncio.Event()

    async def initialize(self):
        try:
            self.couch = aiocouch.CouchDB(COUCHDB_URL)
            self.schedule_db = await self.couch.get_or_create_database(SCHEDULE_DB)
            self.data_db = await self.couch.get_or_create_database(DATA_DB)
            self.docker_client = docker.from_env()
            self.session = aiohttp.ClientSession()
            await self.setup_views()
        except Exception as e:
            logger.error(f"Initialization error: {str(e)}")
            await self.cleanup()
            raise

    async def cleanup(self):
        logger.info("Cleaning up resources...")
        if self.session and not self.session.closed:
            await self.session.close()
        if self.couch:
            await self.couch.close()
        if self.docker_client:
            self.docker_client.close()

    async def setup_views(self):
        design_doc = {
            '_id': '_design/tasks',
            'views': {
                'by_node': {
                    'map': f'''function(doc) {{
                        if (doc.schedule && doc.schedule.level_info) {{
                            for (var level in doc.schedule.level_info) {{
                                doc.schedule.level_info[level].forEach(function(task) {{
                                    if (task.node === "{NODE_ID}") {{
                                        emit([doc._id, doc.workflow_id, level, task.app_id], task);
                                    }}
                                }});
                            }}
                        }}
                    }}'''
                }
            }
        }
        try:
            await self.schedule_db.create_document(design_doc)
        except aiocouch.exceptions.DocumentConflictError:
            existing_doc = await self.schedule_db.get_document('_design/tasks')
            existing_doc.update(design_doc)
            await existing_doc.save()

    async def run(self):
        try:
            async for change in self.schedule_db.changes(feed='continuous', heartbeat=1000, include_docs=True):
                if self._shutdown_event.is_set():
                    break
                if change.get('id'):
                    await self.handle_change(change)
        except Exception as e:
            logger.error(f"Error in run method: {str(e)}")
        finally:
            await self.cleanup()

    async def handle_change(self, change):
        doc = change['doc']
        if doc.get('status') == 'ACTIVE' and 'schedule' in doc:
            workflow_id = doc['workflow_id']
            schedule_id = doc['_id']
            async for row in self.schedule_db.view('tasks/by_node', key=[schedule_id, workflow_id]):
                task = row.value
                if await self.is_task_ready(task, doc):
                    await self.execute_task(workflow_id, schedule_id, task)

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

    async def execute_task(self, workflow_id: str, schedule_id: str, task: Dict[str, Any]):
        app_id = task['app_id']
        input_data = await self.get_input_data(workflow_id, schedule_id, app_id)

        container_name = f"sawtooth-{app_id}"
        try:
            container = self.docker_client.containers.get(container_name)
            result = await asyncio.to_thread(container.exec_run, f"python /app/process.py '{json.dumps(input_data)}'")
            if result.exit_code != 0:
                raise Exception(
                    f"Container execution failed with exit code {result.exit_code}: {result.output.decode()}")
            output_data = json.loads(result.output.decode())

            await self.store_output_data(workflow_id, schedule_id, app_id, output_data)
            await self.update_task_status(workflow_id, schedule_id, app_id, 'COMPLETED')
            await self.propagate_data_to_next_tasks(workflow_id, schedule_id, task, output_data)
            logger.info(f"Task {app_id} completed successfully for workflow {workflow_id}, schedule {schedule_id}")
        except docker.errors.NotFound:
            logger.error(f"Container {container_name} not found")
            await self.update_task_status(workflow_id, schedule_id, app_id, 'FAILED')
        except Exception as e:
            logger.error(f"Error executing task {app_id} for workflow {workflow_id}, schedule {schedule_id}: {str(e)}")
            await self.update_task_status(workflow_id, schedule_id, app_id, 'FAILED')

    async def get_input_data(self, workflow_id: str, schedule_id: str, app_id: str) -> Dict[str, Any]:
        data_id = self._generate_data_id(workflow_id, schedule_id, app_id, 'input')
        try:
            doc = await self.data_db.get_document(data_id)
            return doc['data']
        except aiocouch.exceptions.DocumentNotFoundError:
            logger.warning(f"Input data not found for task {app_id} in workflow {workflow_id}, schedule {schedule_id}")
            return {}

    async def store_output_data(self, workflow_id: str, schedule_id: str, app_id: str, output_data: Dict[str, Any]):
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
            existing_doc = await self.data_db.get_document(data_id)
            existing_doc.update(document)
            await existing_doc.save()

    async def update_task_status(self, workflow_id: str, schedule_id: str, app_id: str, status: str):
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
                                           output_data: Dict[str, Any]):
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

    def signal_shutdown(self):
        self._shutdown_event.set()


async def main():
    executor = TaskExecutor()
    try:
        await executor.initialize()

        # Set up signal handlers
        loop = asyncio.get_running_loop()
        for signame in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(
                getattr(signal, signame),
                lambda: asyncio.create_task(shutdown(executor, loop))
            )

        await executor.run()
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        await executor.cleanup()


async def shutdown(executor, loop):
    logger.info("Shutdown signal received")
    executor.signal_shutdown()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


if __name__ == "__main__":
    asyncio.run(main())
