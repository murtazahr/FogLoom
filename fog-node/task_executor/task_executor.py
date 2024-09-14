import couchdb
import docker
from docker import errors
import json
import logging
import os
from typing import Dict, Any
import asyncio
import aiohttp
from couchdb.design import ViewDefinition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COUCHDB_URL = f"http://{os.getenv('COUCHDB_USER')}:{os.getenv('COUCHDB_PASSWORD')}@{os.getenv('COUCHDB_HOST', 'couchdb-0:5984')}"
SCHEDULE_DB = 'schedules'
DATA_DB = 'task_data'

NODE_ID = os.getenv('HOSTNAME')


class TaskExecutor:
    def __init__(self):
        self.couch = couchdb.Server(COUCHDB_URL)
        self.schedule_db = self.couch[SCHEDULE_DB]
        self.data_db = self.couch[DATA_DB]
        self.docker_client = docker.from_env()
        self.setup_views()
        self.port_mapping = {}  # To store app_id to port mappings

    def setup_views(self):
        # View to get tasks for the current node
        node_tasks_view = ViewDefinition('tasks', 'by_node',
                                         '''function(doc) {
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
                                         )
        node_tasks_view.sync(self.schedule_db)

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
            tasks = self.schedule_db.view('tasks/by_node', key=[schedule_id, workflow_id])
            for task in tasks:
                if await self.is_task_ready(task.value, doc):
                    await self.execute_task(workflow_id, schedule_id, task.value, session)

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
            port = self.get_container_port(container, app_id)

            output_data = await self.send_data_to_container('localhost', port, input_data)

            await self.store_output_data(workflow_id, schedule_id, app_id, output_data, session)
            await self.update_task_status(workflow_id, schedule_id, app_id, 'COMPLETED', session)
            await self.propagate_data_to_next_tasks(workflow_id, schedule_id, task, output_data, session)
            logger.info(f"Task {app_id} completed successfully for workflow {workflow_id}, schedule {schedule_id}")
        except docker.errors.NotFound:
            logger.error(f"Container {container_name} not found")
        except Exception as e:
            logger.error(f"Error executing task {app_id} for workflow {workflow_id}, schedule {schedule_id}: {str(e)}")
            await self.update_task_status(workflow_id, schedule_id, app_id, 'FAILED', session)

    def get_container_port(self, container, app_id):
        if app_id not in self.port_mapping:
            # Find the mapped port for 12345
            port_info = container.attrs['NetworkSettings']['Ports']['12345/tcp'][0]
            self.port_mapping[app_id] = int(port_info['HostPort'])
        return self.port_mapping[app_id]

    async def send_data_to_container(self, host: str, port: int, input_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            reader, writer = await asyncio.open_connection(host, port)

            writer.write(json.dumps(input_data).encode())
            await writer.drain()

            data = await reader.read(4096)
            writer.close()
            await writer.wait_closed()

            return json.loads(data.decode())
        except Exception as e:
            logger.error(f"Error communicating with container on port {port}: {str(e)}")
            raise

    async def get_input_data(self, workflow_id: str, schedule_id: str, app_id: str, session) -> Dict[str, Any]:
        data_id = self._generate_data_id(workflow_id, schedule_id, app_id, 'input')
        try:
            async with session.get(f"{COUCHDB_URL}/{DATA_DB}/{data_id}") as resp:
                if resp.status == 200:
                    return (await resp.json())['data']
                else:
                    logger.warning(
                        f"Input data not found for task {app_id} in workflow {workflow_id}, schedule {schedule_id}")
                    return {}
        except Exception as e:
            logger.error(
                f"Error fetching input data for task {app_id} in workflow {workflow_id}, schedule {schedule_id}: {str(e)}")
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
        async with session.put(f"{COUCHDB_URL}/{DATA_DB}/{data_id}", json=document) as resp:
            if resp.status not in (201, 202):
                logger.error(
                    f"Failed to store output data for task {app_id} in workflow {workflow_id}, schedule {schedule_id}")

    async def update_task_status(self, workflow_id: str, schedule_id: str, app_id: str, status: str, session):
        async with session.get(f"{COUCHDB_URL}/{SCHEDULE_DB}/{schedule_id}") as resp:
            if resp.status == 200:
                schedule = await resp.json()
                if 'schedule' in schedule and 'level_info' in schedule['schedule']:
                    for level_info in schedule['schedule']['level_info'].values():
                        for task in level_info:
                            if task['app_id'] == app_id:
                                task['status'] = status
                                break
                    async with session.put(f"{COUCHDB_URL}/{SCHEDULE_DB}/{schedule_id}", json=schedule) as put_resp:
                        if put_resp.status not in (201, 202):
                            logger.error(
                                f"Failed to update task status for {app_id} in workflow {workflow_id}, schedule {schedule_id}")
                else:
                    logger.error(f"Invalid schedule structure for workflow {workflow_id}, schedule {schedule_id}")
            else:
                logger.error(f"Failed to fetch schedule {schedule_id} for workflow {workflow_id}")

    async def propagate_data_to_next_tasks(self, workflow_id: str, schedule_id: str, task: Dict[str, Any],
                                           output_data: Dict[str, Any], session):
        for next_app_id in task.get('next', []):
            next_input_data_id = self._generate_data_id(workflow_id, schedule_id, next_app_id, 'input')
            try:
                async with session.get(f"{COUCHDB_URL}/{DATA_DB}/{next_input_data_id}") as resp:
                    if resp.status == 200:
                        existing_input = (await resp.json())['data']
                    else:
                        existing_input = {}
                existing_input.update(output_data)
                document = {
                    '_id': next_input_data_id,
                    'data': existing_input,
                    'workflow_id': workflow_id,
                    'schedule_id': schedule_id
                }
                async with session.put(f"{COUCHDB_URL}/{DATA_DB}/{next_input_data_id}", json=document) as put_resp:
                    if put_resp.status not in (201, 202):
                        logger.error(
                            f"Failed to propagate data to task {next_app_id} in workflow {workflow_id}, schedule {schedule_id}")
            except Exception as e:
                logger.error(
                    f"Error propagating data to task {next_app_id} in workflow {workflow_id}, schedule {schedule_id}: {str(e)}")

    def _generate_data_id(self, workflow_id: str, schedule_id: str, app_id: str, data_type: str):
        return f"{workflow_id}_{schedule_id}_{app_id}_{data_type}"


if __name__ == "__main__":
    executor = TaskExecutor()
    asyncio.run(executor.run())
