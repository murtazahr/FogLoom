import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import aiohttp
import couchdb
import docker
from docker import errors

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

COUCHDB_URL = f"http://{os.getenv('COUCHDB_USER')}:{os.getenv('COUCHDB_PASSWORD')}@{os.getenv('COUCHDB_HOST', 'couch-db-0:5984')}"
COUCHDB_DB = 'resource_registry'
COUCHDB_SCHEDULE_DB = 'schedules'
COUCHDB_DATA_DB = 'task_data'
CURRENT_NODE = os.getenv('NODE_ID')


class TaskExecutor:
    def __init__(self):
        self.couch_server = None
        self.schedule_db = None
        self.data_db = None
        self.task_queue = asyncio.PriorityQueue()
        self.docker_client = docker.from_env()
        self.thread_pool = ThreadPoolExecutor()
        self.loop = asyncio.get_event_loop()
        self.change_feed_task = None
        self.process_tasks_task = None
        logger.info("TaskExecutor initialized")

    async def initialize(self):
        logger.info("Initializing TaskExecutor")
        await self.connect_to_couchdb()

        # Start the change feed listener and task processor as background tasks
        self.change_feed_task = self.loop.create_task(self.listen_for_changes())
        self.process_tasks_task = self.loop.create_task(self.process_tasks())

        logger.info("TaskExecutor initialization complete")

    async def connect_to_couchdb(self):
        try:
            self.couch_server = await self.loop.run_in_executor(self.thread_pool, couchdb.Server, COUCHDB_URL)
            self.schedule_db = await self.loop.run_in_executor(self.thread_pool, self.couch_server.__getitem__,
                                                               COUCHDB_SCHEDULE_DB)
            self.data_db = await self.loop.run_in_executor(self.thread_pool, self.couch_server.__getitem__,
                                                           COUCHDB_DATA_DB)
            logger.info("Connected to CouchDB successfully")
        except Exception as e:
            logger.error(f"Failed to connect to CouchDB: {str(e)}")
            raise

    async def listen_for_changes(self):
        logger.info("Starting to listen for changes in the schedule database")
        while True:
            try:
                changes_func = partial(self.schedule_db.changes, feed='continuous', include_docs=True, heartbeat=1000)
                changes = await self.loop.run_in_executor(self.thread_pool, changes_func)
                logger.debug("Initiated changes feed")
                async for change in AsyncIterator(changes):
                    logger.debug(f"Received change: {change}")
                    if not isinstance(change, dict) or 'doc' not in change:
                        logger.warning(f"Unexpected change data structure: {change}")
                        continue
                    doc = change['doc']
                    logger.debug(f"Processing document: {doc}")
                    if doc.get('status') == 'ACTIVE':
                        logger.info(f"New active schedule detected: {doc['_id']}")
                        await self.handle_new_schedule(doc)
                    else:
                        logger.debug(f"Skipping non-active document: {doc['_id']}, status: {doc.get('status')}")
            except asyncio.CancelledError:
                logger.info("Change feed listener cancelled")
                break
            except Exception as e:
                logger.error(f"Error in change feed listener: {str(e)}", exc_info=True)
                await asyncio.sleep(5)  # Wait before trying to reconnect
            logger.info("Restarting change feed listener after error or completion")

    async def handle_new_schedule(self, schedule_doc):
        logger.info(f"Handling new schedule: {schedule_doc['_id']}")
        schedule = schedule_doc.get('schedule')
        if not schedule:
            logger.warning(f"Schedule document {schedule_doc['_id']} does not contain a 'schedule' field")
            return
        node_schedule = schedule.get('node_schedule', {})

        logger.debug(f"Node schedule: {node_schedule}")
        logger.debug(f"Current node: {CURRENT_NODE}")

        if CURRENT_NODE in node_schedule:
            for app_id in node_schedule[CURRENT_NODE]:
                timestamp = schedule_doc.get('timestamp')
                workflow_id = schedule_doc.get('workflow_id')
                schedule_id = schedule_doc['_id']

                logger.info(f"Checking dependencies for app_id: {app_id}")
                if await self.check_dependencies(schedule, app_id):
                    logger.info(f"Dependencies met for app_id: {app_id}. Adding to task queue.")
                    await self.task_queue.put((timestamp, workflow_id, schedule_id, app_id))
                else:
                    logger.info(f"Dependencies not met for app_id: {app_id}. Task not added to queue.")
        else:
            logger.info(f"No tasks for current node {CURRENT_NODE} in this schedule")

    async def check_dependencies(self, schedule, app_id):
        logger.info(f"Checking dependencies for app_id: {app_id}")
        level_info = schedule['level_info']
        for level, tasks in level_info.items():
            for task in tasks:
                if task['app_id'] == app_id:
                    if level == '0':
                        logger.info(f"App_id {app_id} is at level 0. No dependencies.")
                        return True
                    else:
                        for prev_level_tasks in level_info[str(int(level) - 1)]:
                            prev_app_id = prev_level_tasks['app_id']
                            if not await self.check_task_completed(prev_app_id):
                                logger.info(f"Dependency {prev_app_id} not completed for app_id {app_id}")
                                return False
                        logger.info(f"All dependencies met for app_id {app_id}")
                        return True
        logger.warning(f"App_id {app_id} not found in level_info")
        return False

    async def check_task_completed(self, app_id):
        logger.info(f"Checking if task completed for app_id: {app_id}")
        try:
            doc = await self.loop.run_in_executor(self.thread_pool, self.data_db.get, f"{app_id}_output")
            logger.info(f"Task completed for app_id: {app_id}")
            return doc is not None
        except couchdb.http.ResourceNotFound:
            logger.info(f"Task not completed for app_id: {app_id}")
            return False

    async def check_all_final_tasks_completed(self, schedule_id):
        logger.info(f"Checking if all final tasks are completed for schedule {schedule_id}")
        try:
            schedule_doc = await self.loop.run_in_executor(self.thread_pool, self.schedule_db.get, schedule_id)
            schedule = schedule_doc['schedule']
            level_info = schedule['level_info']

            # Find the highest level
            max_level = max(map(int, level_info.keys()))

            # Get all tasks in the highest level
            highest_level_tasks = level_info[str(max_level)]

            # Check if all tasks in the highest level are completed
            for task in highest_level_tasks:
                if not await self.check_task_completed(task['app_id']):
                    logger.info(f"Final task {task['app_id']} not yet completed for schedule {schedule_id}")
                    return False

            logger.info(f"All final tasks completed for schedule {schedule_id}")
            return True
        except Exception as e:
            logger.error(f"Error checking if all final tasks are completed: {str(e)}", exc_info=True)
            return False

    async def execute_task(self, workflow_id, schedule_id, app_id):
        logger.info(f"Executing task: workflow_id={workflow_id}, schedule_id={schedule_id}, app_id={app_id}")

        # Fetch the schedule document to get dependency information
        schedule_doc = await self.loop.run_in_executor(self.thread_pool, self.schedule_db.get, schedule_id)
        if not schedule_doc:
            raise Exception(f"Schedule document not found for schedule_id: {schedule_id}")

        schedule = schedule_doc.get('schedule', {})
        level_info = schedule.get('level_info', {})

        # Determine the current task's level and its dependencies
        current_level = None
        dependencies = []
        for level, tasks in level_info.items():
            for task in tasks:
                if task['app_id'] == app_id:
                    current_level = int(level)
                    break
            if current_level is not None:
                break

        if current_level is None:
            raise Exception(f"Task {app_id} not found in schedule level_info")

        if current_level > 0:
            prev_level_tasks = level_info.get(str(current_level - 1), [])
            for task in prev_level_tasks:
                if 'next' in task and app_id in task['next']:
                    dependencies.append(task['app_id'])

        logger.info(f"Dependencies for task {app_id}: {dependencies}")

        # Fetch input data
        input_data = []
        for dep_app_id in dependencies:
            dep_key = f"{workflow_id}_{schedule_id}_{dep_app_id}_output"
            input_doc = await self.fetch_data_with_retry(dep_key)
            if input_doc and 'data' in input_doc:
                input_data.extend(input_doc['data'])

        if not dependencies:
            # If there are no dependencies, fetch the original input
            input_key = f"{workflow_id}_{schedule_id}_{app_id}_input"
            input_doc = await self.fetch_data_with_retry(input_key)
            if input_doc and 'data' in input_doc:
                input_data = input_doc['data']

        if not input_data:
            raise Exception(f"No input data found for task: {app_id}")

        # Execute the task using the Docker container
        container_name = f"sawtooth-{app_id}"
        try:
            output_data = await self.run_docker_task(container_name, input_data)
        except Exception as e:
            logger.error(f"Error running Docker task for {container_name}: {str(e)}", exc_info=True)
            raise

        # Store the output
        output_key = f"{workflow_id}_{schedule_id}_{app_id}_output"
        try:
            await self.loop.run_in_executor(self.thread_pool, self.data_db.save, {
                '_id': output_key,
                'data': output_data,
                'workflow_id': workflow_id,
                'schedule_id': schedule_id
            })
            logger.info(f"Task output stored: {output_key}")
        except Exception as e:
            logger.error(f"Error storing task output for {output_key}: {str(e)}", exc_info=True)
            raise

    async def fetch_data_with_retry(self, key):
        max_retries = 3
        retry_delay = 1  # seconds

        for attempt in range(max_retries):
            try:
                doc = await self.loop.run_in_executor(self.thread_pool, self.data_db.get, key)
                logger.debug(f"Retrieved document for key {key}: {doc}")
                return doc
            except couchdb.http.ResourceNotFound:
                logger.warning(f"Document not found for key: {key}")
            except Exception as e:
                logger.error(f"Error fetching data for {key}: {str(e)}", exc_info=True)

            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(retry_delay)

        logger.error(f"Failed to retrieve document after {max_retries} attempts for key: {key}")
        return None

    async def process_tasks(self):
        logger.info("Starting task processing loop")
        while True:
            try:
                timestamp, workflow_id, schedule_id, app_id = await self.task_queue.get()
                logger.info(f"Processing task: workflow_id={workflow_id}, schedule_id={schedule_id}, app_id={app_id}")
                try:
                    await self.execute_task(workflow_id, schedule_id, app_id)

                    if await self.check_all_final_tasks_completed(schedule_id):
                        logger.info(f"All final tasks completed for schedule: {schedule_id}")
                        await self.update_schedule_status(schedule_id, "FINALIZED")
                    else:
                        logger.info(f"Not all final tasks completed yet for schedule: {schedule_id}")
                except Exception as e:
                    logger.error(f"Error executing task {app_id}: {str(e)}", exc_info=True)
                    await self.update_schedule_status(schedule_id, "FAILED")
                finally:
                    self.task_queue.task_done()
            except asyncio.CancelledError:
                logger.info("Task processing loop cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error in task processing loop: {str(e)}", exc_info=True)
                await asyncio.sleep(5)  # Wait before continuing the loop

    async def run_docker_task(self, container_name, input_data):
        logger.info(f"Running Docker task for container: {container_name}")
        try:
            container = self.docker_client.containers.get(container_name)
            container_info = container.attrs

            # Find the exposed port
            exposed_port = None
            for port, host_config in container_info['NetworkSettings']['Ports'].items():
                if host_config:
                    exposed_port = host_config[0]['HostPort']
                    break

            if not exposed_port:
                logger.error(f"No exposed port found for container {container_name}")
                raise Exception(f"No exposed port found for container {container_name}")

            logger.info(f"Found exposed port {exposed_port} for container {container_name}")

            # Use the exposed port to communicate with the container
            async with aiohttp.ClientSession() as session:
                async with session.post(f'http://localhost:{exposed_port}',
                                        json={'data': input_data}) as response:
                    result = await response.json()
                    logger.info(f"Received response from container {container_name}")
                    return result

        except docker.errors.NotFound:
            logger.error(f"Container {container_name} not found")
            raise Exception(f"Container {container_name} not found")
        except docker.errors.APIError as e:
            logger.error(f"Docker API error for container {container_name}: {str(e)}")
            raise Exception(f"Docker API error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error running Docker task for {container_name}: {str(e)}", exc_info=True)
            raise

    async def update_schedule_status(self, schedule_id, status):
        logger.info(f"Updating schedule status: schedule_id={schedule_id}, status={status}")
        try:
            doc = await self.loop.run_in_executor(self.thread_pool, self.schedule_db.get, schedule_id)
            doc['status'] = status
            await self.loop.run_in_executor(self.thread_pool, self.schedule_db.save, doc)
            logger.info(f"Schedule status updated: schedule_id={schedule_id}, status={status}")
        except Exception as e:
            logger.error(f"Error updating schedule status for {schedule_id}: {str(e)}", exc_info=True)
            raise

    async def cleanup(self):
        logger.info("Cleaning up TaskExecutor")
        if self.change_feed_task:
            self.change_feed_task.cancel()
            try:
                await self.change_feed_task
            except asyncio.CancelledError:
                pass
        if self.process_tasks_task:
            self.process_tasks_task.cancel()
            try:
                await self.process_tasks_task
            except asyncio.CancelledError:
                pass
        if self.docker_client:
            self.docker_client.close()
        self.thread_pool.shutdown()
        logger.info("TaskExecutor cleanup complete")


class AsyncIterator:
    def __init__(self, sync_iterator):
        self.sync_iterator = sync_iterator

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            next_func = partial(next, self.sync_iterator)
            item = await asyncio.get_event_loop().run_in_executor(None, next_func)
            logger.debug(f"AsyncIterator yielded: {item}")
            return item
        except StopIteration:
            logger.debug("AsyncIterator completed")
            raise StopAsyncIteration


async def main():
    logger.info("Starting main application")
    executor = TaskExecutor()
    await executor.initialize()

    logger.info(f"TaskExecutor initialized and running. Waiting for changes on node: {CURRENT_NODE}")

    try:
        while True:
            await asyncio.sleep(10)
            logger.debug("Main loop still running...")
    except asyncio.CancelledError:
        logger.info("Received cancellation signal. Shutting down...")
    finally:
        await executor.cleanup()

    logger.info("Main application shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error in main: {str(e)}", exc_info=True)
