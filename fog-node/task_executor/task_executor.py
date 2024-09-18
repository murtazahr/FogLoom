import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import aiohttp
import couchdb
import docker

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

COUCHDB_URL = f"http://{os.getenv('COUCHDB_USER')}:{os.getenv('COUCHDB_PASSWORD')}@{os.getenv('COUCHDB_HOST', 'couchdb-0:5984')}"
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

        # Start the change feed listener and task processor
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
                    if doc.get('status') == 'ACTIVE':
                        logger.info(f"New active schedule detected: {doc['_id']}")
                        await self.handle_new_schedule(doc)
                    elif doc.get('status') == 'TASK_COMPLETED':
                        logger.info(f"Task completion update detected for schedule: {doc['_id']}")
                        await self.handle_task_completion(doc)
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
                    logger.info(
                        f"Dependencies not met for app_id: {app_id}. Will be checked again on task completions.")

    async def handle_task_completion(self, schedule_doc):
        logger.info(f"Handling task completion for schedule: {schedule_doc['_id']}")
        schedule_id = schedule_doc['_id']
        completed_app_id = schedule_doc.get('completed_app_id')

        if not completed_app_id:
            logger.warning(f"No completed_app_id found in schedule document: {schedule_id}")
            return

        schedule = schedule_doc.get('schedule')
        if not schedule:
            logger.warning(f"Schedule document {schedule_id} does not contain a 'schedule' field")
            return

        node_schedule = schedule.get('node_schedule', {})
        if CURRENT_NODE in node_schedule:
            for app_id in node_schedule[CURRENT_NODE]:
                if await self.check_dependencies(schedule, app_id):
                    logger.info(f"Dependencies now met for app_id: {app_id}. Adding to task queue.")
                    timestamp = schedule_doc.get('timestamp')
                    workflow_id = schedule_doc.get('workflow_id')
                    await self.task_queue.put((timestamp, workflow_id, schedule_id, app_id))

    async def check_dependencies(self, schedule, app_id):
        logger.info(f"Checking dependencies for app_id: {app_id}")
        level_info = schedule.get('level_info', {})

        current_level = None
        for level, tasks in level_info.items():
            if any(task['app_id'] == app_id for task in tasks):
                current_level = int(level)
                break

        if current_level is None:
            logger.warning(f"App_id {app_id} not found in level_info")
            return False

        if current_level == 0:
            logger.info(f"App_id {app_id} is at level 0. No dependencies.")
            return True

        prev_level_tasks = level_info.get(str(current_level - 1), [])
        for task in prev_level_tasks:
            if 'next' in task and app_id in task['next']:
                if not await self.check_task_completed(task['app_id']):
                    logger.info(f"Dependency {task['app_id']} not completed for app_id {app_id}")
                    return False

        logger.info(f"All dependencies met for app_id {app_id}")
        return True

    async def check_task_completed(self, app_id):
        logger.info(f"Checking if task completed for app_id: {app_id}")
        try:
            output_key = f"{app_id}_output"
            doc = await self.loop.run_in_executor(self.thread_pool, self.data_db.get, output_key)
            return doc is not None
        except couchdb.http.ResourceNotFound:
            logger.info(f"Task not completed for app_id: {app_id}")
            return False

    async def update_schedule_status(self, schedule_id, status):
        logger.info(f"Updating schedule status: schedule_id={schedule_id}, status={status}")
        try:
            schedule_doc = await self.loop.run_in_executor(self.thread_pool, self.schedule_db.get, schedule_id)
            schedule_doc['status'] = status
            await self.loop.run_in_executor(self.thread_pool, self.schedule_db.save, schedule_doc)
            logger.info(f"Schedule status updated: schedule_id={schedule_id}, status={status}")
        except Exception as e:
            logger.error(f"Error updating schedule status for {schedule_id}: {str(e)}", exc_info=True)
            raise

    async def process_tasks(self):
        logger.info("Starting task processing loop")
        while True:
            try:
                timestamp, workflow_id, schedule_id, app_id = await self.task_queue.get()
                logger.info(f"Processing task: workflow_id={workflow_id}, schedule_id={schedule_id}, app_id={app_id}")
                try:
                    await self.execute_task(workflow_id, schedule_id, app_id)
                    # Check if this was the final task in the schedule
                    if await self.is_final_task(schedule_id, app_id):
                        await self.update_schedule_status(schedule_id, "FINALIZED")
                    else:
                        await self.update_schedule_status(schedule_id, "IN_PROGRESS")
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

    async def is_final_task(self, schedule_id, app_id):
        try:
            schedule_doc = await self.loop.run_in_executor(self.thread_pool, self.schedule_db.get, schedule_id)
            schedule = schedule_doc.get('schedule', {})
            level_info = schedule.get('level_info', {})

            # Find the highest level
            max_level = max(map(int, level_info.keys()))

            # Check if the app_id is in the highest level and is the last task
            highest_level_tasks = level_info[str(max_level)]
            is_final = app_id == highest_level_tasks[-1]['app_id']

            logger.info(f"Checked if task {app_id} is final for schedule {schedule_id}: {is_final}")
            return is_final
        except Exception as e:
            logger.error(f"Error checking if task is final: {str(e)}", exc_info=True)
            return False

    async def execute_task(self, workflow_id, schedule_id, app_id):
        logger.info(f"Executing task: workflow_id={workflow_id}, schedule_id={schedule_id}, app_id={app_id}")
        input_key = f"{workflow_id}_{schedule_id}_{app_id}_input"
        try:
            input_doc = await self.loop.run_in_executor(self.thread_pool, self.data_db.get, input_key)
            input_data = input_doc['data']
        except Exception as e:
            logger.error(f"Error fetching input data for {input_key}: {str(e)}", exc_info=True)
            raise

        try:
            output_data = await self.run_docker_task(app_id, input_data)
        except Exception as e:
            logger.error(f"Error running Docker task for {app_id}: {str(e)}", exc_info=True)
            raise

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

        await self.update_schedule_on_task_completion(schedule_id, app_id)

    async def run_docker_task(self, app_id, input_data):
        container_name = f"sawtooth-{app_id}"
        try:
            container = self.docker_client.containers.get(container_name)
            container_info = container.attrs

            exposed_port = None
            for port, host_config in container_info['NetworkSettings']['Ports'].items():
                if host_config:
                    exposed_port = host_config[0]['HostPort']
                    break

            if not exposed_port:
                raise Exception(f"No exposed port found for container {container_name}")

            logger.info(f"Found exposed port {exposed_port} for container {container_name}")

            async with aiohttp.ClientSession() as session:
                async with session.post(f'http://localhost:{exposed_port}',
                                        json={'data': input_data}) as response:
                    result = await response.json()
                    logger.info(f"Received response from container {container_name}")
                    return result
        except Exception as e:
            logger.error(f"Error in run_docker_task for {container_name}: {str(e)}", exc_info=True)
            raise

    async def update_schedule_on_task_completion(self, schedule_id, completed_app_id):
        try:
            schedule_doc = await self.loop.run_in_executor(self.thread_pool, self.schedule_db.get, schedule_id)
            schedule_doc['status'] = 'TASK_COMPLETED'
            schedule_doc['completed_app_id'] = completed_app_id
            await self.loop.run_in_executor(self.thread_pool, self.schedule_db.save, schedule_doc)
            logger.info(f"Updated schedule {schedule_id} with completed task {completed_app_id}")
        except Exception as e:
            logger.error(f"Error updating schedule on task completion: {str(e)}", exc_info=True)

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
