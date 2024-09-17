import asyncio
import logging
import os
import aiohttp
import couchdb
import docker
from docker import errors

# Logging setup remains the same
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# Environment variables remain the same
COUCHDB_URL = f"http://{os.getenv('COUCHDB_USER')}:{os.getenv('COUCHDB_PASSWORD')}@{os.getenv('COUCHDB_HOST', 'couch-db-0:5984')}"
COUCHDB_SCHEDULE_DB = 'schedules'
COUCHDB_DATA_DB = 'task_data'

# Get the current node's name from environment variable
CURRENT_NODE = os.getenv('NODE_NAME')


class TaskExecutor:
    def __init__(self):
        self.couch_server = None
        self.schedule_db = None
        self.data_db = None
        self.task_queue = asyncio.PriorityQueue()
        self.docker_client = docker.from_env()
        self.change_feed_task = None
        self.process_tasks_task = None
        logger.info("TaskExecutor initialized")

    async def initialize(self):
        logger.info("Initializing TaskExecutor")

        # Connect to CouchDB
        await self.connect_to_couchdb()

        # Start the change feed listener and task processor
        self.change_feed_task = asyncio.create_task(self.listen_for_changes())
        self.process_tasks_task = asyncio.create_task(self.process_tasks())

        # Wait for both tasks to start
        await asyncio.gather(self.change_feed_task, self.process_tasks_task)

        logger.info("TaskExecutor initialization complete")

    async def connect_to_couchdb(self):
        try:
            self.couch_server = await asyncio.to_thread(couchdb.Server, COUCHDB_URL)
            self.schedule_db = await asyncio.to_thread(self.couch_server.__getitem__, COUCHDB_SCHEDULE_DB)
            self.data_db = await asyncio.to_thread(self.couch_server.__getitem__, COUCHDB_DATA_DB)
            logger.info("Connected to CouchDB successfully")
        except Exception as e:
            logger.error(f"Failed to connect to CouchDB: {str(e)}")
            raise

    async def listen_for_changes(self):
        logger.info("Starting to listen for changes in the schedule database")
        while True:
            try:
                changes = await asyncio.to_thread(self.schedule_db.changes, feed='continuous', include_docs=True,
                                                  heartbeat=1000)
                async for change in AsyncIterator(changes):
                    if change.get('deleted', False):
                        continue
                    doc = change['doc']
                    if doc.get('status') == 'ACTIVE':
                        logger.info(f"New active schedule detected: {doc['_id']}")
                        await self.handle_new_schedule(doc)
            except asyncio.CancelledError:
                logger.info("Change feed listener cancelled")
                break
            except Exception as e:
                logger.error(f"Error in change feed listener: {str(e)}")
                await asyncio.sleep(5)  # Wait before trying to reconnect

    async def handle_new_schedule(self, schedule_doc):
        logger.info(f"Handling new schedule: {schedule_doc['_id']}")
        schedule = schedule_doc['schedule']
        node_schedule = schedule['node_schedule']

        if CURRENT_NODE in node_schedule:
            for app_id in node_schedule[CURRENT_NODE]:
                timestamp = schedule_doc['timestamp']
                workflow_id = schedule_doc['workflow_id']
                schedule_id = schedule_doc['_id']

                logger.info(f"Checking dependencies for app_id: {app_id}")
                if await self.check_dependencies(schedule, app_id):
                    logger.info(f"Dependencies met for app_id: {app_id}. Adding to task queue.")
                    await self.task_queue.put((timestamp, workflow_id, schedule_id, app_id))
                else:
                    logger.info(f"Dependencies not met for app_id: {app_id}. Task not added to queue.")

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
            doc = await asyncio.to_thread(self.data_db.get, f"{app_id}_output")
            logger.info(f"Task completed for app_id: {app_id}")
            return doc is not None
        except couchdb.http.ResourceNotFound:
            logger.info(f"Task not completed for app_id: {app_id}")
            return False

    async def process_tasks(self):
        logger.info("Starting task processing loop")
        while True:
            timestamp, workflow_id, schedule_id, app_id = await self.task_queue.get()
            logger.info(f"Processing task: workflow_id={workflow_id}, schedule_id={schedule_id}, app_id={app_id}")
            try:
                await self.execute_task(workflow_id, schedule_id, app_id)

                # Check if all final tasks are completed
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

    async def check_all_final_tasks_completed(self, schedule_id):
        logger.info(f"Checking if all final tasks are completed for schedule {schedule_id}")
        try:
            schedule_doc = await asyncio.to_thread(self.schedule_db.get, schedule_id)
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
        # Fetch input data
        input_key = f"{workflow_id}_{schedule_id}_{app_id}_input"
        try:
            input_doc = await asyncio.to_thread(self.data_db.get, input_key)
            input_data = input_doc['data']
        except Exception as e:
            logger.error(f"Error fetching input data for {input_key}: {str(e)}", exc_info=True)
            raise

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
            await asyncio.to_thread(self.data_db.save, {
                '_id': output_key,
                'data': output_data,
                'workflow_id': workflow_id,
                'schedule_id': schedule_id
            })
            logger.info(f"Task output stored: {output_key}")
        except Exception as e:
            logger.error(f"Error storing task output for {output_key}: {str(e)}", exc_info=True)
            raise

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
                async with session.post(f'http://{container_name}:{exposed_port}',
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
            doc = await asyncio.to_thread(self.schedule_db.get, schedule_id)
            doc['status'] = status
            await asyncio.to_thread(self.schedule_db.save, doc)
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
        logger.info("TaskExecutor cleanup complete")


class AsyncIterator:
    def __init__(self, sync_iterator):
        self.sync_iterator = sync_iterator

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await asyncio.to_thread(next, self.sync_iterator)
        except StopIteration:
            raise StopAsyncIteration


async def main():
    logger.info("Starting main application")
    executor = TaskExecutor()
    await executor.initialize()

    try:
        # Keep the main coroutine running until interrupted
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("Received cancellation signal. Shutting down...")
    finally:
        # Perform any necessary cleanup
        await executor.cleanup()

    logger.info("Main application shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error in main: {str(e)}", exc_info=True)
