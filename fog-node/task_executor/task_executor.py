import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

import couchdb
import docker
from redis import RedisCluster, RedisError
from docker import errors

from helper.blockchain_task_status_updater import status_update_transactor
from helper.response_manager import ResponseManager

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

COUCHDB_URL = f"http://{os.getenv('COUCHDB_USER')}:{os.getenv('COUCHDB_PASSWORD')}@{os.getenv('COUCHDB_HOST', 'couchdb-0:5984')}"
COUCHDB_DB = 'resource_registry'
COUCHDB_SCHEDULE_DB = 'schedules'
COUCHDB_DATA_DB = 'task_data'
CURRENT_NODE = os.getenv('NODE_ID')

REDIS_URL = os.getenv('REDIS_URL', 'redis://redis-cluster:6379')


class TaskExecutor:
    def __init__(self):
        self.last_completed_app_id = None
        self.couch_server = None
        self.schedule_db = None
        self.data_db = None
        self.task_queue = asyncio.PriorityQueue()
        self.docker_client = docker.from_env()
        self.thread_pool = ThreadPoolExecutor(max_workers=5)
        self.loop = asyncio.get_event_loop()
        self.redis_subscriber = None
        self.redis_pubsub = None
        self.redis_client = None
        self.process_tasks_task = None
        self.task_status = {}
        logger.info("TaskExecutor initialized")

    async def initialize(self):
        logger.info("Initializing TaskExecutor")
        await self.connect_to_couchdb()
        await self.connect_to_redis()

        # Start the Redis subscriber and task processor
        self.redis_subscriber = self.loop.create_task(self.subscribe_to_redis())
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

    async def connect_to_redis(self):
        try:
            self.redis_client = RedisCluster.from_url(REDIS_URL, decode_responses=True)
            self.redis_pubsub = self.redis_client.pubsub()
            logger.info("Connected to Redis successfully")
        except RedisError as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise

    async def subscribe_to_redis(self):
        logger.info("Starting to subscribe to Redis channel")
        try:
            await self.loop.run_in_executor(self.thread_pool, self.redis_pubsub.subscribe, 'schedule')
            logger.info("Subscribed to Redis 'schedule' channel successfully")

            while True:
                message = await self.loop.run_in_executor(self.thread_pool, self.redis_pubsub.get_message)
                if message and message['type'] == 'message':
                    data = json.loads(message['data'])
                    await self.handle_redis_message(data)
        except asyncio.CancelledError:
            logger.info("Redis subscriber cancelled")
        except Exception as e:
            logger.error(f"Error in Redis subscriber: {str(e)}", exc_info=True)

    async def handle_redis_message(self, data):
        schedule_id = data.get('schedule_id')
        current_status = data.get('status')
        logger.info(f"Handling Redis message for schedule: {schedule_id}, status: {current_status}")

        if current_status == 'ACTIVE':
            logger.info(f"Active schedule detected: {schedule_id}")
            await self.handle_new_schedule(data)
        elif current_status == 'TASK_COMPLETED':
            logger.info(f"Task completion update detected for schedule: {schedule_id}")
            await self.handle_task_completion(data)
        else:
            logger.info(f"Unhandled status {current_status} for schedule: {schedule_id}")

    async def handle_new_schedule(self, schedule_data):
        schedule_id = schedule_data.get('schedule_id')
        logger.info(f"Handling new schedule: {schedule_id}")
        schedule = schedule_data.get('schedule')
        if not schedule:
            logger.warning(f"Schedule document {schedule_id} does not contain a 'schedule' field")
            return
        node_schedule = schedule.get('node_schedule', {})

        workflow_id = schedule_data.get('workflow_id')

        if CURRENT_NODE in node_schedule:
            for app_id in node_schedule[CURRENT_NODE]:
                timestamp = schedule_data.get('timestamp')
                source_url = schedule_data.get('source_url')
                source_public_key = schedule_data.get('source_public_key')
                task_key = (schedule_id, app_id)

                logger.info(f"Checking dependencies for app_id: {app_id} in schedule {schedule_id}")
                if await self.check_dependencies(schedule_id, app_id, schedule):
                    logger.info(
                        f"Dependencies met for app_id: {app_id} in schedule {schedule_id}. Adding to task queue.")
                    await self.task_queue.put((source_url, source_public_key, timestamp, workflow_id, schedule_id, app_id))
                    self.task_status[task_key] = 'QUEUED'
                else:
                    logger.info(
                        f"Dependencies not met for app_id: {app_id} in schedule {schedule_id}. Will be checked again "
                        f"on task completions.")
                    self.task_status[task_key] = 'WAITING'
        else:
            logger.info(f"No tasks for current node {CURRENT_NODE} in this schedule")

    async def handle_task_completion(self, schedule_data):
        schedule_id = schedule_data.get('schedule_id')
        completed_app_id = schedule_data.get('completed_app_id')
        logger.info(f"Handling task completion for schedule: {schedule_id}, completed app: {completed_app_id}")

        if not completed_app_id:
            logger.warning(f"No completed_app_id found in schedule document: {schedule_id}")
            return

        # Mark the task as completed
        task_key = (schedule_id, completed_app_id)
        self.task_status[task_key] = 'COMPLETED'

        schedule = schedule_data.get('schedule')
        if not schedule:
            logger.warning(f"Schedule document {schedule_id} does not contain a 'schedule' field")
            return

        workflow_id = schedule_data.get('workflow_id')
        node_schedule = schedule.get('node_schedule', {})

        if CURRENT_NODE in node_schedule:
            for app_id in node_schedule[CURRENT_NODE]:
                task_key = (schedule_id, app_id)
                current_status = self.task_status.get(task_key)

                # Skip if this task has already been completed or is in progress
                if current_status in ['COMPLETED', 'IN_PROGRESS', 'QUEUED']:
                    logger.debug(f"Skipping task: {app_id} for schedule {schedule_id}. Status: {current_status}")
                    continue

                if await self.check_dependencies(schedule_id, app_id, schedule):
                    logger.info(
                        f"Dependencies now met for app_id: {app_id} in schedule {schedule_id}. Adding to task queue.")
                    timestamp = schedule_data.get('timestamp')
                    source_url = schedule_data.get('source_url')
                    source_public_key = schedule_data.get('source_public_key')
                    await self.task_queue.put((source_url, source_public_key, timestamp, workflow_id, schedule_id, app_id))
                    self.task_status[task_key] = 'QUEUED'
                else:
                    logger.debug(f"Dependencies not yet met for app_id: {app_id} in schedule {schedule_id}")
                    self.task_status[task_key] = 'WAITING'

    async def check_dependencies(self, schedule_id, app_id, schedule):
        current_level, dependencies = await self.get_task_dependencies(schedule, app_id)

        if current_level is None:
            return False

        if current_level == 0:
            return True

        for task in dependencies:
            dep_app_id = task['app_id']
            dep_key = (schedule_id, dep_app_id)
            if self.task_status.get(dep_key) != 'COMPLETED':
                logger.info(f"Dependency {dep_app_id} not completed for app_id {app_id} in schedule {schedule_id}")
                return False

        logger.info(f"All dependencies met for app_id {app_id} in schedule {schedule_id}")
        return True

    async def process_tasks(self):
        logger.info("Starting task processing loop")
        while True:
            try:
                source_url, source_public_key, timestamp, workflow_id, schedule_id, app_id = await self.task_queue.get()
                logger.info(f"Processing task: workflow_id={workflow_id}, schedule_id={schedule_id}, app_id={app_id}")

                response_manager = ResponseManager(source_url, source_public_key)

                try:
                    connect_response_manager = self.loop.run_in_executor(self.thread_pool, response_manager.connect)
                    execute_task = self.execute_task(workflow_id, schedule_id, app_id)

                    result = (await asyncio.gather(connect_response_manager, execute_task))[1]

                    # Check if this was the final task in the schedule
                    if await self.is_final_task(schedule_id, app_id):
                        update_redis_status = self.update_schedule_status_redis(schedule_id, "FINALIZED")
                        update_blockchain_status = self.loop.run_in_executor(
                            self.thread_pool,
                            status_update_transactor.create_and_send_transaction,
                            workflow_id,
                            schedule_id,
                            "FINALIZED")
                        send_response_to_client = self.loop.run_in_executor(
                            self.thread_pool,
                            response_manager.send_message,
                            json.dumps(result))

                        await asyncio.gather(update_redis_status, update_blockchain_status, send_response_to_client)

                except Exception as e:
                    logger.error(f"Error executing task {app_id}: {str(e)}", exc_info=True)
                    self.task_status[(schedule_id, app_id)] = 'FAILED'
                    update_redis_status = self.update_schedule_status_redis(schedule_id, "FAILED")
                    update_blockchain_status = self.loop.run_in_executor(
                        self.thread_pool,
                        status_update_transactor.create_and_send_transaction,
                        workflow_id,
                        schedule_id,
                        "FAILED")
                    disconnect_response_manager = self.loop.run_in_executor(self.thread_pool, response_manager.disconnect)

                    await asyncio.gather(update_redis_status, update_blockchain_status, disconnect_response_manager)
                finally:
                    self.task_queue.task_done()
            except asyncio.CancelledError:
                logger.info("Task processing loop cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error in task processing loop: {str(e)}", exc_info=True)
                await asyncio.sleep(5)

    async def execute_task(self, workflow_id, schedule_id, app_id):
        logger.info(f"Executing task: workflow_id={workflow_id}, schedule_id={schedule_id}, app_id={app_id}")
        task_key = (schedule_id, app_id)
        self.task_status[task_key] = 'IN_PROGRESS'

        # Fetch schedule document from Redis
        key = f"schedule_{schedule_id}"
        schedule_data = await self.loop.run_in_executor(self.thread_pool, self.redis_client.get, key)

        if not schedule_data:
            raise Exception(f"Schedule {schedule_id} not found in Redis")

        schedule_doc = json.loads(schedule_data)
        schedule = schedule_doc.get('schedule', {})

        current_level, _ = await self.get_task_dependencies(schedule, app_id)

        if current_level is None:
            raise Exception(f"Task {app_id} not found in schedule level_info")

        if current_level == 0:
            input_key = f"{workflow_id}_{schedule_id}_{app_id}_input"
            try:
                input_doc = await self.fetch_data_with_retry(self.data_db, input_key)
                if input_doc is None:
                    raise Exception(f"Input document not found for key: {input_key}")
                if 'data' not in input_doc:
                    raise Exception(f"'data' field not found in input document for key: {input_key}")
                input_data = input_doc['data']
                logger.debug(f"Input data fetched for task {app_id}: {input_data[:100]}...")  # Log first 100 characters
            except Exception as e:
                logger.error(f"Error fetching input data for {input_key}: {str(e)}", exc_info=True)
                raise
        else:
            try:
                input_data = await self.fetch_dependency_outputs(workflow_id, schedule_id, app_id, schedule)
            except Exception as e:
                logger.error(f"Error fetching dependency outputs for task {app_id}: {str(e)}", exc_info=True)
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
                'data': output_data['data'],
                'workflow_id': workflow_id,
                'schedule_id': schedule_id
            })
            logger.info(f"Task output stored: {output_key}")
            self.task_status[task_key] = 'COMPLETED'
        except Exception as e:
            logger.error(f"Error storing task output for {output_key}: {str(e)}", exc_info=True)
            raise

        # Update schedule status in Redis
        self.last_completed_app_id = app_id
        await self.update_schedule_status_redis(schedule_id, "TASK_COMPLETED")

        return output_data

    async def update_schedule_status_redis(self, schedule_id, status):
        logger.info(f"Updating schedule status in Redis: schedule_id={schedule_id}, status={status}")
        try:
            key = f"schedule_{schedule_id}"
            schedule_data = await self.loop.run_in_executor(self.thread_pool, self.redis_client.get, key)
            if schedule_data:
                schedule_doc = json.loads(schedule_data)
                schedule_doc['status'] = status
                if status == 'TASK_COMPLETED':
                    schedule_doc['completed_app_id'] = self.last_completed_app_id
                updated_data = json.dumps(schedule_doc)

                # Perform SET operation
                set_result = await self.loop.run_in_executor(
                    self.thread_pool,
                    self.redis_client.set,
                    key,
                    updated_data
                )
                if not set_result:
                    raise Exception(f"Failed to update schedule {schedule_id} in Redis")

                # Perform PUBLISH operation
                publish_result = await self.loop.run_in_executor(
                    self.thread_pool,
                    self.redis_client.publish,
                    'schedule',
                    updated_data
                )
                if publish_result == 0:
                    logger.warning(f"No clients received the published update for schedule {schedule_id}")

                logger.info(f"Schedule status updated in Redis: schedule_id={schedule_id}, status={status}")
            else:
                logger.error(f"Schedule {schedule_id} not found in Redis")
        except Exception as e:
            logger.error(f"Error updating schedule status in Redis for {schedule_id}: {str(e)}", exc_info=True)
            raise

    async def fetch_dependency_outputs(self, workflow_id, schedule_id, app_id, schedule):
        logger.info(f"Fetching dependency outputs for task: {app_id} in schedule {schedule_id}")

        current_level, dependencies = await self.get_task_dependencies(schedule, app_id)

        if current_level is None or current_level == 0:
            raise Exception(f"Invalid level or no dependencies for task {app_id} in schedule {schedule_id}")

        dependency_outputs = []
        for task in dependencies:
            dep_app_id = task['app_id']
            output_key = f"{workflow_id}_{schedule_id}_{dep_app_id}_output"
            try:
                output_doc = await self.fetch_data_with_retry(self.data_db, output_key)
                dependency_outputs.extend(output_doc['data'])
            except Exception as e:
                logger.error(f"Error fetching dependency output for {output_key}: {str(e)}", exc_info=True)
                raise

        if not dependency_outputs:
            raise Exception(f"No dependency outputs found for task {app_id} in schedule {schedule_id}")

        return dependency_outputs

    async def run_docker_task(self, app_id, input_data):
        container_name = f"sawtooth-{app_id}"
        logger.info(f"Starting run_docker_task for container: {container_name}")
        try:
            logger.debug(f"Attempting to get container: {container_name}")
            container = self.docker_client.containers.get(container_name)
            logger.debug(f"Container {container_name} retrieved successfully")

            logger.debug(f"Fetching container attributes for {container_name}")
            container_info = container.attrs

            logger.debug(f"Searching for exposed port in {container_name}")
            exposed_port = None
            for port, host_config in container_info['NetworkSettings']['Ports'].items():
                logger.debug(f"Checking port mapping: {port} -> {host_config}")
                if host_config:
                    exposed_port = host_config[0]['HostPort']
                    logger.debug(f"Found exposed port: {exposed_port}")
                    break

            if not exposed_port:
                logger.error(f"No exposed port found for container {container_name}")
                raise Exception(f"No exposed port found for container {container_name}")

            logger.info(f"Found exposed port {exposed_port} for container {container_name}")

            logger.debug(f"Preparing to send data to container {container_name}")
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection('localhost', exposed_port),
                    timeout=5
                )
                logger.debug(f"Connected to {container_name} at localhost:{exposed_port}")

                payload = json.dumps({'data': input_data}).encode()
                logger.debug(f"Sending payload to {container_name}. Size: {len(payload)} bytes")

                writer.write(payload)
                await writer.drain()
                logger.debug(f"Payload sent to {container_name}, waiting for response")

                response_data = await asyncio.wait_for(reader.read(), timeout=30)
                logger.debug(f"Received response from {container_name}. Size: {len(response_data)} bytes")

                writer.close()
                await writer.wait_closed()

                result = json.loads(response_data.decode())
                logger.info(f"Successfully parsed JSON response from container {container_name}")
                return result

            except asyncio.TimeoutError:
                logger.error(f"Timeout occurred while communicating with {container_name}")
                raise
            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON response from {container_name}")
                raise
            except Exception as e:
                logger.error(f"Error in communication with {container_name}: {str(e)}")
                raise

        except docker.errors.NotFound:
            logger.error(f"Container {container_name} not found")
            raise
        except docker.errors.APIError as api_error:
            logger.error(f"Docker API error for {container_name}: {str(api_error)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in run_docker_task for {container_name}: {str(e)}", exc_info=True)
            raise

    async def is_final_task(self, schedule_id, app_id):
        try:
            # Fetch schedule document from Redis
            key = f"schedule_{schedule_id}"
            schedule_data = await self.loop.run_in_executor(self.thread_pool, self.redis_client.get, key)

            if not schedule_data:
                logger.error(f"Schedule {schedule_id} not found in Redis")
                return False

            schedule_doc = json.loads(schedule_data)
            schedule = schedule_doc.get('schedule', {})
            level_info = schedule.get('level_info', {})

            # Find the highest level
            max_level = max(map(int, level_info.keys()))

            # Get all tasks in the highest level
            highest_level_tasks = level_info[str(max_level)]

            # Check if the current app_id is in the highest level
            if not any(task['app_id'] == app_id for task in highest_level_tasks):
                logger.info(f"Task {app_id} is not in the final level for schedule {schedule_id}")
                return False

            # Check if all tasks in the highest level are completed
            for task in highest_level_tasks:
                if task['app_id'] != app_id:
                    task_app_id = task['app_id']
                    task_key = (schedule_id, task_app_id)
                    if self.task_status.get(task_key) != 'COMPLETED':
                        logger.info(f"Task {task_app_id} in schedule {schedule_id} is not completed")
                        return False

            logger.info(f"All final level tasks are completed for schedule {schedule_id}")
            return True
        except Exception as e:
            logger.error(f"Error checking if task is final: {str(e)}", exc_info=True)
            return False

    @staticmethod
    async def get_task_dependencies(schedule, app_id):
        logger.info(f"Getting dependencies for app_id: {app_id}")
        level_info = schedule.get('level_info', {})

        current_level = None
        for level, tasks in level_info.items():
            if any(task['app_id'] == app_id for task in tasks):
                current_level = int(level)
                break

        if current_level is None:
            logger.warning(f"App_id {app_id} not found in level_info")
            return None, []

        if current_level == 0:
            logger.info(f"App_id {app_id} is at level 0. No dependencies.")
            return 0, []

        prev_level_tasks = level_info.get(str(current_level - 1), [])
        dependencies = [task for task in prev_level_tasks if 'next' in task and app_id in task['next']]

        return current_level, dependencies

    async def fetch_data_with_retry(self, db, key, max_retries=5, initial_delay=0.1):
        delay = initial_delay
        for attempt in range(max_retries):
            try:
                doc = await self.loop.run_in_executor(self.thread_pool, db.get, key)
                if doc is None:
                    raise couchdb.http.ResourceNotFound
                return doc
            except couchdb.http.ResourceNotFound:
                if attempt == max_retries - 1:
                    logger.error(f"Data not found for key {key} after {max_retries} attempts")
                    raise
                logger.warning(
                    f"Data not found for key {key}, retrying in {delay:.2f} seconds (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(delay)
                delay *= 2  # Exponential backoff
            except Exception as e:
                logger.error(f"Error fetching data for key {key}: {str(e)}", exc_info=True)
                raise

    async def cleanup(self):
        logger.info("Cleaning up TaskExecutor")
        if self.redis_subscriber:
            self.redis_subscriber.cancel()
            try:
                await self.redis_subscriber
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
        if self.redis_pubsub:
            await self.loop.run_in_executor(self.thread_pool, self.redis_pubsub.close)
        if self.redis_client:
            await self.loop.run_in_executor(self.thread_pool, self.redis_client.close)
        self.thread_pool.shutdown()
        self.task_status.clear()
        logger.info("TaskExecutor cleanup complete")


async def main():
    logger.info("Starting main application")
    executor = TaskExecutor()
    await executor.initialize()

    logger.info(f"TaskExecutor initialized and running. Waiting for Redis messages on node: {CURRENT_NODE}")

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
