import asyncio
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import docker
from cachetools import TTLCache
from docker import errors
from redis import RedisError, RedisCluster

from helper.blockchain_task_status_updater import status_update_transactor
from helper.response_manager import ResponseManager

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

CURRENT_NODE = os.getenv('NODE_ID')
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis-cluster:6379')


class TaskExecutor:
    def __init__(self):
        self.task_queue = asyncio.PriorityQueue()
        self.docker_client = docker.from_env()
        self.thread_pool = ThreadPoolExecutor()
        self.loop = asyncio.get_event_loop()
        self.stream_listener_task = None
        self.process_tasks_task = None
        self.processed_changes = TTLCache(maxsize=1000, ttl=300)  # Cache for 5 minutes
        self.task_status = {}
        self.redis = None
        self.stream_name = 'schedule_stream'
        logger.info("TaskExecutor initialized")

    async def connect_to_redis(self):
        try:
            # Use run_in_executor to run the synchronous RedisCluster.from_url in a separate thread
            self.redis = await self.loop.run_in_executor(
                self.thread_pool,
                lambda: RedisCluster.from_url(REDIS_URL, decode_responses=True)
            )
            logger.info("Connected to Redis cluster successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise

    async def initialize(self):
        logger.info("Initializing TaskExecutor")
        await self.connect_to_redis()

        # Start the Redis Stream listener and task processor
        self.stream_listener_task = self.loop.create_task(self.listen_for_stream_messages())
        self.process_tasks_task = self.loop.create_task(self.process_tasks())

        logger.info("TaskExecutor initialization complete")

    async def listen_for_stream_messages(self):
        logger.info(f"Starting to listen for messages on the '{self.stream_name}' stream")
        last_id = '0'  # Start with the earliest message
        while True:
            try:
                # Read new messages from the stream
                messages = self.redis.xread({self.stream_name: last_id}, count=1, block=0)

                if messages:
                    for stream, message_list in messages:
                        for message_id, message_data in message_list:
                            await self.process_schedule(json.loads(message_data['data']))
                            last_id = message_id

            except asyncio.CancelledError:
                logger.info("Redis stream listener cancelled")
                break
            except RedisError as e:
                logger.error(f"Redis error: {str(e)}")
                await asyncio.sleep(1)  # Wait before retrying
            except Exception as e:
                logger.error(f"Error in Redis stream listener: {str(e)}", exc_info=True)
                await asyncio.sleep(5)  # Wait before trying to reconnect

        logger.info("Redis stream listener stopped")

    async def process_schedule(self, schedule_data):
        try:
            schedule_id = schedule_data.get('schedule_id')
            current_status = schedule_data.get('status')

            if not schedule_id or not current_status:
                logger.warning(f"Received invalid schedule data: {schedule_data}")
                return

            # Check if we've already processed this change and if the status is the same
            if schedule_id in self.processed_changes:
                prev_status = self.processed_changes[schedule_id]['status']
                if prev_status == current_status:
                    logger.debug(f"Skipping already processed schedule with unchanged status: {schedule_id}")
                    return
                else:
                    logger.info(f"Re-processing schedule {schedule_id} due to status change: {prev_status} -> {current_status}")

            # Process the schedule based on its status
            if current_status == 'ACTIVE':
                logger.info(f"Active schedule detected: {schedule_id}")
                await self.handle_new_schedule(schedule_data)
            elif current_status == 'TASK_COMPLETED':
                logger.info(f"Task completion update detected for schedule: {schedule_id}")
                await self.handle_task_completion(schedule_data)
            else:
                logger.info(f"Unhandled status {current_status} for schedule: {schedule_id}")

            # Mark this change as processed with its current status
            self.processed_changes[schedule_id] = {
                'timestamp': time.time(),
                'status': current_status
            }

        except Exception as e:
            logger.error(f"Error processing schedule: {str(e)}", exc_info=True)

    async def handle_new_schedule(self, schedule_doc):
        logger.info(f"Handling new schedule: {schedule_doc['schedule_id']}")
        schedule = schedule_doc.get('schedule')
        if not schedule:
            logger.warning(f"Schedule document {schedule_doc['schedule_id']} does not contain a 'schedule' field")
            return
        node_schedule = schedule.get('node_schedule', {})

        workflow_id = schedule_doc.get('workflow_id')
        schedule_id = schedule_doc['schedule_id']

        if CURRENT_NODE in node_schedule:
            for app_id in node_schedule[CURRENT_NODE]:
                timestamp = schedule_doc.get('timestamp')
                source_url = schedule_doc.get('source_url')
                source_public_key = schedule_doc.get('source_public_key')
                task_key = (schedule_id, app_id)

                logger.info(f"Checking dependencies for app_id: {app_id} in schedule {schedule_id}")
                if await self.check_dependencies(schedule_id, app_id):
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

    async def handle_task_completion(self, schedule_doc):
        logger.info(f"Handling task completion for schedule: {schedule_doc['schedule_id']}")
        schedule_id = schedule_doc['schedule_id']
        completed_app_id = schedule_doc.get('completed_app_id')

        if not completed_app_id:
            logger.warning(f"No completed_app_id found in schedule document: {schedule_id}")
            return

        # Mark the task as completed
        task_key = (schedule_id, completed_app_id)
        self.task_status[task_key] = 'COMPLETED'

        schedule = schedule_doc.get('schedule')
        if not schedule:
            logger.warning(f"Schedule document {schedule_id} does not contain a 'schedule' field")
            return

        workflow_id = schedule_doc.get('workflow_id')
        node_schedule = schedule.get('node_schedule', {})

        if CURRENT_NODE in node_schedule:
            for app_id in node_schedule[CURRENT_NODE]:
                task_key = (schedule_id, app_id)
                current_status = self.task_status.get(task_key)

                # Skip if this task has already been completed or is in progress
                if current_status in ['COMPLETED', 'IN_PROGRESS', 'QUEUED']:
                    logger.debug(f"Skipping task: {app_id} for schedule {schedule_id}. Status: {current_status}")
                    continue

                if await self.check_dependencies(schedule_id, app_id):
                    logger.info(
                        f"Dependencies now met for app_id: {app_id} in schedule {schedule_id}. Adding to task queue.")
                    timestamp = schedule_doc.get('timestamp')
                    source_url = schedule_doc.get('source_url')
                    source_public_key = schedule_doc.get('source_public_key')
                    await self.task_queue.put((source_url, source_public_key, timestamp, workflow_id, schedule_id, app_id))
                    self.task_status[task_key] = 'QUEUED'
                else:
                    logger.debug(f"Dependencies not yet met for app_id: {app_id} in schedule {schedule_id}")
                    self.task_status[task_key] = 'WAITING'

    async def check_dependencies(self, schedule_id, app_id):
        schedule_key = f"schedule_{schedule_id}"
        schedule_json = await self.fetch_data_with_retry(schedule_key)
        schedule_doc = json.loads(schedule_json)
        schedule = schedule_doc.get('schedule', {})

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
                        update_local_status = self.update_schedule_status(schedule_id, "FINALIZED")
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

                        await asyncio.gather(update_local_status, update_blockchain_status, send_response_to_client)

                except Exception as e:
                    logger.error(f"Error executing task {app_id}: {str(e)}", exc_info=True)
                    self.task_status[(schedule_id, app_id)] = 'FAILED'
                    update_local_status = self.update_schedule_status(schedule_id, "FAILED")
                    update_blockchain_status = self.loop.run_in_executor(
                        self.thread_pool,
                        status_update_transactor.create_and_send_transaction,
                        workflow_id,
                        schedule_id,
                        "FAILED")
                    disconnect_response_manager = self.loop.run_in_executor(self.thread_pool, response_manager.disconnect)

                    await asyncio.gather(update_local_status, update_blockchain_status, disconnect_response_manager)
                finally:
                    self.task_queue.task_done()
            except asyncio.CancelledError:
                logger.info("Task processing loop cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error in task processing loop: {str(e)}", exc_info=True)
                await asyncio.sleep(5)  # Wait before continuing the loop

    async def update_schedule_status(self, schedule_id, status):
        logger.info(f"Updating schedule status: schedule_id={schedule_id}, status={status}")
        try:
            # Retrieve the schedule from Redis
            schedule_key = f"schedule_{schedule_id}"
            schedule_json = await self.loop.run_in_executor(self.thread_pool, self.redis.get, schedule_key)

            if not schedule_json:
                raise Exception(f"Schedule {schedule_id} not found in Redis")

            schedule_doc = json.loads(schedule_json)
            schedule_doc['status'] = status

            # Save the updated schedule back to Redis
            updated_schedule_json = json.dumps(schedule_doc)
            await self.loop.run_in_executor(self.thread_pool, self.redis.set, schedule_key, updated_schedule_json)

            # Publish the updated schedule to the Redis stream
            await self.loop.run_in_executor(
                self.thread_pool,
                self.redis.xadd,
                self.stream_name,
                {'schedule_id': schedule_id, 'status': status}
            )

            logger.info(f"Schedule status updated: schedule_id={schedule_id}, status={status}")
        except RedisError as e:
            logger.error(f"Redis error updating schedule status for {schedule_id}: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Error updating schedule status for {schedule_id}: {str(e)}", exc_info=True)
            raise

    async def execute_task(self, workflow_id, schedule_id, app_id):
        logger.info(f"Executing task: workflow_id={workflow_id}, schedule_id={schedule_id}, app_id={app_id}")
        task_key = (schedule_id, app_id)
        self.task_status[task_key] = 'IN_PROGRESS'

        schedule_key = f"schedule_{schedule_id}"
        schedule_json = await self.fetch_data_with_retry(schedule_key)
        schedule_doc = json.loads(schedule_json)
        schedule = schedule_doc.get('schedule', {})

        current_level, _ = await self.get_task_dependencies(schedule, app_id)

        if current_level is None:
            raise Exception(f"Task {app_id} not found in schedule level_info")

        if current_level == 0:
            input_key = f"iot_data_{workflow_id}_{schedule_id}_{app_id}_input"
            try:
                input_json = await self.fetch_data_with_retry(input_key)
                if input_json is None:
                    raise Exception(f"Input data not found for key: {input_key}")
                input_doc = json.loads(input_json)
                if 'data' not in input_doc:
                    raise Exception(f"'data' field not found in input document for key: {input_key}")
                input_data = input_doc['data']
                logger.debug(f"Input data fetched for task {app_id}: {str(input_data)[:100]}...")  # Log first 100 characters
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

        output_key = f"iot_data_{workflow_id}_{schedule_id}_{app_id}_output"
        try:
            output_json = json.dumps({
                'data': output_data['data'],
                'workflow_id': workflow_id,
                'schedule_id': schedule_id
            })
            await self.loop.run_in_executor(self.thread_pool, self.redis.set, output_key, output_json)
            logger.info(f"Task output stored: {output_key}")
            self.task_status[task_key] = 'COMPLETED'
        except Exception as e:
            logger.error(f"Error storing task output for {output_key}: {str(e)}", exc_info=True)
            raise

        await self.update_schedule_on_task_completion(schedule_id, app_id)

        return output_data

    async def update_schedule_on_task_completion(self, schedule_id, completed_app_id):
        try:
            schedule_key = f"schedule_{schedule_id}"
            schedule_json = await self.fetch_data_with_retry(schedule_key)
            schedule_doc = json.loads(schedule_json)
            schedule_doc['status'] = 'TASK_COMPLETED'
            schedule_doc['completed_app_id'] = completed_app_id
            updated_schedule_json = json.dumps(schedule_doc)

            # Update the schedule in Redis
            await self.loop.run_in_executor(self.thread_pool, self.redis.set, schedule_key, updated_schedule_json)

            # Publish the updated schedule to the Redis stream
            await self.loop.run_in_executor(
                self.thread_pool,
                self.redis.xadd,
                self.stream_name,
                {'schedule_id': schedule_id, 'status': 'TASK_COMPLETED', 'completed_app_id': completed_app_id}
            )

            logger.info(f"Updated schedule {schedule_id} with completed task {completed_app_id}")
        except Exception as e:
            logger.error(f"Error updating schedule on task completion: {str(e)}", exc_info=True)

    async def fetch_data_with_retry(self, key, max_retries=5, initial_delay=0.1):
        delay = initial_delay
        for attempt in range(max_retries):
            try:
                data = await self.loop.run_in_executor(self.thread_pool, self.redis.get, key)
                if data is None:
                    raise KeyError(f"Data not found for key: {key}")
                return data
            except KeyError:
                if attempt == max_retries - 1:
                    logger.error(f"Data not found for key {key} after {max_retries} attempts")
                    raise
                logger.warning(
                    f"Data not found for key {key}, retrying in {delay:.2f} seconds (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(delay)
                delay *= 2  # Exponential backoff
            except RedisError as e:
                logger.error(f"Redis error fetching data for key {key}: {str(e)}", exc_info=True)
                raise
            except Exception as e:
                logger.error(f"Unexpected error fetching data for key {key}: {str(e)}", exc_info=True)
                raise

    async def fetch_dependency_outputs(self, workflow_id, schedule_id, app_id, schedule):
        logger.info(f"Fetching dependency outputs for task: {app_id} in schedule {schedule_id}")

        current_level, dependencies = await self.get_task_dependencies(schedule, app_id)

        if current_level is None or current_level == 0:
            raise Exception(f"Invalid level or no dependencies for task {app_id} in schedule {schedule_id}")

        dependency_outputs = []
        for task in dependencies:
            dep_app_id = task['app_id']
            output_key = f"iot_data_{workflow_id}_{schedule_id}_{dep_app_id}_output"
            try:
                output_json = await self.fetch_data_with_retry(output_key)
                output_doc = json.loads(output_json)
                dependency_outputs.extend(output_doc['data'])
            except (RedisError, json.JSONDecodeError) as e:
                logger.error(f"Error fetching or parsing dependency output for {output_key}: {str(e)}", exc_info=True)
                raise
            except KeyError as e:
                logger.error(f"Missing 'data' field in output document for {output_key}: {str(e)}", exc_info=True)
                raise
            except Exception as e:
                logger.error(f"Unexpected error fetching dependency output for {output_key}: {str(e)}", exc_info=True)
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
            schedule_key = f"schedule_{schedule_id}"
            schedule_json = await self.fetch_data_with_retry(schedule_key)
            schedule_doc = json.loads(schedule_json)
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
                    output_key = f"iot_data_{schedule_doc['workflow_id']}_{schedule_id}_{task_app_id}_output"
                    try:
                        # Check if the output exists in Redis
                        exists = await self.loop.run_in_executor(self.thread_pool, self.redis.exists, output_key)
                        if not exists:
                            logger.info(f"Output not found for task {task_app_id} in schedule {schedule_id}")
                            return False
                    except RedisError as e:
                        logger.error(f"Redis error checking output for task {task_app_id}: {str(e)}", exc_info=True)
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

    async def cleanup(self):
        logger.info("Cleaning up TaskExecutor")

        # Cancel the Redis stream listener task
        if hasattr(self, 'stream_listener_task') and self.stream_listener_task:
            self.stream_listener_task.cancel()
            try:
                await self.stream_listener_task
            except asyncio.CancelledError:
                pass

        if self.process_tasks_task:
            self.process_tasks_task.cancel()
            try:
                await self.process_tasks_task
            except asyncio.CancelledError:
                pass

        # Close the Redis connection
        if hasattr(self, 'redis') and self.redis:
            await self.redis.close()
            logger.info("Redis connection closed")

        # Close the Docker client (unchanged)
        if self.docker_client:
            self.docker_client.close()

        # Shutdown the thread pool (unchanged)
        self.thread_pool.shutdown()

        # Clear the task status (unchanged)
        self.task_status.clear()

        logger.info("TaskExecutor cleanup complete")


async def main():
    logger.info("Starting main application")
    executor = TaskExecutor()
    await executor.initialize()

    logger.info(f"TaskExecutor initialized and running. Listening for Redis stream messages on node: {CURRENT_NODE}")

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
