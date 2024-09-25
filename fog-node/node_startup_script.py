import requests
import docker
import logging
import time
import os
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_max_node_id():
    node_id = os.getenv('NODE_ID', '')
    match = re.search(r'sawtooth-fog-node-(\d+)', node_id)
    if match:
        return int(match.group(1))
    else:
        logger.error(f"Invalid NODE_ID format: {node_id}")
        return 0


def get_docker_state(hostname):
    try:
        url = f"http://{hostname}:5000/docker-state"
        logger.info(f"Attempting to fetch Docker state from {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching Docker state from {hostname}: {str(e)}")
        return None


def setup_docker_environment(state):
    client = docker.from_env()
    for container_name, container_info in state.items():
        try:
            # Check if the container already exists
            existing_containers = client.containers.list(all=True, filters={"name": container_name})
            if existing_containers:
                logger.info(f"Container {container_name} already exists. Skipping.")
                continue

            # Pull the image
            logger.info(f"Pulling image: {container_info['image_name']}")
            client.images.pull(container_info['image_name'])

            # Create and start the container
            logger.info(f"Creating and starting container: {container_name}")
            client.containers.run(
                container_info['image_name'],
                name=container_name,
                detach=True,
                ports=container_info['ports'],
                labels=container_info['labels']
            )
            logger.info(f"Container {container_name} created and started successfully")
        except docker.errors.APIError as e:
            logger.error(f"Error setting up container {container_name}: {str(e)}")


def node_startup():
    max_node_id = get_max_node_id()

    for i in range(max_node_id):
        hostname = f"sawtooth-{i}"
        docker_state = get_docker_state(hostname)
        if docker_state:
            logger.info(f"Successfully retrieved Docker state from {hostname}")
            setup_docker_environment(docker_state)
            return True

    logger.error(f"Failed to retrieve Docker state from any node up to sawtooth-{max_node_id - 1}")
    return False


if __name__ == "__main__":
    max_retries = 5
    retry_delay = 30  # seconds

    for attempt in range(max_retries):
        try:
            if node_startup():
                logger.info("Node startup completed successfully")
                break
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {str(e)}")

        if attempt < max_retries - 1:
            logger.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        else:
            logger.error("Max retries reached. Node startup failed.")
