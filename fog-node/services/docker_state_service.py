import docker
from flask import Flask, jsonify
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
docker_client = docker.from_env()


@app.route('/docker-state', methods=['GET'])
def get_docker_state():
    try:
        containers = docker_client.containers.list()
        state = {}
        for container in containers:
            image = container.image
            state[container.name] = {
                'image_name': image.tags[0] if image.tags else image.id,
                'image_hash': image.id,
                'status': container.status,
                'created': container.attrs['Created'],
                'ports': container.ports,
                'labels': container.labels
            }
        logger.info(f"Retrieved state for {len(state)} containers")
        return jsonify(state)
    except Exception as e:
        logger.error(f"Error retrieving Docker state: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200


if __name__ == '__main__':
    logger.info("Starting Docker State API")
    app.run(host='0.0.0.0', port=5000)
