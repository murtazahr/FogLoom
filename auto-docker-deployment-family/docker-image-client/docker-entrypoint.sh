#!/bin/bash
set -e

# Start the Docker daemon
dockerd &

# Wait for the Docker daemon to start
while ! docker info >/dev/null 2>&1; do
    echo "Waiting for Docker daemon to start..."
    sleep 1
done

# Add the registry as an insecure registry
echo '{"insecure-registries": ["sawtooth-registry:5000"]}' > /etc/docker/daemon.json
# shellcheck disable=SC2046
kill -SIGHUP $(pidof dockerd)

# Wait for Docker to reload its configuration
sleep 5

# Activate conda environment
source /opt/conda/bin/activate myenv

# Execute the main command
exec "$@"