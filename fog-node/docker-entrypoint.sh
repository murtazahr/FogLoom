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

# Run node_startup_script.py if IS_NEW_ADDITION is true
if [ "$IS_NEW_ADDITION" = "true" ]; then
    echo "Running node startup script..."
    python /app/node_startup_script.py
fi

# Execute the main command
exec "$@"