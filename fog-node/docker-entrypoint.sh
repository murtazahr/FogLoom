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

echo "Waiting for couchdb setup to complete..."
while [ ! -f /shared/couch_db_setup_done ]; do
    sleep 1
done
echo "Couch DB setup has completed. Starting main process..."

# Execute the main command
exec "$@"