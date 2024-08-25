#!/usr/bin/env bash

# Script Name: deploy-network.sh
# Description: Used to build all necessary images and start up a sawtooth network
# Author: Murtaza Rangwala
# Date Created: 2024-08-08
# Last Modified: 2024-08-21

# Usage: "./deploy-network.sh [poet|pbft]"

# Check if at least one argument is provided
if [ $# -eq 0 ]; then
    echo "Error: No consensus mechanism specified. Please use 'poet' or 'pbft'."
    echo "Usage: $0 [poet|pbft]"
    exit 1
fi

# Get the consensus mechanism from the first argument
CONSENSUS=$1

# Validate the consensus mechanism
if [ "$CONSENSUS" != "poet" ] && [ "$CONSENSUS" != "pbft" ]; then
    echo "Error: Invalid consensus mechanism. Please use 'poet' or 'pbft'."
    echo "Usage: $0 [poet|pbft]"
    exit 1
fi

# Set the base compose file based on the consensus mechanism
BASE_COMPOSE_FILE="${CONSENSUS}-docker-compose.yaml"

# Main script content starts here

WORK_DIR=$(pwd)
TEST_APP_DIR=$(pwd)/example-application

# Make sure user is in the correct working directory
cd "$WORK_DIR" || exit

# Bring docker compose down in case it is up
docker-compose -f poet-docker-compose.yaml -f fogbus-docker-compose.yaml down --remove-orphans -v
docker-compose -f pbft-docker-compose.yaml -f fogbus-docker-compose.yaml down --remove-orphans -v

# Building docker image for test docker application
cd "$TEST_APP_DIR" || exit
docker build -t temp-anomaly-detection:latest -f Dockerfile .

# Make sure user is in the correct working directory
cd "$WORK_DIR" || exit

# Export test docker application
docker save -o auto-docker-deployment/docker-image-client/temp-anomaly-detection.tar temp-anomaly-detection

# Run docker compose
docker-compose -f "$BASE_COMPOSE_FILE" -f fogbus-docker-compose.yaml build
docker-compose -f "$BASE_COMPOSE_FILE" -f fogbus-docker-compose.yaml up