#!/usr/bin/env bash

# Script Name: deploy-network.sh
# Description: Used to build all necessary images and start up a sawtooth network
# Author: Murtaza Rangwala
# Date Created: 2024-08-08
# Last Modified: 2024-08-08

# Usage: "./deploy-network.sh

# Main script content starts here

WORK_DIR=$(pwd)
TEST_APP_DIR=$(pwd)/test-application

# Make sure user is in the correct working directory
cd "$WORK_DIR" || exit

# Freeze dependencies
pip freeze > fog-node/requirements.txt
cp fog-node/requirements.txt auto-docker-deployment-family/docker-image-client/.
cp fog-node/requirements.txt auto-docker-deployment-family/docker-image-tp/.

# Bring docker compose down incase it is up
docker-compose down -v --rmi all

# Building docker image for test docker application
cd "$TEST_APP_DIR" || exit
docker build -t temp-anomaly-detection:latest -f Dockerfile .

# Make sure user is in the correct working directory
cd "$WORK_DIR" || exit

# Export test docker application
docker save -o auto-docker-deployment-family/docker-image-client/temp-anomaly-detection.tar temp-anomaly-detection

# Run docker compose
docker-compose build
docker-compose up
