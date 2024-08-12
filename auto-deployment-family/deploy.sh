#!/usr/bin/env bash

# Script Name: deploy.sh
# Description: Used to build all necessary images and start up a sawtooth network
# Author: Murtaza Rangwala
# Date Created: 2024-08-08
# Last Modified: 2024-08-08

# Usage: "./deploy.sh

# Main script content starts here

WORK_DIR=$(pwd)
TEST_APP_DIR=$(pwd)/../sample_application

# Make sure user is in the correct working directory
cd "$WORK_DIR" || exit

# Bring docker compose down incase it is up
docker-compose -f sawtooth-poet.yaml down

# Clean up existing docker environment
docker container rm "$(docker container ls -aq)"
docker rmi -f "$(docker images -aq)"
docker volume rm auto-deployment-family_poet-shared
docker network rm auto-deployment-family_default

# Building docker image for test docker application
cd "$TEST_APP_DIR" || exit
docker build -t temp-anomaly-detection:latest -f Dockerfile .

# Make sure user is in the correct working directory
cd "$WORK_DIR" || exit

# Export test docker application
docker save -o client/temp-anomaly-detection.tar temp-anomaly-detection

# Build docker images for tp, event handler and client
docker build -t sawtooth-auto-docker-deployment-tp:latest -f processor.Dockerfile .
docker build -t sawtooth-auto-docker-deployment-event-handler:latest -f eventHandler.Dockerfile .
docker build -t sawtooth-auto-docker-deployment-client:latest -f client.Dockerfile .

# Run docker compose
docker-compose -f sawtooth-poet.yaml up
