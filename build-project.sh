#!/bin/bash

# Main script content starts here

WORK_DIR=$(pwd)
TEST_APP_DIR=$(pwd)/sample-apps
DOCKER_USERNAME=murtazahr

# Building docker image for test docker application
cd "$TEST_APP_DIR/arrhythmia-detection" || exit
docker build -t arrhythmia-detection:latest -f Dockerfile .

# Make sure user is in the correct working directory
cd "$WORK_DIR" || exit

# Export test docker application
docker save -o auto-docker-deployment/docker-image-client/arrhythmia-detection.tar arrhythmia-detection

# Build peer-registry-tp image
docker build -t $DOCKER_USERNAME/peer-registry-tp:latest ./peer-registry/peer-registry-tp
# Build docker-image-tp image
docker build -t $DOCKER_USERNAME/docker-image-tp:latest ./auto-docker-deployment/docker-image-tp
# Build docker-image-client image
docker build -t $DOCKER_USERNAME/docker-image-client:latest ./auto-docker-deployment/docker-image-client
# Build dependency-management-tp image
docker build -t $DOCKER_USERNAME/dependency-management-tp:latest ./manage-dependency-workflow/dependency-management-tp
# Build dependency-management-client image
docker build -t $DOCKER_USERNAME/dependency-management-client:latest ./manage-dependency-workflow/dependency-management-client
# Build scheduling-tp image
docker build -t $DOCKER_USERNAME/scheduling-tp:latest ./scheduling/scheduling-tp
# Build schedule-status-update-tp image
docker build -t $DOCKER_USERNAME/schedule-status-update-tp:latest ./scheduling/status-update-tp
# Build iot-data-tp image
docker build -t $DOCKER_USERNAME/iot-data-tp:latest ./scheduling/iot-data-tp
# Build iot-node image
docker build -t $DOCKER_USERNAME/iot-node:latest ./iot-node
# Build fog-node image
docker build -t $DOCKER_USERNAME/fog-node:latest ./fog-node


# Push images to Docker Hub
docker push $DOCKER_USERNAME/peer-registry-tp:latest
docker push $DOCKER_USERNAME/docker-image-tp:latest
docker push $DOCKER_USERNAME/docker-image-client:latest
docker push $DOCKER_USERNAME/dependency-management-tp:latest
docker push $DOCKER_USERNAME/dependency-management-client:latest
docker push $DOCKER_USERNAME/scheduling-tp:latest
docker push $DOCKER_USERNAME/schedule-status-update-tp:latest
docker push $DOCKER_USERNAME/iot-data-tp:latest
docker push $DOCKER_USERNAME/fog-node:latest
docker push $DOCKER_USERNAME/iot-node:latest

echo "Images built and pushed to registry successfully"