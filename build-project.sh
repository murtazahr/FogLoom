#!/bin/bash

# Main script content starts here

WORK_DIR=$(pwd)
TEST_APP_DIR=$(pwd)/example-application-object-detection
DOCKER_USERNAME=murtazahr

# Building docker image for test docker application
cd "$TEST_APP_DIR/object-detection" || exit
docker build -t yolo-object-detection:latest -f Dockerfile .

cd "$TEST_APP_DIR/output-image-generation" || exit
docker build -t bounding-box-image-generation:latest -f Dockerfile .

# Make sure user is in the correct working directory
cd "$WORK_DIR" || exit

# Export test docker application
docker save -o auto-docker-deployment/docker-image-client/yolo-object-detection.tar yolo-object-detection
docker save -o auto-docker-deployment/docker-image-client/bounding-box-image-generation.tar bounding-box-image-generation

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
# Build scheduling-client image
docker build -t $DOCKER_USERNAME/scheduling-client:latest ./scheduling/scheduling-client
# Build fog-node image
docker build -t $DOCKER_USERNAME/fog-node:latest ./fog-node


# Push images to Docker Hub
docker push $DOCKER_USERNAME/peer-registry-tp:latest
docker push $DOCKER_USERNAME/docker-image-tp:latest
docker push $DOCKER_USERNAME/docker-image-client:latest
docker push $DOCKER_USERNAME/dependency-management-tp:latest
docker push $DOCKER_USERNAME/dependency-management-client:latest
docker push $DOCKER_USERNAME/scheduling-tp:latest
docker push $DOCKER_USERNAME/scheduling-client:latest
docker push $DOCKER_USERNAME/fog-node:latest

echo "Images built and pushed to registry successfully"