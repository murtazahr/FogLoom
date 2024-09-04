#!/bin/bash

# Main script content starts here

WORK_DIR=$(pwd)
TEST_APP_DIR=$(pwd)/example-application
K8S_DIR=$(pwd)/k8s-manifests/sawtooth-network
DOCKER_USERNAME=murtazahr

# Building docker image for test docker application
cd "$TEST_APP_DIR" || exit
docker build -t temp-anomaly-detection:latest -f Dockerfile .

# Make sure user is in the correct working directory
cd "$WORK_DIR" || exit

# Export test docker application
docker save -o auto-docker-deployment/docker-image-client/temp-anomaly-detection.tar temp-anomaly-detection

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
# Build fog-node image
docker build -t $DOCKER_USERNAME/fog-node:latest ./fog-node

# Push images to Docker Hub
docker push $DOCKER_USERNAME/peer-registry-tp:latest
docker push $DOCKER_USERNAME/docker-image-tp:latest
docker push $DOCKER_USERNAME/docker-image-client:latest
docker push $DOCKER_USERNAME/dependency-management-tp:latest
docker push $DOCKER_USERNAME/dependency-management-client:latest
docker push $DOCKER_USERNAME/fog-node:latest

echo "Images built and pushed to registry successfully"

cd "$K8S_DIR" || exit

# Cleanup kubernetes environment if it exists.
kubectl delete -f blockchain-network-deployment.yaml
kubectl delete -f local-docker-registry-deployment.yaml
kubectl delete -f couchdb-cluster-deployment.yaml
kubectl delete -f config-and-secrets.yaml

# Bring up network
kubectl apply -f config-and-secrets.yaml
kubectl apply -f local-docker-registry-deployment.yaml
kubectl apply -f couchdb-cluster-deployment.yaml
kubectl apply -f blockchain-network-deployment.yaml