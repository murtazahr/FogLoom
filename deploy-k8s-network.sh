#!/bin/bash

# Main script content starts here

WORK_DIR=$(pwd)
TEST_APP_DIR=$(pwd)/example-application
K8S_DIR=$(pwd)/k8s-manifests/sawtooth-network

# Building docker image for test docker application
cd "$TEST_APP_DIR" || exit
docker build -t temp-anomaly-detection:latest -f Dockerfile .

# Make sure user is in the correct working directory
cd "$WORK_DIR" || exit

# Export test docker application
docker save -o auto-docker-deployment/docker-image-client/temp-anomaly-detection.tar temp-anomaly-detection

# Build peer-registry-tp image
docker build -t peer-registry-tp:local ./peer-registry/peer-registry-tp
# Build docker-image-tp image
docker build -t docker-image-tp:local ./auto-docker-deployment/docker-image-tp
# Build docker-image-client image
docker build -t docker-image-client:local ./auto-docker-deployment/docker-image-client
# Build fog-node image
docker build -t fog-node:local ./fog-node

# Import images into k3s
sudo k3s ctr images import docker.io/library/peer-registry-tp:local
sudo k3s ctr images import docker.io/library/docker-image-tp:local
sudo k3s ctr images import docker.io/library/docker-image-client:local
sudo k3s ctr images import docker.io/library/fog-node:local

echo "Images built and imported into k3s successfully"

cd "$K8S_DIR" || exit

# Cleanup kubernetes environment if it exists.
sudo kubectl delete -f sawtooth-k8s-default-pbft.yaml

# Bring up network
sudo kubectl apply -f pbft-keys-configmap.yaml
sudo kubectl apply -f sawtooth-k8s-default-pbft.yaml