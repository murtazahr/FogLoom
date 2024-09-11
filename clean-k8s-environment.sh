#!/bin/bash

# Cleanup kubernetes environment if it exists.
kubectl delete -f kubernetes-manifests/generated/blockchain-network-deployment.yaml
kubectl delete -f kubernetes-manifests/static/local-docker-registry-deployment.yaml
kubectl delete -f kubernetes-manifests/generated/couchdb-cluster-deployment.yaml
kubectl delete -f kubernetes-manifests/generated/config-and-secrets.yaml
kubectl delete -f kubernetes-manifests/generated/pbft-key-generation-job.yaml

sudo rm -f kubernetes-manifests/generated/*