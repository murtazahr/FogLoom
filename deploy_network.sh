#!/bin/bash

# Apply to kubernetes environment.
kubectl apply -f kubernetes-manifests/generated/config-and-secrets.yaml
kubectl apply -f kubernetes-manifests/generated/couchdb-cluster-deployment.yaml
kubectl apply -f kubernetes-manifests/static/local-docker-registry-deployment.yaml
kubectl apply -f kubernetes-manifests/generated/blockchain-network-deployment.yaml