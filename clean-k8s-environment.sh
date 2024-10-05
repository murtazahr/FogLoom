#!/bin/bash

# Cleanup kubernetes environment if it exists.
kubectl delete -f kubernetes-manifests/generated/redis-cluster.yaml
kubectl delete -f kubernetes-manifests/generated/blockchain-network-deployment.yaml
kubectl delete -f kubernetes-manifests/static/local-docker-registry-deployment.yaml
kubectl delete -f kubernetes-manifests/generated/couchdb-cluster-deployment.yaml
kubectl delete -f kubernetes-manifests/generated/config-and-secrets.yaml
kubectl delete secret couchdb-secrets
kubectl delete secret redis-certificates
kubectl delete secret redis-password
kubectl delete pvc -l app=redis-cluster

sudo rm -f kubernetes-manifests/generated