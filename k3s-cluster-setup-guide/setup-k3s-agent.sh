#!/bin/bash

curl -sfL https://get.k3s.io | K3S_URL=$K3S_URL K3S_TOKEN=$K3S_TOKEN K3S_NODE_NAME=$K3S_NODE_NAME sh -