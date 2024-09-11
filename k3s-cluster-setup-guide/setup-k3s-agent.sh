#!/bin/bash

export K3S_TOKEN="<Token from server setup>"
export K3S_URL="<URL from server setup>"
export K3S_NODE_NAME="<Node name>"

curl -sfL https://get.k3s.io | K3S_URL=$K3S_URL K3S_TOKEN=$K3S_TOKEN K3S_NODE_NAME=$K3S_NODE_NAME sh -