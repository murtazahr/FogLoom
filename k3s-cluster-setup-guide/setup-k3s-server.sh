#!/bin/bash

export K3S_NODE_NAME=client-console

curl -sfL https://get.k3s.io | K3S_NODE_NAME=$K3S_NODE_NAME sh -

# Gain access to kubectl command without using sudo
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown "$USER" ~/.kube/config
sudo chmod 600 ~/.kube/config
echo 'export KUBECONFIG=~/.kube/config' >> ~/.bashrc

echo "============== K3S_TOKEN =============="
sudo cat /var/lib/rancher/k3s/server/node-token
echo "======================================="