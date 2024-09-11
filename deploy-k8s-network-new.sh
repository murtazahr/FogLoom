#!/bin/bash

# Function to generate PBFT keys
generate_pbft_keys() {
    local num_fog_nodes=$1
    local keys=""
    for ((i=0; i<num_fog_nodes; i++)); do
        priv_key=$(openssl ecparam -name secp256k1 -genkey | openssl ec -text -noout | grep priv -A 3 | tail -n +2 | tr -d '\n[:space:]:' | sed 's/^00//')
        pub_key=$(openssl ecparam -name secp256k1 -genkey | openssl ec -text -noout | grep pub -A 5 | tail -n +2 | tr -d '\n[:space:]:' | sed 's/^04//')
        keys+="      pbft${i}priv: $priv_key"$'\n'
        keys+="      pbft${i}pub: $pub_key"$'\n'
    done
    echo "$keys"
}

# Function to check if a node exists in the cluster
check_node_exists() {
    local node_name=$1
    kubectl get nodes | grep -q "$node_name"
    return $?
}

# Main script starts here
echo "Enter the number of fog nodes:"
read num_fog_nodes
echo "Enter the number of IoT nodes:"
read num_iot_nodes

# Part 1: Verify inputs and check node existence
if [ "$num_fog_nodes" -lt 3 ]; then
    echo "Error: The number of fog nodes must be at least 3."
    exit 1
fi

echo "Checking for fog nodes..."
for ((i=1; i<=num_fog_nodes; i++)); do
    if ! check_node_exists "fog-node-$i"; then
        echo "Error: fog-node-$i does not exist in the cluster."
        exit 1
    fi
done

echo "Checking for IoT nodes..."
for ((i=1; i<=num_iot_nodes; i++)); do
    if ! check_node_exists "iot-node-$i"; then
        echo "Error: iot-node-$i does not exist in the cluster."
        exit 1
    fi
done

echo "All required nodes are present in the cluster."

# Part 2: Generate YAML file and apply to cluster
generated_keys=$(generate_pbft_keys "$num_fog_nodes")

cat << EOF > kubernetes-manifests/generated/config-and-secrets.yaml
apiVersion: v1
kind: List

items:
  # --------------------------=== Blockchain Setup Keys ===----------------------
  - apiVersion: v1
    kind: ConfigMap
    metadata:
      name: keys-config
    data:
$generated_keys
  # --------------------------=== CouchDB Secrets ===---------------------------
  - apiVersion: v1
    kind: Secret
    metadata:
      name: couchdb-secrets
    type: Opaque
    stringData:
      COUCHDB_USER: fogbus
      COUCHDB_PASSWORD: mwg478jR04vAonMu2QnFYF3sVyVKUujYrGrzVsrq3I
      COUCHDB_SECRET: LEv+K7x24ITqcAYp0R0e1GzBqiE98oSSarPD1sdeOyM=
      ERLANG_COOKIE: jT7egojgnPLzOncq9MQU/zqwqHm6ZiPUU7xJfFLA8MA=

  # --------------------------=== Docker Registry Secret ===----------------------
  - apiVersion: v1
    kind: Secret
    metadata:
      name: registry-secret
    type: Opaque
    stringData:
      http-secret: Y74bs7QpaHmI1NKDGO8I3JdquvVxL+5K15NupwxhSbc=
EOF

echo "Generated YAML file has been saved to kubernetes-manifests/generated/config-and-secrets.yaml"

# Apply the generated YAML to the cluster
kubectl apply -f kubernetes-manifests/generated/config-and-secrets.yaml

echo "Configuration and secrets have been applied to the cluster."