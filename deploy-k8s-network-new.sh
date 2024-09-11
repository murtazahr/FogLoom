#!/bin/bash

# Function to generate PBFT keys
generate_pbft_keys() {
    local num_fog_nodes=$1
    local keys=""
    for ((i=0; i<num_fog_nodes; i++)); do
        priv=$(openssl ecparam -name secp256k1 -genkey -noout | openssl ec -text -noout | grep priv -A 3 | tail -n +2 | tr -d '\n[:space:]:' | sed 's/^00//')
        pub=$(openssl ec -in <(echo "$priv" | xxd -r -p | openssl ec -inform d -text -noout | grep pub -A 5 | tail -n +2 | tr -d '\n[:space:]:') -pubout -outform DER | tail -c 65 | xxd -p -c 65)
        keys="${keys}      pbft${i}priv: ${priv}\n      pbft${i}pub: ${pub}\n"
    done
    echo -e "$keys"
}

# Get user inputs
read -p "Enter the number of fog nodes: " num_fog_nodes
read -p "Enter the number of IoT nodes: " num_iot_nodes

# Part 1: Verify inputs and check node existence
if [ "$num_fog_nodes" -lt 3 ]; then
    echo "Error: Number of fog nodes must be at least 3."
    exit 1
fi

echo "Verifying node existence..."
for ((i=1; i<=num_fog_nodes; i++)); do
    if ! kubectl get node "fog-node-$i" &> /dev/null; then
        echo "Error: fog-node-$i does not exist in the cluster."
        exit 1
    fi
done

for ((i=1; i<=num_iot_nodes; i++)); do
    if ! kubectl get node "iot-node-$i" &> /dev/null; then
        echo "Error: iot-node-$i does not exist in the cluster."
        exit 1
    fi
done

echo "All required nodes exist in the cluster."

# Part 2: Generate and apply YAML
echo "Generating YAML file..."

# Generate PBFT keys
pbft_keys=$(generate_pbft_keys "$num_fog_nodes")

cat << EOF > kubernetes-manifests/sawtooth-network/config-and-secrets.yaml
apiVersion: v1
kind: List

items:
  # --------------------------=== Blockchain Setup Keys ===----------------------
  - apiVersion: v1
    kind: ConfigMap
    metadata:
      name: keys-config
    data:
$pbft_keys
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

echo "YAML file generated and saved to kubernetes-manifests/sawtooth-network/config-and-secrets.yaml"

# Apply the YAML file
echo "Applying YAML file to the cluster..."
kubectl apply -f kubernetes-manifests/sawtooth-network/config-and-secrets.yaml

echo "Deployment script completed successfully."