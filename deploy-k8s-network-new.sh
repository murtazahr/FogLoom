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

# Function to generate CouchDB cluster deployment YAML
generate_couchdb_yaml() {
    local num_fog_nodes=$1
    local yaml_content="apiVersion: v1
kind: List

items:"

    # Generate PVCs
    for ((i=0; i<num_fog_nodes; i++)); do
        yaml_content+="
  - apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: couchdb${i}-data
    spec:
      accessModes:
        - ReadWriteOnce
      resources:
        requests:
          storage: 1Gi"
    done

    # Generate Deployments
    for ((i=0; i<num_fog_nodes; i++)); do
        yaml_content+="
  - apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: couchdb-${i}
    spec:
      selector:
        matchLabels:
          app: couchdb-${i}
      replicas: 1
      template:
        metadata:
          labels:
            app: couchdb-${i}
        spec:
          nodeSelector:
            kubernetes.io/hostname: fogchain-node-$((i+1))
          containers:
            - name: couchdb
              image: couchdb:3
              ports:
                - containerPort: 5984
              env:
                - name: COUCHDB_USER
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_USER
                - name: COUCHDB_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_PASSWORD
                - name: COUCHDB_SECRET
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_SECRET
                - name: ERL_FLAGS
                  value: \"-setcookie \\\"\${ERLANG_COOKIE}\\\" -kernel inet_dist_listen_min 9100 -kernel inet_dist_listen_max 9200\"
                - name: NODENAME
                  value: \"couchdb-${i}.default.svc.cluster.local\"
              volumeMounts:
                - name: couchdb-data
                  mountPath: /opt/couchdb/data
              readinessProbe:
                httpGet:
                  path: /
                  port: 5984
                initialDelaySeconds: 5
                periodSeconds: 10
          volumes:
            - name: couchdb-data
              persistentVolumeClaim:
                claimName: couchdb${i}-data"
    done

    # Generate Services
    for ((i=0; i<num_fog_nodes; i++)); do
        yaml_content+="
  - apiVersion: v1
    kind: Service
    metadata:
      name: couchdb-${i}
    spec:
      clusterIP: None
      selector:
        app: couchdb-${i}
      ports:
        - port: 5984
          targetPort: 5984"
    done

    # Generate CouchDB Cluster Setup Job
    yaml_content+="
  - apiVersion: batch/v1
    kind: Job
    metadata:
      name: couchdb-setup
    spec:
      template:
        metadata:
          name: couchdb-setup
        spec:
          restartPolicy: OnFailure
          containers:
            - name: couchdb-setup
              image: curlimages/curl:latest
              command:
                - /bin/sh
              args:
                - -c
                - |
                  DB_NAME=\"resource_registry\" &&
                  echo \"Starting CouchDB cluster setup\" &&
                  for i in \$(seq 0 $((num_fog_nodes-1))); do
                    echo \"http://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${i}.default.svc.cluster.local:5984\"
                    until curl -s \"http://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${i}.default.svc.cluster.local:5984\" > /dev/null; do
                      echo \"Waiting for CouchDB on couchdb-\${i} to be ready...\"
                      sleep 5
                    done
                    echo \"CouchDB on couchdb-\${i} is ready\"
                  done &&
                  echo \"Adding nodes to the cluster\" &&
                  for num in \$(seq 1 $((num_fog_nodes-1))); do
                    response=\$(curl -X POST -H 'Content-Type: application/json' \"http://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:5984/_cluster_setup\" -d \"{\\\"action\\\": \\\"enable_cluster\\\", \\\"bind_address\\\":\\\"0.0.0.0\\\", \\\"username\\\": \\\"\${COUCHDB_USER}\\\", \\\"password\\\":\\\"\${COUCHDB_PASSWORD}\\\", \\\"port\\\": 5984, \\\"node_count\\\": \\\"$num_fog_nodes\\\", \\\"remote_node\\\": \\\"couchdb-\${num}.default.svc.cluster.local\\\", \\\"remote_current_user\\\": \\\"\${COUCHDB_USER}\\\", \\\"remote_current_password\\\": \\\"\${COUCHDB_PASSWORD}\\\" }\")
                    echo \"Enable cluster on couchdb-\${num} response: \${response}\"
                    response=\$(curl -s -X POST -H 'Content-Type: application/json' \"http://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:5984/_cluster_setup\" -d \"{\\\"action\\\": \\\"add_node\\\", \\\"host\\\":\\\"couchdb-\${num}.default.svc.cluster.local\\\", \\\"port\\\": 5984, \\\"username\\\": \\\"\${COUCHDB_USER}\\\", \\\"password\\\":\\\"\${COUCHDB_PASSWORD}\\\"}\")
                    echo \"Adding node couchdb-\${num} response: \${response}\"
                  done &&
                  echo \"Finishing cluster setup\" &&
                  response=\$(curl -s -X POST -H 'Content-Type: application/json' \"http://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:5984/_cluster_setup\" -d \"{\\\"action\\\": \\\"finish_cluster\\\"}\") &&
                  echo \"Finish cluster response: \${response}\" &&
                  echo \"Checking cluster membership\" &&
                  membership=\$(curl -s -X GET \"http://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:5984/_membership\") &&
                  echo \"Cluster membership: \${membership}\" &&
                  echo \"Creating \${RESOURCE_REGISTRY_DB}, \${TASK_DATA_DB} and \${SCHEDULES_DB} database on all nodes\" &&
                  response=\$(curl -s -X PUT \"http://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:5984/\${RESOURCE_REGISTRY_DB}\") &&
                  echo \"Creating \${RESOURCE_REGISTRY_DB} on couchdb-0 response: \${response}\" &&
                  response=\$(curl -s -X PUT \"http://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:5984/\${SCHEDULES_DB}\") &&
                  echo \"Creating \${SCHEDULES_DB} on couchdb-0 response: \${response}\" &&
                  response=\$(curl -s -X PUT \"http://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:5984/\${TASK_DATA_DB}\") &&
                  echo \"Creating \${TASK_DATA_DB} on couchdb-0 response: \${response}\" &&
                  echo \"Waiting for \${RESOURCE_REGISTRY_DB}, \${TASK_DATA_DB} and \${SCHEDULES_DB} to be available on all nodes\" &&
                  for db in \${RESOURCE_REGISTRY_DB} \${SCHEDULES_DB} \${TASK_DATA_DB}; do
                    for i in \$(seq 0 $((num_fog_nodes-1))); do
                      until curl -s \"http://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${i}.default.svc.cluster.local:5984/\${db}\" | grep -q \"\${db}\"; do
                        echo \"Waiting for \${db} on couchdb-\${i}...\"
                        sleep 5
                      done
                      echo \"\${db} is available on couchdb-\${i}\"
                    done
                  done &&
                  echo \"CouchDB cluster setup completed and \${RESOURCE_REGISTRY_DB}, \${SCHEDULES_DB} & \${TASK_DATA_DB} is available on all nodes\"
              env:
                - name: RESOURCE_REGISTRY_DB
                  value: \"resource_registry\"
                - name: SCHEDULES_DB
                  value: \"schedules\"
                - name: TASK_DATA_DB
                  value: \"task_data\"
                - name: COUCHDB_USER
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_USER
                - name: COUCHDB_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_PASSWORD"

    echo "$yaml_content"
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

# Part 2: Generate YAML file for config and secrets
generated_keys=$(generate_pbft_keys "$num_fog_nodes")

mkdir -p kubernetes-manifests/generated

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

echo "Generated YAML file for config and secrets has been saved to kubernetes-manifests/generated/config-and-secrets.yaml"

# Part 3: Generate CouchDB cluster deployment YAML
couchdb_yaml=$(generate_couchdb_yaml "$num_fog_nodes")

# Save the generated CouchDB YAML to a file
echo "$couchdb_yaml" > kubernetes-manifests/generated/couchdb-cluster-deployment.yaml

echo "Generated CouchDB cluster deployment YAML has been saved to kubernetes-manifests/generated/couchdb-cluster-deployment.yaml"

echo "Script execution completed successfully."