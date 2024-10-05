#!/bin/bash

# Check if the number of nodes is provided as an argument
if [ $# -eq 0 ]; then
    echo "Error: Number of nodes not provided"
    echo "Usage: $0 <number_of_nodes>"
    exit 1
fi

num_nodes=$1

# Function to generate SSL certificates
generate_ssl_certificates() {
    local num_nodes=$1
    mkdir -p certs

    # Generate CA key and certificate
    openssl genrsa -out certs/ca.key 2048
    openssl req -x509 -new -nodes -key certs/ca.key -sha256 -days 1024 -out certs/ca.crt -subj "/CN=CouchDB-CA"

    # Generate certificates for each node
    for i in $(seq 0 $((num_nodes-1))); do
        openssl genrsa -out certs/couchdb-$i.key 2048
        openssl req -new -key certs/couchdb-$i.key -out certs/couchdb-$i.csr -subj "/CN=couchdb-$i"
        openssl x509 -req -in certs/couchdb-$i.csr -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/couchdb-$i.crt -days 365 -sha256
    done
}

# Function to generate CouchDB cluster deployment YAML
generate_couchdb_yaml() {
    local num_nodes=$1
    local yaml_content="apiVersion: v1
kind: List

items:"

    # Generate PVCs
    for ((i=0; i<num_nodes; i++)); do
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

    # Generate Secrets for SSL certificates
    yaml_content+="
  - apiVersion: v1
    kind: Secret
    metadata:
      name: couchdb-certs
    type: Opaque
    data:"

    for ((i=0; i<num_nodes; i++)); do
        yaml_content+="
      couchdb-${i}.crt: $(base64 -w 0 certs/couchdb-$i.crt)
      couchdb-${i}.key: $(base64 -w 0 certs/couchdb-$i.key)"
    done

    yaml_content+="
      ca.crt: $(base64 -w 0 certs/ca.crt)"

    # Generate Deployments
    for ((i=0; i<num_nodes; i++)); do
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
            kubernetes.io/hostname: fog-node-$((i+1))
          containers:
            - name: couchdb
              image: couchdb:3
              ports:
                - containerPort: 5984
                - containerPort: 6984
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
                - name: COUCHDB_ENABLE_CORS
                  value: \"true\"
                - name: COUCHDB_CERT_FILE
                  value: /opt/couchdb/ssl/couchdb-${i}.crt
                - name: COUCHDB_KEY_FILE
                  value: /opt/couchdb/ssl/couchdb-${i}.key
                - name: COUCHDB_CACERT_FILE
                  value: /opt/couchdb/ssl/ca.crt
              volumeMounts:
                - name: couchdb-data
                  mountPath: /opt/couchdb/data
                - name: couchdb-ssl
                  mountPath: /opt/couchdb/ssl
          volumes:
            - name: couchdb-data
              persistentVolumeClaim:
                claimName: couchdb${i}-data
            - name: couchdb-ssl
              secret:
                secretName: couchdb-certs
                items:
                  - key: couchdb-${i}.crt
                    path: couchdb-${i}.crt
                  - key: couchdb-${i}.key
                    path: couchdb-${i}.key
                  - key: ca.crt
                    path: ca.crt"
    done

    # Generate Services
    for ((i=0; i<num_nodes; i++)); do
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
          targetPort: 5984
          name: http
        - port: 6984
          targetPort: 6984
          name: https"
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
                  echo \"Starting CouchDB cluster setup\" &&
                  for i in \$(seq 0 $((num_nodes-1))); do
                    echo \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${i}.default.svc.cluster.local:6984\"
                    until curl -k -s \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${i}.default.svc.cluster.local:6984\" > /dev/null; do
                      echo \"Waiting for CouchDB on couchdb-\${i} to be ready...\"
                      sleep 5
                    done
                    echo \"CouchDB on couchdb-\${i} is ready\"
                  done &&
                  echo \"Adding nodes to the cluster\" &&
                  for num in \$(seq 1 $((num_nodes-1))); do
                    response=\$(curl -k -X POST -H 'Content-Type: application/json' \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:6984/_cluster_setup\" -d \"{\\\"action\\\": \\\"enable_cluster\\\", \\\"bind_address\\\":\\\"0.0.0.0\\\", \\\"username\\\": \\\"\${COUCHDB_USER}\\\", \\\"password\\\":\\\"\${COUCHDB_PASSWORD}\\\", \\\"port\\\": 6984, \\\"node_count\\\": \\\"$num_nodes\\\", \\\"remote_node\\\": \\\"couchdb-\${num}.default.svc.cluster.local\\\", \\\"remote_current_user\\\": \\\"\${COUCHDB_USER}\\\", \\\"remote_current_password\\\": \\\"\${COUCHDB_PASSWORD}\\\" }\")
                    echo \"Enable cluster on couchdb-\${num} response: \${response}\"
                    response=\$(curl -k -s -X POST -H 'Content-Type: application/json' \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:6984/_cluster_setup\" -d \"{\\\"action\\\": \\\"add_node\\\", \\\"host\\\":\\\"couchdb-\${num}.default.svc.cluster.local\\\", \\\"port\\\": 6984, \\\"username\\\": \\\"\${COUCHDB_USER}\\\", \\\"password\\\":\\\"\${COUCHDB_PASSWORD}\\\"}\")
                    echo \"Adding node couchdb-\${num} response: \${response}\"
                  done &&
                  echo \"Finishing cluster setup\" &&
                  response=\$(curl -k -s -X POST -H 'Content-Type: application/json' \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:6984/_cluster_setup\" -d \"{\\\"action\\\": \\\"finish_cluster\\\"}\") &&
                  echo \"Finish cluster response: \${response}\" &&
                  echo \"Checking cluster membership\" &&
                  membership=\$(curl -k -s -X GET \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:6984/_membership\") &&
                  echo \"Cluster membership: \${membership}\" &&
                  echo \"Creating \${RESOURCE_REGISTRY_DB} and \${TASK_DATA_DB} database on all nodes\" &&
                  response=\$(curl -k -s -X PUT \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:6984/\${RESOURCE_REGISTRY_DB}\") &&
                  echo \"Creating \${RESOURCE_REGISTRY_DB} on couchdb-0 response: \${response}\" &&
                  response=\$(curl -k -s -X PUT \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:6984/\${TASK_DATA_DB}\") &&
                  echo \"Creating \${TASK_DATA_DB} on couchdb-0 response: \${response}\" &&
                  echo \"Waiting for \${RESOURCE_REGISTRY_DB} & \${TASK_DATA_DB} to be available on all nodes\" &&
                  for db in \${RESOURCE_REGISTRY_DB} \${TASK_DATA_DB}; do
                    for i in \$(seq 0 $((num_nodes-1))); do
                      until curl -k -s \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${i}.default.svc.cluster.local:6984/\${db}\" | grep -q \"\${db}\"; do
                        echo \"Waiting for \${db} on couchdb-\${i}...\"
                        sleep 5
                      done
                      echo \"\${db} is available on couchdb-\${i}\"
                    done
                  done &&
                  echo \"CouchDB cluster setup completed and \${RESOURCE_REGISTRY_DB} & \${TASK_DATA_DB} is available on all nodes\"
              env:
                - name: RESOURCE_REGISTRY_DB
                  value: \"resource_registry\"
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

# Generate SSL certificates
generate_ssl_certificates "$num_nodes"

# Generate CouchDB cluster deployment YAML
couchdb_yaml=$(generate_couchdb_yaml "$num_nodes")

# Save the generated CouchDB YAML to a file
echo "$couchdb_yaml" > kubernetes-manifests/generated/couchdb-cluster-deployment.yaml

echo "Generated CouchDB cluster deployment YAML has been saved to kubernetes-manifests/generated/couchdb-cluster-deployment.yaml"

# Apply the CouchDB deployment
kubectl apply -f kubernetes-manifests/generated/couchdb-cluster-deployment.yaml

echo "CouchDB cluster setup completed."