#!/bin/bash

# Function to wait for a job to complete
wait_for_job() {
    local job_prefix=$1
    echo "Waiting for job $job_prefix to complete..."
    while true; do
        job_name=$(kubectl get jobs --selector=job-name=$job_prefix -o jsonpath='{.items[*].metadata.name}')
        if [ -n "$job_name" ]; then
            status=$(kubectl get job $job_name -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}')
            if [ "$status" == "True" ]; then
                echo "Job $job_name completed successfully."
                break
            fi
        fi
        sleep 5
    done
}

# Function to check if a node exists in the cluster
check_node_exists() {
    local node_name=$1
    kubectl get nodes | grep -q "$node_name"
    return $?
}

generate_ssl_certificates() {
    local num_nodes=$1
    local cert_dir="kubernetes-manifests/generated/certs"

    mkdir -p "$cert_dir"

    # Generate CA key and certificate
    openssl genrsa -out "$cert_dir/ca.key" 4096
    openssl req -new -x509 -key "$cert_dir/ca.key" -out "$cert_dir/ca.crt" -days 365 -subj "/CN=CouchDB CA"

    # Generate certificates for each node
    for ((i=0; i<num_nodes; i++)); do
        openssl genrsa -out "$cert_dir/node$i.key" 2048
        openssl req -new -key "$cert_dir/node$i.key" -out "$cert_dir/node$i.csr" -subj "/CN=couchdb-$i.default.svc.cluster.local"
        openssl x509 -req -in "$cert_dir/node$i.csr" -CA "$cert_dir/ca.crt" -CAkey "$cert_dir/ca.key" -CAcreateserial -out "$cert_dir/node$i.crt" -days 365
    done

    # Create Kubernetes secret for certificates
    kubectl create secret generic couchdb-certs \
        --from-file="$cert_dir/ca.crt" \
        $(for ((i=0; i<num_nodes; i++)); do echo "--from-file=node${i}_crt=$cert_dir/node$i.crt --from-file=node${i}_key=$cert_dir/node$i.key"; done)
}

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
            kubernetes.io/hostname: fog-node-$((i+1))
          initContainers:
            - name: init-config
              image: busybox
              command: ['sh', '-c', 'cp /tmp/couchdb-config/* /opt/couchdb/etc/local.d/ && echo \"Config files copied\" && ls -la /opt/couchdb/etc/local.d']
              volumeMounts:
                - name: couchdb-config
                  mountPath: /tmp/couchdb-config
                - name: config-storage
                  mountPath: /opt/couchdb/etc/local.d
          containers:
            - name: couchdb
              image: couchdb:3
              command: ["/bin/bash", "-c"]
              args:
                - |
                  echo "Starting CouchDB with verbose logging"
                  echo "Debugging: Listing /opt/couchdb/etc/local.d"
                  ls -la /opt/couchdb/etc/local.d
                  echo "Debugging: Contents of ssl.ini"
                  cat /opt/couchdb/etc/local.d/ssl.ini
                  echo "Debugging: Listing /opt/couchdb/certs"
                  ls -la /opt/couchdb/certs
                  echo "Debugging: Environment variables"
                  env | grep COUCH
                  /opt/couchdb/bin/couchdb -couch_ini /opt/couchdb/etc/default.ini /opt/couchdb/etc/local.ini /opt/couchdb/etc/local.d/ssl.ini -vv
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
                - name: COUCHDB_NODE_ID
                  value: \"${i}\"
              volumeMounts:
                - name: couchdb-data
                  mountPath: /opt/couchdb/data
                - name: config-storage
                  mountPath: /opt/couchdb/etc/local.d
                - name: couchdb-certs
                  mountPath: /opt/couchdb/certs
              readinessProbe:
                httpGet:
                  path: /
                  port: 6984
                  scheme: HTTPS
                initialDelaySeconds: 30
                periodSeconds: 10
                failureThreshold: 3
            - name: logger
              image: busybox
              command: ["/bin/sh", "-c"]
              args:
                - |
                  mkdir -p /opt/couchdb/log
                  touch /opt/couchdb/log/couch.log
                  tail -f /opt/couchdb/log/* /opt/couchdb/etc/local.d/*
              volumeMounts:
                - name: couchdb-data
                  mountPath: /opt/couchdb/log
                - name: config-storage
                  mountPath: /opt/couchdb/etc/local.d
          volumes:
            - name: couchdb-data
              persistentVolumeClaim:
                claimName: couchdb${i}-data
            - name: couchdb-config
              configMap:
                name: couchdb-config
            - name: config-storage
              emptyDir: {}
            - name: couchdb-certs
              secret:
                secretName: couchdb-certs"
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
        - name: http
          port: 5984
          targetPort: 5984
        - name: https
          port: 6984
          targetPort: 6984"
    done

    # Generate ConfigMap for CouchDB configuration
    yaml_content+="
  - apiVersion: v1
    kind: ConfigMap
    metadata:
      name: couchdb-config
    data:
      ssl.ini: |
        [ssl]
        enable = true
        cert_file = /opt/couchdb/certs/node\${COUCHDB_NODE_ID}_crt
        key_file = /opt/couchdb/certs/node\${COUCHDB_NODE_ID}_key
        cacert_file = /opt/couchdb/certs/ca.crt
        verify_ssl = false

        [chttpd]
        bind_address = 0.0.0.0
        port = 6984

        [httpd]
        bind_address = 0.0.0.0
        port = 5984

        [couch_httpd_auth]
        require_valid_user = true"

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
                  for i in \$(seq 0 $((num_fog_nodes-1))); do
                    echo \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${i}.default.svc.cluster.local:6984\"
                    until curl -k -s \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${i}.default.svc.cluster.local:6984\" > /dev/null; do
                      echo \"Waiting for CouchDB on couchdb-\${i} to be ready...\"
                      sleep 5
                    done
                    echo \"CouchDB on couchdb-\${i} is ready\"
                  done &&
                  echo \"Adding nodes to the cluster\" &&
                  for num in \$(seq 1 $((num_fog_nodes-1))); do
                    response=\$(curl -k -X POST -H 'Content-Type: application/json' \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:6984/_cluster_setup\" -d \"{\\\"action\\\": \\\"enable_cluster\\\", \\\"bind_address\\\":\\\"0.0.0.0\\\", \\\"username\\\": \\\"\${COUCHDB_USER}\\\", \\\"password\\\":\\\"\${COUCHDB_PASSWORD}\\\", \\\"port\\\": 6984, \\\"node_count\\\": \\\"$num_fog_nodes\\\", \\\"remote_node\\\": \\\"couchdb-\${num}.default.svc.cluster.local\\\", \\\"remote_current_user\\\": \\\"\${COUCHDB_USER}\\\", \\\"remote_current_password\\\": \\\"\${COUCHDB_PASSWORD}\\\" }\")
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
                    for i in \$(seq 0 $((num_fog_nodes-1))); do
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

# Function to generate blockchain network deployment YAML
generate_blockchain_network_yaml() {
    local num_fog_nodes=$1
    local num_iot_nodes=$2
    local yaml_content="apiVersion: v1
kind: List

items:"

    # Generate Fog Node Deployments and Services
    for ((i=0; i<num_fog_nodes; i++)); do
        local hostname="fog-node-$((i+1))"
        local deployment_name="pbft-$i"
        local service_name="sawtooth-$i"

        yaml_content+="
  # --------------------------=== $hostname ===--------------------------

  - apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: $deployment_name
    spec:
      selector:
        matchLabels:
          name: $deployment_name
      replicas: 1
      template:
        metadata:
          labels:
            name: $deployment_name
        spec:
          nodeSelector:
            kubernetes.io/hostname: $hostname
          volumes:
            - name: proc
              hostPath:
                path: /proc
            - name: sys
              hostPath:
                path: /sys
          initContainers:
            - name: wait-for-registry
              image: busybox
              command: [ 'sh', '-c', 'until nc -z sawtooth-registry 5000; do echo waiting for sawtooth-registry; sleep 2; done;' ]
            - name: wait-for-couchdb-setup
              image: curlimages/curl:latest
              command:
                - 'sh'
                - '-c'
                - |
                  for db in \${RESOURCE_REGISTRY_DB} \${TASK_DATA_DB}; do
                    for i in \$(seq 0 $((num_fog_nodes-1))); do
                      until curl -s \"http://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${i}.default.svc.cluster.local:5984/\${db}\" | grep -q \"\${db}\"; do
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
                      key: COUCHDB_PASSWORD
          containers:
            - name: peer-registry-tp
              image: murtazahr/peer-registry-tp:latest
              env:
                - name: MAX_UPDATES_PER_NODE
                  value: \"100\"
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"

            - name: docker-image-tp
              image: murtazahr/docker-image-tp:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"

            - name: dependency-management-tp
              image: murtazahr/dependency-management-tp:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"

            - name: schedule-status-update-tp
              image: murtazahr/schedule-status-update-tp:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"

            - name: iot-data-tp
              image: murtazahr/iot-data-tp:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"
                - name: COUCHDB_HOST
                  value: \"couchdb-$i.default.svc.cluster.local:5984\"
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
                - name: REDIS_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: redis-password
                      key: password
                - name: REDIS_SSL_CERT
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: redis.crt
                - name: REDIS_SSL_KEY
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: redis.key
                - name: REDIS_SSL_CA
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: ca.crt

            - name: scheduling-tp
              image: murtazahr/scheduling-tp:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"
                - name: REDIS_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: redis-password
                      key: password
                - name: REDIS_SSL_CERT
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: redis.crt
                - name: REDIS_SSL_KEY
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: redis.key
                - name: REDIS_SSL_CA
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: ca.crt

            - name: sawtooth-pbft-engine
              image: hyperledger/sawtooth-pbft-engine:chime
              command:
                - bash
              args:
                - -c
                - \"pbft-engine -vv --connect tcp://\$HOSTNAME:5050\"

            - name: sawtooth-settings-tp
              image: hyperledger/sawtooth-settings-tp:chime
              command:
                - bash
              args:
                - -c
                - \"settings-tp -vv -C tcp://\$HOSTNAME:4004\"

            - name: sawtooth-rest-api
              image: hyperledger/sawtooth-rest-api:chime
              ports:
                - name: api
                  containerPort: 8008
              command:
                - bash
              args:
                - -c
                - \"sawtooth-rest-api -vv -C tcp://\$HOSTNAME:4004 -B 0.0.0.0:8008\"
              readinessProbe:
                httpGet:
                  path: /status
                  port: 8008
                initialDelaySeconds: 15
                periodSeconds: 10

            - name: sawtooth-shell
              image: hyperledger/sawtooth-shell:chime
              command:
                - bash
              args:
                - -c
                - \"sawtooth keygen && tail -f /dev/null\"

            - name: fog-node
              image: murtazahr/fog-node:latest
              securityContext:
                privileged: true
              volumeMounts:
                - name: proc
                  mountPath: /host/proc
                  readOnly: true
                - name: sys
                  mountPath: /host/sys
                  readOnly: true
              env:
                - name: REGISTRY_URL
                  value: \"sawtooth-registry:5000\"
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"
                - name: NODE_ID
                  value: \"sawtooth-fog-node-$i\"
                - name: COUCHDB_HOST
                  value: \"couchdb-$i.default.svc.cluster.local:5984\"
                - name: RESOURCE_UPDATE_INTERVAL
                  value: \"300\"
                - name: RESOURCE_UPDATE_BATCH_SIZE
                  value: \"10\"
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
                - name: REDIS_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: redis-password
                      key: password
                - name: REDIS_SSL_CERT
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: redis.crt
                - name: REDIS_SSL_KEY
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: redis.key
                - name: REDIS_SSL_CA
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: ca.crt
                - name: IS_NEW_ADDITION
                  value: \"false\"

            - name: sawtooth-validator
              image: hyperledger/sawtooth-validator:chime
              ports:
                - name: tp
                  containerPort: 4004
                - name: consensus
                  containerPort: 5050
                - name: validators
                  containerPort: 8800"

        if [ "$i" -eq 0 ]; then
            local pbft_members=$(for ((j=0; j<num_fog_nodes; j++)); do
                echo -n "\\\"\$pbft${j}pub\\\""
                if [ $j -lt $((num_fog_nodes-1)) ]; then
                    echo -n ","
                fi
            done)

            yaml_content+="
              envFrom:
                - configMapRef:
                    name: keys-config
              command:
                - bash
              args:
                - -c
                - |
                  if [ ! -e /etc/sawtooth/keys/validator.priv ]; then
                    echo \$pbft0priv > /etc/sawtooth/keys/validator.priv
                    echo \$pbft0pub > /etc/sawtooth/keys/validator.pub
                  fi &&
                  if [ ! -e /root/.sawtooth/keys/my_key.priv ]; then
                    sawtooth keygen my_key
                  fi &&
                  if [ ! -e config-genesis.batch ]; then
                    sawset genesis -k /root/.sawtooth/keys/my_key.priv -o config-genesis.batch
                  fi &&
                  sleep 30 &&
                  echo sawtooth.consensus.pbft.members=[\"${pbft_members}\"] &&
                  if [ ! -e config.batch ]; then
                    sawset proposal create -k /root/.sawtooth/keys/my_key.priv sawtooth.consensus.algorithm.name=pbft sawtooth.consensus.algorithm.version=1.0 sawtooth.consensus.pbft.members=[\"${pbft_members}\"] sawtooth.publisher.max_batches_per_block=1200 sawtooth.validator.state_pruning_enabled=true sawtooth.validator.state_pruning_block_depth=1000 sawtooth.validator.state_pruning_grace_period=10 sawtooth.validator.max_database_size_mb=3096 sawtooth.validator.state_pruning_check_interval=100 -o config.batch
                  fi && if [ ! -e /var/lib/sawtooth/genesis.batch ]; then
                    sawadm genesis config-genesis.batch config.batch
                  fi &&
                  sawtooth-validator -vv --endpoint tcp://\$SAWTOOTH_0_SERVICE_HOST:8800 --bind component:tcp://eth0:4004 --bind consensus:tcp://eth0:5050 --bind network:tcp://eth0:8800 --scheduler parallel --peering static --maximum-peer-connectivity 10000"
        else
            yaml_content+="
              env:
                - name: pbft${i}priv
                  valueFrom:
                    configMapKeyRef:
                      name: keys-config
                      key: pbft${i}priv
                - name: pbft${i}pub
                  valueFrom:
                    configMapKeyRef:
                      name: keys-config
                      key: pbft${i}pub
              command:
                - bash
              args:
                - -c
                - |
                  if [ ! -e /etc/sawtooth/keys/validator.priv ]; then
                    echo \$pbft${i}priv > /etc/sawtooth/keys/validator.priv
                    echo \$pbft${i}pub > /etc/sawtooth/keys/validator.pub
                  fi &&
                  sawtooth keygen my_key &&
                  sawtooth-validator -vv --endpoint tcp://\$SAWTOOTH_${i}_SERVICE_HOST:8800 --bind component:tcp://eth0:4004 --bind consensus:tcp://eth0:5050 --bind network:tcp://eth0:8800 --scheduler parallel --peering static --maximum-peer-connectivity 10000 $(for ((j=0; j<i; j++)); do echo -n "--peers tcp://\$SAWTOOTH_${j}_SERVICE_HOST:8800 "; done)"
        fi

        yaml_content+="

  - apiVersion: v1
    kind: Service
    metadata:
      name: $service_name
    spec:
      type: ClusterIP
      selector:
        name: $deployment_name
      ports:
        - name: \"4004\"
          protocol: TCP
          port: 4004
          targetPort: 4004
        - name: \"5050\"
          protocol: TCP
          port: 5050
          targetPort: 5050
        - name: \"8008\"
          protocol: TCP
          port: 8008
          targetPort: 8008
        - name: \"8080\"
          protocol: TCP
          port: 8080
          targetPort: 8080
        - name: \"5000\"
          protocol: TCP
          port: 5000
          targetPort: 5000
        - name: \"8800\"
          protocol: TCP
          port: 8800
          targetPort: 8800"
    done

    # Generate IoT Node Deployments
    for ((i=0; i<num_iot_nodes; i++)); do
        local hostname="iot-node-$((i+1))"
        local deployment_name="iot-$i"
        local service_name="iot-$i"

        yaml_content+="

  # -------------------------=== iot-node-$((i+1)) ===------------------

  - apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: $deployment_name
    spec:
      selector:
        matchLabels:
          name: $deployment_name
      replicas: 1
      template:
        metadata:
          labels:
            name: $deployment_name
        spec:
          nodeSelector:
            kubernetes.io/hostname: iot-node-$((i+1))
          containers:
            - name: iot-node
              image: murtazahr/iot-node:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://sawtooth-0:4004\"
                - name: IOT_URL
                  value: \"tcp://$service_name\"

  - apiVersion: v1
    kind: Service
    metadata:
      name: $service_name
    spec:
      type: ClusterIP
      selector:
        name: $deployment_name
      ports:
        - name: \"5555\"
          protocol: TCP
          port: 5555
          targetPort: 5555"

    done

    # Add Client Console Deployment
    yaml_content+="

  # -------------------------=== client-console ===------------------

  - apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: network-management-console
    spec:
      selector:
        matchLabels:
          name: network-management-console
      replicas: 1
      template:
        metadata:
          labels:
            name: network-management-console
        spec:
          nodeSelector:
            kubernetes.io/hostname: client-console
          containers:
            - name: application-deployment-client
              image: murtazahr/docker-image-client:latest
              securityContext:
                privileged: true
              env:
                - name: REGISTRY_URL
                  value: \"sawtooth-registry:5000\"
                - name: VALIDATOR_URL
                  value: \"tcp://sawtooth-0:4004\"

            - name: dependency-management-client
              image: murtazahr/dependency-management-client:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://sawtooth-0:4004\"

            - name: scheduling-client
              image: murtazahr/scheduling-client:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://sawtooth-0:4004\""

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

# Generate SSL/TLS certificates
generate_ssl_certificates "$num_fog_nodes"

# Part 2: Create redis cluster
mkdir -p kubernetes-manifests/generated

/bin/bash ./redis-setup.sh

# Part 3: Generate YAML file for config and secrets
# Create the PBFT key generation job YAML
cat << EOF > kubernetes-manifests/generated/pbft-key-generation-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: pbft-keys
spec:
  template:
    metadata:
      labels:
        job-name: pbft-keys
    spec:
      containers:
        - name: pbft-keys-generator
          image: hyperledger/sawtooth-shell
          command:
            - bash
          args:
            - -c
            - "for i in {0..$(($num_fog_nodes-1))}; do sawadm keygen -q pbft\${i}; done && cd /etc/sawtooth/keys/ && grep '' * | sed 's/\\\\.//' | sed 's/:/:\ /'"
      restartPolicy: Never
  backoffLimit: 4
EOF

echo "Generated PBFT key generation job YAML has been saved to kubernetes-manifests/generated/pbft-key-generation-job.yaml"

# Apply the job YAML
kubectl apply -f kubernetes-manifests/generated/pbft-key-generation-job.yaml

# Wait for the job to complete
wait_for_job pbft-keys

# Get the pod name
pod_name=$(kubectl get pods --selector=job-name=pbft-keys --output=jsonpath='{.items[*].metadata.name}')

# Fetch the keys from the pod logs
generated_keys=$(kubectl logs "$pod_name")

# Delete the job YAML
kubectl delete -f kubernetes-manifests/generated/pbft-key-generation-job.yaml

echo "PBFT key generation job has been deleted."

# Process the generated keys to add proper indentation
indented_keys=$(echo "$generated_keys" | sed 's/^/      /')

# Create the config and secrets YAML
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
$indented_keys
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

# Part 4: Generate CouchDB cluster deployment YAML
couchdb_yaml=$(generate_couchdb_yaml "$num_fog_nodes")

# Save the generated CouchDB YAML to a file
echo "$couchdb_yaml" > kubernetes-manifests/generated/couchdb-cluster-deployment.yaml

echo "Generated CouchDB cluster deployment YAML has been saved to kubernetes-manifests/generated/couchdb-cluster-deployment.yaml"

# Part 5: Generate blockchain network deployment YAML
blockchain_network_yaml=$(generate_blockchain_network_yaml "$num_fog_nodes" "$num_iot_nodes")

# Save the generated blockchain network YAML to a file
echo "$blockchain_network_yaml" > kubernetes-manifests/generated/blockchain-network-deployment.yaml

echo "Generated blockchain network deployment YAML has been saved to kubernetes-manifests/generated/blockchain-network-deployment.yaml"

# Part 6: deploy network
echo "Deploying Network"
# Apply to kubernetes environment.
kubectl apply -f kubernetes-manifests/generated/config-and-secrets.yaml
kubectl apply -f kubernetes-manifests/generated/couchdb-cluster-deployment.yaml
kubectl apply -f kubernetes-manifests/static/local-docker-registry-deployment.yaml
kubectl apply -f kubernetes-manifests/generated/blockchain-network-deployment.yaml

echo "Script execution completed successfully."