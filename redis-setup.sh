#!/bin/bash

# Function to generate SSL certificates
generate_ssl_certs() {
    mkdir -p ssl
    # Generate CA key and certificate
    openssl genrsa -out ssl/ca.key 4096
    openssl req -x509 -new -nodes -key ssl/ca.key -sha256 -days 1024 -out ssl/ca.crt -subj "/CN=Redis-Cluster-CA"

    # Generate server key and certificate signed by the CA
    openssl genrsa -out ssl/redis.key 2048
    openssl req -new -key ssl/redis.key -out ssl/redis.csr -subj "/CN=redis-cluster"
    openssl x509 -req -in ssl/redis.csr -CA ssl/ca.crt -CAkey ssl/ca.key -CAcreateserial -out ssl/redis.crt -days 365 -sha256

    # Create Kubernetes secret with all certificates
    kubectl create secret generic redis-certificates --from-file=ssl
}

generate_password() {
    openssl rand -base64 32 | tr -d "=+/" | cut -c1-32
}

# Function to create Redis password secret
create_redis_password_secret() {
    local redis_password=$(generate_password)
    kubectl create secret generic redis-password --from-literal=password=$redis_password
    echo "$redis_password"
}

# Updated function to generate Redis Cluster YAML with SSL, AUTH, and CA cert
generate_redis_cluster_yaml() {
    local num_redis_nodes=10
    local redis_password=$1

    cat << EOF
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: redis-cluster-config
data:
  redis.conf: |
    port 0
    tls-port 6379
    tls-cert-file /ssl/redis.crt
    tls-key-file /ssl/redis.key
    tls-ca-cert-file /ssl/ca.crt
    tls-auth-clients no
    tls-replication yes
    tls-cluster yes
    requirepass ${redis_password}
    masterauth ${redis_password}
    protected-mode no
    cluster-enabled yes
    cluster-config-file nodes.conf
    cluster-node-timeout 5000
    appendonly yes
    appendfsync everysec
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis-cluster
spec:
  serviceName: redis-cluster
  replicas: $num_redis_nodes
  selector:
    matchLabels:
      app: redis-cluster
  template:
    metadata:
      labels:
        app: redis-cluster
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchExpressions:
                  - key: app
                    operator: In
                    values:
                      - redis-cluster
              topologyKey: "kubernetes.io/hostname"
      containers:
      - name: redis
        image: redis:6.2
        ports:
        - containerPort: 6379
          name: tls
        command: ["redis-server", "/conf/redis.conf"]
        resources:
          requests:
            cpu: 500m
            memory: 1Gi
          limits:
            cpu: 1
            memory: 2Gi
        volumeMounts:
        - name: conf
          mountPath: /conf
          readOnly: false
        - name: data
          mountPath: /data
        - name: ssl
          mountPath: /ssl
          readOnly: true
      volumes:
      - name: conf
        configMap:
          name: redis-cluster-config
          items:
          - key: redis.conf
            path: redis.conf
      - name: ssl
        secret:
          secretName: redis-certificates
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 5Gi
---
apiVersion: v1
kind: Service
metadata:
  name: redis-cluster
spec:
  selector:
    app: redis-cluster
  clusterIP: None
  ports:
  - port: 6379
    targetPort: 6379
    name: tls
EOF
}

# Main script execution
generate_ssl_certs

# Create Redis password secret and capture the password
redis_password=$(create_redis_password_secret)

# Generate and apply the Redis Cluster YAML
generate_redis_cluster_yaml "$redis_password" > redis-cluster.yaml
kubectl apply -f redis-cluster.yaml

# Wait for all pods to be ready
echo "Waiting for Redis Cluster pods to be ready..."
kubectl wait --for=condition=Ready pod -l app=redis-cluster --timeout=300s

# Get the list of Redis nodes
nodes=$(kubectl get pods -l app=redis-cluster -o jsonpath='{range.items[*]}{.status.podIP}:6379 ')

# Get the Redis password
redis_password=$(kubectl get secret redis-password -o jsonpath="{.data.password}" | base64 --decode)

# Modify the cluster creation command to use TLS
echo "Creating Redis Cluster with 3 shards..."
kubectl exec -it redis-cluster-0 -- redis-cli --tls --cert /ssl/redis.crt --key /ssl/redis.key --cacert /ssl/ca.crt --cluster-yes --cluster create $nodes --cluster-replicas 2 -a $redis_password

# Modify the cluster status verification command
echo "Verifying cluster status..."
kubectl exec -it redis-cluster-0 -- redis-cli --tls --cert /ssl/redis.crt --key /ssl/redis.key --cacert /ssl/ca.crt -a $redis_password cluster info

echo "Secure Redis Cluster setup complete."

# Print connection information
echo "To connect to your Redis Cluster:"
echo "1. Use kubectl port-forward to access a Redis node:"
echo "   kubectl port-forward redis-cluster-0 6379:6379"
echo "2. Then use redis-cli with TLS and password:"
echo "   redis-cli -h 127.0.0.1 -p 6379 --tls --cert ./ssl/redis.crt --key ./ssl/redis.key -a $redis_password"
echo "3. In your application, ensure you're using a Redis client that supports TLS and clustering."
echo "   Configure it with the cluster nodes, TLS certificates, and password."