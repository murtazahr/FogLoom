#!/bin/bash

# CouchDB cluster configuration
NODES=("couch-db-0" "couch-db-1" "couch-db-2" "couch-db-3" "couch-db-4")
PORT=5984
DB_NAME="resource_registry"

# Function to setup bidirectional replication between two nodes
setup_bidirectional_replication() {
    local source=$1
    local target=$2

    # Setup replication from source to target
    curl -X POST "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${source}:${PORT}/_replicator" \
         -H "Content-Type: application/json" \
         -d "{
              \"_id\": \"${source}_to_${target}_${DB_NAME}\",
              \"source\": \"http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${source}:${PORT}/${DB_NAME}\",
              \"target\": \"http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${target}:${PORT}/${DB_NAME}\",
              \"continuous\": true
             }"

    # Setup replication from target to source
    curl -X POST "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${target}:${PORT}/_replicator" \
         -H "Content-Type: application/json" \
         -d "{
              \"_id\": \"${target}_to_${source}_${DB_NAME}\",
              \"source\": \"http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${target}:${PORT}/${DB_NAME}\",
              \"target\": \"http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${source}:${PORT}/${DB_NAME}\",
              \"continuous\": true
             }"
}

# Create the database on all nodes
for node in "${NODES[@]}"; do
    curl -X PUT "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${node}:${PORT}/${DB_NAME}"
done

# Setup bidirectional replication between all node pairs
for ((i=0; i<${#NODES[@]}; i++)); do
    for ((j=i+1; j<${#NODES[@]}; j++)); do
        setup_bidirectional_replication "${NODES[i]}" "${NODES[j]}"
    done
done

echo "Replication setup completed."