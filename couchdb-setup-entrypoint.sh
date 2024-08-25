#!/bin/sh

# CouchDB cluster configuration
PORT=5984
DB_NAME="resource_registry"

# Function to log messages
log_message() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Function to setup bidirectional replication between two nodes
setup_bidirectional_replication() {
    source=$1
    target=$2

    log_message "Setting up bidirectional replication between ${source} and ${target}"

    # Setup replication from source to target
    response=$(curl -s -X POST "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${source}:${PORT}/_replicator" \
         -H "Content-Type: application/json" \
         -d "{
              \"_id\": \"${source}_to_${target}_${DB_NAME}\",
              \"source\": \"http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${source}:${PORT}/${DB_NAME}\",
              \"target\": \"http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${target}:${PORT}/${DB_NAME}\",
              \"continuous\": true
             }")
    log_message "Replication from ${source} to ${target} response: ${response}"

    # Setup replication from target to source
    response=$(curl -s -X POST "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${target}:${PORT}/_replicator" \
         -H "Content-Type: application/json" \
         -d "{
              \"_id\": \"${target}_to_${source}_${DB_NAME}\",
              \"source\": \"http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${target}:${PORT}/${DB_NAME}\",
              \"target\": \"http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${source}:${PORT}/${DB_NAME}\",
              \"continuous\": true
             }")
    log_message "Replication from ${target} to ${source} response: ${response}"
}

log_message "Starting CouchDB cluster setup"

sleep 5

response=$(curl -X POST -H 'Content-Type: application/json' "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couch-db-0:5984/_cluster_setup/" -d "{\"action\": \"enable_cluster\", \"bind_address\":\"0.0.0.0\", \"username\": \"${COUCHDB_USER}\", \"password\":\"${COUCHDB_PASSWORD}\", \"node_count\":\"5\"}")
log_message "Initializing Cluster with 5 nodes: ${response}"

log_message "Adding nodes to the cluster"
for num in 0 1 2 3 4; do
  response=$(curl -s -X POST -H 'Content-Type: application/json' "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couch-db-0:5984/_cluster_setup" -d "{\"action\": \"add_node\", \"host\":\"couch-db-$num\", \"port\": 5984, \"username\": \"${COUCHDB_USER}\", \"password\":\"${COUCHDB_PASSWORD}\"}")
  log_message "Adding node couch-db-$num response: ${response}"
done

sleep 3
log_message "Finishing cluster setup"
response=$(curl -s -X POST -H 'Content-Type: application/json' "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couch-db-0:5984/_cluster_setup" -d "{\"action\": \"finish_cluster\"}")
log_message "Finish cluster response: ${response}"

sleep 3

log_message "Creating system databases on all nodes"
for num in 0 1 2 3 4; do
    for db in _users _replicator; do
      response=$(curl -s -X PUT "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couch-db-$num:5984/$db")
      log_message "Creating $db on couch-db-$num response: ${response}"
    done
done

log_message "Checking cluster membership"
membership=$(curl -s -X GET "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couch-db-0:5984/_membership")
log_message "Cluster membership: ${membership}"

log_message "Creating ${DB_NAME} database on all nodes"
for node in couch-db-0 couch-db-1 couch-db-2 couch-db-3 couch-db-4; do
    response=$(curl -s -X PUT "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${node}:${PORT}/${DB_NAME}")
    log_message "Creating ${DB_NAME} on ${node} response: ${response}"
done

log_message "Setting up bidirectional replication between all node pairs"
for i in 0 1 2 3; do
    for j in $(seq $((i+1)) 4); do
        setup_bidirectional_replication "couch-db-$i" "couch-db-$j"
    done
done

log_message "Replication setup completed."