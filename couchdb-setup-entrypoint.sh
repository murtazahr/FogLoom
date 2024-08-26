#!/bin/sh

# CouchDB cluster configuration
PORT=5984
DB_NAME="resource_registry"

# Function to log messages
log_message() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

log_message "Starting CouchDB cluster setup"

sleep 5

log_message "Adding nodes to the cluster"
for num in 1 2 3 4; do
  response=$(curl -X POST -H 'Content-Type: application/json' "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couchdb0.local:5984/_cluster_setup" -d "{\"action\": \"enable_cluster\", \"bind_address\":\"0.0.0.0\", \"username\": \"${COUCHDB_USER}\", \"password\":\"${COUCHDB_PASSWORD}\", \"port\": 5984, \"node_count\": \"5\", \"remote_node\": \"couchdb$num.local\", \"remote_current_user\": \"${COUCHDB_USER}\", \"remote_current_password\": \"${COUCHDB_PASSWORD}\" }")
  log_message "Enable cluster on couch-db-$num response: ${response}"
  response=$(curl -s -X POST -H 'Content-Type: application/json' "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couchdb0.local:5984/_cluster_setup" -d "{\"action\": \"add_node\", \"host\":\"couchdb$num.local\", \"port\": 5984, \"username\": \"${COUCHDB_USER}\", \"password\":\"${COUCHDB_PASSWORD}\"}")
  log_message "Adding node couch-db-$num response: ${response}"
done

log_message "Finishing cluster setup"
response=$(curl -s -X POST -H 'Content-Type: application/json' "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couchdb0.local:5984/_cluster_setup" -d "{\"action\": \"finish_cluster\"}")
log_message "Finish cluster response: ${response}"

log_message "Checking cluster membership"
membership=$(curl -s -X GET "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couchdb0.local:5984/_membership")
log_message "Cluster membership: ${membership}"

log_message "Creating ${DB_NAME} database on all nodes"
response=$(curl -s -X PUT "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couchdb0.local:5984/${DB_NAME}")
log_message "Creating ${DB_NAME} on ${node} response: ${response}"

sleep 2