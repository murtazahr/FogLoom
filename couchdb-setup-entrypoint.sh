#!/bin/sh

# CouchDB cluster configuration
DB_NAME="resource_registry"

# Function to log messages
log_message() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

log_message "Starting CouchDB cluster setup"

# Wait for all CouchDB instances to be ready
for i in 0 1 2 3 4; do
  until curl -s "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@sawtooth-$i:5984" > /dev/null; do
    log_message "Waiting for CouchDB on sawtooth-$i to be ready..."
    sleep 5
  done
  log_message "CouchDB on sawtooth-$i is ready"
done

log_message "Adding nodes to the cluster"
for num in 1 2 3 4; do
  response=$(curl -X POST -H 'Content-Type: application/json' "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@sawtooth-0:5984/_cluster_setup" -d "{\"action\": \"enable_cluster\", \"bind_address\":\"0.0.0.0\", \"username\": \"${COUCHDB_USER}\", \"password\":\"${COUCHDB_PASSWORD}\", \"port\": 5984, \"node_count\": \"5\", \"remote_node\": \"sawtooth-$num\", \"remote_current_user\": \"${COUCHDB_USER}\", \"remote_current_password\": \"${COUCHDB_PASSWORD}\" }")
  log_message "Enable cluster on sawtooth-$num response: ${response}"
  response=$(curl -s -X POST -H 'Content-Type: application/json' "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@sawtooth-0:5984/_cluster_setup" -d "{\"action\": \"add_node\", \"host\":\"sawtooth-$num\", \"port\": 5984, \"username\": \"${COUCHDB_USER}\", \"password\":\"${COUCHDB_PASSWORD}\"}")
  log_message "Adding node sawtooth-$num response: ${response}"
done

log_message "Finishing cluster setup"
response=$(curl -s -X POST -H 'Content-Type: application/json' "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@sawtooth-0:5984/_cluster_setup" -d "{\"action\": \"finish_cluster\"}")
log_message "Finish cluster response: ${response}"

log_message "Checking cluster membership"
membership=$(curl -s -X GET "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@sawtooth-0:5984/_membership")
log_message "Cluster membership: ${membership}"

log_message "Creating ${DB_NAME} database on all nodes"
response=$(curl -s -X PUT "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@sawtooth-0:5984/${DB_NAME}")
log_message "Creating ${DB_NAME} on sawtooth-0 response: ${response}"

log_message "CouchDB cluster setup completed"