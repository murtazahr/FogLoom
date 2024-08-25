#!/bin/sh

# CouchDB cluster configuration
PORT=5984
DB_NAME="resource_registry"

# Function to setup bidirectional replication between two nodes
setup_bidirectional_replication() {
    source=$1
    target=$2

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

sleep 5

for num in 0 1 2 3 4; do
  curl -X POST -H 'Content-Type: application/json' "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couch-db-0:5984/_cluster_setup" -d "{\"action\": \"add_node\", \"host\":\"couch-db-$num\", \"port\": 5984, \"username\": \"${COUCHDB_USER}\", \"password\":\"${COUCHDB_PASSWORD}\"}"
done

curl -X POST -H 'Content-Type: application/json' "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couch-db-0:5984/_cluster_setup" -d "{\"action\": \"finish_cluster\"}"

sleep 3

for num in 0 1 2 3 4; do
    for db in _users _replicator; do
      curl -X PUT "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couch-db-$num:5984/$db"
    done
done

curl -X GET "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couch-db-0:5984/_membership"

# Create the database on all nodes
for node in couch-db-0 couch-db-1 couch-db-2 couch-db-3 couch-db-4; do
    curl -X PUT "http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${node}:${PORT}/${DB_NAME}"
done

# Setup bidirectional replication between all node pairs
for i in 0 1 2 3; do
    for j in $(seq $((i+1)) 4); do
        setup_bidirectional_replication "couch-db-$i" "couch-db-$j"
    done
done

echo "Replication setup completed."