#!/bin/sh
set -e

# Run the main command
"$@"

# Signal Completion
echo "CouchDB setup completed" > /shared/couch_db_setup_done

# Keep container running to maintain the shared volume
tail -f /dev/null