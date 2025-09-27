#!/usr/bin/env bash
set -euo pipefail

# Ensure these exist on the mounted /data and are writable for all Spark containers
mkdir -p /data/bronze /data/raw_vault /data/checkpoints /data/spark/warehouse

# If different Spark containers use different UIDs, broad perms avoids cross-user failures
chmod -R 0777 /data

# Drop to appuser (185:185) to run the app
exec gosu 185:185 "$@"