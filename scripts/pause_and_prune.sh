#!/usr/bin/env bash
set -euo pipefail

CONTROL_DIR="${CONTROL_DIR:-/app/control}"
FLAG="${CONTROL_DIR}/PRUNE_REQUEST"
STATUS="${CONTROL_DIR}/PRUNE_STATUS"

echo "[PRUNE-ORCH] requesting pause"
mkdir -p "$CONTROL_DIR"
touch "$FLAG"

# wait until app pauses the stream
echo -n "[PRUNE-ORCH] waiting for PAUSED"
for i in {1..180}; do
  if [[ -f "$STATUS" ]] && grep -q "PAUSED" "$STATUS"; then
    echo " -> PAUSED"
    break
  fi
  echo -n "."
  sleep 1
done

# run pruning (inside app container context)
echo "[PRUNE-ORCH] running prune job"
python -m maintenance.prune_bronze

# allow a beat for metastore/file handles to settle
sleep 2

# resume
echo "[PRUNE-ORCH] resuming stream"
rm -f "$FLAG"

# (optional) wait for RUNNING
echo -n "[PRUNE-ORCH] waiting for RUNNING"
for i in {1..60}; do
  if [[ -f "$STATUS" ]] && grep -q "RUNNING" "$STATUS"; then
    echo " -> RUNNING"
    exit 0
  fi
  echo -n "."
  sleep 1
done

echo
echo "[PRUNE-ORCH] warning: did not observe RUNNING; the watchdog will resume shortly."
exit 0
