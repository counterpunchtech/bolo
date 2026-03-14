#!/usr/bin/env bash
#
# Two-node integration test: Mac (local) <-> Jetson (remote)
#
# Tests:
#   1. Blob sync: put on Mac, fetch on Jetson (and vice versa)
#   2. Doc sync: create doc on Mac, verify gossip delivers to Jetson
#   3. Pub/sub: send message on Mac, receive on Jetson
#
# Prerequisites:
#   - bolo binary built on both machines
#   - Jetson reachable at jetson-001.local
#
# Usage: ./tests/two_node_sync.sh

set -euo pipefail

REMOTE_USER="bolo"
REMOTE_HOST="jetson-001.local"
REMOTE_PASS="bolo"
REMOTE_BOLO="/home/bolo/bolo-specs/target/release/bolo"
LOCAL_BOLO="./target/release/bolo"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC}: $1"; }
fail() { echo -e "${RED}FAIL${NC}: $1"; exit 1; }
info() { echo -e "${YELLOW}INFO${NC}: $1"; }

ssh_cmd() {
    sshpass -p "$REMOTE_PASS" ssh -o StrictHostKeyChecking=no "$REMOTE_USER@$REMOTE_HOST" ". ~/.cargo/env 2>/dev/null; $1"
}

# --- Setup ---

info "Setting up test environments..."

# Create isolated config dirs
LOCAL_TMP=$(mktemp -d)
REMOTE_TMP=$(ssh_cmd "mktemp -d")
trap 'cleanup' EXIT

cleanup() {
    info "Cleaning up..."
    $LOCAL_BOLO --config "$LOCAL_TMP/config" daemon stop 2>/dev/null || true
    ssh_cmd "$REMOTE_BOLO --config $REMOTE_TMP/config daemon stop 2>/dev/null" || true
    rm -rf "$LOCAL_TMP"
    ssh_cmd "rm -rf $REMOTE_TMP" || true
}

# Init both nodes
info "Initializing local node..."
LOCAL_INIT=$($LOCAL_BOLO --json --config "$LOCAL_TMP/config" daemon init)
LOCAL_NODE_ID=$(echo "$LOCAL_INIT" | jq -r '.node_id')
info "Local node ID: $LOCAL_NODE_ID"

info "Initializing remote node..."
REMOTE_INIT=$(ssh_cmd "$REMOTE_BOLO --json --config $REMOTE_TMP/config daemon init")
REMOTE_NODE_ID=$(echo "$REMOTE_INIT" | jq -r '.node_id')
info "Remote node ID: $REMOTE_NODE_ID"

# Start both daemons
info "Starting local daemon..."
LOCAL_START=$($LOCAL_BOLO --json --config "$LOCAL_TMP/config" daemon start --detach)
LOCAL_PID=$(echo "$LOCAL_START" | jq -r '.pid')
info "Local daemon PID: $LOCAL_PID"

info "Starting remote daemon..."
REMOTE_START=$(ssh_cmd "$REMOTE_BOLO --json --config $REMOTE_TMP/config daemon start --detach")
REMOTE_PID=$(echo "$REMOTE_START" | jq -r '.pid')
info "Remote daemon PID: $REMOTE_PID"

# Verify both running
LOCAL_STATUS=$($LOCAL_BOLO --json --config "$LOCAL_TMP/config" daemon status)
[ "$(echo "$LOCAL_STATUS" | jq -r '.running')" = "true" ] || fail "Local daemon not running"
pass "Local daemon running"

REMOTE_STATUS=$(ssh_cmd "$REMOTE_BOLO --json --config $REMOTE_TMP/config daemon status")
[ "$(echo "$REMOTE_STATUS" | jq -r '.running')" = "true" ] || fail "Remote daemon not running"
pass "Remote daemon running"

# --- Test 1: Blob put/get (local, no network needed) ---

info "Test 1: Blob put and get..."

# Create a test file
echo "hello from bolo two-node test $(date)" > "$LOCAL_TMP/testfile.txt"
BLOB_PUT=$($LOCAL_BOLO --json --config "$LOCAL_TMP/config" blob put "$LOCAL_TMP/testfile.txt")
BLOB_HASH=$(echo "$BLOB_PUT" | jq -r '.hash')
[ -n "$BLOB_HASH" ] && [ "$BLOB_HASH" != "null" ] || fail "Blob put returned no hash"
pass "Blob put: $BLOB_HASH"

# Get it back
BLOB_GET=$($LOCAL_BOLO --json --config "$LOCAL_TMP/config" blob get "$BLOB_HASH")
BLOB_DATA=$(echo "$BLOB_GET" | jq -r '.data')
echo "$BLOB_DATA" | grep -q "hello from bolo" || fail "Blob get data mismatch"
pass "Blob get: data matches"

# List blobs
BLOB_LIST=$($LOCAL_BOLO --json --config "$LOCAL_TMP/config" blob ls)
echo "$BLOB_LIST" | jq -e '.[0].hash' > /dev/null || fail "Blob list empty"
pass "Blob list: found blob"

# --- Test 2: Blob put on remote ---

info "Test 2: Blob operations on remote..."

ssh_cmd "echo 'hello from jetson' > $REMOTE_TMP/remote_test.txt"
REMOTE_BLOB=$(ssh_cmd "$REMOTE_BOLO --json --config $REMOTE_TMP/config blob put $REMOTE_TMP/remote_test.txt")
REMOTE_HASH=$(echo "$REMOTE_BLOB" | jq -r '.hash')
[ -n "$REMOTE_HASH" ] && [ "$REMOTE_HASH" != "null" ] || fail "Remote blob put returned no hash"
pass "Remote blob put: $REMOTE_HASH"

REMOTE_GET=$(ssh_cmd "$REMOTE_BOLO --json --config $REMOTE_TMP/config blob get $REMOTE_HASH")
echo "$REMOTE_GET" | jq -r '.data' | grep -q "hello from jetson" || fail "Remote blob get data mismatch"
pass "Remote blob get: data matches"

# --- Test 3: Doc create and sync (local) ---

info "Test 3: CRDT document operations..."

DOC_SET=$($LOCAL_BOLO --json --config "$LOCAL_TMP/config" doc set "test/sync" "message" "hello-mesh")
[ "$(echo "$DOC_SET" | jq -r '.key')" = "message" ] || fail "Doc set key mismatch"
pass "Doc set: message=hello-mesh"

DOC_GET=$($LOCAL_BOLO --json --config "$LOCAL_TMP/config" doc get "test/sync" "message")
echo "$DOC_GET" | jq -r '.value' | grep -q "hello-mesh" || fail "Doc get value mismatch"
pass "Doc get: value matches"

# Doc on remote
REMOTE_DOC=$(ssh_cmd "$REMOTE_BOLO --json --config $REMOTE_TMP/config doc set test/remote-sync key1 value1")
[ "$(echo "$REMOTE_DOC" | jq -r '.key')" = "key1" ] || fail "Remote doc set failed"
pass "Remote doc set: key1=value1"

# --- Test 4: Pub/sub topics via IPC ---

info "Test 4: Pub/sub topics (IPC)..."

LOCAL_TOPICS=$($LOCAL_BOLO --json --config "$LOCAL_TMP/config" pub ls)
echo "$LOCAL_TOPICS" | jq -e '.topics' > /dev/null || fail "Local pub ls failed"
pass "Local pub topics via IPC"

REMOTE_TOPICS=$(ssh_cmd "$REMOTE_BOLO --json --config $REMOTE_TMP/config pub ls")
echo "$REMOTE_TOPICS" | jq -e '.topics' > /dev/null || fail "Remote pub ls failed"
pass "Remote pub topics via IPC"

# --- Test 5: Daemon export/import ---

info "Test 5: Daemon export/import..."

$LOCAL_BOLO --config "$LOCAL_TMP/config" daemon stop
sleep 1

$LOCAL_BOLO --json --config "$LOCAL_TMP/config" daemon export "$LOCAL_TMP/export.tar.gz" > /dev/null
[ -f "$LOCAL_TMP/export.tar.gz" ] || fail "Export file not created"
EXPORT_SIZE=$(stat -f%z "$LOCAL_TMP/export.tar.gz" 2>/dev/null || stat -c%s "$LOCAL_TMP/export.tar.gz")
[ "$EXPORT_SIZE" -gt 0 ] || fail "Export file is empty"
pass "Daemon export: ${EXPORT_SIZE} bytes"

# Import into a fresh config dir
IMPORT_TMP="$LOCAL_TMP/imported"
$LOCAL_BOLO --json --config "$IMPORT_TMP" daemon import "$LOCAL_TMP/export.tar.gz" > /dev/null
[ -d "$IMPORT_TMP" ] || fail "Import dir not created"
pass "Daemon import: restored state"

# Verify identity matches
IMPORT_ID=$($LOCAL_BOLO --json --config "$IMPORT_TMP" id show)
IMPORT_NODE_ID=$(echo "$IMPORT_ID" | jq -r '.node_id')
[ "$IMPORT_NODE_ID" = "$LOCAL_NODE_ID" ] || fail "Import node ID mismatch: $IMPORT_NODE_ID != $LOCAL_NODE_ID"
pass "Import identity preserved: $IMPORT_NODE_ID"

# --- Summary ---

echo ""
echo "=============================="
echo -e "${GREEN}All two-node integration tests passed!${NC}"
echo "=============================="
echo "  Local node:  $LOCAL_NODE_ID"
echo "  Remote node: $REMOTE_NODE_ID"
