#!/bin/bash

set -euo pipefail

usage() {
  echo "Usage: $0 <function_name> <node{1|2|3|4}> [host_port=auto] [function_type] [function_category] [function_step]" >&2
  echo "Example: $0 recognizer__adult node2 8001 recognizer ml inference" >&2
  echo "Note: If function_type/category not provided, extracts parent dir from function_name (e.g., recognizer__adult -> recognizer)" >&2
}

if [ $# -lt 2 ]; then
  usage
  exit 1
fi

FUNCTION_NAME="$1"
TARGET_NODE="$2"      # node1|node2|node3|node4
HOST_PORT="${3:-auto}"

# Extract parent directory before __ using regex
if [[ "$FUNCTION_NAME" =~ ^([^_]+)__ ]]; then
  PARENT_DIR="${BASH_REMATCH[1]}"
else
  PARENT_DIR="unknown"
fi

FUNCTION_TYPE="${4:-$PARENT_DIR}"
FUNCTION_CATEGORY="${5:-$PARENT_DIR}"
FUNCTION_STEP="${6:-process}"

BASE_COMPOSE="./docker/docker-compose-full-cluster.yml"

if ! command -v docker &>/dev/null; then
  echo "❌ docker not found" >&2
  exit 1
fi
if ! docker compose version &>/dev/null; then
  echo "❌ docker compose not found" >&2
  exit 1
fi

if [ ! -f "$BASE_COMPOSE" ]; then
  echo "❌ $BASE_COMPOSE not found. Run from repo root." >&2
  exit 1
fi

# # Check if base environment is running
# if ! docker network ls | grep -q "dataflower_shared_network"; then
#   echo "❌ Base environment not running. Please start it first:" >&2
#   echo "   docker compose -f $BASE_COMPOSE up -d" >&2
#   exit 1
# fi

# # Check if target node network exists
# if ! docker network ls | grep -q "dataflower_${TARGET_NODE}_network"; then
#   echo "❌ Target node network 'dataflower_${TARGET_NODE}_network' not found. Please start the base environment:" >&2
#   echo "   docker compose -f $BASE_COMPOSE up -d" >&2
#   exit 1
# fi

if ! docker network ls --format '{{.Name}}' | grep -q "^docker_dataflower-shared$"; then
  echo "❌ Base environment not running. Please start it first:" >&2
  echo "   docker compose -f $BASE_COMPOSE up -d" >&2
  exit 1
fi

if ! docker network ls --format '{{.Name}}' | grep -q "^docker_${TARGET_NODE}-network$"; then
  echo "❌ Target node network 'docker_${TARGET_NODE}-network' not found. Please start the base environment:" >&2
  echo "   docker compose -f $BASE_COMPOSE up -d" >&2
  exit 1
fi

# Validate node
case "$TARGET_NODE" in
  node1|node2|node3|node4) ;;
  *) echo "❌ Invalid node: $TARGET_NODE (expected node1|node2|node3|node4)" >&2; exit 1;;
esac

NODE_NETWORK_NAME="docker_${TARGET_NODE}-network"
SHARED_NETWORK_NAME="docker_dataflower-shared"

# Resolve Dockerfile path - functions are organized as functions/<parent_dir>/<function_name>/Dockerfile
DF_PATH="functions/${PARENT_DIR}/${FUNCTION_NAME}/Dockerfile"
if [ ! -f "$DF_PATH" ]; then
  echo "❌ Dockerfile not found at $DF_PATH" >&2
  exit 1
fi

# Optional port mapping
PORT_MAPPING=""
if [ "$HOST_PORT" != "auto" ]; then
  PORT_MAPPING="      - \"${HOST_PORT}:8000\""
fi

IMAGE_TAG="${FUNCTION_NAME}:latest"

echo "🔍 Extracted parent directory: $PARENT_DIR"
echo "🔨 Building function image: $IMAGE_TAG (Dockerfile: $DF_PATH)"
docker build -t "$IMAGE_TAG" -f "$DF_PATH" .

# Create a temporary override compose file
TMP_DIR=".tmp"
mkdir -p "$TMP_DIR"
OVERRIDE_FILE="$TMP_DIR/docker-compose-${FUNCTION_NAME}.override.yml"

cat > "$OVERRIDE_FILE" <<EOF
services:
  ${FUNCTION_NAME}:
    image: ${IMAGE_TAG}
    build:
      context: .
      dockerfile: ${DF_PATH}
    container_name: ${FUNCTION_NAME}
    volumes:
      - ./functions/${PARENT_DIR}/${FUNCTION_NAME}/main.py:/app/functions/${PARENT_DIR}/${FUNCTION_NAME}/main.py
    environment:
      - NODE_NAME=${TARGET_NODE}
      - FUNCTION_TYPE=${FUNCTION_TYPE}
      - FUNCTION_CATEGORY=${FUNCTION_CATEGORY}
      - FUNCTION_STEP=${FUNCTION_STEP}
    networks:
      - ${TARGET_NODE}_network
      - shared_network
    ${HOST_PORT:+ports:}
${PORT_MAPPING}
    labels:
      - "app=${FUNCTION_NAME}"
      - "function-type=${FUNCTION_TYPE}"
      - "function-category=${FUNCTION_CATEGORY}"
      - "step=${FUNCTION_STEP}"
      - "deployment-tier=function"

networks:
  ${TARGET_NODE}_network:
    external: true
    name: ${NODE_NETWORK_NAME}
  shared_network:
    external: true
    name: ${SHARED_NETWORK_NAME}
EOF

echo "🚀 Deploying ${FUNCTION_NAME} to ${TARGET_NODE}"
docker compose -f "$BASE_COMPOSE" -f "$OVERRIDE_FILE" up -d "$FUNCTION_NAME"

echo "📊 Status:"
docker compose -f "$BASE_COMPOSE" ps | sed -n '1,2p;/^$/,$p' | cat

echo "🔎 Connectivity checks (same-node should work, cross-node should fail):"
echo "  docker exec ${TARGET_NODE}_worker ping -c 1 ${FUNCTION_NAME}"
echo "  curl http://localhost:${HOST_PORT:-<mapped_port>}/ || echo 'If no host port, access via network only'"

echo "✅ Done. Override file: $OVERRIDE_FILE"

