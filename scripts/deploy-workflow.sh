#!/bin/bash

set -euo pipefail

usage() {
  echo "Usage: $0 [--cleanup] [--verify]" >&2
  echo "Deploys the complete recognizer workflow across multiple nodes" >&2
  echo "" >&2
  echo "Options:" >&2
  echo "  --cleanup    Remove all deployed functions" >&2
  echo "  --verify      Test the deployed workflow" >&2
  echo "" >&2
  echo "Workflow:" >&2
  echo "  node1: recognizer__upload (port 8001)" >&2
  echo "  node2: recognizer__adult (port 8002)" >&2
  echo "  node3: recognizer__violence (port 8003)" >&2
  echo "  node4: recognizer__extract (port 8004)" >&2
  echo "  node1: recognizer__translate (port 8005)" >&2
  echo "  node2: recognizer__censor (port 8006)" >&2
  echo "  node3: recognizer__mosaic (port 8007)" >&2
}

# Parse arguments
CLEANUP=false
VERIFY=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --cleanup)
      CLEANUP=true
      shift
      ;;
    --verify)
      VERIFY=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

BASE_COMPOSE="docker-compose-multi-node.yml"

# Check prerequisites
if [ ! -f "$BASE_COMPOSE" ]; then
  echo "❌ $BASE_COMPOSE not found. Run from repo root." >&2
  exit 1
fi

if ! command -v docker &>/dev/null; then
  echo "❌ docker not found" >&2
  exit 1
fi

if ! docker compose version &>/dev/null; then
  echo "❌ docker compose not found" >&2
  exit 1
fi

# Check if base environment is running
if ! docker network ls | grep -q "dataflower_shared_network"; then
  echo "❌ Base environment not running. Please start it first:" >&2
  echo "   docker compose -f $BASE_COMPOSE up -d" >&2
  exit 1
fi

# Function to deploy a function to a specific node
deploy_function() {
  local function_name="$1"
  local node="$2"
  local port="$3"
  
  echo "🔍 Checking if $function_name is already running..."
  
  # Check if container exists and is running
  if docker ps --format "table {{.Names}}" | grep -q "^${function_name}$"; then
    echo "✅ $function_name is already running - skipping deployment"
    return 0
  fi
  
  # Check if container exists but is stopped
  if docker ps -a --format "table {{.Names}}" | grep -q "^${function_name}$"; then
    echo "🔄 $function_name exists but is stopped - starting it..."
    docker start "$function_name"
    return 0
  fi
  
  # Container doesn't exist - deploy it
  echo "🚀 Deploying $function_name to $node on port $port"
  ./scripts/deploy-function-compose.sh "$function_name" "$node" "$port"
  echo ""
}

# Function to cleanup a function
cleanup_function() {
  local function_name="$1"
  echo "🧹 Cleaning up $function_name"
  docker rm -f "$function_name" 2>/dev/null || true
}

# Function to verify a function is running
verify_function() {
  local function_name="$1"
  local port="$2"
  
  echo "🔍 Verifying $function_name on port $port"
  if docker ps | grep -q "$function_name"; then
    echo "✅ $function_name is running"
    if [ "$port" != "none" ]; then
      if curl -s "http://localhost:$port/" >/dev/null 2>&1; then
        echo "✅ $function_name responds on port $port"
      else
        echo "⚠️  $function_name not responding on port $port"
      fi
    fi
  else
    echo "❌ $function_name is not running"
  fi
  echo ""
}

if [ "$CLEANUP" = true ]; then
  echo "🧹 Cleaning up all deployed functions..."
  
  # List of all functions to cleanup
  FUNCTIONS=(
    "recognizer__upload"
    "recognizer__adult"
    "recognizer__violence"
    "recognizer__extract"
    "recognizer__translate"
    "recognizer__censor"
    "recognizer__mosaic"
  )
  
  for func in "${FUNCTIONS[@]}"; do
    cleanup_function "$func"
  done
  
  echo "✅ Cleanup complete"
  exit 0
fi

echo "🎯 Deploying complete recognizer workflow across multiple nodes"
echo ""

# Deploy workflow functions to different nodes
echo "📋 Workflow Deployment Plan:"
echo "  node1: recognizer__upload (port 8001) - Entry point"
echo "  node2: recognizer__adult (port 8002) - Adult content detection"
echo "  node3: recognizer__violence (port 8003) - Violence detection"
echo "  node4: recognizer__extract (port 8004) - Text extraction"
echo "  node1: recognizer__translate (port 8005) - Translation"
echo "  node2: recognizer__censor (port 8006) - Text censoring"
echo "  node3: recognizer__mosaic (port 8007) - Image mosaicing"
echo ""

# Deploy each function to its designated node
deploy_function "recognizer__upload" "node1" "8001"
deploy_function "recognizer__adult" "node2" "8002"
deploy_function "recognizer__violence" "node3" "8003"
deploy_function "recognizer__extract" "node4" "8004"
deploy_function "recognizer__translate" "node1" "8005"
deploy_function "recognizer__censor" "node2" "8006"
deploy_function "recognizer__mosaic" "node3" "8007"

echo "🎉 Workflow deployment complete!"
echo ""

# Show status
echo "📊 Current Status:"
docker compose -f "$BASE_COMPOSE" ps | sed -n '1,2p;/^$/,$p' | cat
echo ""

if [ "$VERIFY" = true ]; then
  echo "🔍 Verifying all functions..."
  echo ""
  
  verify_function "recognizer__upload" "8001"
  verify_function "recognizer__adult" "8002"
  verify_function "recognizer__violence" "8003"
  verify_function "recognizer__extract" "8004"
  verify_function "recognizer__translate" "8005"
  verify_function "recognizer__censor" "8006"
  verify_function "recognizer__mosaic" "8007"
  
  echo "🔗 Testing network isolation..."
  echo "  Same-node communication (should work):"
  docker exec node1_worker ping -c 1 recognizer__upload 2>/dev/null && echo "✅ node1_worker → recognizer__upload" || echo "❌ node1_worker → recognizer__upload"
  docker exec node2_worker ping -c 1 recognizer__adult 2>/dev/null && echo "✅ node2_worker → recognizer__adult" || echo "❌ node2_worker → recognizer__adult"
  
  echo "  Cross-node communication (should fail):"
  docker exec node1_worker ping -c 1 recognizer__adult 2>/dev/null && echo "⚠️  node1_worker → recognizer__adult (unexpected)" || echo "✅ node1_worker → recognizer__adult (correctly blocked)"
  docker exec node2_worker ping -c 1 recognizer__upload 2>/dev/null && echo "⚠️  node2_worker → recognizer__upload (unexpected)" || echo "✅ node2_worker → recognizer__upload (correctly blocked)"
fi

echo ""
echo "🎯 Workflow Summary:"
echo "  • 7 functions deployed across 4 nodes"
echo "  • Each function isolated to its node network"
echo "  • Shared network for Redis communication"
echo "  • Ports 8001-8007 available for testing"
echo ""
echo "📝 Next Steps:"
echo "  • Test individual functions: curl http://localhost:8001/"
echo "  • Run workflow tests with your orchestrator"
echo "  • Monitor resource usage: docker stats"
echo "  • Cleanup: $0 --cleanup"
echo ""
echo "✅ Complete workflow ready for testing!"
