#!/bin/bash

set -euo pipefail

usage() {
  echo "Usage: $0 <workflow_name> [--no-cache] [--parallel] [--single <function_name>]" >&2
  echo "Build all function images for a given workflow" >&2
  echo "" >&2
  echo "Arguments:" >&2
  echo "  workflow_name    Name of the workflow (e.g., recognizer, wordcount, svd)" >&2
  echo "" >&2
  echo "Options:" >&2
  echo "  --no-cache       Build without Docker cache" >&2
  echo "  --parallel       Build images in parallel (faster but uses more resources)" >&2
  echo "  --single <func>  Build only the specified function (useful for retries)" >&2
  echo "" >&2
  echo "Examples:" >&2
  echo "  $0 recognizer                    # Build all recognizer functions" >&2
  echo "  $0 recognizer --no-cache         # Build with fresh cache" >&2
  echo "  $0 recognizer --parallel         # Build in parallel" >&2
  echo "  $0 recognizer --single recognizer_adult   # Build only adult function" >&2
  echo "  $0 wordcount --no-cache --parallel # Build wordcount with both options" >&2
}

if [ $# -lt 1 ]; then
  usage
  exit 1
fi

WORKFLOW_NAME="$1"
shift

# Parse options
NO_CACHE=""
PARALLEL=false
SINGLE_FUNCTION=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --no-cache)
      NO_CACHE="--no-cache"
      shift
      ;;
    --parallel)
      PARALLEL=true
      shift
      ;;
    --single)
      if [ -n "$2" ] && [[ "$2" != --* ]]; then
        SINGLE_FUNCTION="$2"
        shift 2
      else
        echo "❌ --single requires a function name" >&2
        usage
        exit 1
      fi
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

# Check prerequisites
if ! command -v docker &>/dev/null; then
  echo "❌ docker not found" >&2
  exit 1
fi

# Check if workflow directory exists
WORKFLOW_DIR="functions/${WORKFLOW_NAME}"
if [ ! -d "$WORKFLOW_DIR" ]; then
  echo "❌ Workflow directory not found: $WORKFLOW_DIR" >&2
  echo "Available workflows:" >&2
  ls functions/ | grep -v __pycache__ | grep -v __init__.py | grep -v dag_loader.py || true
  exit 1
fi

echo "🔍 Scanning workflow: $WORKFLOW_NAME"
echo "📁 Directory: $WORKFLOW_DIR"

# Find function directories that have Dockerfiles
ALL_FUNCTIONS=()
for func_dir in "$WORKFLOW_DIR"/*/; do
  if [ -d "$func_dir" ] && [ -f "${func_dir}Dockerfile" ]; then
    func_name=$(basename "$func_dir")
    ALL_FUNCTIONS+=("$func_name")
  fi
done

if [ ${#ALL_FUNCTIONS[@]} -eq 0 ]; then
  echo "❌ No function Dockerfiles found in $WORKFLOW_DIR" >&2
  echo "Expected structure: functions/$WORKFLOW_NAME/<function_name>/Dockerfile" >&2
  exit 1
fi

# Handle --single option
if [ -n "$SINGLE_FUNCTION" ]; then
  if [[ " ${ALL_FUNCTIONS[*]} " =~ " ${SINGLE_FUNCTION} " ]]; then
    FUNCTIONS=("$SINGLE_FUNCTION")
    echo "🔍 Building single function: $SINGLE_FUNCTION"
    echo "📁 Directory: $WORKFLOW_DIR"
    echo ""
  else
    echo "❌ Function '$SINGLE_FUNCTION' not found in workflow '$WORKFLOW_NAME'" >&2
    echo "Available functions:" >&2
    for func in "${ALL_FUNCTIONS[@]}"; do
      echo "  • $func" >&2
    done
    exit 1
  fi
else
  FUNCTIONS=("${ALL_FUNCTIONS[@]}")
  echo "✅ Found ${#FUNCTIONS[@]} functions to build:"
  for func in "${FUNCTIONS[@]}"; do
    echo "  • $func"
  done
  echo ""
fi

# Build images
if [ "$PARALLEL" = true ]; then
  echo "🚀 Building function images in parallel..."
  echo "💡 This may use significant CPU and memory resources"
  echo ""

  # Build in parallel using background jobs
  pids=()
  for func in "${FUNCTIONS[@]}"; do
    (
      echo "🏗️  Building $func..."
      if docker build $NO_CACHE -t "${func}:latest" -f "$WORKFLOW_DIR/$func/Dockerfile" . >/dev/null 2>&1; then
        echo "✅ $func: Built successfully"
      else
        echo "❌ $func: Build failed"
        exit 1
      fi
    ) &
    pids+=($!)
  done

  # Wait for all builds to complete
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      echo "❌ Some parallel builds failed" >&2
      exit 1
    fi
  done

else
  echo "🚀 Building function images sequentially..."
  echo ""

  # Build sequentially
  for func in "${FUNCTIONS[@]}"; do
    echo "🏗️  Building $func..."
    if docker build $NO_CACHE -t "${func}:latest" -f "$WORKFLOW_DIR/$func/Dockerfile" .; then
      echo "✅ $func: Built successfully"
      echo ""
    else
      echo "❌ $func: Build failed" >&2
      exit 1
    fi
  done
fi

echo ""
echo "🎉 All function images built successfully!"
echo ""
echo "📋 Summary:"
echo "  Workflow: $WORKFLOW_NAME"
echo "  Functions built: ${#FUNCTIONS[@]}"
echo "  Images created:"
for func in "${FUNCTIONS[@]}"; do
  image_id=$(docker images -q "${func}:latest")
  echo "    • ${func}:latest ($image_id)"
done

echo ""
echo "💡 Next steps:"
echo "  • Deploy functions: ./scripts/deploy-workflow.sh --verify"
echo "  • Test workflow: python -m test.test_workflow_execution --config testing --input data/test.png"
echo ""
echo "🔧 Manual build commands (if needed):"
for func in "${FUNCTIONS[@]}"; do
  echo "  docker build $NO_CACHE -t ${func}:latest -f $WORKFLOW_DIR/$func/Dockerfile ."
done
