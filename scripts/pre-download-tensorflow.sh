#!/bin/bash

# Script to pre-download TensorFlow and cache it for Docker builds
# This prevents timeout issues during Docker builds

set -e

CACHE_DIR="./.tmp/tensorflow_cache"
TENSORFLOW_VERSION="2.16.1"

echo "📥 Pre-downloading TensorFlow ${TENSORFLOW_VERSION}..."
echo "📁 Cache directory: ${CACHE_DIR}"

# Create cache directory if it doesn't exist
mkdir -p "${CACHE_DIR}"

# Download TensorFlow wheel
echo "⏳ Downloading TensorFlow (this may take a few minutes)..."
pip download \
    --dest "${CACHE_DIR}" \
    --no-cache-dir \
    --only-binary=all \
    "tensorflow==${TENSORFLOW_VERSION}"

# Verify download
if ls "${CACHE_DIR}"/*.whl >/dev/null 2>&1; then
    echo "✅ TensorFlow downloaded successfully!"
    echo "📦 Cached files:"
    ls -lh "${CACHE_DIR}"/*.whl
else
    echo "❌ Failed to download TensorFlow"
    exit 1
fi

echo ""
echo "💡 Now you can build Docker images without timeout issues!"
echo "   Run: ./scripts/build-function-images.sh recognizer --single recognizer__adult"
