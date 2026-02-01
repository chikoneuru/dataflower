#!/bin/bash

# Script to pre-download TensorFlow and related libraries for Docker builds
# This prevents timeout issues during Docker builds

set -e

CACHE_DIR="./.tmp/python_pkgs_whls"
# TENSORFLOW_VERSION="2.16.1"
PILLOW_VERSION="10.2.0"
OPENCV_VERSION="4.9.0.80"
PYTESSERACT_VERSION="0.3.10"
FASTAPI_VERSION="0.116.1"
UVICORN_VERSION="0.34.0"
REDIS_VERSION="6.4.0"
PYMULTIPART_VERSION="0.0.9"
REQUESTS_VERSION="2.32.5"
# Match cached AWS SDK wheels we ship
BOTO3_VERSION="1.40.44"
BOTOCORE_VERSION="1.40.44"

echo "📥 Pre-downloading Python packages..."
echo "📁 Cache directory: ${CACHE_DIR}"

# Create cache directory if it doesn't exist
mkdir -p "${CACHE_DIR}"

# Download packages with proper dependency resolution
echo "⏳ Downloading packages (this may take a few minutes)..."

# Download all packages together to resolve dependencies properly
pip download \
    --dest "${CACHE_DIR}" \
    --no-cache-dir \
    --only-binary=all \
    "Pillow==${PILLOW_VERSION}" \
    "opencv-python-headless==${OPENCV_VERSION}" \
    "pytesseract==${PYTESSERACT_VERSION}" \
    "fastapi==${FASTAPI_VERSION}" \
    "uvicorn==${UVICORN_VERSION}" \
    "redis==${REDIS_VERSION}" \
    "python-multipart==${PYMULTIPART_VERSION}" \
    "requests==${REQUESTS_VERSION}" \
    "boto3==${BOTO3_VERSION}" \
    "botocore==${BOTOCORE_VERSION}"

# Verify download
if ls "${CACHE_DIR}"/*.whl >/dev/null 2>&1; then
    echo "✅ All packages downloaded successfully!"
    echo "📦 Cached files:"
    ls -lh "${CACHE_DIR}"/*.whl
    echo ""
    echo "📊 Total cached files: $(ls "${CACHE_DIR}"/*.whl | wc -l)"
else
    echo "❌ Failed to download packages"
    exit 1
fi

echo ""
echo "💡 Now you can build Docker images without timeout issues!"
echo "   The cached packages include: TensorFlow, Pillow, OpenCV, Tesseract, FastAPI, Uvicorn, Redis, Requests, and AWS SDK"
echo "   Run: ./scripts/build-function-images.sh recognizer --single recognizer__adult"
