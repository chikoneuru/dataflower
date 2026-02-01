#!/bin/bash

# MinIO Setup Script for DataFlower Large File Storage
# Perfect for: images, videos, text files, npy arrays

set -e

# Get the absolute path to the docker-compose file
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$(cd "$SCRIPT_DIR/../docker" && pwd)/docker-compose-minio.yml"

# Debug: Show the resolved path
echo "🔍 Debug: COMPOSE_FILE path: $COMPOSE_FILE"

echo "🚀 Setting up MinIO for DataFlower Large File Storage"
echo "============================================================================================"

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if shared network exists, create if not
NETWORK_NAME="shared_network"
if ! docker network inspect "$NETWORK_NAME" &> /dev/null; then
    echo "🔧 Creating shared network: $NETWORK_NAME"
    docker network create "$NETWORK_NAME"
else
    echo "✅ Network $NETWORK_NAME already exists"
fi

# Stop existing MinIO containers if running
echo "🛑 Stopping any existing MinIO containers..."
docker compose -f "$COMPOSE_FILE" down 2>/dev/null || true

# Start MinIO
echo "🚀 Starting MinIO for large file storage..."
docker compose -f "$COMPOSE_FILE" up -d

# Wait for MinIO to be ready
echo "⏳ Waiting for MinIO to be ready..."
sleep 15

# Check if MinIO is healthy
if curl -f http://localhost:9000/minio/health/live &> /dev/null; then
    echo "✅ MinIO is running and healthy!"
    echo
    echo "📋 MinIO Configuration:"
    echo "  🌐 API Endpoint: http://localhost:9000"
    echo "  🖥️  Web Console: http://localhost:9001"
    echo "  👤 Username: dataflower"
    echo "  🔑 Password: dataflower123"
    echo "  🪣 Bucket: dataflower-storage"
    echo
    echo "🎯 Perfect for storing:"
    echo "  • 🖼️  Images (JPEG, PNG, etc.)"
    echo "  • 🎥 Videos (MP4, AVI, etc.)"
    echo "  • 📄 Text files (logs, configs)"
    echo "  • 🔢 NumPy arrays (.npy files)"
    echo "  • 📊 Any large data files"
    echo
    echo "🧪 Test the setup:"
    echo "  python provider/minio_example.py"
    echo
    echo "🔧 To stop MinIO:"
    echo "  docker compose -f \"$COMPOSE_FILE\" down"
else
    echo "❌ MinIO failed to start properly"
    echo "📋 Checking container status..."
    docker compose -f "$COMPOSE_FILE" ps
    echo
    echo "📋 Checking container logs..."
    docker compose -f "$COMPOSE_FILE" logs minio
    exit 1
fi

# Optional: Install Python dependencies
echo "📦 Checking Python dependencies..."
if python -c "import boto3" 2>/dev/null; then
    echo "✅ boto3 is installed"
else
    echo "⚠️  boto3 not found. Install with: pip install boto3"
fi

if python -c "import numpy" 2>/dev/null; then
    echo "✅ numpy is installed"  
else
    echo "⚠️  numpy not found. Install with: pip install numpy"
fi

echo
echo "🎉 MinIO setup complete! Ready for large file storage."
