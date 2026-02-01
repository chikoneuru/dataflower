#!/bin/bash

# Setup script for Docker Compose multi-node serverless environment
set -e

echo "🚀 Setting up Docker Compose multi-node serverless environment..."

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if Docker Compose is available
if ! docker-compose version &> /dev/null; then
    echo "❌ Docker Compose is not available. Please install Docker Compose first."
    exit 1
fi

echo "📋 Prerequisites check passed"

# Build worker image
echo "🔨 Building worker image..."
docker build -t serverless_worker:latest -f worker/Dockerfile .

# Start the multi-node environment
echo "🏗️  Starting multi-node environment..."
docker-compose -f docker-compose-multi-node.yml up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 10

# Check service status
echo "📊 Service status:"
docker-compose -f docker-compose-multi-node.yml ps

# Show network information
echo "🌐 Network information:"
echo "Node 1: 172.20.1.0/24"
echo "Node 2: 172.20.2.0/24"
echo "Node 3: 172.20.3.0/24"
echo "Node 4: 172.20.4.0/24"
echo "Shared network: 172.20.0.0/24"

# Show container information
echo "📦 Container information:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}\t{{.Networks}}"

echo "✅ docker-compose multi-node environment setup complete!"
echo ""
echo "🔍 Useful commands:"
echo "   docker-compose -f docker-compose-multi-node.yml ps"
echo "   docker-compose -f docker-compose-multi-node.yml logs"
echo "   docker-compose -f docker-compose-multi-node.yml logs node1_worker"
echo "   docker-compose -f docker-compose-multi-node.yml down"
echo ""
echo "🏷️  Node labels:"
echo "   node-type: node-1/2/3"
echo "   node-id: node1/node2/node3"
echo "   node-tier: standard"
echo ""
echo "🏷️  Function labels:"
echo "   app: function name"
echo "   function-type: recognizer/svd/video/etc"
echo "   function-category: ml/compute/video/etc"
echo "   step: inference/process/etc"
echo "   deployment-tier: function"
echo ""
echo "🌐 Access points:"
echo "   Redis: localhost:6379"
echo "   Node 1 Worker: localhost:5001"
echo "   Node 2 Worker: localhost:5002"
echo "   Node 3 Worker: localhost:5003"
echo "   Node 4 Worker: localhost:5004"
