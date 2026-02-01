#!/bin/bash
# Start MinIO locally for testing cross-node data transfer

echo "🚀 Starting MinIO locally for testing..."

# Check if MinIO is already running
if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo "✅ MinIO is already running on localhost:9000"
    echo "🌐 MinIO Console: http://localhost:9001"
    echo "🔑 Access Key: dataflower"
    echo "🔑 Secret Key: dataflower123"
    exit 0
fi

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker to run MinIO."
    exit 1
fi

# Start MinIO using Docker
echo "📦 Starting MinIO container..."
docker run -d \
    --name dataflower-minio \
    -p 9000:9000 \
    -p 9001:9001 \
    -e MINIO_ROOT_USER=dataflower \
    -e MINIO_ROOT_PASSWORD=dataflower123 \
    quay.io/minio/minio server /data --console-address ":9001"

# Wait for MinIO to start
echo "⏳ Waiting for MinIO to start..."
sleep 5

# Check if MinIO is running
if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo "✅ MinIO started successfully!"
    echo "🌐 MinIO Console: http://localhost:9001"
    echo "🔑 Access Key: dataflower"
    echo "🔑 Secret Key: dataflower123"
    echo "📦 Bucket: dataflower-storage"
    echo ""
    echo "📝 To stop MinIO: docker stop dataflower-minio"
    echo "📝 To remove MinIO: docker rm dataflower-minio"
else
    echo "❌ Failed to start MinIO. Please check Docker logs:"
    echo "   docker logs dataflower-minio"
    exit 1
fi
