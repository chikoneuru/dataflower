#!/bin/bash

# Setup script for warm function container pools
# Creates and manages the per-function container environment
set -e

echo "🏊 Setting up Warm Function Container Pools..."

# Configuration
DOCKER_COMPOSE_FILE="docker/docker-compose-warm-pools.yml"
FUNCTIONS_TO_BUILD=${1:-"recognizer__upload,recognizer__adult,recognizer__violence,recognizer__extract,recognizer__translate,recognizer__censor,recognizer__mosaic"}

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if Docker Compose is available
if ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose is not available. Please install Docker Compose first."
    exit 1
fi

# Check if compose file exists
if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
    echo "❌ Docker Compose file not found: $DOCKER_COMPOSE_FILE"
    exit 1
fi

echo "📋 Prerequisites check passed"

# Build function images if they don't exist
echo "🔨 Building function container images..."
IFS=',' read -ra FUNCTION_ARRAY <<< "$FUNCTIONS_TO_BUILD"

for function_name in "${FUNCTION_ARRAY[@]}"; do
    function_name=$(echo "$function_name" | xargs) # Trim whitespace
    image_name="dataflower/${function_name}:latest"
    
    # Check if image exists
    if ! docker images | grep -q "dataflower/${function_name}"; then
        echo "🏗️  Building image for $function_name..."
        
        # Find function directory
        function_dir=""
        if [ -d "functions/recognizer/${function_name}" ]; then
            function_dir="functions/recognizer/${function_name}"
        elif [ -d "benchmark/template_functions/${function_name}" ]; then
            function_dir="benchmark/template_functions/${function_name}"
        else
            echo "⚠️  Function directory not found for $function_name, skipping..."
            continue
        fi
        
        # Build the image using the same context as docker-compose
        if [ -f "$function_dir/Dockerfile" ]; then
            docker build -t "$image_name" -f "$function_dir/Dockerfile" .
            echo "✅ Built image: $image_name"
        else
            echo "⚠️  No Dockerfile found for $function_name, skipping..."
        fi
    else
        echo "✅ Image already exists: $image_name"
    fi
done

# Stop existing containers if running
echo "🔄 Stopping existing containers..."
docker compose -f "$DOCKER_COMPOSE_FILE" down --remove-orphans

# Clean up any conflicting networks
echo "🧹 Cleaning up conflicting networks..."
docker network rm docker_node1-network docker_node2-network docker_node3-network docker_dataflower-shared 2>/dev/null || true
docker network rm node1-network node2-network node3-network dataflower-shared 2>/dev/null || true
docker network prune -f

# Start the warm container pools
echo "🚀 Starting warm function container pools..."
docker compose -f "$DOCKER_COMPOSE_FILE" up -d

# Wait for containers to be ready
echo "⏳ Waiting for containers to be ready..."
sleep 15

# Check deployment status
echo "📊 Container deployment status:"
docker compose -f "$DOCKER_COMPOSE_FILE" ps

echo ""
echo "📊 Function container distribution:"
docker ps --filter "label=dataflower-type=function" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}\t{{.Labels}}" | head -20

# Test connectivity to function containers
echo ""
echo "🔍 Testing function container connectivity..."

# Get container info and test health endpoints
container_count=0
healthy_count=0

while IFS= read -r container_info; do
    if [[ "$container_info" == *"dataflower-type=function"* ]]; then
        container_count=$((container_count + 1))
        
        # Extract container name and port
        container_name=$(echo "$container_info" | awk '{print $1}')
        ports=$(echo "$container_info" | awk '{print $3}')
        
        # Extract host port (format: 0.0.0.0:8001->8080/tcp)
        if [[ "$ports" =~ 0\.0\.0\.0:([0-9]+) ]]; then
            host_port="${BASH_REMATCH[1]}"
            
            # Test health endpoint
            if curl -s -f "http://127.0.0.1:$host_port/" > /dev/null 2>&1; then
                echo "✅ $container_name (port $host_port)"
                healthy_count=$((healthy_count + 1))
            else
                echo "⚠️  $container_name (port $host_port) - not responding"
            fi
        else
            echo "⚠️  $container_name - no port mapping found"
        fi
    fi
done < <(docker ps --filter "label=dataflower-type=function" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}\t{{.Labels}}" | tail -n +2)

echo ""
echo "📊 Health Check Summary: $healthy_count/$container_count containers responding"

# Show network information
echo ""
echo "🌐 Network Configuration:"
echo "   Shared Network: 172.25.0.0/24"
echo "   Node 1 Network: 172.25.1.0/24 (High-Performance)"
echo "   Node 2 Network: 172.25.2.0/24 (Balanced)"
echo "   Node 3 Network: 172.25.3.0/24 (Lightweight)"

# Show container pool summary
echo ""
echo "🏊 Container Pool Summary:"
echo "   Node 1: 2x adult + 1x violence = 3 containers (High-Performance)"
echo "   Node 2: 3x upload + 2x extract = 5 containers (Balanced)"
echo "   Node 3: 1x translate + 1x censor + 1x mosaic = 3 containers (Lightweight)"
echo "   Infrastructure: Redis + MinIO"
echo "   Total: 11 function containers + 2 infrastructure"

echo ""
echo "🎉 Warm function container pools setup complete!"
echo ""
echo "🔍 Next steps:"
echo "   1. Test the system: python integration/test_placement_routing_system.py"
echo "   2. Monitor containers: docker compose -f $DOCKER_COMPOSE_FILE logs -f"
echo "   3. Scale functions: docker compose -f $DOCKER_COMPOSE_FILE up --scale <service>=<replicas>"
echo ""
echo "🔧 Useful commands:"
echo "   docker compose -f $DOCKER_COMPOSE_FILE ps"
echo "   docker compose -f $DOCKER_COMPOSE_FILE logs <service>"
echo "   docker compose -f $DOCKER_COMPOSE_FILE down"
echo "   docker stats --no-stream"
echo ""
echo "🌐 Access points:"
echo "   MinIO Console: http://localhost:9001 (dataflower/dataflower123)"
echo "   Redis: localhost:6379"
echo "   Function containers: ports 8001-8022"
