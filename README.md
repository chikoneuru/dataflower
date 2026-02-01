# Dataflower: Serverless Workflow Orchestration

Complete serverless workflow orchestration system with multi-node simulation and data locality management.

## Quick Start Commands

### 1. Install Dependencies
```bash
pip install -r requirements.txt
pip install -r worker/requirements.txt
```

### 2. Build Docker Images

**Pre-download TensorFlow** (for fast function builds):
```bash
# Download TensorFlow and dependencies to local cache (one-time setup)
./scripts/pre-download-tensorflow.sh

# This creates .tmp/tensorflow_cache/ with all TensorFlow wheels
# Eliminates 590MB+ downloads during Docker builds
# Takes 2-5 minutes but saves time on every future build
```

**Build function images** (for specific workflows):
```bash
# Build all functions for a workflow automatically
./scripts/build-function-images.sh recognizer

# Build with fresh cache (recommended after function changes)
./scripts/build-function-images.sh recognizer --no-cache

# Build in parallel for faster builds
./scripts/build-function-images.sh recognizer --parallel

# Note: Functions with TensorFlow (adult, violence) will use local cache if available
# Run ./scripts/pre-download-tensorflow.sh first for fastest builds

# Manual build commands
docker build -t recognizer__adult:latest -f functions/recognizer/recognizer__adult/Dockerfile .
```

**Build multi-node compose images**:
```bash
docker compose -f docker/docker-compose-multi-node.yml up -d --build

# Or using MinIO setup script (recommended) for additional checks and cleanup
./scripts/setup-minio.sh
```

### 3. Initialize Multi-Node Environment

**Start multi-node cluster** (recommended for full testing):
```bash
# Start multi-node environment with multiple worker nodes
docker compose -f docker/docker-compose-multi-node.yml up -d

# Check all services are running
docker compose -f docker/docker-compose-multi-node.yml ps

# View node logs
docker compose -f docker/docker-compose-multi-node.yml logs node1_worker
```

**Start MinIO** (for remote storage):
```bash
# Start MinIO S3-compatible storage
docker compose -f docker/docker-compose-minio.yml up -d

# Access MinIO Console at http://localhost:9001
# Username: dataflower, Password: dataflower123
```

**Alternative: Manual Docker Setup**
```bash
# Create networks
docker network create dataflower_shared_network
docker network create dataflower_node1_network
docker network create dataflower_node2_network
docker network create dataflower_node3_network
docker network create dataflower_node4_network

# Start individual workers
docker run -d --name node1_worker \
  --network dataflower_node1_network \
  -p 5001:5000 \
  -v "$(pwd)":/app \
  -e NODE_ID=node1 \
  --label dataflower-role=worker \
  dataflower/worker:latest

# Repeat for node2, node3, node4...
```

### 4. Test Function Deployment
**Deploy individual functions**:
```bash
# Deploy specific function to specific node
./scripts/deploy-function-compose.sh recognizer__adult node2 8002

# Deploy without host port (internal only)
./scripts/deploy-function-compose.sh recognizer__violence node3

# Expected: Function container running, accessible on specified port
```
**Test function endpoints**:
```bash
# Test individual deployed functions
curl http://localhost:8002/health  # recognizer__adult
```

**Deploy complete workflow** (7 functions across 4 nodes):
```bash
# Make scripts executable
chmod +x scripts/deploy-workflow.sh
chmod +x scripts/deploy-function-compose.sh
chmod +x scripts/build-function-images.sh
chmod +x scripts/pre-download-tensorflow.sh

# Deploy all functions and verify
./scripts/deploy-workflow.sh --verify

# Expected: 7 function containers running on ports 8001-8007
```

### 5. Main Execution Commands

**Run complete workflow** (main command of the repo):
```bash
# Test workflow execution script
python -m test.test_workflow_execution --config testing --input data/test.png

# Run with dedicated execution script
python -m scripts.execute_workflow \
  --dag functions/recognizer/recognizer_dag.yaml \
  --input data/test.png

# Use main.py gateway (alternative)
python main.py
```

## Testing Commands

### Unit Tests
```bash
# Test workflow execution
python -m unittest test.test_workflow_execution -v

# Test individual components
python -m unittest test.test_imports -v
python -m unittest test.test_cost_models -v
python -m unittest test.test_data_sharing -v

# Run all tests
python -m unittest discover test -v

# Test with MinIO storage
SKIP_MINIO_TESTS=false python -m unittest test.test_data_sharing -v
```

### Integration Tests
```bash
# Test complete workflow with data
python -m test.test_workflow_execution --config development --input data/test.png

# Test MinIO integration (interactive demo)
python -m test.test_minio_integration

# Test cluster failover
docker stop node1_worker
python -m test.test_workflow_execution --config development
docker start node1_worker
```

**Test worker image** (optional - mainly for development):
```bash
# Note: Worker image is automatically built by docker-compose-multi-node.yml
# Only build separately if you need to test/debug the worker image specifically

# Build worker image
docker build -t dataflower/worker:latest -f worker/Dockerfile .

# Rebuild from scratch (recommended after code changes)
docker build --no-cache -t dataflower/worker:latest -f worker/Dockerfile .

# Test the worker image
docker run -it --rm dataflower/worker:latest
```

## Function Development & Reloading

### When Functions Change

**Quick reload** (development):
```bash
# Edit function
nano functions/recognizer/recognizer__adult/main.py

# Restart specific container
docker restart recognizer__adult

# Test updated function
curl -X POST http://localhost:8002/run?fn=recognizer__adult \
  -H "Content-Type: application/octet-stream" \
  --data-binary @data/test.png
```

**Full rebuild** (recommended):
```bash
# Stop all services
docker compose -f docker/docker-compose-multi-node.yml down
docker compose -f docker/docker-compose-minio.yml down

# Remove function containers
docker rm -f $(docker ps -aq --filter "label=app")

# Rebuild worker image
docker build --no-cache -t dataflower/worker:latest worker/

# Restart environment
docker compose -f docker/docker-compose-multi-node.yml up -d
docker compose -f docker/docker-compose-minio.yml up -d

# Test integration
python -m test.test_workflow_execution --config development --input data/test.png
```

## Available Workflows & Functions

### Workflows
- **Recognizer** (`functions/recognizer/`): Image processing pipeline
  - Upload → Adult Check + Violence Check (parallel) → Text Extract → Translate → Censor → Mosaic
- **WordCount** (`functions/wordcount/`): Text processing workflow
- **SVD** (`functions/svd/`): Matrix decomposition workflow

## Configuration
Configure system behavior via `configs/`:

- **testing.json**: Mock storage, basic scheduling
- **development.json**: MinIO storage, advanced features
- **production.json**: Production-ready settings

## Monitoring & Troubleshooting

### Monitoring
```bash
# Check all containers
docker ps

# Monitor resource usage
docker stats

# View specific logs
docker logs node1_worker
docker logs recognizer__adult
docker logs dataflower_minio

# Check networks
docker network ls | grep dataflower

# Test connectivity
docker exec node1_worker ping -c 1 node2_worker
docker exec node1_worker ping -c 1 recognizer__adult  # Should fail (different nodes)
```

### Common Issues

**No workers found**:
```bash
# Check worker containers
docker ps --filter "label=dataflower-role=worker"

# Verify worker health
curl http://localhost:5001/health
curl http://localhost:5002/health
```

**Function build fails**:
```bash
# Rebuild function images with fresh cache
./scripts/build-function-images.sh recognizer --no-cache

# Check specific function Dockerfile
ls functions/recognizer/recognizer__adult/Dockerfile

# Manual build for debugging (note: use project root as build context)
docker build -t recognizer__adult:latest -f functions/recognizer/recognizer__adult/Dockerfile .
```

**TensorFlow build timeouts** (common with adult/violence functions):
```bash
# Pre-download TensorFlow cache (one-time setup, prevents timeouts)
./scripts/pre-download-tensorflow.sh

# Then build functions (will use local cache, much faster)
./scripts/build-function-images.sh recognizer --single recognizer__adult

# Expected: Fast build using cached TensorFlow instead of 590MB download
# Cache location: .tmp/tensorflow_cache/
```

**Function deployment fails**:
```bash
# Check base environment
docker compose -f docker/docker-compose-multi-node.yml ps

# Verify networks
docker network ls | grep dataflower

# Check function Dockerfile
ls functions/recognizer/recognizer__adult/Dockerfile
```

**Check configurations**
    ```bash
# Check configuration
python -c "
from scheduler.ours.config import initialize_config, get_config
initialize_config('development')
print(f'Config loaded: {get_config() is not None}')
"
```

**Port conflicts**:
    ```bash
# Find what's using ports
netstat -tulpn | grep :8001

# Use different ports
./scripts/deploy-function-compose.sh recognizer__adult node2 8101
```

**MinIO connection issues**:
    ```bash
# Check MinIO status
docker logs dataflower_minio

# Test MinIO health
curl http://localhost:9000/minio/health/live

# Access MinIO console
open http://localhost:9001  # Username: dataflower, Password: dataflower123
```

## Cleanup

### Quick Cleanup
```bash
# Remove all deployed functions
./scripts/deploy-workflow.sh --cleanup

# Stop environment
docker compose -f docker/docker-compose-multi-node.yml down
docker compose -f docker/docker-compose-minio.yml down
```

### Complete Cleanup
```bash
# Stop and remove everything
docker compose -f docker/docker-compose-multi-node.yml down --volumes --remove-orphans
docker compose -f docker/docker-compose-minio.yml down --volumes --remove-orphans

# Clean up orphaned containers
docker system prune -f

# Remove all dataflower networks
docker network rm $(docker network ls | grep dataflower | awk '{print $1}')

# Remove dataflower images
docker rmi $(docker images | grep dataflower | awk '{print $3}')
```

## Architecture

```
┌─────────────────┐     ┌──────────────────┐    ┌─────────────────┐
│   Cost Models   │───▶│ Mode Classifier  │───▶│Scheduling Policy│
│                 │     │                  │    │                 │
│ • Compute Cost  │     │ • CCR Analysis   │    │ • Network-bound │
│ • Transfer Cost │     │ • Mode Decision  │    │ • Compute-bound │
│ • Cold Start    │     │ • Weight Factors │    │ • Task Priority │
└─────────────────┘     └──────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │   Orchestrator  │
                        │                 │
                        │ • DAG Execution │
                        │ • Parallel/Switch│
                        │ • Data Flow      │
                        └─────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────────┐
                    │   Multi-Node Cluster        │
                    │  node1  node2  node3  node4  │
                    │  ┌───┐  ┌───┐  ┌───┐  ┌───┐ │
                    │  │fn │  │fn │  │fn │  │fn │ │
                    │  └───┘  └───┘  └───┘  └───┘ │
                    └─────────────────────────────┘
```

## Next Steps

1. **Run basic workflow**: `python -m test.test_workflow_execution --config testing --input data/test.png`
2. **Deploy full cluster**: `./scripts/deploy-workflow.sh --verify`
3. **Test with MinIO**: Use `--config development` for remote storage
4. **Monitor system**: `docker stats` and `docker logs`
5. **Develop functions**: Add new functions in `functions/` directory
6. **Scale cluster**: Add more worker nodes or function instances