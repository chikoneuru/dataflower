#!/bin/bash
# Scheduler Management Script
# Manages different scheduler setups to avoid port conflicts

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKER_DIR="$PROJECT_ROOT/docker"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to stop all containers
stop_all() {
    print_status "Stopping all scheduler containers..."
    
    # Stop all compose files
    docker compose -f "$DOCKER_DIR/docker-compose-warm-pools.yml" down --remove-orphans 2>/dev/null || true
    docker compose -f "$DOCKER_DIR/docker-compose-ditto.yml" down --remove-orphans 2>/dev/null || true
    docker compose -f "$DOCKER_DIR/docker-compose-multi-node.yml" down --remove-orphans 2>/dev/null || true
    
    # Clean up any remaining containers
    docker ps --format '{{.Names}}' | grep -E '(ditto-worker|recognizer-|dataflower_redis|serverless_redis)' | xargs -r docker rm -f 2>/dev/null || true
    
    print_success "All containers stopped"
}

# Function to start warm pools (shared infrastructure)
start_warm_pools() {
    print_status "Starting warm pools (shared infrastructure)..."
    
    docker compose -f "$DOCKER_DIR/docker-compose-warm-pools.yml" up -d
    
    print_success "Warm pools started"
    print_status "Available services:"
    print_status "  - Redis: localhost:6379"
    print_status "  - MinIO: localhost:9000 (console: localhost:9001)"
    print_status "  - Ditto Workers: localhost:5001-5003"
    print_status "  - Function containers: localhost:8001-8032"
}

# Function to start specific scheduler
start_scheduler() {
    local scheduler="$1"
    
    case "$scheduler" in
        "ours")
            print_status "Starting Ours scheduler..."
            # Ours uses existing warm pools, no additional containers needed
            print_success "Ours scheduler ready (using warm pools)"
            ;;
        "faasprs")
            print_status "Starting FaasPRS scheduler..."
            # FaasPRS uses existing warm pools, no additional containers needed
            print_success "FaasPRS scheduler ready (using warm pools)"
            ;;
        "ditto")
            print_status "Starting Ditto scheduler..."
            # Ditto uses existing warm pools, no additional containers needed
            print_success "Ditto scheduler ready (using warm pools)"
            ;;
        *)
            print_error "Unknown scheduler: $scheduler"
            print_status "Available schedulers: ours, faasprs, ditto"
            exit 1
            ;;
    esac
}

# Function to check if warm pools are running
check_warm_pools() {
    if ! docker ps --format '{{.Names}}' | grep -q 'dataflower_redis'; then
        print_warning "Warm pools not running. Starting them..."
        start_warm_pools
    else
        print_success "Warm pools already running"
    fi
}

# Function to run experiments
run_experiments() {
    local schedulers="$1"
    
    if [ -z "$schedulers" ]; then
        schedulers="ours,faasprs,ditto"
    fi
    
    print_status "Running experiments for schedulers: $schedulers"
    
    # Check if warm pools are running
    check_warm_pools
    
    # Run experiments
    cd "$PROJECT_ROOT"
    python -u -m integration.experiment_runner LOG_LEVEL=INFO
}

# Main script logic
case "${1:-help}" in
    "stop")
        stop_all
        ;;
    "start-warm-pools")
        start_warm_pools
        ;;
    "start")
        if [ -z "$2" ]; then
            print_error "Please specify scheduler: ours, faasprs, or ditto"
            exit 1
        fi
        check_warm_pools
        start_scheduler "$2"
        ;;
    "run")
        run_experiments "$2"
        ;;
    "status")
        print_status "Container status:"
        docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | grep -E '(dataflower|ditto|recognizer)' || print_warning "No scheduler containers running"
        ;;
    "help"|*)
        echo "Scheduler Management Script"
        echo ""
        echo "Usage: $0 <command> [args...]"
        echo ""
        echo "Commands:"
        echo "  stop                    Stop all scheduler containers"
        echo "  start-warm-pools        Start shared infrastructure (Redis, MinIO, workers, function containers)"
        echo "  start <scheduler>       Start specific scheduler (ours|faasprs|ditto)"
        echo "  run [schedulers]        Run experiments (default: all schedulers)"
        echo "  status                  Show container status"
        echo "  help                    Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0 start-warm-pools"
        echo "  $0 start ours"
        echo "  $0 run ours,faasprs"
        echo "  $0 status"
        echo "  $0 stop"
        ;;
esac
