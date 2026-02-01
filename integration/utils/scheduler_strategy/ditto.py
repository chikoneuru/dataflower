import logging
import time
from typing import Any, Dict

from integration.utils.config import FunctionExecutionResult
from provider.container_manager import ContainerManager
from scheduler.ditto.function_orchestrator import FunctionOrchestrator

from .interface import SchedulerInterface

logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class DittoScheduler(SchedulerInterface):
    """Ditto scheduler wrapper implementing the common scheduler interface."""

    def __init__(self) -> None:
        self.orchestrator = None
        self.container_manager = None

    def initialize(self, container_manager: Any) -> bool:
        """Initialize Ditto scheduler with a shared container manager."""
        try:
            # Always use the injected container manager (should have discovered workers)
            self.container_manager = container_manager
            self.orchestrator = FunctionOrchestrator(container_manager=self.container_manager)
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Ditto scheduler: {e}")
            return False

    def execute_function(self, function_name: str, payload: bytes, request_id: str) -> FunctionExecutionResult:
        """Execute a single function using Ditto orchestrator."""
        start_time = time.time()
        try:
            result = self.orchestrator.invoke_function(function_name, request_id, payload)
            execution_time = (time.time() - start_time) * 1000

            if isinstance(result, dict):
                status = result.get('status')
                success = status != 'error'
            else:
                success = True

            return FunctionExecutionResult(
                function_name=function_name,
                request_id=request_id,
                success=success,
                execution_time_ms=execution_time,
                node_id=result.get('node_id') if isinstance(result, dict) else None,
                container_id=result.get('container_id') if isinstance(result, dict) else None,
                error_message=result.get('message') if isinstance(result, dict) and not success else None
            )
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Ditto execute_function error: {e}")
            return FunctionExecutionResult(
                function_name=function_name,
                request_id=request_id,
                success=False,
                execution_time_ms=execution_time,
                error_message=str(e)
            )

    def get_metrics(self) -> Dict[str, Any]:
        """Return Ditto scheduler metrics (snapshot-based only)."""
        metrics: Dict[str, Any] = {
            "scheduler_type": "ditto",
        }
        if self.orchestrator:
            try:
                metrics["cluster_resource_status"] = self.orchestrator.get_cluster_resource_status()
                
            except Exception as e:
                logger.warning(f"Failed to get Ditto metrics: {e}")
                metrics["resource_error"] = str(e)
        return metrics
    
    def _get_ditto_resource_utilization(self) -> Dict[str, Any]:
        """Get comprehensive resource utilization from all Ditto workers."""
        try:
            import docker
            import concurrent.futures
            import threading
            
            docker_client = docker.from_env()
            
            # Get all Ditto worker containers
            workers = self.container_manager.get_workers()
            if not workers:
                return {
                    "total_containers": 0,
                    "total_cpu_percent": 0.0,
                    "total_memory_mb": 0.0,
                    "total_gpu_utilization": 0.0,
                    "function_aggregates": {},
                    "node_capacity": {},
                    "worker_nodes": {"total_nodes": 0},
                    "error": "No workers discovered"
                }
            
            # Thread-safe aggregation variables
            aggregation_lock = threading.Lock()
            total_containers = 0
            total_cpu_percent = 0.0
            total_memory_mb = 0.0
            total_gpu_utilization = 0.0
            node_capacity = {}
            
            def get_worker_stats(worker):
                """Get resource stats for a single worker container."""
                worker_id = worker.get('worker_id', worker.get('name', 'unknown'))
                container_id = worker.get('container_id')
                
                if not container_id:
                    return None
                
                try:
                    # Get container stats from Docker
                    container = docker_client.containers.get(container_id)
                    stats = container.stats(stream=False)
                    
                    # Calculate CPU usage percentage with better error handling
                    cpu_percent = 0.0
                    try:
                        cpu_stats = stats.get('cpu_stats', {})
                        precpu_stats = stats.get('precpu_stats', {})
                        
                        if cpu_stats and precpu_stats:
                            cpu_delta = cpu_stats['cpu_usage']['total_usage'] - precpu_stats['cpu_usage']['total_usage']
                            system_delta = cpu_stats['system_cpu_usage'] - precpu_stats['system_cpu_usage']
                            
                            if system_delta > 0 and cpu_delta > 0:
                                cpu_count = len(cpu_stats['cpu_usage'].get('percpu_usage', [1]))
                                cpu_percent = (cpu_delta / system_delta) * cpu_count * 100.0
                    except (KeyError, ZeroDivisionError, TypeError):
                        cpu_percent = 0.0
                    
                    # Calculate memory usage with better error handling
                    memory_mb = 0.0
                    memory_percent = 0.0
                    try:
                        memory_stats = stats.get('memory_stats', {})
                        if memory_stats:
                            memory_usage = memory_stats.get('usage', 0)
                            memory_limit = memory_stats.get('limit', 0)
                            memory_mb = memory_usage / (1024 * 1024)  # Convert to MB
                            memory_percent = (memory_usage / memory_limit) * 100.0 if memory_limit > 0 else 0.0
                    except (KeyError, ZeroDivisionError, TypeError):
                        memory_mb = 0.0
                        memory_percent = 0.0
                    
                    # Get container resource limits
                    cpu_limit_cores = 4.0  # Default
                    memory_limit_gb = 8.0  # Default
                    try:
                        container_info = container.attrs
                        host_config = container_info.get('HostConfig', {})
                        
                        # CPU limit calculation
                        cpu_quota = host_config.get('CpuQuota', 0)
                        cpu_period = host_config.get('CpuPeriod', 100000)
                        if cpu_quota > 0 and cpu_period > 0:
                            cpu_limit_cores = cpu_quota / cpu_period
                        
                        # Memory limit calculation
                        memory_limit_bytes = host_config.get('Memory', 0)
                        if memory_limit_bytes > 0:
                            memory_limit_gb = memory_limit_bytes / (1024**3)
                    except (KeyError, TypeError):
                        pass
                    
                    return {
                        'worker_id': worker_id,
                        'container_id': container_id,
                        'cpu_percent': cpu_percent,
                        'memory_mb': memory_mb,
                        'memory_percent': memory_percent,
                        'cpu_limit_cores': cpu_limit_cores,
                        'memory_limit_gb': memory_limit_gb
                    }
                    
                except Exception as e:
                    logger.warning(f"Failed to get stats for worker {worker_id}: {e}")
                    return None
            
            # Use ThreadPoolExecutor to get stats from all workers in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(workers), 10)) as executor:
                # Submit all worker stat collection tasks
                future_to_worker = {executor.submit(get_worker_stats, worker): worker for worker in workers}
                
                # Collect results as they complete
                for future in concurrent.futures.as_completed(future_to_worker):
                    worker_stats = future.result()
                    if worker_stats is None:
                        continue
                    
                    with aggregation_lock:
                        # Aggregate metrics
                        total_containers += 1
                        total_cpu_percent += worker_stats['cpu_percent']
                        total_memory_mb += worker_stats['memory_mb']
                        
                        # Store node capacity information (for monitoring only, not scheduling)
                        node_capacity[worker_stats['worker_id']] = {
                            'capacity': {
                                'cpu_cores': worker_stats['cpu_limit_cores'],
                                'memory_gb': worker_stats['memory_limit_gb']
                            },
                            'cpu_utilization_percent': worker_stats['cpu_percent'],
                            'memory_utilization_percent': worker_stats['memory_percent'],
                            'active_containers': 1
                        }
            
            # Calculate worker node totals
            worker_nodes = {
                'total_nodes': len(workers),
                'total_cpu_usage_percent': total_cpu_percent,
                'total_memory_usage_mb': total_memory_mb,
                'total_cpu_limit_cores': sum(node_info['capacity']['cpu_cores'] for node_info in node_capacity.values()),
                'total_memory_limit_gb': sum(node_info['capacity']['memory_gb'] for node_info in node_capacity.values()),
                'avg_cpu_utilization_percent': total_cpu_percent / len(workers) if workers else 0,
                'avg_memory_utilization_percent': sum(node_info['memory_utilization_percent'] for node_info in node_capacity.values()) / len(workers) if workers else 0
            }
            # Per-node normalization (compact)
            normalized = {
                'cpu_percent_per_node': (total_cpu_percent / worker_nodes['total_nodes']) if worker_nodes['total_nodes'] else 0.0,
                'memory_mb_per_node': (total_memory_mb / worker_nodes['total_nodes']) if worker_nodes['total_nodes'] else 0.0,
            }
            
            return {
                "total_containers": total_containers,
                "total_cpu_percent": total_cpu_percent,
                "total_memory_mb": total_memory_mb,
                "total_gpu_utilization": total_gpu_utilization,
                "function_aggregates": {},  # Ditto doesn't track per-function aggregates
                "node_capacity": node_capacity,
                "worker_nodes": worker_nodes,
                "normalized": normalized,
                "node_summaries": node_capacity,  # Alias for compatibility
                "resource_monitor_summaries": {}  # Not applicable for Ditto
            }
            
        except Exception as e:
            logger.error(f"Failed to get Ditto resource utilization: {e}")
            return {
                "total_containers": 0,
                "total_cpu_percent": 0.0,
                "total_memory_mb": 0.0,
                "total_gpu_utilization": 0.0,
                "function_aggregates": {},
                "node_capacity": {},
                "worker_nodes": {"total_nodes": 0},
                "normalized": {"cpu_percent_per_node": 0.0, "memory_mb_per_node": 0.0},
                "error": str(e)
            }
