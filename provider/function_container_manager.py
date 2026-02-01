"""
Function Container Manager: Manages per-function containers instead of worker containers.
Each function has its own container image, deployed as needed across nodes.
"""

import json
import logging
import os
import subprocess
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import docker
import requests
from .resource_monitor import resource_monitor

# Node capacity definitions - total resources available per node
# Updated to match actual container resource limits from docker-compose-full-cluster.yml
# Each node's capacity equals the sum of all container resource limits on that node
NODE_CAPACITY = {
    'node1': {
        'cpu_cores': 11.75,   # Sum of all container CPU limits on node1 (10 containers)
        'memory_gb': 12.0,    # Sum of all container memory limits on node1 (10 containers)
        'bandwidth_mbps': 1000,
        'node_type': 'high-performance'
    },
    'node2': {
        'cpu_cores': 8.0,     # Sum of all container CPU limits on node2 (9 containers)
        'memory_gb': 8.0,     # Sum of all container memory limits on node2 (9 containers)
        'bandwidth_mbps': 1000,
        'node_type': 'balanced'
    },
    'node3': {
        'cpu_cores': 4.125,   # Sum of all container CPU limits on node3 (9 containers)
        'memory_gb': 4.125,   # Sum of all container memory limits on node3 (9 containers)
        'bandwidth_mbps': 1000,
        'node_type': 'lightweight'
    },
    'node4': {
        'cpu_cores': 17.25,   # Sum of all container CPU limits on node4 (11 containers)
        'memory_gb': 19.5,    # Sum of all container memory limits on node4 (11 containers)
        'bandwidth_mbps': 1000,
        'node_type': 'compute-optimized'
    }
}

# Bottleneck thresholds
BOTTLENECK_THRESHOLDS = {
    'cpu_percent': 80.0,      # CPU ≥ 80%
    'memory_percent': 85.0,   # Memory ≥ 85%
    'network_percent': 70.0   # Network ≥ 70%
}


class ContainerStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass
class FunctionContainer:
    """Represents a function container instance"""
    container_id: str
    function_name: str
    node_id: str
    image_name: str
    host_port: int
    status: ContainerStatus
    created_time: float
    last_accessed: float = field(default_factory=time.time)
    request_count: int = 0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    container_object: Optional[object] = None  # Docker container object
    # Resource limits (for accounting during removal/scaling)
    cpu_limit: float = 0.0
    memory_limit: int = 0  # MB


@dataclass
class NodeResources:
    """Represents available resources on a node"""
    node_id: str
    cpu_cores: float
    memory_gb: float
    bandwidth_mbps: float = 1000.0
    available_cpu: float = 0.0
    available_memory: float = 0.0
    running_containers: Dict[str, FunctionContainer] = field(default_factory=dict)
    labels: Dict[str, str] = field(default_factory=dict)


class FunctionContainerManager:
    """
    Manages per-function containers across multiple nodes.
    Handles placement, routing, scaling, and lifecycle management.
    """
    
    def __init__(self, namespace: str = "serverless"):
        self.client = docker.from_env()
        self.docker_client = self.client  # Alias for compatibility with resource monitor
        self.namespace = namespace
        self.logger = logging.getLogger(__name__)
        # Time-series sampling (disabled by default)
        self._sampler_thread = None
        self._sampler_stop_flag = False
        self._sampler_interval_s = 0.2
        self._timeseries_file = os.path.join("results", "resource_timeseries.csv")
        
        # Container registry
        self.function_containers: Dict[str, List[FunctionContainer]] = {}  # function_name -> [containers]
        self.node_resources: Dict[str, NodeResources] = {}  # node_id -> resources
        
        # Thread safety for container discovery
        self._discovery_lock = threading.Lock()
        
        # Node capacity management
        self.node_capacity = NODE_CAPACITY.copy()
        self.bottleneck_thresholds = BOTTLENECK_THRESHOLDS.copy()
        
        # Container configuration
        self.default_cpu_limit = 1.0  # CPU cores
        # Increase default memory to better handle larger payloads (e.g., 25MB images)
        self.default_memory_limit = 1024  # MB
        self.port_range_start = 8000
        self.port_range_end = 9000
        self.used_ports: Set[int] = set()
        
        # Lifecycle settings
        self.idle_timeout = 300  # 5 minutes
        self.max_containers_per_function = 10
        self.min_containers_per_function = 0
        
        self._initialize()

    # ------------------------------------------------------------
    # Lightweight time-series sampler for container resource stats
    # ------------------------------------------------------------
    def start_timeseries_sampler(self, interval_s: float = 0.2, file_path: Optional[str] = None, 
                                scheduler_type: str = "unknown", experiment_id: str = "unknown") -> bool:
        """Start background sampler that appends per-container metrics to a JSON file."""
        try:
            if self._sampler_thread and self._sampler_thread.is_alive():
                return True
            self._sampler_interval_s = max(0.05, float(interval_s))
            if file_path:
                self._timeseries_file = file_path
            # Store context for this sampling session
            self._sampler_scheduler_type = scheduler_type
            self._sampler_experiment_id = experiment_id
            # Ensure output directory
            try:
                os.makedirs(os.path.dirname(self._timeseries_file) or ".", exist_ok=True)
            except Exception as e:
                print(f"\033[91mERROR: {e}\033[0m")
            # Write a small header record (as a comment line) once if file is empty/new
            try:
                if not os.path.exists(self._timeseries_file) or os.path.getsize(self._timeseries_file) == 0:
                    with open(self._timeseries_file, "a", encoding="utf-8") as f:
                        f.write("timestamp, container_name, function_name, node_id, cpu_percent, memory_rss_mb, host_port, scheduler_type, experiment_id\n")
                else:
                    # Clear existing file and write new header for fresh start
                    with open(self._timeseries_file, "w", encoding="utf-8") as f:
                        f.write("timestamp, container_name, function_name, node_id, cpu_percent, memory_rss_mb, host_port, scheduler_type, experiment_id\n")
            except Exception as e:
                print(f"\033[91mERROR: {e}\033[0m")

            self._sampler_stop_flag = False

            import threading

            def _run_sampler():
                iteration_count = 0
                while not self._sampler_stop_flag:
                    try:
                        metrics = self.get_all_containers_resource_metrics()
                        ts = time.time()
                        iteration_count += 1
                        
                        if metrics:
                            with open(self._timeseries_file, "a", encoding="utf-8") as f:
                                for cid, m in metrics.items():
                                    # Write CSV format
                                    record = [
                                        str(ts),
                                        m.get("container_name", "unknown"),
                                        m.get("function_name", "unknown"),
                                        m.get("node_id", "unknown"),
                                        str(float(m.get("cpu_percent", 0.0))),
                                        str(float(m.get("memory_rss_mb", 0.0))),
                                        str(m.get("host_port", "unknown")),
                                        self._sampler_scheduler_type,
                                        self._sampler_experiment_id,
                                    ]
                                    f.write(",".join(record) + "\n")
                            
                            # Debug: print every 10th iteration
                            if iteration_count % 10 == 0:
                                print(f"DEBUG: Sampler iteration {iteration_count}, wrote {len(metrics)} container records")
                        else:
                            if iteration_count % 10 == 0:
                                print(f"DEBUG: Sampler iteration {iteration_count}, no container metrics found")
                                
                    except Exception as e:
                        # Keep sampling even if one iteration fails
                        self.logger.debug(f"Time-series sampler iteration failed: {e}")
                        print(f"\033[91mERROR: {e}\033[00m")
                    finally:
                        time.sleep(self._sampler_interval_s)

            self._sampler_thread = threading.Thread(target=_run_sampler, name="resource-ts-sampler", daemon=True)
            self._sampler_thread.start()
            self.logger.info(f"Started time-series sampler: interval={self._sampler_interval_s}s file={self._timeseries_file} scheduler={scheduler_type} experiment={experiment_id}")
            return True
        except Exception as e:
            self.logger.warning(f"Failed to start time-series sampler: {e}")
            return False

    def stop_timeseries_sampler(self, timeout_s: float = 1.0) -> None:
        """Stop the background sampler if running."""
        try:
            self._sampler_stop_flag = True
            if self._sampler_thread and self._sampler_thread.is_alive():
                self._sampler_thread.join(timeout=max(0.0, float(timeout_s)))
        except Exception as e:
            print(f"\033[91mERROR: {e}\033[0m")
        finally:
            self._sampler_thread = None
    
    def _initialize(self):
        """Initialize the function container manager"""
        try:
            # Initialize node resources based on NODE_CAPACITY
            self._initialize_node_resources()
            
            # Validate container limits against node capacity
            self._validate_container_limits()
            
            # Discover existing containers
            self.discover_existing_containers()
            
            self.logger.info("FunctionContainerManager initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize FunctionContainerManager: {e}")
    
    def _initialize_node_resources(self):
        """Initialize node resources based on NODE_CAPACITY"""
        for node_id, capacity in self.node_capacity.items():
            self.node_resources[node_id] = NodeResources(
                node_id=node_id,
                cpu_cores=capacity['cpu_cores'],
                memory_gb=capacity['memory_gb'],
                bandwidth_mbps=capacity['bandwidth_mbps']
            )
            self.logger.info(f"Initialized node {node_id}: {capacity['cpu_cores']} CPU cores, {capacity['memory_gb']}GB memory")
    
    def _validate_container_limits(self):
        """Validate that container resource limits don't exceed node capacity"""
        try:
            # Get all function containers from docker-compose
            containers = self.client.containers.list(
                    filters={'label': 'dataflower-type=function'}
                )
                
            # Group containers by node
            node_container_limits = {}
            for container in containers:
                try:
                    labels = container.attrs.get('Config', {}).get('Labels', {})
                    node_id = labels.get('dataflower-node', 'unknown')
                    
                    # Extract resource limits from container
                    host_config = container.attrs.get('HostConfig', {})
                    cpu_limit = self._extract_cpu_limit_from_config(host_config)
                    memory_limit = self._extract_memory_limit_from_config(host_config)
                    
                    if node_id not in node_container_limits:
                        node_container_limits[node_id] = {'cpu': 0, 'memory': 0}
                    
                    node_container_limits[node_id]['cpu'] += cpu_limit
                    node_container_limits[node_id]['memory'] += memory_limit
                    
                except Exception as e:
                    self.logger.warning(f"Failed to extract limits from container {container.name}: {e}")
            
            # Validate against node capacity
            for node_id, limits in node_container_limits.items():
                if node_id in self.node_capacity:
                    capacity = self.node_capacity[node_id]
                    cpu_capacity = capacity['cpu_cores']
                    memory_capacity = capacity['memory_gb']
                    
                    cpu_excess = limits['cpu'] - cpu_capacity
                    memory_excess = limits['memory'] - memory_capacity
                    
                    if cpu_excess > 0 or memory_excess > 0:
                        self.logger.warning(f"Node {node_id} capacity exceeded:")
                        if cpu_excess > 0:
                            self.logger.warning(f"  CPU: {limits['cpu']:.1f} cores > {cpu_capacity:.1f} cores (excess: {cpu_excess:.1f})")
                        if memory_excess > 0:
                            self.logger.warning(f"  Memory: {limits['memory']:.1f}GB > {memory_capacity:.1f}GB (excess: {memory_excess:.1f})")
                    else:
                        self.logger.info(f"Node {node_id} capacity OK: CPU {limits['cpu']:.1f}/{cpu_capacity:.1f}, Memory {limits['memory']:.1f}/{memory_capacity:.1f}")
            
        except Exception as e:
            self.logger.error(f"Failed to validate container limits: {e}")
    
    def discover_nodes(self):
        """Discover available nodes - now uses NODE_CAPACITY instead of worker containers"""
        # Nodes are now defined by NODE_CAPACITY, no need to discover from containers
        self.logger.info(f"Using predefined nodes from NODE_CAPACITY: {list(self.node_capacity.keys())}")
        return list(self.node_capacity.keys())
    
    def _extract_container_resources(self, container) -> Tuple[int, float]:
        """Extract CPU and memory resources from Docker container configuration"""
        try:
            host_config = container.attrs.get('HostConfig', {})
            
            # Extract CPU limit
            cpu_cores = 4  # Default
            if 'CpuCount' in host_config and host_config['CpuCount']:
                cpu_cores = host_config['CpuCount']
            elif 'NanoCpus' in host_config and host_config['NanoCpus']:
                # NanoCpus is in units of 1e-9 CPUs, so divide by 1e9
                cpu_cores = int(host_config['NanoCpus'] / 1e9)
            elif 'CpuQuota' in host_config and 'CpuPeriod' in host_config:
                if host_config['CpuQuota'] > 0 and host_config['CpuPeriod'] > 0:
                    cpu_cores = host_config['CpuQuota'] // host_config['CpuPeriod']
            
            # Extract memory limit
            memory_gb = 8.0  # Default
            if 'Memory' in host_config and host_config['Memory']:
                memory_gb = host_config['Memory'] / (1024**3)  # Convert bytes to GB
            
            # Ensure minimums
            cpu_cores = max(1, cpu_cores)
            memory_gb = max(1.0, memory_gb)
            
            self.logger.debug(f"Extracted resources for {container.name}: {cpu_cores} CPU, {memory_gb:.1f}GB RAM")
            return cpu_cores, memory_gb
            
        except Exception as e:
            self.logger.warning(f"Failed to extract resources from {container.name}: {e}")
            return 4, 8.0  # Fallback defaults
    
    def discover_existing_containers(self):
        """Discover existing function containers (thread-safe)"""
        with self._discovery_lock:
            try:
                # Reset registries to avoid duplicate accumulation across discoveries
                self.function_containers = {}
                for node in self.node_resources.values():
                    node.running_containers.clear()
                # Discover function containers
                self._discover_function_containers()
                
            except Exception as e:
                self.logger.error(f"Failed to discover existing containers: {e}")
    
    def _discover_function_containers(self):
        """Discover existing function containers"""
        try:
            # Find containers with function labels
            containers = self.client.containers.list(
                filters={'label': 'dataflower-type=function'}
            )
            
            # If no labeled containers found, fallback to name-based discovery
            if not containers:
                self.logger.info("No labeled function containers found, using name-based discovery")
                all_containers = self.client.containers.list()
                containers = [c for c in all_containers if c.name.startswith('recognizer-') and 'dataflower' not in c.name]
            
            discovered_containers = set()  # Track discovered containers to prevent duplicates
            
            for container in containers:
                try:
                    # Skip if already processed
                    if container.id in discovered_containers:
                        self.logger.debug(f"Skipping duplicate container: {container.id}")
                        continue
                    discovered_containers.add(container.id)
                    
                    labels = container.attrs.get('Config', {}).get('Labels', {})
                    function_name = labels.get('dataflower-function')
                    node_id = labels.get('dataflower-node', 'unknown')
                    
                    # Fallback: extract from container name if no labels
                    if not function_name and container.name.startswith('recognizer-'):
                        # Parse name like: recognizer-adult-node1-1
                        name_parts = container.name.split('-')
                        if len(name_parts) >= 3:
                            function_name = f"recognizer__{name_parts[1]}"
                            node_id = name_parts[2]
                    
                    if not function_name:
                        continue
                    
                    # Extract port information (robust across compose/docker)
                    host_port = None
                    # First try HostConfig.PortBindings
                    try:
                        ports_cfg = container.attrs.get('HostConfig', {}).get('PortBindings', {}) or {}
                        for port_spec, port_bindings in ports_cfg.items():
                            if port_bindings:
                                host_port = int(port_bindings[0].get('HostPort'))
                                break
                    except Exception:
                        host_port = None
                    # Fallback to NetworkSettings.Ports
                    if host_port is None:
                        try:
                            ports_ns = container.attrs.get('NetworkSettings', {}).get('Ports', {}) or {}
                            # Prefer container port 8080/tcp
                            bindings = ports_ns.get('8080/tcp') or []
                            if bindings:
                                host_port = int(bindings[0].get('HostPort'))
                            else:
                                # fallback: pick first mapped port
                                for _p, _bindings in ports_ns.items():
                                    if _bindings:
                                        host_port = int(_bindings[0].get('HostPort'))
                                        break
                        except Exception:
                            host_port = None
                    
                    if host_port:
                        func_container = FunctionContainer(
                            container_id=container.id,
                            function_name=function_name,
                            node_id=node_id,
                            image_name=container.attrs.get('Config', {}).get('Image', ''),
                            host_port=host_port,
                            status=ContainerStatus.RUNNING if container.status in ['running', 'restarting'] else ContainerStatus.STOPPED,
                            created_time=time.time(),  # Simplified
                            container_object=container
                        )
                        
                        if function_name not in self.function_containers:
                            self.function_containers[function_name] = []
                        self.function_containers[function_name].append(func_container)
                        self.used_ports.add(host_port)
                        
                        # Update node resources
                        if node_id in self.node_resources:
                            self.node_resources[node_id].running_containers[container.id] = func_container
                        
                        # self.logger.info(f"Discovered function container: {function_name} on {node_id} (port {host_port})")
                
                except Exception as e:
                    self.logger.warning(f"Failed to process container {container.name}: {e}")
            
            total_discovered = sum(len(containers) for containers in self.function_containers.values())
            self.logger.info(f"Discovered {total_discovered} function containers across {len(self.function_containers)} functions")
            
        except Exception as e:
            self.logger.error(f"Failed to discover function containers: {e}")
    
    def get_worker_node_resource_utilization(self) -> Dict[str, Dict]:
        """Get resource utilization for worker nodes - now returns empty since worker nodes are removed"""
        # Worker nodes have been removed, return empty dict
        return {}
    
    def _extract_cpu_usage_from_stats(self, stats: Dict) -> float:
        """Extract CPU usage percentage from Docker stats"""
        try:
            cpu_stats = stats.get('cpu_stats', {})
            precpu_stats = stats.get('precpu_stats', {})
            
            if not cpu_stats or not precpu_stats:
                return 0.0
            
            # Calculate CPU usage percentage
            cpu_delta = cpu_stats.get('cpu_usage', {}).get('total_usage', 0) - precpu_stats.get('cpu_usage', {}).get('total_usage', 0)
            system_delta = cpu_stats.get('system_cpu_usage', 0) - precpu_stats.get('system_cpu_usage', 0)
            
            if system_delta > 0 and cpu_delta > 0:
                cpu_percent = (cpu_delta / system_delta) * 100.0
                # Get number of CPUs
                cpu_count = len(cpu_stats.get('cpu_usage', {}).get('percpu_usage', []))
                if cpu_count > 0:
                    cpu_percent = cpu_percent * cpu_count
                return cpu_percent
            
            return 0.0
            
        except Exception as e:
            self.logger.warning(f"Failed to extract CPU usage from stats: {e}")
            return 0.0
    
    def _extract_memory_usage_from_stats(self, stats: Dict) -> float:
        """Extract memory usage in MB from Docker stats"""
        try:
            memory_stats = stats.get('memory_stats', {})
            usage = memory_stats.get('usage', 0)
            return usage / (1024 * 1024)  # Convert bytes to MB
            
        except Exception as e:
            self.logger.warning(f"Failed to extract memory usage from stats: {e}")
            return 0.0
    
    def _extract_cpu_limit_from_config(self, host_config: Dict) -> float:
        """Extract CPU limit from host config"""
        try:
            # Check for CpuQuota and CpuPeriod (Docker's CPU limit mechanism)
            if 'CpuQuota' in host_config and 'CpuPeriod' in host_config:
                quota = host_config.get('CpuQuota', 0)
                period = host_config.get('CpuPeriod', 100000)
                if quota > 0 and period > 0:
                    return quota / period
            
            # Check for NanoCpus (newer Docker CPU limit mechanism)
            if 'NanoCpus' in host_config:
                return host_config['NanoCpus'] / 1_000_000_000
            
            # Check for CpuCount (simple CPU count)
            if 'CpuCount' in host_config:
                return float(host_config['CpuCount'])
            
            # If no CPU limit found, return default
            return 1.0
            
        except Exception as e:
            self.logger.warning(f"Failed to extract CPU limit from config: {e}")
            return 1.0
    
    def _extract_memory_limit_from_config(self, host_config: Dict) -> float:
        """Extract memory limit from host config"""
        try:
            if 'Memory' in host_config:
                return host_config['Memory'] / (1024**3)  # Convert to GB
            return 8.0  # Default
        except Exception:
            return 8.0
    
    def detect_node_bottlenecks(self) -> Dict[str, Dict]:
        """
        Detect bottlenecks when actual usage approaches node capacity.
        Returns bottleneck information for each node.
        """
        try:
            bottlenecks = {}
            
            # Get current resource utilization for all containers
            container_metrics = self.get_all_containers_resource_metrics()
            
            # Group containers by node and calculate total usage
            node_usage = {}
            for container_id, metrics in container_metrics.items():
                # Find which node this container belongs to
                node_id = None
                for function_name, containers in self.function_containers.items():
                    for container in containers:
                        if container.container_id == container_id:
                            node_id = container.node_id
                            break
                    if node_id:
                        break
                
                if node_id and node_id in self.node_capacity:
                    if node_id not in node_usage:
                        node_usage[node_id] = {'cpu_percent': 0, 'memory_mb': 0, 'containers': 0}
                    
                    node_usage[node_id]['cpu_percent'] += metrics.get('cpu_percent', 0)
                    node_usage[node_id]['memory_mb'] += metrics.get('memory_rss_mb', 0)
                    node_usage[node_id]['containers'] += 1
            
            # Check each node for bottlenecks
            for node_id, usage in node_usage.items():
                if node_id in self.node_capacity:
                    capacity = self.node_capacity[node_id]
                    
                    # Calculate utilization percentages
                    cpu_utilization = usage['cpu_percent']
                    memory_utilization = (usage['memory_mb'] / (capacity['memory_gb'] * 1024)) * 100
                    
                    # Check bottleneck thresholds
                    cpu_bottleneck = cpu_utilization >= self.bottleneck_thresholds['cpu_percent']
                    memory_bottleneck = memory_utilization >= self.bottleneck_thresholds['memory_percent']
                    
                    # Network bottleneck (simplified - assume high if many containers)
                    network_utilization = min(usage['containers'] * 10, 100)  # Rough estimate
                    network_bottleneck = network_utilization >= self.bottleneck_thresholds['network_percent']
                    
                    is_bottlenecked = cpu_bottleneck or memory_bottleneck or network_bottleneck
                    
                    bottlenecks[node_id] = {
                        'is_bottlenecked': is_bottlenecked,
                        'cpu_utilization_percent': cpu_utilization,
                        'memory_utilization_percent': memory_utilization,
                        'network_utilization_percent': network_utilization,
                        'cpu_bottleneck': cpu_bottleneck,
                        'memory_bottleneck': memory_bottleneck,
                        'network_bottleneck': network_bottleneck,
                        'active_containers': usage['containers'],
                        'capacity': capacity,
                        'thresholds': self.bottleneck_thresholds
                    }
                    
                    if is_bottlenecked:
                        self.logger.warning(f"Node {node_id} bottleneck detected:")
                        if cpu_bottleneck:
                            self.logger.warning(f"  CPU: {cpu_utilization:.1f}% >= {self.bottleneck_thresholds['cpu_percent']}%")
                        if memory_bottleneck:
                            self.logger.warning(f"  Memory: {memory_utilization:.1f}% >= {self.bottleneck_thresholds['memory_percent']}%")
                        if network_bottleneck:
                            self.logger.warning(f"  Network: {network_utilization:.1f}% >= {self.bottleneck_thresholds['network_percent']}%")
            
            return bottlenecks
            
        except Exception as e:
            self.logger.error(f"Failed to detect node bottlenecks: {e}")
            return {}
    
    def get_node_capacity_summary(self) -> Dict[str, Dict]:
        """Get summary of node capacity vs usage"""
        try:
            summary = {}
            bottlenecks = self.detect_node_bottlenecks()
            
            for node_id, capacity in self.node_capacity.items():
                bottleneck_info = bottlenecks.get(node_id, {})
                
                summary[node_id] = {
                    'capacity': capacity,
                    'is_bottlenecked': bottleneck_info.get('is_bottlenecked', False),
                    'cpu_utilization_percent': bottleneck_info.get('cpu_utilization_percent', 0),
                    'memory_utilization_percent': bottleneck_info.get('memory_utilization_percent', 0),
                    'network_utilization_percent': bottleneck_info.get('network_utilization_percent', 0),
                    'active_containers': bottleneck_info.get('active_containers', 0),
                    'thresholds': self.bottleneck_thresholds
                }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Failed to get node capacity summary: {e}")
            return {}
    
    def get_available_port(self) -> int:
        """Get an available port for a new container"""
        for port in range(self.port_range_start, self.port_range_end):
            if port not in self.used_ports:
                self.used_ports.add(port)
                return port
        raise RuntimeError("No available ports in range")
    
    def _parse_cpu(self, cpu_str: str) -> int:
        """Parse CPU string to cores"""
        if cpu_str.endswith('m'):
            return int(cpu_str[:-1]) // 1000
        return int(cpu_str)
    
    def _parse_memory(self, mem_str: str) -> float:
        """Parse memory string to GB"""
        if mem_str.endswith('Ki'):
            return float(mem_str[:-2]) / (1024 * 1024)
        elif mem_str.endswith('Mi'):
            return float(mem_str[:-2]) / 1024
        elif mem_str.endswith('Gi'):
            return float(mem_str[:-2])
        return float(mem_str) / (1024**3)
    
    def get_nodes(self) -> List[NodeResources]:
        """Get all available nodes"""
        return list(self.node_resources.values())
    
    def get_function_containers(self, function_name: str) -> List[FunctionContainer]:
        """Get all containers for a specific function"""
        return self.function_containers.get(function_name, [])
    
    def get_running_containers(self, function_name: str) -> List[FunctionContainer]:
        """Get running containers for a specific function"""
        containers = self.get_function_containers(function_name)
        return [c for c in containers if c.status == ContainerStatus.RUNNING]
    
    def deploy_function_container(self, function_name: str, target_node: str, 
                                image_name: str = None, cpu_limit: float = None, 
                                memory_limit: int = None) -> Optional[FunctionContainer]:
        """
        Deploy a new function container to a specific node.
        This is the PLACEMENT logic - decides initial allocation.
        Now includes resource capacity enforcement.
        """
        try:
            # Set default resource limits if not provided
            cpu_limit = cpu_limit or self.default_cpu_limit
            memory_limit = memory_limit or self.default_memory_limit
            
            # First, check if we already have running containers for this function
            existing_containers = self.get_running_containers(function_name)
            if existing_containers:
                self.logger.info(f"Using existing warm container for {function_name}")
                # Return the first available container
                return existing_containers[0]
            
            # If no existing containers, try to find warm containers that match our requirements
            self.logger.info(f"No existing containers for {function_name}, discovering warm containers...")
            self.discover_existing_containers()
            
            # Check again after discovery
            existing_containers = self.get_running_containers(function_name)
            if existing_containers:
                self.logger.info(f"Found warm container for {function_name} after discovery")
                return existing_containers[0]
            
            # RESOURCE CAPACITY ENFORCEMENT: Check if target node can accommodate the container
            if not self.can_place_container(target_node, cpu_limit, memory_limit):
                self.logger.warning(f"Cannot place {function_name} on {target_node}: insufficient resources "
                                  f"(CPU: {cpu_limit}, Memory: {memory_limit}MB)")
                
                # Try to find alternative nodes with sufficient capacity
                alternative_nodes = self._find_nodes_with_capacity(cpu_limit, memory_limit)
                if alternative_nodes:
                    target_node = alternative_nodes[0]  # Use first available node
                    self.logger.info(f"Redirecting {function_name} to {target_node} with sufficient capacity")
                else:
                    self.logger.error(f"No nodes available with sufficient capacity for {function_name}")
                    return None
            
            # Create and deploy new container with resource limits
            return self._create_and_deploy_container(function_name, target_node, image_name, cpu_limit, memory_limit)
            
        except Exception as e:
            self.logger.error(f"Failed to deploy function container: {e}")
            return None
    
    def route_request(self, function_name: str) -> Optional[FunctionContainer]:
        """
        Route a request to an existing function container.
        This is the ROUTING logic - chooses which container gets the request.
        """
        running_containers = self.get_running_containers(function_name)
        
        if not running_containers:
            return None
        
        # Simple load balancing strategies
        # Strategy 1: Least recently used
        container = min(running_containers, key=lambda c: c.last_accessed)
        
        # Update access tracking
        container.last_accessed = time.time()
        container.request_count += 1
        
        return container
    
    def scale_function(self, function_name: str, target_replicas: int) -> bool:
        """Scale function to target number of replicas with resource capacity enforcement"""
        try:
            current_containers = self.get_running_containers(function_name)
            current_count = len(current_containers)
            
            if target_replicas > current_count:
                # Scale up with capacity enforcement
                needed = target_replicas - current_count
                
                # Find nodes with sufficient capacity
                available_nodes = self._find_nodes_with_capacity(self.default_cpu_limit, self.default_memory_limit)
                
                if len(available_nodes) < needed:
                    self.logger.warning(f"Insufficient capacity to scale {function_name} to {target_replicas}. "
                                      f"Available nodes: {len(available_nodes)}, Needed: {needed}")
                    
                    # Scale to maximum possible with available capacity
                    actual_target = current_count + len(available_nodes)
                    self.logger.info(f"Scaling {function_name} to {actual_target} instead of {target_replicas}")
                    needed = len(available_nodes)
                
                # Deploy containers on available nodes
                for i in range(needed):
                    target_node = available_nodes[i]
                    container = self.deploy_function_container(function_name, target_node)
                    if not container:
                        self.logger.warning(f"Failed to deploy container {i+1}/{needed} for {function_name}")
                
            elif target_replicas < current_count:
                # Scale down
                to_remove = current_count - target_replicas
                # Remove least recently used containers
                containers_to_remove = sorted(current_containers, key=lambda c: c.last_accessed)[:to_remove]
                
                for container in containers_to_remove:
                    self.remove_function_container(container.container_id)
            
            final_count = len(self.get_running_containers(function_name))
            self.logger.info(f"Scaled {function_name} from {current_count} to {final_count} replicas")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to scale function {function_name}: {e}")
            return False
    
    def remove_function_container(self, container_id: str) -> bool:
        """Remove a specific function container"""
        try:
            # Find container in registry
            container_to_remove = None
            function_name = None
            
            for fname, containers in self.function_containers.items():
                for container in containers:
                    if container.container_id == container_id:
                        container_to_remove = container
                        function_name = fname
                        break
                if container_to_remove:
                    break
            
            if not container_to_remove:
                self.logger.warning(f"Container {container_id} not found in registry")
                return False
            
            # Stop and remove Docker container
            if container_to_remove.container_object:
                container_to_remove.container_object.stop()
                container_to_remove.container_object.remove()
            
            # Update registries
            self.function_containers[function_name].remove(container_to_remove)
            self.used_ports.discard(container_to_remove.host_port)
            
            # Update node resources - release actual container resources
            node = self.node_resources.get(container_to_remove.node_id)
            if node and container_id in node.running_containers:
                del node.running_containers[container_id]
                # Release the actual resources used by this container
                node.available_cpu += container_to_remove.cpu_limit
                node.available_memory += container_to_remove.memory_limit / 1024
            
            self.logger.info(f"Removed container {container_id} for function {function_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to remove container {container_id}: {e}")
            return False
    
    def cleanup_idle_containers(self):
        """Clean up idle containers based on idle timeout"""
        current_time = time.time()
        containers_to_remove = []
        
        for function_name, containers in self.function_containers.items():
            running_containers = [c for c in containers if c.status == ContainerStatus.RUNNING]
            
            # Keep at least min_containers_per_function
            if len(running_containers) <= self.min_containers_per_function:
                continue
            
            for container in running_containers:
                idle_time = current_time - container.last_accessed
                if idle_time > self.idle_timeout:
                    containers_to_remove.append(container.container_id)
        
        for container_id in containers_to_remove:
            self.remove_function_container(container_id)
        
        if containers_to_remove:
            self.logger.info(f"Cleaned up {len(containers_to_remove)} idle containers")
    
    def update_real_time_resources(self):
        """Update real-time available resources by calculating actual container usage and system metrics"""
        for node_id, node in self.node_resources.items():
            # Reset to node capacity
            node.available_cpu = node.cpu_cores
            node.available_memory = node.memory_gb
            
            # Get actual system resource usage if possible
            try:
                system_metrics = self._get_system_resource_usage(node_id)
                if system_metrics:
                    # Use actual system utilization
                    node.available_cpu = node.cpu_cores * (1 - system_metrics['cpu_utilization'])
                    node.available_memory = node.memory_gb * (1 - system_metrics['memory_utilization'])
                else:
                    # Fallback: Calculate based on container limits
                    self._calculate_resources_from_containers(node)
            except Exception as e:
                self.logger.warning(f"Failed to get system metrics for node {node_id}, using container-based calculation: {e}")
                self._calculate_resources_from_containers(node)
            
            # Ensure we don't go negative
            node.available_cpu = max(0, node.available_cpu)
            node.available_memory = max(0, node.available_memory)
            
            self.logger.debug(f"Node {node_id}: {node.available_cpu:.1f} CPU, {node.available_memory:.1f}GB available")
    
    def _get_system_resource_usage(self, node_id: str) -> Optional[Dict[str, float]]:
        """Get actual system resource usage for a node"""
        try:
            # For local Docker setup, use psutil if available
            import psutil
            
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                'cpu_utilization': cpu_percent / 100.0,
                'memory_utilization': memory.percent / 100.0,
                'disk_utilization': (disk.total - disk.free) / disk.total,
                'memory_total_gb': memory.total / (1024**3),
                'memory_available_gb': memory.available / (1024**3),
                'cpu_count': psutil.cpu_count(),
                'load_average': psutil.getloadavg()[0] if hasattr(psutil, 'getloadavg') else 0.0
            }
        except ImportError:
            self.logger.debug("psutil not available for system metrics")
            return None
        except Exception as e:
            self.logger.warning(f"Failed to get system metrics: {e}")
            return None
    
    def _calculate_resources_from_containers(self, node: NodeResources):
        """Calculate available resources based on container limits (fallback method)"""
        # Subtract resources used by all function containers on this node
        for container in node.running_containers.values():
            try:
                # Get actual container resource limits from Docker
                if container.container_object:
                    host_config = container.container_object.attrs.get('HostConfig', {})
                    
                    # Subtract CPU usage
                    if 'NanoCpus' in host_config and host_config['NanoCpus']:
                        cpu_used = host_config['NanoCpus'] / 1e9
                        node.available_cpu -= cpu_used
                    elif 'CpuCount' in host_config and host_config['CpuCount']:
                        node.available_cpu -= host_config['CpuCount']
                    else:
                        node.available_cpu -= 1.0  # Default assumption
                    
                    # Subtract memory usage
                    if 'Memory' in host_config and host_config['Memory']:
                        memory_used_gb = host_config['Memory'] / (1024**3)
                        node.available_memory -= memory_used_gb
                    else:
                        node.available_memory -= 1.0  # Default assumption
                        
            except Exception as e:
                self.logger.warning(f"Failed to get resources for container {container.container_id}: {e}")
    
    def get_node_available_resources(self, node_id: str) -> Dict[str, float]:
        """Get real-time available resources for a specific node"""
        if node_id not in self.node_resources:
            return {'cpu': 0, 'memory': 0, 'error': 'Node not found'}
        
        # Update real-time resources first
        self.update_real_time_resources()
        
        node = self.node_resources[node_id]
        return {
            'available_cpu': node.available_cpu,
            'available_memory': node.available_memory,
            'total_cpu': node.cpu_cores,
            'total_memory': node.memory_gb,
            'cpu_utilization': (node.cpu_cores - node.available_cpu) / node.cpu_cores,
            'memory_utilization': (node.memory_gb - node.available_memory) / node.memory_gb,
            'running_containers': len(node.running_containers)
        }
    
    def can_place_container(self, node_id: str, cpu_required: float, memory_required: float) -> bool:
        """Check if a container can be placed on a node based on real-time resources"""
        resources = self.get_node_available_resources(node_id)
        return (resources['available_cpu'] >= cpu_required and 
                resources['available_memory'] >= memory_required)
    
    def _find_nodes_with_capacity(self, cpu_required: float, memory_required: float) -> List[str]:
        """Find nodes that have sufficient capacity for the required resources"""
        available_nodes = []
        
        # Update real-time resources first
        self.update_real_time_resources()
        
        for node_id in self.node_resources.keys():
            if self.can_place_container(node_id, cpu_required, memory_required):
                available_nodes.append(node_id)
        
        # Sort by available capacity (prefer nodes with more available resources)
        available_nodes.sort(key=lambda node_id: (
            self.get_node_available_resources(node_id)['available_cpu'] +
            self.get_node_available_resources(node_id)['available_memory']
        ), reverse=True)
        
        return available_nodes
    
    def _create_and_deploy_container(self, function_name: str, target_node: str, 
                                   image_name: str, cpu_limit: float, memory_limit: int) -> Optional[FunctionContainer]:
        """Create and deploy a new container with resource limits"""
        try:
            # Determine image name if not provided
            if not image_name:
                image_name = f"dataflower/{function_name}:latest"
            
            # Generate unique container name
            container_name = f"{function_name}-{target_node}-{int(time.time())}"
            
            # Find available host port
            host_port = self._get_available_port()
            if host_port is None:
                self.logger.error(f"No available ports for {function_name}")
                return None
            
            # Prepare environment variables
            env_vars = {
                'FUNCTION_NAME': function_name,
                'NODE_ID': target_node,
                # Functions generally listen on 8080 inside container
                'PORT': '8080',
                'REDIS_HOST': 'dataflower_redis',
                'REDIS_PORT': '6379'
            }
            
            # Prepare resource limits
            deploy_params = {
                'image': image_name,
                'name': container_name,
                'environment': env_vars,
                # Map container 8080 -> chosen host_port
                'ports': {'8080/tcp': host_port},
                'labels': {
                    'dataflower-function': function_name,
                    'dataflower-node': target_node,
                    'dataflower-type': 'function-container',
                    'dataflower-port': str(host_port)
                },
                'detach': True,
                'remove': False,
                'restart_policy': {'Name': 'unless-stopped'}
            }
            
            # Add resource limits if supported
            try:
                deploy_params['mem_limit'] = f"{memory_limit}m"
                deploy_params['cpu_quota'] = int(cpu_limit * 100000)  # Convert to microseconds
                deploy_params['cpu_period'] = 100000
            except Exception as e:
                self.logger.warning(f"Could not set resource limits: {e}")
            
            # Create and start container
            self.logger.info(f"Creating container {container_name} with CPU: {cpu_limit}, Memory: {memory_limit}MB (host_port={host_port})")
            container = self.client.containers.run(**deploy_params)
            
            # Optional: wait for health endpoint to be ready
            try:
                url = f"http://127.0.0.1:{host_port}/health"
                deadline = time.time() + 15.0
                while time.time() < deadline:
                    try:
                        resp = requests.get(url, timeout=0.5)
                        if resp.status_code == 200:
                            break
                    except Exception:
                        pass
                    time.sleep(0.2)
            except Exception:
                pass

            # Create FunctionContainer object
            function_container = FunctionContainer(
                container_id=container.id,
                function_name=function_name,
                node_id=target_node,
                image_name=image_name,
                host_port=host_port,
                status=ContainerStatus.RUNNING,
                created_time=time.time(),
                last_accessed=time.time(),
                request_count=0,
                container_object=container,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit
            )
            
            # Register container
            if function_name not in self.function_containers:
                self.function_containers[function_name] = []
            self.function_containers[function_name].append(function_container)
            
            # Update node resources
            if target_node in self.node_resources:
                self.node_resources[target_node].running_containers[container.id] = function_container
                self.node_resources[target_node].available_cpu -= cpu_limit
                self.node_resources[target_node].available_memory -= memory_limit / 1024
            
            self.logger.info(f"Successfully deployed {function_name} container {container.id} on {target_node}")
            return function_container
            
        except Exception as e:
            self.logger.error(f"Failed to create container for {function_name}: {e}")
            return None
    
    def _get_available_port(self) -> Optional[int]:
        """Get an available port in the configured range"""
        for port in range(self.port_range_start, self.port_range_end + 1):
            if port not in self.used_ports:
                self.used_ports.add(port)
                return port
        return None
    
    def get_cluster_capacity_status(self) -> Dict[str, Any]:
        """Get overall cluster capacity status for monitoring and alerting"""
        try:
            # Update real-time resources first
            self.update_real_time_resources()
            
            total_cpu = 0.0
            total_memory = 0.0
            available_cpu = 0.0
            available_memory = 0.0
            overloaded_nodes = []
            underutilized_nodes = []
            
            for node_id, node in self.node_resources.items():
                total_cpu += node.cpu_cores
                total_memory += node.memory_gb
                available_cpu += node.available_cpu
                available_memory += node.available_memory
                
                # Check if node is overloaded
                cpu_utilization = (node.cpu_cores - node.available_cpu) / node.cpu_cores
                memory_utilization = (node.memory_gb - node.available_memory) / node.memory_gb
                
                if (cpu_utilization > self.bottleneck_thresholds['cpu_percent'] / 100.0 or
                    memory_utilization > self.bottleneck_thresholds['memory_percent'] / 100.0):
                    overloaded_nodes.append({
                        'node_id': node_id,
                        'cpu_utilization': cpu_utilization,
                        'memory_utilization': memory_utilization,
                        'running_containers': len(node.running_containers)
                    })
                
                # Check if node is underutilized (less than 20% utilization)
                if cpu_utilization < 0.2 and memory_utilization < 0.2:
                    underutilized_nodes.append({
                        'node_id': node_id,
                        'cpu_utilization': cpu_utilization,
                        'memory_utilization': memory_utilization,
                        'running_containers': len(node.running_containers)
                    })
            
            overall_cpu_utilization = (total_cpu - available_cpu) / total_cpu if total_cpu > 0 else 0.0
            overall_memory_utilization = (total_memory - available_memory) / total_memory if total_memory > 0 else 0.0
            
            return {
                'timestamp': time.time(),
                'cluster_summary': {
                    'total_nodes': len(self.node_resources),
                    'total_cpu_cores': total_cpu,
                    'total_memory_gb': total_memory,
                    'available_cpu_cores': available_cpu,
                    'available_memory_gb': available_memory,
                    'overall_cpu_utilization': overall_cpu_utilization,
                    'overall_memory_utilization': overall_memory_utilization
                },
                'overloaded_nodes': overloaded_nodes,
                'underutilized_nodes': underutilized_nodes,
                'capacity_warnings': {
                    'high_utilization': overall_cpu_utilization > 0.8 or overall_memory_utilization > 0.8,
                    'overloaded_nodes_count': len(overloaded_nodes),
                    'underutilized_nodes_count': len(underutilized_nodes)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get cluster capacity status: {e}")
            return {'error': str(e), 'timestamp': time.time()}
    
    def get_all_nodes_resources(self) -> Dict[str, Dict[str, float]]:
        """Get real-time resource information for all nodes"""
        # Update real-time resources first
        self.update_real_time_resources()
        
        nodes_resources = {}
        for node_id, node in self.node_resources.items():
            nodes_resources[node_id] = {
                'available_cpu': node.available_cpu,
                'available_memory': node.available_memory,
                'total_cpu': node.cpu_cores,
                'total_memory': node.memory_gb,
                'cpu_utilization': (node.cpu_cores - node.available_cpu) / node.cpu_cores,
                'memory_utilization': (node.memory_gb - node.available_memory) / node.memory_gb,
                'running_containers': len(node.running_containers),
                'labels': node.labels.copy()
            }
        
        return nodes_resources
    
    def get_cluster_status(self) -> Dict:
        """Get overall cluster status with real-time resource tracking"""
        # Update real-time resources first
        self.update_real_time_resources()
        
        total_containers = sum(len(containers) for containers in self.function_containers.values())
        running_containers = sum(len(self.get_running_containers(fname)) 
                               for fname in self.function_containers.keys())
        
        return {
            'total_nodes': len(self.node_resources),
            'total_containers': total_containers,
            'running_containers': running_containers,
            'functions': list(self.function_containers.keys()),
            'node_resources': {
                node_id: {
                    'total_cpu': node.cpu_cores,
                    'available_cpu': node.available_cpu,
                    'cpu_utilization': (node.cpu_cores - node.available_cpu) / node.cpu_cores,
                    'total_memory': node.memory_gb,
                    'available_memory': node.available_memory,
                    'memory_utilization': (node.memory_gb - node.available_memory) / node.memory_gb,
                    'running_containers': len(node.running_containers),
                    'container_details': [
                        {
                            'name': container.container_id[:12],
                            'function': container.function_name,
                            'status': container.status.value
                        }
                        for container in node.running_containers.values()
                    ]
                }
                for node_id, node in self.node_resources.items()
            }
        }
    
    def get_container_resource_metrics(self, container_id: str) -> Optional[Dict]:
        """Get real-time resource metrics for a container"""
        try:
            if not self.docker_client:
                return None
            
            container = self.docker_client.containers.get(container_id)
            
            # Get real-time stats from Docker
            stats = container.stats(stream=False)
            
            # Extract CPU usage
            cpu_percent = 0.0
            try:
                cpu_stats = stats.get('cpu_stats', {})
                precpu_stats = stats.get('precpu_stats', {})
                
                if cpu_stats and precpu_stats:
                    cpu_usage = cpu_stats.get('cpu_usage', {})
                    precpu_usage = precpu_stats.get('cpu_usage', {})
                    
                    cpu_delta = cpu_usage.get('total_usage', 0) - precpu_usage.get('total_usage', 0)
                    system_delta = cpu_stats.get('system_cpu_usage', 0) - precpu_stats.get('system_cpu_usage', 0)
                    
                    if system_delta > 0 and cpu_delta > 0:
                        percpu_usage = cpu_usage.get('percpu_usage', [])
                        if percpu_usage is None or not isinstance(percpu_usage, list):
                            percpu_usage = []
                        cpu_count = len(percpu_usage) if percpu_usage else 1
                        cpu_percent = (cpu_delta / system_delta) * cpu_count * 100.0
            except Exception as e:
                self.logger.debug(f"Failed to calculate CPU usage for {container_id}: {e}")
            
            # Extract memory usage - use RSS (Resident Set Size) for accurate memory reporting
            memory_mb = 0.0
            try:
                memory_stats = stats.get('memory_stats', {})
                if memory_stats:
                    # Use RSS (Resident Set Size) instead of total usage for more accurate reporting
                    # RSS represents actual physical memory used by the process
                    if 'rss' in memory_stats:
                        memory_mb = memory_stats['rss'] / (1024 * 1024)  # Convert to MB
                    elif 'usage' in memory_stats:
                        # Fallback to usage but apply a conservative factor
                        # Docker's 'usage' includes cached memory which can be misleading
                        memory_usage = memory_stats['usage']
                        memory_mb = (memory_usage * 0.6) / (1024 * 1024)  # Apply 60% factor
                    else:
                        memory_mb = 0.0
            except Exception as e:
                self.logger.debug(f"Failed to calculate memory usage for {container_id}: {e}")
            
            # Get container info
            container_info = container.attrs
            labels = container_info.get('Config', {}).get('Labels', {})
            
            return {
                'container_id': container_id,
                'container_name': container.name,
                'function_name': labels.get('dataflower-function', 'unknown'),
                'node_id': labels.get('dataflower-node', 'unknown'),
                'timestamp': time.time(),
                'cpu_percent': cpu_percent,
                'memory_rss_mb': memory_mb,
                'memory_vms_mb': memory_mb,  # Docker doesn't distinguish RSS/VMS
                'memory_percent': 0.0,  # Would need memory limit to calculate
                'gpu_utilization': None,
                'gpu_memory_used_mb': None,
                'gpu_memory_total_mb': None,
                'network_bytes_sent': 0,
                'network_bytes_recv': 0,
                'disk_read_bytes': 0,
                'disk_write_bytes': 0,
                'status': container.status,
                'created': container_info.get('Created', ''),
                'image': container_info.get('Config', {}).get('Image', '')
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to get resource metrics for container {container_id}: {e}")
            return None
    
    def _get_container_metrics_fast(self, docker_container, container: FunctionContainer) -> Optional[Dict]:
        """Fast container metrics extraction using cached container object"""
        try:
            # Get real-time stats from Docker
            stats = docker_container.stats(stream=False)
            
            # Extract CPU usage
            cpu_percent = 0.0
            try:
                cpu_stats = stats.get('cpu_stats', {})
                precpu_stats = stats.get('precpu_stats', {})
                
                if cpu_stats and precpu_stats:
                    cpu_usage = cpu_stats.get('cpu_usage', {})
                    precpu_usage = precpu_stats.get('cpu_usage', {})
                    
                    cpu_delta = cpu_usage.get('total_usage', 0) - precpu_usage.get('total_usage', 0)
                    system_delta = cpu_stats.get('system_cpu_usage', 0) - precpu_stats.get('system_cpu_usage', 0)
                    
                    if system_delta > 0 and cpu_delta > 0:
                        percpu_usage = cpu_usage.get('percpu_usage', [])
                        if percpu_usage is None or not isinstance(percpu_usage, list):
                            percpu_usage = []
                        cpu_count = len(percpu_usage) if percpu_usage else 1
                        cpu_percent = (cpu_delta / system_delta) * cpu_count * 100.0
            except Exception as e:
                self.logger.debug(f"Failed to calculate CPU usage for {container.container_id}: {e}")
            
            # Extract memory usage - use RSS (Resident Set Size) for accurate memory reporting
            memory_mb = 0.0
            try:
                memory_stats = stats.get('memory_stats', {})
                if memory_stats:
                    # Use RSS (Resident Set Size) instead of total usage for more accurate reporting
                    # RSS represents actual physical memory used by the process
                    if 'rss' in memory_stats:
                        memory_mb = memory_stats['rss'] / (1024 * 1024)  # Convert to MB
                    elif 'usage' in memory_stats:
                        # Fallback to usage but apply a conservative factor
                        # Docker's 'usage' includes cached memory which can be misleading
                        memory_usage = memory_stats['usage']
                        memory_mb = (memory_usage * 0.6) / (1024 * 1024)  # Apply 60% factor
                    else:
                        memory_mb = 0.0
            except Exception as e:
                self.logger.debug(f"Failed to calculate memory usage for {container.container_id}: {e}")
            
            # Get container info
            container_info = docker_container.attrs
            labels = container_info.get('Config', {}).get('Labels', {})
            
            return {
                'container_id': container.container_id,
                'container_name': docker_container.name,
                'function_name': labels.get('dataflower-function', container.function_name),
                'node_id': labels.get('dataflower-node', container.node_id),
                'timestamp': time.time(),
                'cpu_percent': cpu_percent,
                'memory_rss_mb': memory_mb,
                'memory_vms_mb': memory_mb,  # Docker doesn't distinguish RSS/VMS
                'memory_percent': 0.0,  # Would need memory limit to calculate
                'gpu_utilization': None,
                'gpu_memory_used_mb': None,
                'gpu_memory_total_mb': None,
                'network_bytes_sent': 0,
                'network_bytes_recv': 0,
                'disk_read_bytes': 0,
                'disk_write_bytes': 0,
                'status': docker_container.status,
                'created': container_info.get('Created', ''),
                'image': container_info.get('Config', {}).get('Image', '')
            }
            
        except Exception as e:
            self.logger.debug(f"Failed to get fast metrics for container {container.container_id}: {e}")
            return None

    def get_all_containers_resource_metrics(self) -> Dict[str, Dict]:
        """Get resource metrics for all running containers - parallel optimized version"""
        metrics = {}
        
        try:
            # Lazy discovery: if function_containers is empty, try discovering them
            # This handles the case where the sampler starts before register_workflow() is called
            # Use lock to prevent race condition with register_workflow's discovery
            if not self.function_containers:
                self.logger.debug("Lazy discovery: function_containers is empty, discovering now")
                print(f"\033[93mDEBUG: Lazy discovery triggered - discovering containers now\033[00m")
                self.discover_existing_containers()  # This will acquire the lock internally
            
            # Get all running containers in one batch call
            all_containers = self.docker_client.containers.list(filters={'status': 'running'})
            container_map = {c.id: c for c in all_containers}
            
            # Collect all containers to query in parallel
            containers_to_query = []
            for function_name, containers in self.function_containers.items():
                for container in containers:
                    if container.status == ContainerStatus.RUNNING and container.container_id in container_map:
                        docker_container = container_map[container.container_id]
                        containers_to_query.append((docker_container, container))
            
            # Parallel stats collection using ThreadPoolExecutor
            # This dramatically speeds up metrics collection (30 containers @ 1s each → ~1-2s total)
            from concurrent.futures import ThreadPoolExecutor, as_completed
            
            def get_single_container_metrics(docker_container, container):
                """Helper to get metrics for a single container"""
                try:
                    container_metrics = self._get_container_metrics_fast(docker_container, container)
                    if container_metrics:
                        return container.container_id, {
                            **container_metrics,
                            'function_name': container.function_name,
                            'node_id': container.node_id,
                            'host_port': container.host_port
                        }
                except Exception as e:
                    self.logger.debug(f"Failed to get metrics for {container.container_id[:12]}: {e}")
                return None, None
            
            # Use ThreadPoolExecutor with max_workers=16 for parallel collection
            with ThreadPoolExecutor(max_workers=16) as executor:
                futures = {
                    executor.submit(get_single_container_metrics, docker_c, func_c): (docker_c, func_c)
                    for docker_c, func_c in containers_to_query
                }
                
                for future in as_completed(futures):
                    container_id, container_metrics = future.result()
                    if container_id and container_metrics:
                        metrics[container_id] = container_metrics
        except Exception as e:
            print(f"\033[91mERROR in get_all_containers_resource_metrics: {e}\033[00m")
            import traceback
            traceback.print_exc()
            self.logger.warning(f"Failed to get batch container metrics: {e}")
            # Fallback to individual calls
            for function_name, containers in self.function_containers.items():
                for container in containers:
                    if container.status == ContainerStatus.RUNNING:
                        container_metrics = self.get_container_resource_metrics(container.container_id)
                        if container_metrics:
                            metrics[container.container_id] = {
                                **container_metrics,
                                'function_name': container.function_name,
                                'node_id': container.node_id,
                                'host_port': container.host_port
                            }
        
        return metrics
    
    def get_function_resource_summary(self, function_name: str) -> Dict:
        """Get resource utilization summary for a specific function"""
        try:
            # Get metrics from resource monitor
            monitor_summary = resource_monitor.get_metrics_summary(function_name)
            
            # Get current container metrics
            containers = self.get_function_containers(function_name)
            running_containers = [c for c in containers if c.status == ContainerStatus.RUNNING]
            
            current_metrics = []
            for container in running_containers:
                container_metrics = self.get_container_resource_metrics(container.container_id)
                if container_metrics:
                    current_metrics.append(container_metrics)
            
            # Calculate current resource usage
            current_cpu = sum(m['cpu_percent'] for m in current_metrics) if current_metrics else 0
            current_memory = sum(m['memory_rss_mb'] for m in current_metrics) if current_metrics else 0
            current_gpu = sum(m['gpu_utilization'] or 0 for m in current_metrics) if current_metrics else 0
            
            return {
                'function_name': function_name,
                'running_containers': len(running_containers),
                'current_cpu_percent': current_cpu,
                'current_memory_mb': current_memory,
                'current_gpu_percent': current_gpu,
                'historical_summary': monitor_summary,
                'container_details': [
                    {
                        'container_id': c.container_id[:12],
                        'node_id': c.node_id,
                        'host_port': c.host_port,
                        'status': c.status.value,
                        'request_count': c.request_count,
                        'last_accessed': c.last_accessed
                    }
                    for c in running_containers
                ]
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get function resource summary for {function_name}: {e}")
            return {'error': str(e)}
    
    def get_node_resource_summary(self, node_id: str) -> Dict:
        """Get resource utilization summary for one node"""
        if node_id not in self.node_resources:
            return {'error': 'Node not found'}
        
        node = self.node_resources[node_id]
        
        # Get container metrics for this node using Docker
        container_metrics = []
        try:
            # Get all containers on this node using Docker labels
            containers = self.docker_client.containers.list(
                filters={'status': 'running', 'label': f'dataflower-node={node_id}'}
            )
            
            for container in containers:
                metrics = self.get_container_resource_metrics(container.id)
                if metrics:
                    container_metrics.append(metrics)
        except Exception as e:
            self.logger.warning(f"Failed to get Docker containers for node {node_id}: {e}")
        
        # Calculate totals from Docker metrics
        total_cpu = sum(m['cpu_percent'] for m in container_metrics)
        total_memory = sum(m['memory_rss_mb'] for m in container_metrics)
        total_gpu = sum(m['gpu_utilization'] or 0 for m in container_metrics)
        
        # Calculate utilization percentages
        cpu_utilization = min(total_cpu / 100.0, 1.0) if node.cpu_cores > 0 else 0
        memory_utilization = min(total_memory / (node.memory_gb * 1024), 1.0) if node.memory_gb > 0 else 0
        
        return {
            'node_id': node_id,
            'total_cpu_cores': node.cpu_cores,
            'available_cpu': node.cpu_cores * (1 - cpu_utilization),
            'cpu_utilization': cpu_utilization,
            'total_memory_gb': node.memory_gb,
            'available_memory': node.memory_gb * (1 - memory_utilization),
            'memory_utilization': memory_utilization,
            'running_containers': len(container_metrics),
            'current_cpu_usage_percent': total_cpu,
            'current_memory_usage_mb': total_memory,
            'current_gpu_usage_percent': total_gpu,
            'container_details': container_metrics,
            'labels': node.labels,
            'timestamp': time.time()
        }
