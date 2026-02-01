"""
Resource Monitoring Module for Function Containers

This module provides comprehensive resource utilization tracking for:
- CPU usage (time, percentage, cores)
- Memory usage (RSS, VMS, percentage)
- GPU usage (if available)
- Network I/O
- Disk I/O
- Function execution metrics

Supports both per-invocation and continuous monitoring modes.
"""

import os
import time
import threading
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union
from enum import Enum
import psutil
import docker
from contextlib import contextmanager

try:
    import GPUtil
    GPU_AVAILABLE = True
except ImportError:
    GPU_AVAILABLE = False

try:
    import nvidia_ml_py3 as nvml
    NVIDIA_ML_AVAILABLE = True
except ImportError:
    NVIDIA_ML_AVAILABLE = False


class ResourceType(Enum):
    CPU = "cpu"
    MEMORY = "memory"
    GPU = "gpu"
    NETWORK = "network"
    DISK = "disk"


@dataclass
class ResourceSnapshot:
    """Snapshot of resource usage at a specific point in time"""
    timestamp: float
    cpu_percent: float
    cpu_time_user: float
    cpu_time_system: float
    memory_rss_mb: float
    memory_vms_mb: float
    memory_percent: float
    gpu_utilization: Optional[float] = None
    gpu_memory_used_mb: Optional[float] = None
    gpu_memory_total_mb: Optional[float] = None
    network_bytes_sent: int = 0
    network_bytes_recv: int = 0
    disk_read_bytes: int = 0
    disk_write_bytes: int = 0


@dataclass
class FunctionInvocationMetrics:
    """Complete metrics for a single function invocation"""
    function_name: str
    invocation_id: str
    start_time: float
    end_time: float = 0.0
    duration: float = 0.0
    container_id: Optional[str] = None
    node_id: Optional[str] = None
    input_size_bytes: int = 0
    output_size_bytes: int = 0
    resource_snapshots: List[ResourceSnapshot] = field(default_factory=list)
    peak_cpu_percent: float = 0.0
    peak_memory_mb: float = 0.0
    peak_gpu_utilization: Optional[float] = None
    total_cpu_time: float = 0.0
    total_memory_mb: float = 0.0
    error: Optional[str] = None


class ResourceMonitor:
    """
    Main resource monitoring class that tracks resource usage for function containers.
    Supports both Docker-based and process-based monitoring.
    """
    
    def __init__(self, monitoring_interval: float = 0.1):
        self.monitoring_interval = monitoring_interval
        self.logger = logging.getLogger(__name__)
        self.docker_client = None
        self.active_monitors: Dict[str, threading.Thread] = {}
        self.monitoring_data: Dict[str, List[ResourceSnapshot]] = {}
        self.function_metrics: Dict[str, FunctionInvocationMetrics] = {}
        
        # Initialize Docker client if available
        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            self.logger.warning(f"Docker client not available: {e}")
        
        # Initialize GPU monitoring if available
        self.gpu_available = self._initialize_gpu_monitoring()
        
        # System baseline metrics
        self.system_baseline = self._get_system_baseline()
    
    def _initialize_gpu_monitoring(self) -> bool:
        """Initialize GPU monitoring capabilities"""
        if NVIDIA_ML_AVAILABLE:
            try:
                nvml.nvmlInit()
                self.logger.info("NVIDIA ML monitoring initialized")
                return True
            except Exception as e:
                self.logger.warning(f"NVIDIA ML initialization failed: {e}")
        
        if GPU_AVAILABLE:
            try:
                gpus = GPUtil.getGPUs()
                if gpus:
                    self.logger.info(f"GPUtil monitoring initialized for {len(gpus)} GPUs")
                    return True
            except Exception as e:
                self.logger.warning(f"GPUtil initialization failed: {e}")
        
        return False
    
    def _get_system_baseline(self) -> Dict[str, Any]:
        """Get baseline system metrics for comparison"""
        try:
            return {
                'cpu_count': psutil.cpu_count(),
                'memory_total_gb': psutil.virtual_memory().total / (1024**3),
                'disk_total_gb': psutil.disk_usage('/').total / (1024**3),
                'boot_time': psutil.boot_time(),
                'gpu_count': len(GPUtil.getGPUs()) if GPU_AVAILABLE else 0
            }
        except Exception as e:
            self.logger.warning(f"Failed to get system baseline: {e}")
            return {}
    
    def _get_process_metrics(self, pid: int) -> Optional[ResourceSnapshot]:
        """Get resource metrics for a specific process"""
        try:
            process = psutil.Process(pid)
            
            # CPU metrics
            cpu_times = process.cpu_times()
            cpu_percent = process.cpu_percent()
            
            # Memory metrics
            memory_info = process.memory_info()
            memory_percent = process.memory_percent()
            
            # Network metrics
            try:
                net_io = process.io_counters()
                network_bytes_sent = net_io.bytes_sent if net_io else 0
                network_bytes_recv = net_io.bytes_recv if net_io else 0
                disk_read_bytes = net_io.read_bytes if net_io else 0
                disk_write_bytes = net_io.write_bytes if net_io else 0
            except (psutil.NoSuchProcess, AttributeError):
                network_bytes_sent = network_bytes_recv = 0
                disk_read_bytes = disk_write_bytes = 0
            
            # GPU metrics
            gpu_utilization = None
            gpu_memory_used_mb = None
            gpu_memory_total_mb = None
            
            if self.gpu_available:
                gpu_metrics = self._get_gpu_metrics()
                if gpu_metrics:
                    gpu_utilization = gpu_metrics.get('utilization')
                    gpu_memory_used_mb = gpu_metrics.get('memory_used_mb')
                    gpu_memory_total_mb = gpu_metrics.get('memory_total_mb')
            
            return ResourceSnapshot(
                timestamp=time.time(),
                cpu_percent=cpu_percent,
                cpu_time_user=cpu_times.user,
                cpu_time_system=cpu_times.system,
                memory_rss_mb=memory_info.rss / (1024*1024),
                memory_vms_mb=memory_info.vms / (1024*1024),
                memory_percent=memory_percent,
                gpu_utilization=gpu_utilization,
                gpu_memory_used_mb=gpu_memory_used_mb,
                gpu_memory_total_mb=gpu_memory_total_mb,
                network_bytes_sent=network_bytes_sent,
                network_bytes_recv=network_bytes_recv,
                disk_read_bytes=disk_read_bytes,
                disk_write_bytes=disk_write_bytes
            )
            
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            self.logger.warning(f"Failed to get metrics for PID {pid}: {e}")
            return None
    
    def _get_container_metrics(self, container_id: str) -> Optional[ResourceSnapshot]:
        """Get resource metrics for a Docker container"""
        if not self.docker_client:
            return None
        
        try:
            container = self.docker_client.containers.get(container_id)
            stats = container.stats(stream=False)
            
            # CPU metrics
            cpu_stats = stats.get('cpu_stats', {})
            precpu_stats = stats.get('precpu_stats', {})
            
            # Ensure cpu_stats and precpu_stats are dictionaries
            if cpu_stats is None or not isinstance(cpu_stats, dict):
                cpu_stats = {}
            if precpu_stats is None or not isinstance(precpu_stats, dict):
                precpu_stats = {}
            
            # Calculate CPU percentage
            cpu_percent = 0.0
            cpu_time_user = 0.0
            cpu_time_system = 0.0
            
            if cpu_stats and precpu_stats:
                cpu_usage = cpu_stats.get('cpu_usage', {})
                precpu_usage = precpu_stats.get('cpu_usage', {})
                
                # Get CPU usage deltas
                cpu_delta = cpu_usage.get('total_usage', 0) - precpu_usage.get('total_usage', 0)
                system_delta = cpu_stats.get('system_cpu_usage', 0) - precpu_stats.get('system_cpu_usage', 0)
                
                # Calculate CPU percentage
                if system_delta > 0 and cpu_delta > 0:
                    percpu_usage = cpu_usage.get('percpu_usage', [])
                    if percpu_usage is None or not isinstance(percpu_usage, list):
                        percpu_usage = []
                    cpu_count = len(percpu_usage)
                    if cpu_count == 0:
                        cpu_count = 1  # Fallback
                    cpu_percent = (cpu_delta / system_delta) * cpu_count * 100.0
                
                # Calculate CPU times
                cpu_time_user = cpu_delta / 1e9  # Convert nanoseconds to seconds
                cpu_time_system = system_delta / 1e9
            
            # Memory metrics
            memory_stats = stats.get('memory_stats', {})
            if memory_stats is None or not isinstance(memory_stats, dict):
                memory_stats = {}
            memory_usage = memory_stats.get('usage', 0)
            memory_limit = memory_stats.get('limit', 1)
            memory_percent = (memory_usage / memory_limit) * 100.0 if memory_limit > 0 else 0.0
            
            # Get detailed memory breakdown
            memory_rss_mb = memory_usage / (1024*1024)
            memory_vms_mb = memory_usage / (1024*1024)  # Docker doesn't distinguish RSS/VMS
            
            # Network metrics
            networks = stats.get('networks', {})
            if networks is None or not isinstance(networks, dict):
                networks = {}
            network_bytes_sent = sum(net.get('tx_bytes', 0) for net in networks.values())
            network_bytes_recv = sum(net.get('rx_bytes', 0) for net in networks.values())
            
            # Disk metrics
            blkio_stats = stats.get('blkio_stats', {})
            if blkio_stats is None or not isinstance(blkio_stats, dict):
                blkio_stats = {}
            disk_read_bytes = 0
            disk_write_bytes = 0
            
            if blkio_stats:
                io_service_bytes = blkio_stats.get('io_service_bytes_recursive', [])
                if io_service_bytes is None or not isinstance(io_service_bytes, list):
                    io_service_bytes = []
                for device in io_service_bytes:
                    if device.get('op') == 'Read':
                        disk_read_bytes += device.get('value', 0)
                    elif device.get('op') == 'Write':
                        disk_write_bytes += device.get('value', 0)
            
            # GPU metrics (if available)
            gpu_utilization = None
            gpu_memory_used_mb = None
            gpu_memory_total_mb = None
            
            if self.gpu_available:
                gpu_metrics = self._get_gpu_metrics()
                if gpu_metrics:
                    gpu_utilization = gpu_metrics.get('utilization')
                    gpu_memory_used_mb = gpu_metrics.get('memory_used_mb')
                    gpu_memory_total_mb = gpu_metrics.get('memory_total_mb')
            
            return ResourceSnapshot(
                timestamp=time.time(),
                cpu_percent=cpu_percent,
                cpu_time_user=cpu_time_user,
                cpu_time_system=cpu_time_system,
                memory_rss_mb=memory_rss_mb,
                memory_vms_mb=memory_vms_mb,
                memory_percent=memory_percent,
                gpu_utilization=gpu_utilization,
                gpu_memory_used_mb=gpu_memory_used_mb,
                gpu_memory_total_mb=gpu_memory_total_mb,
                network_bytes_sent=network_bytes_sent,
                network_bytes_recv=network_bytes_recv,
                disk_read_bytes=disk_read_bytes,
                disk_write_bytes=disk_write_bytes
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to get container metrics for {container_id}: {e}")
            return None
    
    def _get_gpu_metrics(self) -> Optional[Dict[str, float]]:
        """Get GPU metrics using available GPU monitoring libraries"""
        if NVIDIA_ML_AVAILABLE:
            try:
                device_count = nvml.nvmlDeviceGetCount()
                if device_count > 0:
                    handle = nvml.nvmlDeviceGetHandleByIndex(0)
                    utilization = nvml.nvmlDeviceGetUtilizationRates(handle)
                    memory_info = nvml.nvmlDeviceGetMemoryInfo(handle)
                    
                    return {
                        'utilization': utilization.gpu,
                        'memory_used_mb': memory_info.used / (1024*1024),
                        'memory_total_mb': memory_info.total / (1024*1024)
                    }
            except Exception as e:
                self.logger.warning(f"NVIDIA ML GPU metrics failed: {e}")
        
        if GPU_AVAILABLE:
            try:
                gpus = GPUtil.getGPUs()
                if gpus:
                    gpu = gpus[0]  # Use first GPU
                    return {
                        'utilization': gpu.load * 100,
                        'memory_used_mb': gpu.memoryUsed,
                        'memory_total_mb': gpu.memoryTotal
                    }
            except Exception as e:
                self.logger.warning(f"GPUtil GPU metrics failed: {e}")
        
        return None
    
    @contextmanager
    def monitor_function_invocation(self, function_name: str, container_id: str = None, 
                                  node_id: str = None, input_size: int = 0):
        """
        Context manager for monitoring a function invocation.
        Returns metrics object that gets populated during execution.
        """
        invocation_id = f"{function_name}_{int(time.time() * 1000)}"
        start_time = time.time()
        
        metrics = FunctionInvocationMetrics(
            function_name=function_name,
            invocation_id=invocation_id,
            start_time=start_time,
            container_id=container_id,
            node_id=node_id,
            input_size_bytes=input_size
        )
        
        self.function_metrics[invocation_id] = metrics
        
        # Start monitoring thread
        monitor_thread = threading.Thread(
            target=self._monitor_invocation,
            args=(invocation_id, container_id),
            daemon=True
        )
        monitor_thread.start()
        self.active_monitors[invocation_id] = monitor_thread
        
        try:
            yield metrics
        except Exception as e:
            metrics.error = str(e)
            raise
        finally:
            # Stop monitoring
            metrics.end_time = time.time()
            metrics.duration = metrics.end_time - metrics.start_time
            
            # Wait for monitoring thread to finish
            if invocation_id in self.active_monitors:
                self.active_monitors[invocation_id].join(timeout=1.0)
                del self.active_monitors[invocation_id]
            
            # Calculate summary metrics
            self._calculate_summary_metrics(metrics)
            
            # Store final metrics
            self.function_metrics[invocation_id] = metrics
    
    def _monitor_invocation(self, invocation_id: str, container_id: str = None):
        """Background monitoring thread for a function invocation"""
        metrics = self.function_metrics.get(invocation_id)
        if not metrics:
            return
        
        while invocation_id in self.active_monitors:
            try:
                snapshot = None
                
                if container_id and self.docker_client:
                    snapshot = self._get_container_metrics(container_id)
                else:
                    # Fallback to process monitoring (current process)
                    snapshot = self._get_process_metrics(os.getpid())
                
                if snapshot:
                    metrics.resource_snapshots.append(snapshot)
                
                time.sleep(self.monitoring_interval)
                
            except Exception as e:
                self.logger.warning(f"Monitoring error for {invocation_id}: {e}")
                break
    
    def _calculate_summary_metrics(self, metrics: FunctionInvocationMetrics):
        """Calculate summary metrics from resource snapshots"""
        if not metrics.resource_snapshots:
            return
        
        # Peak values
        metrics.peak_cpu_percent = max(s.cpu_percent for s in metrics.resource_snapshots)
        metrics.peak_memory_mb = max(s.memory_rss_mb for s in metrics.resource_snapshots)
        
        if any(s.gpu_utilization is not None for s in metrics.resource_snapshots):
            metrics.peak_gpu_utilization = max(
                s.gpu_utilization for s in metrics.resource_snapshots 
                if s.gpu_utilization is not None
            )
        
        # Total values (integrated over time)
        if len(metrics.resource_snapshots) > 1:
            total_cpu_time = 0.0
            total_memory_mb = 0.0
            
            for i in range(1, len(metrics.resource_snapshots)):
                prev_snapshot = metrics.resource_snapshots[i-1]
                curr_snapshot = metrics.resource_snapshots[i]
                
                time_delta = curr_snapshot.timestamp - prev_snapshot.timestamp
                
                # CPU time (average percentage over time interval)
                avg_cpu_percent = (prev_snapshot.cpu_percent + curr_snapshot.cpu_percent) / 2
                total_cpu_time += avg_cpu_percent * time_delta / 100.0
                
                # Memory (average usage over time interval)
                avg_memory_mb = (prev_snapshot.memory_rss_mb + curr_snapshot.memory_rss_mb) / 2
                total_memory_mb += avg_memory_mb * time_delta
            
            metrics.total_cpu_time = total_cpu_time
            metrics.total_memory_mb = total_memory_mb
    
    def get_all_containers_metrics(self) -> Dict[str, ResourceSnapshot]:
        """Get resource metrics for all running containers"""
        if not self.docker_client:
            return {}
        
        try:
            containers = self.docker_client.containers.list(filters={'status': 'running'})
            metrics = {}
            
            for container in containers:
                try:
                    snapshot = self._get_container_metrics(container.id)
                    if snapshot:
                        metrics[container.id] = snapshot
                except Exception as e:
                    self.logger.warning(f"Failed to get metrics for container {container.name}: {e}")
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Failed to get all containers metrics: {e}")
            return {}
    
    def get_container_metrics_by_name(self, container_name: str) -> Optional[ResourceSnapshot]:
        """Get resource metrics for a container by name"""
        if not self.docker_client:
            return None
        
        try:
            container = self.docker_client.containers.get(container_name)
            return self._get_container_metrics(container.id)
        except Exception as e:
            self.logger.warning(f"Failed to get metrics for container {container_name}: {e}")
            return None
    
    def get_node_containers_metrics(self, node_id: str) -> Dict[str, ResourceSnapshot]:
        """Get resource metrics for all containers on a specific node"""
        if not self.docker_client:
            return {}
        
        try:
            # Find containers with the node label
            containers = self.docker_client.containers.list(
                filters={'status': 'running', 'label': f'dataflower-node={node_id}'}
            )
            metrics = {}
            
            for container in containers:
                try:
                    snapshot = self._get_container_metrics(container.id)
                    if snapshot:
                        metrics[container.id] = snapshot
                except Exception as e:
                    self.logger.warning(f"Failed to get metrics for container {container.name}: {e}")
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Failed to get node containers metrics for {node_id}: {e}")
            return {}
    
    def get_function_containers_metrics(self, function_name: str) -> Dict[str, ResourceSnapshot]:
        """Get resource metrics for all containers of a specific function"""
        if not self.docker_client:
            return {}
        
        try:
            # Find containers with the function label
            containers = self.docker_client.containers.list(
                filters={'status': 'running', 'label': f'dataflower-function={function_name}'}
            )
            metrics = {}
            
            for container in containers:
                try:
                    snapshot = self._get_container_metrics(container.id)
                    if snapshot:
                        metrics[container.id] = snapshot
                except Exception as e:
                    self.logger.warning(f"Failed to get metrics for container {container.name}: {e}")
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Failed to get function containers metrics for {function_name}: {e}")
            return {}
    
    def get_all_metrics(self) -> Dict[str, FunctionInvocationMetrics]:
        """Get all collected function metrics"""
        return self.function_metrics.copy()
    
    def get_metrics_summary(self, function_name: str = None) -> Dict[str, Any]:
        """Get summary statistics for function metrics"""
        metrics_list = list(self.function_metrics.values())
        
        if function_name:
            metrics_list = [m for m in metrics_list if m.function_name == function_name]
        
        if not metrics_list:
            return {}
        
        durations = [m.duration for m in metrics_list]
        peak_cpus = [m.peak_cpu_percent for m in metrics_list]
        peak_memories = [m.peak_memory_mb for m in metrics_list]
        
        summary = {
            'total_invocations': len(metrics_list),
            'avg_duration': sum(durations) / len(durations),
            'min_duration': min(durations),
            'max_duration': max(durations),
            'avg_peak_cpu': sum(peak_cpus) / len(peak_cpus),
            'max_peak_cpu': max(peak_cpus),
            'avg_peak_memory': sum(peak_memories) / len(peak_memories),
            'max_peak_memory': max(peak_memories),
            'error_rate': sum(1 for m in metrics_list if m.error) / len(metrics_list)
        }
        
        # GPU statistics if available
        gpu_metrics = [m for m in metrics_list if m.peak_gpu_utilization is not None]
        if gpu_metrics:
            peak_gpus = [m.peak_gpu_utilization for m in gpu_metrics]
            summary.update({
                'gpu_invocations': len(gpu_metrics),
                'avg_peak_gpu': sum(peak_gpus) / len(peak_gpus),
                'max_peak_gpu': max(peak_gpus)
            })
        
        return summary
    
    def export_metrics(self, format: str = 'json') -> Union[str, Dict]:
        """Export metrics in specified format"""
        if format == 'json':
            return {
                'system_baseline': self.system_baseline,
                'function_metrics': {
                    inv_id: {
                        'function_name': m.function_name,
                        'invocation_id': m.invocation_id,
                        'start_time': m.start_time,
                        'end_time': m.end_time,
                        'duration': m.duration,
                        'container_id': m.container_id,
                        'node_id': m.node_id,
                        'input_size_bytes': m.input_size_bytes,
                        'output_size_bytes': m.output_size_bytes,
                        'peak_cpu_percent': m.peak_cpu_percent,
                        'peak_memory_mb': m.peak_memory_mb,
                        'peak_gpu_utilization': m.peak_gpu_utilization,
                        'total_cpu_time': m.total_cpu_time,
                        'total_memory_mb': m.total_memory_mb,
                        'error': m.error,
                        'resource_snapshots': [
                            {
                                'timestamp': s.timestamp,
                                'cpu_percent': s.cpu_percent,
                                'cpu_time_user': s.cpu_time_user,
                                'cpu_time_system': s.cpu_time_system,
                                'memory_rss_mb': s.memory_rss_mb,
                                'memory_vms_mb': s.memory_vms_mb,
                                'memory_percent': s.memory_percent,
                                'gpu_utilization': s.gpu_utilization,
                                'gpu_memory_used_mb': s.gpu_memory_used_mb,
                                'gpu_memory_total_mb': s.gpu_memory_total_mb,
                                'network_bytes_sent': s.network_bytes_sent,
                                'network_bytes_recv': s.network_bytes_recv,
                                'disk_read_bytes': s.disk_read_bytes,
                                'disk_write_bytes': s.disk_write_bytes
                            }
                            for s in m.resource_snapshots
                        ]
                    }
                    for inv_id, m in self.function_metrics.items()
                },
                'summary': self.get_metrics_summary()
            }
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def clear_metrics(self, older_than_hours: float = 24):
        """Clear metrics older than specified hours"""
        cutoff_time = time.time() - (older_than_hours * 3600)
        
        to_remove = [
            inv_id for inv_id, metrics in self.function_metrics.items()
            if metrics.start_time < cutoff_time
        ]
        
        for inv_id in to_remove:
            del self.function_metrics[inv_id]
        
        self.logger.info(f"Cleared {len(to_remove)} old metrics")


# Global resource monitor instance
resource_monitor = ResourceMonitor()
