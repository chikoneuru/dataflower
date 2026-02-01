import time
import psutil
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import numpy as np

@dataclass
class TaskProfile:
    """Task execution profile"""
    task_id: str
    function_name: str
    input_size: int  # bytes
    estimated_compute_time: float  # seconds
    memory_requirement: int  # MB
    cpu_intensity: float  # 0-1 scale

@dataclass
class NodeProfile:
    """Node resource profile"""
    node_id: str
    cpu_cores: int
    memory_total: int  # MB
    memory_available: int  # MB
    cpu_utilization: float  # 0-1
    network_bandwidth: float  # Mbps
    container_cache: Dict[str, bool]  # {function_name: is_warm}

class CostModels:
    def __init__(self):
        self.task_profiles = {}  # Historical task execution data
        self.node_profiles = {}  # Current node states
        
    def estimate_compute_cost(self, task: TaskProfile, node: NodeProfile) -> float:
        """
        Estimate compute time: task_size / available_cpu * load_factor
        """
        # Base compute time from task profile
        base_time = task.estimated_compute_time
        
        # Adjust for CPU availability (considering current load)
        available_cpu = node.cpu_cores * (1 - node.cpu_utilization)
        cpu_factor = max(0.1, available_cpu / node.cpu_cores)  # Minimum 10% efficiency
        
        # Adjust for memory pressure
        memory_factor = min(1.0, node.memory_available / task.memory_requirement)
        
        # Load factor combines CPU and memory constraints
        load_factor = 1.0 / (cpu_factor * memory_factor)
        
        return base_time * load_factor
    
    def estimate_transfer_cost(self, data_size: int, src_node: NodeProfile, 
                             dst_node: NodeProfile) -> float:
        """
        Estimate transfer time: data_size / available_bandwidth
        """
        # Use minimum bandwidth between source and destination
        available_bandwidth = min(src_node.network_bandwidth, dst_node.network_bandwidth)
        
        # Convert to bytes per second
        bandwidth_bps = available_bandwidth * 1024 * 1024 / 8
        
        # Add network congestion factor (simplified)
        congestion_factor = 1.2  # 20% overhead
        
        return (data_size / bandwidth_bps) * congestion_factor
    
    def estimate_cold_start_cost(self, function_name: str, node: NodeProfile) -> float:
        """
        Estimate cold start penalty
        """
        if node.container_cache.get(function_name, False):
            return 0.0  # Warm container
        
        # Cold start penalty (seconds)
        base_cold_start = 2.0  # Base container startup time
        
        # Adjust based on node load
        load_factor = 1.0 + node.cpu_utilization
        
        return base_cold_start * load_factor
    
    def estimate_interference_cost(self, task: TaskProfile, node: NodeProfile) -> float:
        """
        Estimate interference penalty from resource contention
        """
        # Higher interference when node is heavily loaded
        cpu_interference = node.cpu_utilization * 0.5  # 50% penalty at 100% CPU
        memory_interference = max(0, (task.memory_requirement - node.memory_available) / node.memory_total)
        
        return cpu_interference + memory_interference
    
    def get_all_costs(self, task: TaskProfile, node: NodeProfile, 
                     data_transfers: List[Tuple[str, int]]) -> Dict[str, float]:
        """
        Get all cost estimates for a task on a node
        """
        costs = {
            'compute': self.estimate_compute_cost(task, node),
            'cold_start': self.estimate_cold_start_cost(task.function_name, node),
            'interference': self.estimate_interference_cost(task, node)
        }
        
        # Add transfer costs for each data dependency
        total_transfer = 0.0
        for src_node_id, data_size in data_transfers:
            if src_node_id in self.node_profiles:
                src_node = self.node_profiles[src_node_id]
                total_transfer += self.estimate_transfer_cost(data_size, src_node, node)
        
        costs['transfer'] = total_transfer
        costs['total'] = sum(costs.values())
        
        return costs

    # ===== MOCK FUNCTIONS FOR TESTING =====
    
    def mock_estimate_compute_cost(self, task: TaskProfile, node: NodeProfile) -> float:
        """
        Mock function: Returns simple compute cost estimate
        """
        # Simple mock: base time * cpu_factor * memory_factor
        base_time = task.estimated_compute_time
        cpu_factor = 1.0 / max(0.1, 1.0 - node.cpu_utilization)
        memory_factor = 1.0 if node.memory_available >= task.memory_requirement else 2.0
        
        return base_time * cpu_factor * memory_factor
    
    def mock_estimate_transfer_cost(self, data_size: int, src_node: NodeProfile, 
                                  dst_node: NodeProfile) -> float:
        """
        Mock function: Returns simple transfer cost estimate
        """
        # Simple mock: data_size / bandwidth with fixed overhead
        bandwidth = min(src_node.network_bandwidth, dst_node.network_bandwidth)
        return (data_size / (bandwidth * 1024 * 1024 / 8)) * 1.2
    
    def mock_estimate_cold_start_cost(self, function_name: str, node: NodeProfile) -> float:
        """
        Mock function: Returns simple cold start cost estimate
        """
        if node.container_cache.get(function_name, False):
            return 0.0
        return 2.0 * (1.0 + node.cpu_utilization)
    
    def mock_estimate_interference_cost(self, task: TaskProfile, node: NodeProfile) -> float:
        """
        Mock function: Returns simple interference cost estimate
        """
        cpu_penalty = node.cpu_utilization * 0.5
        memory_penalty = max(0, (task.memory_requirement - node.memory_available) / node.memory_total)
        return cpu_penalty + memory_penalty
    
    def mock_get_all_costs(self, task: TaskProfile, node: NodeProfile, 
                          data_transfers: List[Tuple[str, int]]) -> Dict[str, float]:
        """
        Mock function: Returns simple cost estimates for testing
        """
        costs = {
            'compute': self.mock_estimate_compute_cost(task, node),
            'cold_start': self.mock_estimate_cold_start_cost(task.function_name, node),
            'interference': self.mock_estimate_interference_cost(task, node)
        }
        
        # Simple transfer cost calculation
        total_transfer = 0.0
        for _, data_size in data_transfers:
            total_transfer += data_size / (node.network_bandwidth * 1024 * 1024 / 8) * 1.2
        
        costs['transfer'] = total_transfer
        costs['total'] = sum(costs.values())
        
        return costs

    @staticmethod
    def create_mock_task(task_id: str, function_name: str) -> TaskProfile:
        """
        Create a mock task profile for testing
        """
        return TaskProfile(
            task_id=task_id,
            function_name=function_name,
            input_size=1024 * 1024,  # 1MB
            estimated_compute_time=1.0,  # 1 second
            memory_requirement=512,  # 512MB
            cpu_intensity=0.5  # Medium intensity
        )
    
    @staticmethod
    def create_mock_node(node_id: str, cpu_cores: int = 4) -> NodeProfile:
        """
        Create a mock node profile for testing
        """
        return NodeProfile(
            node_id=node_id,
            cpu_cores=cpu_cores,
            memory_total=8192,  # 8GB
            memory_available=4096,  # 4GB
            cpu_utilization=0.3,  # 30% utilization
            network_bandwidth=1000,  # 1Gbps
            container_cache={}  # No warm containers
        )