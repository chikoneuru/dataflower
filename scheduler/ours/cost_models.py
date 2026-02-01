import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import psutil
from .static_profiling import StaticProfilingManager


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
class NodeConstraints:
    """Node placement constraints"""
    # Resource constraints
    min_cpu_cores: int = 1
    min_memory_gb: float = 1.0
    max_cpu_utilization: float = 0.9  # Don't place if CPU > 90%
    max_memory_utilization: float = 0.9  # Don't place if memory > 90%
    
    # Placement constraints
    required_labels: Dict[str, str] = field(default_factory=dict)
    forbidden_labels: Dict[str, str] = field(default_factory=dict)
    
    # Network constraints
    min_network_bandwidth: float = 100.0  # Mbps
    max_network_latency: float = 100.0  # ms
    
    # Storage constraints
    min_disk_space_gb: float = 1.0
    requires_ssd: bool = False
    
    # Security constraints
    security_zone: Optional[str] = None
    requires_isolation: bool = False

@dataclass
class NodeProfile:
    """Enhanced node resource profile with real system data"""
    node_id: str
    
    # Hardware specifications (real values)
    cpu_cores: int
    cpu_frequency: float  # GHz
    memory_total: float  # GB
    memory_available: float  # GB
    disk_total: float  # GB
    disk_available: float  # GB
    
    # Current utilization (real-time)
    cpu_utilization: float  # 0-1
    memory_utilization: float  # 0-1
    disk_utilization: float  # 0-1
    network_bandwidth: float  # Mbps
    
    # Node metadata
    labels: Dict[str, str] = field(default_factory=dict)
    constraints: NodeConstraints = field(default_factory=NodeConstraints)
    
    # Container state
    container_cache: Dict[str, bool] = field(default_factory=dict)  # {function_name: is_warm}
    running_containers: int = 0
    
    # Network and location info
    zone: Optional[str] = None
    region: Optional[str] = None
    network_latency_map: Dict[str, float] = field(default_factory=dict)  # {other_node_id: latency_ms}
    
    # Timestamps
    last_updated: float = field(default_factory=time.time)
    
    @property
    def memory_available_mb(self) -> int:
        """Backward compatibility - memory in MB"""
        return int(self.memory_available * 1024)
    
    @property
    def memory_total_mb(self) -> int:
        """Backward compatibility - memory in MB"""
        return int(self.memory_total * 1024)
    
    def is_overloaded(self) -> bool:
        """Check if node is overloaded based on constraints"""
        return (self.cpu_utilization > self.constraints.max_cpu_utilization or 
                self.memory_utilization > self.constraints.max_memory_utilization)
    
    def can_satisfy_constraints(self, required_constraints: NodeConstraints) -> bool:
        """Check if this node can satisfy the given constraints"""
        # Check resource requirements
        if (self.cpu_cores < required_constraints.min_cpu_cores or
            self.memory_total < required_constraints.min_memory_gb or
            self.network_bandwidth < required_constraints.min_network_bandwidth or
            self.disk_available < required_constraints.min_disk_space_gb):
            return False
        
        # Check utilization limits
        if (self.cpu_utilization > required_constraints.max_cpu_utilization or
            self.memory_utilization > required_constraints.max_memory_utilization):
            return False
        
        
        # Check required labels
        for key, value in required_constraints.required_labels.items():
            if self.labels.get(key) != value:
                return False
        
        # Check forbidden labels
        for key, value in required_constraints.forbidden_labels.items():
            if self.labels.get(key) == value:
                return False
        
        # Check security zone
        if (required_constraints.security_zone and 
            self.labels.get('security-zone') != required_constraints.security_zone):
            return False
        
        return True

class CostModels:
    def __init__(self, use_real_resources: bool = True, profiler=None, congestion_coefficient: float = 0.3):
        self.task_profiles = {}  # Historical task execution data
        self.node_profiles = {}  # Current node states
        self.use_real_resources = use_real_resources
        self.container_manager = None  # Will be injected for real resource access
        
        # Congestion coefficient for load effects: (1 + κ * u_r)
        self.congestion_coefficient = congestion_coefficient
        
        # Initialize static profiling manager
        self.static_profiler = StaticProfilingManager()
        self.static_profiler.load_profiles()  # Load existing profiles if available
        
    def estimate_compute_cost(self, task: TaskProfile, node: NodeProfile) -> float:
        """
        Estimate compute time using calibrated profiles: t_comp(i,r) = (α_i * b_i^η_i) * f_node(r)
        
        Where:
        - α_i, η_i: from function's calibrated profile (global fit)
        - b_i: input size in MB
        - f_node(r): node-specific scaling factor from per_node_base_time_ms or cpu_efficiency
        """
        # Try static profiling first
        if self.static_profiler.has_profiles():
            input_size_mb = task.input_size / (1024 * 1024)  # Convert bytes to MB
            
            # Get function profile for α_i and η_i
            func_profile = self.static_profiler.get_function_profile(task.function_name)
            node_profile = self.static_profiler.get_node_profile(node.node_id)
            
            if func_profile and node_profile:
                # Use calibrated formula: t_comp(i,r) = (α_i * b_i^η_i) * f_node(r)
                alpha_i = func_profile.base_execution_time_ms
                eta_i = func_profile.scaling_exponent
                
                # Calculate base time: α_i * b_i^η_i
                base_time_ms = alpha_i * (input_size_mb ** eta_i)
                
                # Node-specific scaling factor f_node(r)
                # Use per-node base time if available, otherwise use cpu_efficiency
                if func_profile.per_node_base_time_ms and node.node_id in func_profile.per_node_base_time_ms:
                    # Per-node α is available - use it directly
                    per_node_alpha = func_profile.per_node_base_time_ms[node.node_id]
                    f_node_r = per_node_alpha / alpha_i  # Scaling factor relative to global α
                else:
                    # Use cpu_efficiency as scaling factor
                    f_node_r = node_profile.cpu_efficiency
                
                # Apply node scaling: (α_i * b_i^η_i) * f_node(r)
                predicted_time_ms = base_time_ms * f_node_r
                
                # Optional: Add runtime load effects (1 + κ * u_r)
                # κ is a congestion coefficient, u_r is current CPU utilization
                load_factor = 1.0 + (self.congestion_coefficient * node.cpu_utilization)
                predicted_time_ms *= load_factor
                
                return predicted_time_ms / 1000.0  # Convert ms to seconds
        
        # Fallback to heuristic method
        return self._estimate_compute_cost_heuristic(task, node)
    
    def _estimate_compute_cost_heuristic(self, task: TaskProfile, node: NodeProfile) -> float:
        """
        Heuristic compute cost estimation (original method as fallback)
        """
        # Update node with real-time data if available
        if self.use_real_resources and self.container_manager:
            self.update_node_resources(node.node_id)
        
        # Base compute time from task profile
        base_time = task.estimated_compute_time
        
        # Adjust for CPU availability (considering current load)
        available_cpu = node.cpu_cores * (1 - node.cpu_utilization)
        cpu_factor = max(0.1, available_cpu / node.cpu_cores)  # Minimum 10% efficiency
        
        # Adjust for memory pressure
        memory_requirement_gb = task.memory_requirement / 1024.0  # Convert MB to GB
        memory_factor = min(1.0, node.memory_available / memory_requirement_gb)
        
        # Load factor combines CPU and memory constraints
        load_factor = 1.0 / (cpu_factor * memory_factor)
        
        return base_time * load_factor
    
    def estimate_transfer_cost(self, data_size: int, src_node: NodeProfile, 
                             dst_node: NodeProfile) -> float:
        """
        Estimate transfer time using calibrated network profiles: 
        t_transfer = base_latency + bytes / bandwidth
        """
        # Try static profiling first
        if self.static_profiler.has_profiles():
            data_size_mb = data_size / (1024 * 1024)  # Convert bytes to MB
            
            # Get network profile for the node pair
            network_profile = self.static_profiler.profiles.network_profiles.get(
                (src_node.node_id, dst_node.node_id)
            )
            
            if network_profile:
                # Use calibrated formula: base_latency + bytes / bandwidth
                base_latency_ms = network_profile.base_latency_ms
                bandwidth_mbps = network_profile.bandwidth_mbps
                
                # Calculate transfer time: (data_size_mb * 8) / bandwidth_mbps
                # Convert MB to Mbits (multiply by 8) and bandwidth from Mbps to MBps (divide by 8)
                # So: data_size_mb * 8 / bandwidth_mbps gives time in seconds, convert to ms
                transfer_time_ms = (data_size_mb * 8 * 1000) / bandwidth_mbps
                
                # Total time = base latency + transfer time
                total_time_ms = base_latency_ms + transfer_time_ms
                
                return total_time_ms / 1000.0  # Convert ms to seconds
        
        # Fallback to heuristic method
        return self._estimate_transfer_cost_heuristic(data_size, src_node, dst_node)
    
    def _estimate_transfer_cost_heuristic(self, data_size: int, src_node: NodeProfile, 
                                        dst_node: NodeProfile) -> float:
        """
        Heuristic transfer cost estimation (original method as fallback)
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
        memory_requirement_gb = task.memory_requirement / 1024.0  # Convert MB to GB
        memory_interference = max(0, (memory_requirement_gb - node.memory_available) / node.memory_total)
        
        return cpu_interference + memory_interference
    
    def estimate_queue_delay(self, function_name: str, node: NodeProfile) -> float:
        """
        Estimate queue delay for a function on a node based on container load
        Formula: t_queue(i,r) = queue_length * avg_processing_time / num_containers
        """
        # Get container queue information if available
        if self.container_manager:
            containers = self.container_manager.get_running_containers(function_name)
            containers_on_node = [c for c in containers if c.node_id == node.node_id]
            
            if containers_on_node:
                # Estimate queue length based on node load
                queue_length = max(0, node.running_containers - len(containers_on_node))
                
                # Estimate average processing time from function profile
                if self.static_profiler.has_profiles():
                    func_profile = self.static_profiler.get_function_profile(function_name)
                    if func_profile:
                        # Use 1MB as reference size for queue processing time
                        avg_processing_time = func_profile.predict_execution_time(1.0, node_id=node.node_id) / 1000.0  # Convert to seconds
                    else:
                        avg_processing_time = 0.5  # Default 500ms
                else:
                    avg_processing_time = 0.5  # Default 500ms
                
                # Queue delay = queue_length * avg_processing_time / num_containers
                # More containers = shorter queue delay
                num_containers = len(containers_on_node)
                if num_containers > 0:
                    queue_delay = (queue_length * avg_processing_time) / num_containers
                    return queue_delay
        
        return 0.0  # No queue delay if no containers or no data available
    
    def net_estimate(self, dag_structure: dict, nodes: list) -> Dict[Tuple[str, str], float]:
        """
        Estimate network costs for all edges in the DAG using calibrated network profiles
        Formula: t_net_ij(r_i, r_j) = base_latency(r_i,r_j) + d_ij / bandwidth(r_i,r_j)
        """
        net_costs = {}
        node_map = {node.node_id: node for node in nodes}
        
        # Iterate through all DAG edges
        for task_id, task_info in dag_structure.items():
            for dependency in task_info.get('dependencies', []):
                dep_task_id = dependency['task_id']
                data_size = dependency['data_size']
                
                # Get source and destination nodes (u, v)
                u = self._get_task_node_assignment(dep_task_id, nodes)  # source node
                v = self._get_task_node_assignment(task_id, nodes)      # dest node
                
                if u and v and u != v:
                    edge_key = (u, v)
                    
                    # Use calibrated network profiles for accurate transfer cost estimation
                    net_costs[edge_key] = self.estimate_transfer_cost_with_profiles(u, v, data_size / (1024 * 1024))
        
        return net_costs
    
    def compute_estimate(self, tasks: list, nodes: list) -> Dict[str, Dict[str, float]]:
        """
        Estimate compute costs for all tasks on all nodes including queue delays
        Formula: t_eff_comp(i,r) = t_comp(i,r) + t_queue(i,r)
        """
        compute_costs = {}
        
        for node in nodes:
            compute_costs[node.node_id] = {}
            
            for task in tasks:
                # Handle both TaskProfile objects and task dictionaries
                if isinstance(task, dict):
                    # Convert dictionary to TaskProfile-like object
                    task_obj = self._dict_to_task_profile(task)
                else:
                    task_obj = task
                
                # Calculate base compute cost using calibrated profiles
                base_cost = self.estimate_compute_cost(task_obj, node)
                
                # Add queue delay if container is overloaded
                queue_delay = self.estimate_queue_delay(task_obj.function_name, node)
                
                # Total effective compute cost
                effective_cost = base_cost + queue_delay
                compute_costs[node.node_id][task_obj.task_id] = effective_cost
        
        return compute_costs
    
    def _dict_to_task_profile(self, task_dict: Dict) -> TaskProfile:
        """Convert task dictionary to TaskProfile object"""
        return TaskProfile(
            task_id=task_dict.get('task_id', 'unknown'),
            function_name=task_dict.get('function_name', 'unknown'),
            input_size=task_dict.get('input_size', 1024 * 1024),  # Default 1MB
            estimated_compute_time=task_dict.get('estimated_compute_time', 0.5),
            memory_requirement=task_dict.get('memory_requirement', 512),
            cpu_intensity=task_dict.get('cpu_intensity', 0.5)
        )
    
    def _get_task_node_assignment(self, task_id: str, nodes: list) -> Optional[str]:
        """Get the node assignment for a task (simplified - can be enhanced with actual scheduler state)"""
        # For now, hash-based assignment for consistency
        # In practice, this should query the actual task-to-node mapping from scheduler
        if nodes:
            node_idx = hash(task_id) % len(nodes)
            return nodes[node_idx].node_id
        return None

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
    
    # set_profiler removed (profiling disabled)
    
    def set_container_manager(self, container_manager):
        """Set the container manager for real-time resource access"""
        self.container_manager = container_manager
    
    def set_congestion_coefficient(self, kappa: float):
        """Set the congestion coefficient for load effects: (1 + κ * u_r)"""
        self.congestion_coefficient = kappa
    
    def estimate_compute_cost_with_profiles(self, function_name: str, node_id: str, 
                                          input_size_mb: float, cpu_utilization: float = 0.0) -> float:
        """
        Convenience method to estimate compute cost using calibrated profiles directly.
        
        Args:
            function_name: Name of the function
            node_id: Target node ID
            input_size_mb: Input size in MB
            cpu_utilization: Current CPU utilization (0.0-1.0)
        
        Returns:
            Estimated compute time in seconds
        """
        if not self.static_profiler.has_profiles():
            return 0.0
        
        # Get profiles
        func_profile = self.static_profiler.get_function_profile(function_name)
        node_profile = self.static_profiler.get_node_profile(node_id)
        
        if not func_profile or not node_profile:
            return 0.0
        
        # Use calibrated formula: t_comp(i,r) = (α_i * b_i^η_i) * f_node(r) * (1 + κ * u_r)
        alpha_i = func_profile.base_execution_time_ms
        eta_i = func_profile.scaling_exponent
        
        # Calculate base time: α_i * b_i^η_i
        base_time_ms = alpha_i * (input_size_mb ** eta_i)
        
        # Node-specific scaling factor f_node(r)
        if func_profile.per_node_base_time_ms and node_id in func_profile.per_node_base_time_ms:
            per_node_alpha = func_profile.per_node_base_time_ms[node_id]
            f_node_r = per_node_alpha / alpha_i
        else:
            f_node_r = node_profile.cpu_efficiency
        
        # Apply node scaling and load effects
        predicted_time_ms = base_time_ms * f_node_r
        load_factor = 1.0 + (self.congestion_coefficient * cpu_utilization)
        predicted_time_ms *= load_factor
        
        return predicted_time_ms / 1000.0  # Convert ms to seconds
    
    def estimate_transfer_cost_with_profiles(self, src_node_id: str, dst_node_id: str, 
                                           data_size_mb: float) -> float:
        """
        Convenience method to estimate transfer cost using calibrated network profiles directly.
        
        Args:
            src_node_id: Source node ID
            dst_node_id: Destination node ID  
            data_size_mb: Data size in MB
        
        Returns:
            Estimated transfer time in seconds
        """
        if not self.static_profiler.has_profiles():
            return 0.0
        
        # Get network profile
        network_profile = self.static_profiler.profiles.network_profiles.get(
            (src_node_id, dst_node_id)
        )
        
        if not network_profile:
            return 0.0
        
        # Use calibrated formula: base_latency + bytes / bandwidth
        base_latency_ms = network_profile.base_latency_ms
        bandwidth_mbps = network_profile.bandwidth_mbps
        
        # Calculate transfer time
        transfer_time_ms = (data_size_mb * 8 * 1000) / bandwidth_mbps
        total_time_ms = base_latency_ms + transfer_time_ms
        
        return total_time_ms / 1000.0  # Convert ms to seconds
        
    def update_node_resources(self, node_id: str = None):
        """Update node resources with real-time data from container manager"""
        if not self.container_manager or not self.use_real_resources:
            return
            
        try:
            if node_id:
                # Update specific node
                if node_id in self.node_profiles:
                    real_resources = self.container_manager.get_node_available_resources(node_id)
                    if 'error' not in real_resources:
                        self._update_node_profile_with_real_data(node_id, real_resources)
            else:
                # Update all nodes
                all_resources = self.container_manager.get_all_nodes_resources()
                for node_id, real_resources in all_resources.items():
                    if node_id in self.node_profiles:
                        self._update_node_profile_with_real_data(node_id, real_resources)
                        
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to update node resources: {e}")
    
    def _update_node_profile_with_real_data(self, node_id: str, real_resources: Dict[str, float]):
        """Update a node profile with real resource data"""
        node = self.node_profiles[node_id]
        
        # Update with real-time data
        node.cpu_cores = int(real_resources.get('total_cpu', node.cpu_cores))
        node.memory_total = real_resources.get('total_memory', node.memory_total)
        node.memory_available = real_resources.get('available_memory', node.memory_available)
        node.cpu_utilization = real_resources.get('cpu_utilization', node.cpu_utilization)
        node.memory_utilization = real_resources.get('memory_utilization', node.memory_utilization)
        node.running_containers = int(real_resources.get('running_containers', node.running_containers))
        node.labels.update(real_resources.get('labels', {}))
        node.last_updated = time.time()

    def add_node_profile(self, node_profile: NodeProfile):
        """Add a new node profile"""
        self.node_profiles[node_profile.node_id] = node_profile
    
    def remove_node_profile(self, node_id: str):
        """Remove a node profile"""
        if node_id in self.node_profiles:
            del self.node_profiles[node_id]
    
    def get_task_profile(self, function_name: str) -> Optional[TaskProfile]:
        """
        Get task profile for a function, creating one if it doesn't exist.
        This is the production method for getting task profiles.
        """
        # Check if we have a cached profile
        if function_name in self.task_profiles:
            return self.task_profiles[function_name]
        
        # Create a new task profile based on function characteristics
        # This is a simplified approach - in production, this could be enhanced
        # with historical data, function analysis, or configuration files
        task_profile = self._create_production_task_profile(function_name)
        
        # Cache the profile for future use
        self.task_profiles[function_name] = task_profile
        
        return task_profile
    
    def _create_production_task_profile(self, function_name: str) -> TaskProfile:
        """
        Create a production task profile based on function name and characteristics.
        This can be enhanced with actual function analysis or configuration.
        """
        # Handle None or empty function names
        if not function_name:
            raise ValueError("Function name cannot be None or empty")
        
        # Function-specific resource requirements based on function name patterns
        if 'recognizer' in function_name.lower():
            # Image processing functions typically need more memory and CPU
            cpu_intensity = 0.7
            memory_requirement = 1024  # 1GB
            estimated_compute_time = 0.5
        elif 'svd' in function_name.lower():
            # SVD computation is CPU intensive
            cpu_intensity = 0.9
            memory_requirement = 2048  # 2GB
            estimated_compute_time = 1.0
        elif 'video' in function_name.lower():
            # Video processing needs significant resources
            cpu_intensity = 0.8
            memory_requirement = 1536  # 1.5GB
            estimated_compute_time = 0.8
        elif 'wordcount' in function_name.lower():
            # Text processing is typically lighter
            cpu_intensity = 0.4
            memory_requirement = 512  # 512MB
            estimated_compute_time = 0.2
        else:
            # Default for unknown functions
            cpu_intensity = 0.5
            memory_requirement = 512  # 512MB
            estimated_compute_time = 0.3
        
        return TaskProfile(
            task_id=f"{function_name}_profile",
            function_name=function_name,
            input_size=1024 * 1024,  # Default 1MB input
            estimated_compute_time=estimated_compute_time,
            memory_requirement=memory_requirement,
            cpu_intensity=cpu_intensity
        )
    
    def get_suitable_nodes(self, task: TaskProfile) -> List[NodeProfile]:
        """Get nodes that can satisfy task constraints with real-time resource check"""
        # Update all nodes with real-time data if available
        if self.use_real_resources and self.container_manager:
            self.update_node_resources()
            
        suitable_nodes = []
        for node in self.node_profiles.values():
            # Check basic constraints
            if hasattr(task, 'constraints'):
                if not node.can_satisfy_constraints(task.constraints):
                    continue
            
            # Check resource requirements
            required_cpu = getattr(task, 'cpu_requirement', 0.5)
            required_memory_mb = getattr(task, 'memory_requirement', 256)  # MB
            required_memory = required_memory_mb / 1024.0  # Convert to GB
            
            if (node.memory_available >= required_memory and 
                node.cpu_cores * (1 - node.cpu_utilization) >= required_cpu):
                suitable_nodes.append(node)
                
        return suitable_nodes
    
    def check_task_constraints(self, task: TaskProfile, node: NodeProfile) -> Tuple[bool, str]:
        """Check if a task can be placed on a node"""
        # Resource constraints
        required_cpu = getattr(task, 'cpu_requirement', 0.5)
        required_memory_mb = getattr(task, 'memory_requirement', 256)  # MB
        required_memory = required_memory_mb / 1024.0  # Convert to GB
        
        if node.memory_available < required_memory:
            return False, f"Insufficient memory: {node.memory_available:.2f}GB < {required_memory:.2f}GB"
        
        available_cpu = node.cpu_cores * (1 - node.cpu_utilization)
        if available_cpu < required_cpu:
            return False, f"Insufficient CPU: {available_cpu:.2f} < {required_cpu:.2f}"
        
        # Check node constraints if they exist
        if hasattr(task, 'constraints') and not node.can_satisfy_constraints(task.constraints):
            return False, "Node constraints not satisfied"
        
        return True, "All constraints satisfied"

    # ===== MOCK FUNCTIONS FOR TESTING =====
    
    def mock_estimate_compute_cost(self, task: TaskProfile, node: NodeProfile) -> float:
        """
        Mock function: Returns simple compute cost estimate
        """
        # Simple mock: base time * cpu_factor * memory_factor
        base_time = task.estimated_compute_time
        cpu_factor = 1.0 / max(0.1, 1.0 - node.cpu_utilization)
        memory_requirement_gb = task.memory_requirement / 1024.0  # Convert MB to GB
        memory_factor = 1.0 if node.memory_available >= memory_requirement_gb else 2.0
        
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
        memory_requirement_gb = task.memory_requirement / 1024.0  # Convert MB to GB
        memory_penalty = max(0, (memory_requirement_gb - node.memory_available) / node.memory_total)
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
    def create_mock_task(task_id: str, function_name: str, **kwargs) -> TaskProfile:
        """
        Create a mock task profile for testing with enhanced fields
        """
        defaults = {
            'input_size': 1024 * 1024,  # 1MB
            'estimated_compute_time': 0.2,  # 0.2 seconds
            'memory_requirement': 0.128,  # 128MB -> 0.128GB
            'cpu_requirement': 0.5,  # 0.5 CPU cores
            'cpu_intensity': 0.3,  # Light-medium intensity
            'disk_requirement': 0.1,  # 100MB
            'network_requirement': 100.0,  # 100 Mbps
            'constraints': NodeConstraints(),
            'anti_affinity': [],
            'affinity': []
        }
        defaults.update(kwargs)
        
        return TaskProfile(
            task_id=task_id,
            function_name=function_name,
            **defaults
        )
    
    @staticmethod
    def create_mock_node(node_id: str, cpu_cores: int = 4, **kwargs) -> NodeProfile:
        """
        Create a mock node profile for testing with enhanced fields
        """
        defaults = {
            'cpu_frequency': 2.4,  # 2.4 GHz
            'memory_total': 8.0,  # 8GB
            'memory_available': 5.0,  # 5GB available
            'disk_total': 100.0,  # 100GB
            'disk_available': 50.0,  # 50GB available
            'cpu_utilization': 0.3,  # 30% utilization
            'memory_utilization': 0.375,  # 37.5% utilization (3GB/8GB)
            'disk_utilization': 0.5,  # 50% utilization
            'network_bandwidth': 1000,  # 1Gbps
            'labels': {},
            'constraints': NodeConstraints(),
            'container_cache': {},
            'running_containers': 0,
            'zone': None,
            'region': None,
            'network_latency_map': {}
        }
        defaults.update(kwargs)
        
        return NodeProfile(
            node_id=node_id,
            cpu_cores=cpu_cores,
            **defaults
        )
    
    @staticmethod
    def create_node_from_container_manager(container_manager) -> List[NodeProfile]:
        """
        Create node profiles from FunctionContainerManager data
        """
        node_profiles = []
        
        for node_resources in container_manager.get_nodes():
            # Convert from container manager format to cost model format
            node_profile = NodeProfile(
                node_id=node_resources.node_id,
                cpu_cores=node_resources.cpu_cores,
                cpu_frequency=2.4,  # Default, could be enhanced
                memory_total=node_resources.memory_gb,
                memory_available=node_resources.available_memory,
                disk_total=100.0,  # Default, could be enhanced
                disk_available=80.0,  # Default, could be enhanced
                cpu_utilization=(node_resources.cpu_cores - node_resources.available_cpu) / node_resources.cpu_cores,
                memory_utilization=(node_resources.memory_gb - node_resources.available_memory) / node_resources.memory_gb,
                disk_utilization=0.2,  # Default
                network_bandwidth=1000.0,  # Default
                labels=node_resources.labels,
                constraints=NodeConstraints(),
                container_cache={},  # Could be populated from container manager
                running_containers=len(node_resources.running_containers),
                zone=node_resources.labels.get('zone'),
                region=node_resources.labels.get('region'),
                network_latency_map={}
            )
            node_profiles.append(node_profile)
        
        return node_profiles