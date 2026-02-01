"""
Function Routing Logic: Decides request path to already placed containers.
Handles load balancing, latency optimization, and utilization across warm containers.
"""

import logging
import random
import statistics
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

from provider.function_container_manager import (ContainerStatus,
                                                 FunctionContainer)


class RoutingStrategy(Enum):
    """Available routing strategies"""
    ROUND_ROBIN = "round_robin"        # Simple round-robin
    LEAST_LOADED = "least_loaded"      # Route to least loaded container


@dataclass
class RoutingRequest:
    """Request for function container routing"""
    function_name: str
    request_id: str
    data_locality_hints: List[str] = None  # Data IDs that should be local
    latency_priority: bool = False  # Prioritize low latency
    preferred_nodes: List[str] = None  # Node preferences
    session_affinity: Optional[str] = None  # Session ID for sticky routing


@dataclass
class RoutingResult:
    """Result of routing decision"""
    target_container: FunctionContainer
    reasoning: str
    expected_latency: float = 0.0
    load_score: float = 0.0




class FunctionRouting:
    """
    Handles routing decisions for function requests.
    Decides WHICH existing container gets each request based on:
    - Load balancing across containers
    - Latency optimization
    - Container utilization
    - Data locality
    - Session affinity
    """
    
    def __init__(self, container_manager=None):
        self.container_manager = container_manager
        self.logger = logging.getLogger(__name__)
        
        # Routing state
        self.round_robin_counters: Dict[str, int] = {}  # function_name -> counter
        self.container_metrics: Dict[str, Dict] = {}    # container_id -> metrics
        self.session_affinity_map: Dict[str, str] = {}  # session_id -> container_id
        self.routing_history: List[Dict] = []
        
        # Configuration
        self.latency_threshold_ms = 100  # Consider containers with latency < 100ms
        self.load_threshold = 0.8       # Consider containers with load < 80%
        self.session_timeout = 300      # Session affinity timeout (5 minutes)
        
        # Metrics collection
        self.container_response_times: Dict[str, List[float]] = {}
        self.container_error_rates: Dict[str, List[bool]] = {}
    
    def route_request(self, request: RoutingRequest,
                     available_containers: List[FunctionContainer],
                     strategy: RoutingStrategy = RoutingStrategy.ROUND_ROBIN) -> Optional[RoutingResult]:
        """
        Route a request to the best available container.
        This is the main ROUTING decision logic.
        """
        if not available_containers:
            self.logger.warning(f"No available containers for function {request.function_name}")
            return None
        
        # Filter to only running containers
        running_containers = [c for c in available_containers if c.status == ContainerStatus.RUNNING]
        if not running_containers:
            self.logger.warning(f"No running containers for function {request.function_name}")
            return None
        
        # Filter containers based on real-time node resources
        suitable_containers = self._filter_containers_by_node_resources(running_containers)
        if not suitable_containers:
            # If no containers pass resource filter, use all running containers
            suitable_containers = running_containers
            self.logger.debug("No containers passed resource filter, using all running containers")
        
        self.logger.debug(f"Routing request {request.request_id} for {request.function_name} using {strategy.value}")
        
        # Check session affinity first
        if request.session_affinity:
            affinity_result = self._check_session_affinity(request, suitable_containers)
            if affinity_result:
                return affinity_result
        
        # Apply routing strategy
        if strategy == RoutingStrategy.ROUND_ROBIN:
            return self._round_robin_routing(request, suitable_containers)
        elif strategy == RoutingStrategy.LEAST_LOADED:
            return self._least_loaded_routing(request, suitable_containers)
        else:
            # Default to least loaded
            return self._least_loaded_routing(request, suitable_containers)
    
    def _filter_containers_by_node_resources(self, containers: List[FunctionContainer]) -> List[FunctionContainer]:
        """Filter containers based on real-time node resource availability"""
        if not self.container_manager:
            return containers  # No filtering if no container manager
        
        suitable_containers = []
        
        for container in containers:
            node_resources = self.container_manager.get_node_available_resources(container.node_id)
            
            if 'error' in node_resources:
                # If we can't get resources, assume container is suitable
                suitable_containers.append(container)
                continue
            
            # Check if node is overloaded
            cpu_utilization = node_resources['cpu_utilization']
            memory_utilization = node_resources['memory_utilization']
            
            # Filter out containers on heavily loaded nodes
            if cpu_utilization < self.load_threshold and memory_utilization < self.load_threshold:
                suitable_containers.append(container)
            else:
                self.logger.debug(f"Filtering out container {container.container_id} on overloaded node {container.node_id} "
                                f"(CPU: {cpu_utilization:.1%}, Memory: {memory_utilization:.1%})")
        
        return suitable_containers
    
    def _check_session_affinity(self, request: RoutingRequest,
                               containers: List[FunctionContainer]) -> Optional[RoutingResult]:
        """Check if request should be routed based on session affinity"""
        if not request.session_affinity:
            return None
        
        # Clean up expired sessions
        current_time = time.time()
        expired_sessions = [
            session_id for session_id, container_id in self.session_affinity_map.items()
            if current_time - self._get_session_timestamp(session_id) > self.session_timeout
        ]
        for session_id in expired_sessions:
            del self.session_affinity_map[session_id]
        
        # Check if session has affinity
        if request.session_affinity in self.session_affinity_map:
            target_container_id = self.session_affinity_map[request.session_affinity]
            
            # Find the container
            for container in containers:
                if container.container_id == target_container_id:
                    return RoutingResult(
                        target_container=container,
                        reasoning=f"Session affinity routing to container {container.container_id}",
                        load_score=self._get_container_load(container)
                    )
        
        return None
    
    def _round_robin_routing(self, request: RoutingRequest,
                            containers: List[FunctionContainer]) -> RoutingResult:
        """Simple round-robin routing"""
        function_name = request.function_name
        
        if function_name not in self.round_robin_counters:
            self.round_robin_counters[function_name] = 0
        
        # Sort containers by container_id for consistent ordering
        sorted_containers = sorted(containers, key=lambda c: c.container_id)
        
        index = self.round_robin_counters[function_name] % len(sorted_containers)
        selected_container = sorted_containers[index]
        
        self.round_robin_counters[function_name] += 1
        
        return RoutingResult(
            target_container=selected_container,
            reasoning=f"Round-robin routing (index {index})",
            load_score=self._get_container_load(selected_container)
        )
    
    def _least_loaded_routing(self, request: RoutingRequest,
                             containers: List[FunctionContainer]) -> RoutingResult:
        """Route to container with least load/active connections"""
        container_loads = []
        
        for container in containers:
            load = self._get_container_load(container)
            container_loads.append((container, load))
        
        # Sort by load (ascending - least loaded first)
        container_loads.sort(key=lambda x: x[1])
        selected_container, load = container_loads[0]
        
        return RoutingResult(
            target_container=selected_container,
            reasoning=f"Least loaded routing (load: {load:.3f})",
            load_score=load
        )
    
    # === HELPER METHODS ===
    
    def _least_connections_routing(self, request: RoutingRequest,
                                  containers: List[FunctionContainer]) -> RoutingResult:
        """Route to container with least active connections/requests"""
        container_loads = []
        
        for container in containers:
            load = self._get_container_load(container)
            container_loads.append((container, load))
        
        # Sort by load (ascending)
        container_loads.sort(key=lambda x: x[1])
        selected_container, load = container_loads[0]
        
        return RoutingResult(
            target_container=selected_container,
            reasoning=f"Least connections routing (load: {load:.3f})",
            load_score=load
        ) 
     
    def _get_container_load(self, container: FunctionContainer) -> float:
        """
        Estimate container load (0.0 = idle, 1.0 = fully loaded).

        Uses both:
        1. Container-level active requests vs concurrency limit
        2. Node-level resource utilization (CPU/memory) as a modifier
        """
        # --- 1. Container-level load ---
        if hasattr(container, "active_requests") and hasattr(container, "max_concurrency"):
            container_load = container.active_requests / max(1, container.max_concurrency)
        else:
            container_load = 0.0  # fallback if not tracked

        # --- 2. Node-level modifier (optional, if metrics available) ---
        node_load = 0.0
        if self.container_manager:
            node_resources = self.container_manager.get_node_available_resources(container.node_id)
            if 'error' not in node_resources:
                cpu_util = node_resources.get('cpu_utilization', 0.0)
                mem_util = node_resources.get('memory_utilization', 0.0)
                node_load = (cpu_util + mem_util) / 2.0

                # --- 3. Weighted combination ---
                # Priority to per-container request load, with node load as secondary factor
                estimated_load = 0.7 * container_load + 0.3 * node_load

                return min(estimated_load, 1.0)
        
        # Fallback: Simplified load calculation based on request count and time
        current_time = time.time()
        
        # Recent request rate (requests per minute)
        recent_requests = getattr(container, 'recent_requests', [])
        recent_requests = [t for t in recent_requests if current_time - t < 60]  # Last minute
        request_rate = len(recent_requests) / 60.0
        
        # Normalize to 0-1 scale (assume max 60 requests/minute = full load)
        load = min(request_rate / 60.0, 1.0)
        
        # Add CPU/memory usage if available
        if hasattr(container, 'cpu_usage') and container.cpu_usage > 0:
            load = max(load, container.cpu_usage / 100.0)
        
        return load
    
    def _get_container_average_latency(self, container: FunctionContainer) -> float:
        """Get average response latency for a container in milliseconds"""
        container_id = container.container_id
        
        if container_id not in self.container_response_times:
            return 50.0  # Default latency assumption
        
        response_times = self.container_response_times[container_id]
        if not response_times:
            return 50.0
        
        # Return average of recent response times
        recent_times = response_times[-10:]  # Last 10 requests
        return statistics.mean(recent_times) * 1000  # Convert to milliseconds
    
    def _get_container_error_rate(self, container: FunctionContainer) -> float:
        """Get error rate for a container (0.0 = no errors, 1.0 = all errors)"""
        container_id = container.container_id
        
        if container_id not in self.container_error_rates:
            return 0.0  # Assume no errors initially
        
        error_history = self.container_error_rates[container_id]
        if not error_history:
            return 0.0
        
        # Calculate error rate from recent requests
        recent_errors = error_history[-20:]  # Last 20 requests
        return sum(recent_errors) / len(recent_errors)
    
    def _get_session_timestamp(self, session_id: str) -> float:
        """Get timestamp for session (simplified)"""
        # In a real implementation, this would track session creation time
        return time.time() - 60  # Assume session created 1 minute ago
    
    def record_request_result(self, container_id: str, response_time: float, 
                             success: bool, session_id: Optional[str] = None):
        """Record the result of a request for metrics tracking"""
        current_time = time.time()
        
        # Record response time
        if container_id not in self.container_response_times:
            self.container_response_times[container_id] = []
        self.container_response_times[container_id].append(response_time)
        
        # Keep only recent response times
        if len(self.container_response_times[container_id]) > 100:
            self.container_response_times[container_id] = self.container_response_times[container_id][-50:]
        
        # Record success/failure
        if container_id not in self.container_error_rates:
            self.container_error_rates[container_id] = []
        self.container_error_rates[container_id].append(not success)
        
        # Keep only recent error history
        if len(self.container_error_rates[container_id]) > 100:
            self.container_error_rates[container_id] = self.container_error_rates[container_id][-50:]
        
        # Update session affinity if provided
        if session_id and success:
            self.session_affinity_map[session_id] = container_id
        
        # Record routing decision
        self.routing_history.append({
            'timestamp': current_time,
            'container_id': container_id,
            'response_time': response_time,
            'success': success,
            'session_id': session_id
        })
        
        # Keep routing history manageable
        if len(self.routing_history) > 1000:
            self.routing_history = self.routing_history[-500:]
    
    def get_routing_statistics(self) -> Dict:
        """Get statistics about routing decisions"""
        if not self.routing_history:
            return {}
        
        # Calculate statistics
        total_requests = len(self.routing_history)
        successful_requests = sum(1 for r in self.routing_history if r['success'])
        success_rate = successful_requests / total_requests
        
        response_times = [r['response_time'] for r in self.routing_history]
        avg_response_time = statistics.mean(response_times) if response_times else 0
        
        # Container usage distribution
        container_usage = {}
        for record in self.routing_history:
            container_id = record['container_id']
            container_usage[container_id] = container_usage.get(container_id, 0) + 1
        
        return {
            'total_requests': total_requests,
            'success_rate': success_rate,
            'average_response_time_ms': avg_response_time * 1000,
            'container_usage_distribution': container_usage,
            'active_sessions': len(self.session_affinity_map),
            'recent_requests': self.routing_history[-10:]  # Last 10 requests
        }
    
    def cleanup_stale_metrics(self):
        """Clean up stale metrics and session data"""
        current_time = time.time()
        
        # Clean up expired sessions
        expired_sessions = [
            session_id for session_id, _ in self.session_affinity_map.items()
            if current_time - self._get_session_timestamp(session_id) > self.session_timeout
        ]
        for session_id in expired_sessions:
            del self.session_affinity_map[session_id]
        
        # Clean up old routing history
        cutoff_time = current_time - 3600  # Keep last hour
        self.routing_history = [r for r in self.routing_history if r['timestamp'] > cutoff_time]
        
        self.logger.debug(f"Cleaned up {len(expired_sessions)} expired sessions and old routing history")

