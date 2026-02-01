"""
Bucket Manager for Palette Scheduler
Handles bucket-to-container mapping and assignment strategies.
"""
import hashlib
import logging
import random
from typing import Dict, List, Optional, Any

from provider.function_container_manager import FunctionContainer


class BucketManager:
    """
    Manages bucket-to-container assignment for a single function.
    Each function gets its own BucketManager instance.
    """
    
    def __init__(self, function_name: str, num_buckets: int, 
                 hash_algorithm: str = "builtin",
                 assignment_strategy: str = "round_robin"):
        """
        Initialize bucket manager for a function.
        
        Args:
            function_name: Name of the function
            num_buckets: Total number of buckets
            hash_algorithm: Hash algorithm to use (builtin, md5, sha256)
            assignment_strategy: Strategy for assigning buckets to containers
        """
        self.function_name = function_name
        self.num_buckets = num_buckets
        self.hash_algorithm = hash_algorithm
        self.assignment_strategy = assignment_strategy
        self.logger = logging.getLogger(__name__)
        
        # Bucket to container mapping: bucket_id → container
        self.bucket_to_container: Dict[int, FunctionContainer] = {}
        
        # Container to buckets mapping (reverse index for rebalancing)
        self.container_to_buckets: Dict[str, List[int]] = {}
        
        # Load metrics per bucket (for monitoring)
        self.bucket_load: Dict[int, int] = {i: 0 for i in range(num_buckets)}
        
    def assign_buckets(self, containers: List[FunctionContainer], 
                       node_capacity: Optional[Dict[str, Dict]] = None) -> None:
        """
        Assign all buckets to available containers.
        
        Args:
            containers: List of available containers
            node_capacity: Optional node capacity info for weighted assignment
        """
        if not containers:
            self.logger.warning(f"No containers available for {self.function_name}")
            return
        
        self.logger.info(f"Assigning {self.num_buckets} buckets to {len(containers)} containers for {self.function_name}")
        
        # Clear existing mappings
        self.bucket_to_container.clear()
        self.container_to_buckets.clear()
        
        if self.assignment_strategy == "round_robin":
            self._assign_round_robin(containers)
        elif self.assignment_strategy == "capacity_weighted":
            self._assign_capacity_weighted(containers, node_capacity)
        elif self.assignment_strategy == "random":
            self._assign_random(containers)
        else:
            self.logger.warning(f"Unknown assignment strategy: {self.assignment_strategy}, using round_robin")
            self._assign_round_robin(containers)
        
        self.logger.info(f"Bucket assignment complete: {len(self.bucket_to_container)} buckets assigned")
        
    def _assign_round_robin(self, containers: List[FunctionContainer]) -> None:
        """Round-robin assignment: distribute buckets evenly across containers"""
        for bucket_id in range(self.num_buckets):
            target_container = containers[bucket_id % len(containers)]
            self.bucket_to_container[bucket_id] = target_container
            
            # Update reverse mapping
            container_id = target_container.container_id
            if container_id not in self.container_to_buckets:
                self.container_to_buckets[container_id] = []
            self.container_to_buckets[container_id].append(bucket_id)
    
    def _assign_capacity_weighted(self, containers: List[FunctionContainer],
                                  node_capacity: Optional[Dict[str, Dict]] = None) -> None:
        """
        Capacity-weighted assignment: assign more buckets to containers on nodes with more capacity.
        Falls back to round-robin if no capacity info available.
        """
        if not node_capacity:
            self.logger.info("No capacity info, falling back to round_robin")
            self._assign_round_robin(containers)
            return
        
        # Calculate weights based on node capacity
        container_weights = []
        total_weight = 0.0
        
        for container in containers:
            node_id = container.node_id
            if node_id in node_capacity:
                # Use CPU cores as weight
                weight = node_capacity[node_id].get('cpu_cores', 1.0)
            else:
                weight = 1.0
            container_weights.append(weight)
            total_weight += weight
        
        # Normalize weights to bucket counts
        bucket_counts = []
        assigned_buckets = 0
        
        for i, weight in enumerate(container_weights):
            if i == len(container_weights) - 1:
                # Last container gets remaining buckets
                bucket_count = self.num_buckets - assigned_buckets
            else:
                bucket_count = int((weight / total_weight) * self.num_buckets)
            bucket_counts.append(bucket_count)
            assigned_buckets += bucket_count
        
        # Assign buckets according to calculated counts
        bucket_id = 0
        for container, count in zip(containers, bucket_counts):
            for _ in range(count):
                if bucket_id < self.num_buckets:
                    self.bucket_to_container[bucket_id] = container
                    
                    # Update reverse mapping
                    container_id = container.container_id
                    if container_id not in self.container_to_buckets:
                        self.container_to_buckets[container_id] = []
                    self.container_to_buckets[container_id].append(bucket_id)
                    
                    bucket_id += 1
    
    def _assign_random(self, containers: List[FunctionContainer]) -> None:
        """Random assignment: randomly assign each bucket to a container"""
        for bucket_id in range(self.num_buckets):
            target_container = random.choice(containers)
            self.bucket_to_container[bucket_id] = target_container
            
            # Update reverse mapping
            container_id = target_container.container_id
            if container_id not in self.container_to_buckets:
                self.container_to_buckets[container_id] = []
            self.container_to_buckets[container_id].append(bucket_id)
    
    def hash_color_to_bucket(self, color: str) -> int:
        """
        Hash a color string to a bucket ID.
        
        Args:
            color: Color hint string
            
        Returns:
            Bucket ID (0 to num_buckets-1)
        """
        if self.hash_algorithm == "builtin":
            return hash(color) % self.num_buckets
        elif self.hash_algorithm == "md5":
            hash_obj = hashlib.md5(color.encode())
            hash_int = int(hash_obj.hexdigest(), 16)
            return hash_int % self.num_buckets
        elif self.hash_algorithm == "sha256":
            hash_obj = hashlib.sha256(color.encode())
            hash_int = int(hash_obj.hexdigest(), 16)
            return hash_int % self.num_buckets
        else:
            # Fallback to builtin
            return hash(color) % self.num_buckets
    
    def get_container_for_color(self, color: str) -> Optional[FunctionContainer]:
        """
        Get the target container for a given color hint.
        
        Args:
            color: Color hint string
            
        Returns:
            Target container, or None if bucket not assigned
        """
        bucket_id = self.hash_color_to_bucket(color)
        container = self.bucket_to_container.get(bucket_id)
        
        # Track load for monitoring
        if container:
            self.bucket_load[bucket_id] += 1
        
        return container
    
    def get_fallback_container(self, color: str, 
                              available_containers: List[FunctionContainer],
                              strategy: str = "same_node") -> Optional[FunctionContainer]:
        """
        Get a fallback container when the primary container is unavailable.
        
        Args:
            color: Original color hint
            available_containers: List of currently available containers
            strategy: Fallback strategy (same_node, rehash, random)
            
        Returns:
            Fallback container, or None if no fallback available
        """
        if not available_containers:
            return None
        
        bucket_id = self.hash_color_to_bucket(color)
        primary_container = self.bucket_to_container.get(bucket_id)
        
        if strategy == "same_node" and primary_container:
            # Try to find another container on the same node
            same_node = [c for c in available_containers if c.node_id == primary_container.node_id]
            if same_node:
                return same_node[0]
        
        if strategy == "rehash":
            # Try adjacent buckets
            for offset in [1, -1, 2, -2]:
                fallback_bucket = (bucket_id + offset) % self.num_buckets
                fallback_container = self.bucket_to_container.get(fallback_bucket)
                if fallback_container and fallback_container in available_containers:
                    return fallback_container
        
        # Default: random selection from available
        return random.choice(available_containers) if available_containers else None
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about bucket assignments and load distribution"""
        stats = {
            'function_name': self.function_name,
            'num_buckets': self.num_buckets,
            'assigned_buckets': len(self.bucket_to_container),
            'num_containers': len(self.container_to_buckets),
            'total_requests': sum(self.bucket_load.values()),
        }
        
        # Calculate load distribution
        if self.container_to_buckets:
            container_loads = {}
            for container_id, buckets in self.container_to_buckets.items():
                load = sum(self.bucket_load[b] for b in buckets)
                container_loads[container_id] = {
                    'num_buckets': len(buckets),
                    'total_load': load,
                }
            stats['container_loads'] = container_loads
        
        return stats
    
    def rebalance(self, containers: List[FunctionContainer],
                 node_capacity: Optional[Dict[str, Dict]] = None) -> None:
        """
        Rebalance bucket assignments when containers change.
        Tries to minimize disruption by only reassigning unassigned buckets.
        """
        self.logger.info(f"Rebalancing buckets for {self.function_name}")
        
        # Find buckets with unavailable containers
        available_container_ids = {c.container_id for c in containers}
        buckets_to_reassign = []
        
        for bucket_id, container in self.bucket_to_container.items():
            if container.container_id not in available_container_ids:
                buckets_to_reassign.append(bucket_id)
        
        if not buckets_to_reassign:
            self.logger.info("No rebalancing needed")
            return
        
        self.logger.info(f"Reassigning {len(buckets_to_reassign)} buckets")
        
        # Reassign using round-robin to minimize disruption
        for i, bucket_id in enumerate(buckets_to_reassign):
            new_container = containers[i % len(containers)]
            old_container = self.bucket_to_container[bucket_id]
            
            # Update mappings
            self.bucket_to_container[bucket_id] = new_container
            
            # Update reverse mappings
            if old_container.container_id in self.container_to_buckets:
                self.container_to_buckets[old_container.container_id].remove(bucket_id)
            
            if new_container.container_id not in self.container_to_buckets:
                self.container_to_buckets[new_container.container_id] = []
            self.container_to_buckets[new_container.container_id].append(bucket_id)

