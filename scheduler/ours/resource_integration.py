"""
Resource Integration Module
Connects cost models with container manager for real resource data
"""

import logging
import time
from typing import Dict, List, Optional

from provider.function_container_manager import (FunctionContainerManager,
                                                 NodeResources)

from .cost_models import CostModels, NodeConstraints, NodeProfile, TaskProfile


class ResourceIntegration:
    """
    Integrates cost models with container manager for real-time resource data
    """
    
    def __init__(self, container_manager: FunctionContainerManager, 
                 cost_models: Optional[CostModels] = None):
        self.container_manager = container_manager
        self.cost_models = cost_models or CostModels(use_real_resources=True)
        self.logger = logging.getLogger(__name__)
        
        # Sync initial node data
        self._sync_nodes_from_container_manager()
    
    def _sync_nodes_from_container_manager(self):
        """Sync node profiles from container manager"""
        try:
            container_nodes = self.container_manager.get_nodes()
            
            for node_resources in container_nodes:
                node_profile = self._convert_node_resources_to_profile(node_resources)
                self.cost_models.add_node_profile(node_profile)
                
            self.logger.info(f"Synced {len(container_nodes)} nodes from container manager")
            
        except Exception as e:
            self.logger.error(f"Failed to sync nodes from container manager: {e}")
    
    def _convert_node_resources_to_profile(self, node_resources: NodeResources) -> NodeProfile:
        """Convert container manager node resources to cost model node profile"""
        
        # No node type classification needed - use actual resources
        
        # Create constraints based on node labels
        constraints = self._create_constraints_from_labels(node_resources.labels)
        
        # Calculate utilization
        cpu_utilization = (node_resources.cpu_cores - node_resources.available_cpu) / node_resources.cpu_cores
        memory_utilization = (node_resources.memory_gb - node_resources.available_memory) / node_resources.memory_gb
        
        # Extract container cache info
        container_cache = {}
        for container in node_resources.running_containers.values():
            container_cache[container.function_name] = True  # Mark as warm
        
        return NodeProfile(
            node_id=node_resources.node_id,
            cpu_cores=node_resources.cpu_cores,
            cpu_frequency=2.4,  # Default, could be enhanced with real detection
            memory_total=node_resources.memory_gb,
            memory_available=node_resources.available_memory,
            disk_total=100.0,  # Default, could be enhanced
            disk_available=80.0,  # Default, could be enhanced
            cpu_utilization=cpu_utilization,
            memory_utilization=memory_utilization,
            disk_utilization=0.2,  # Default
            network_bandwidth=1000.0,  # Default
            labels=node_resources.labels,
            constraints=constraints,
            container_cache=container_cache,
            running_containers=len(node_resources.running_containers),
            zone=node_resources.labels.get('zone'),
            region=node_resources.labels.get('region'),
            last_updated=time.time()
        )
    
    
    def _create_constraints_from_labels(self, labels: Dict[str, str]) -> NodeConstraints:
        """Create node constraints based on labels"""
        constraints = NodeConstraints()
        
        # No node type constraints needed - use actual resource constraints
        
        # Set security constraints
        if 'security-zone' in labels:
            constraints.security_zone = labels['security-zone']
        
        if labels.get('isolated', 'false').lower() == 'true':
            constraints.requires_isolation = True
        
        # Set storage constraints
        if labels.get('storage-type') == 'ssd':
            constraints.requires_ssd = True
        
        return constraints
    
    def update_all_node_resources(self):
        """Update all node resources from container manager and real system"""
        try:
            # Update real-time resources in container manager first
            self.container_manager.update_real_time_resources()
            
            # Update from container manager
            self._sync_nodes_from_container_manager()
            
            # Update with real system resources if available
            for node_id in self.cost_models.node_profiles.keys():
                self.cost_models.update_node_resources(node_id)
            
            self.logger.info("Updated all node resources")
            
        except Exception as e:
            self.logger.error(f"Failed to update node resources: {e}")
    
    def create_placement_component(self):
        """Create placement component with integrated resource access"""
        from .placement import FunctionPlacement
        return FunctionPlacement(cost_models=self.cost_models, container_manager=self.container_manager)
    
    def create_routing_component(self):
        """Create routing component with integrated resource access"""
        from .routing import FunctionRouting
        return FunctionRouting(container_manager=self.container_manager)
    
    def get_real_time_cluster_resources(self) -> Dict[str, Dict[str, float]]:
        """Get real-time cluster resource information"""
        try:
            return self.container_manager.get_all_nodes_resources()
        except Exception as e:
            self.logger.error(f"Failed to get cluster resources: {e}")
            return {}
    
    def check_node_capacity(self, node_id: str, cpu_required: float, memory_required: float) -> bool:
        """Check if a node has capacity for the required resources"""
        try:
            return self.container_manager.can_place_container(node_id, cpu_required, memory_required)
        except Exception as e:
            self.logger.error(f"Failed to check node capacity: {e}")
            return False
    
    def get_placement_candidates(self, task_profile: TaskProfile) -> List[NodeProfile]:
        """Get nodes that can host the task based on constraints"""
        return self.cost_models.get_suitable_nodes(task_profile)
    
    def estimate_placement_cost(self, task_profile: TaskProfile, 
                              node_id: str, data_transfers: List = None) -> Dict[str, float]:
        """Estimate cost of placing task on specific node"""
        if node_id not in self.cost_models.node_profiles:
            return {'error': 'Node not found', 'total': float('inf')}
        
        node_profile = self.cost_models.node_profiles[node_id]
        data_transfers = data_transfers or []
        
        return self.cost_models.get_all_costs(task_profile, node_profile, data_transfers)
    
    def get_best_placement(self, task_profile: TaskProfile, 
                          data_transfers: List = None) -> Optional[str]:
        """Find the best node for task placement"""
        suitable_nodes = self.get_placement_candidates(task_profile)
        
        if not suitable_nodes:
            self.logger.warning(f"No suitable nodes found for task {task_profile.task_id}")
            return None
        
        best_node = None
        best_cost = float('inf')
        
        for node in suitable_nodes:
            costs = self.estimate_placement_cost(task_profile, node.node_id, data_transfers)
            if costs['total'] < best_cost:
                best_cost = costs['total']
                best_node = node.node_id
        
        return best_node
    
    def create_task_profile(self, function_name: str, task_id: str, 
                           **task_requirements) -> TaskProfile:
        """Create task profile with requirements and constraints"""
        
        # Set defaults based on function type
        defaults = self._get_function_defaults(function_name)
        defaults.update(task_requirements)
        
        # Convert memory from GB to MB if needed
        if 'memory_requirement' in defaults and defaults['memory_requirement'] < 10:
            defaults['memory_requirement'] = int(defaults['memory_requirement'] * 1024)
        
        # Remove fields that don't belong to TaskProfile
        task_profile_fields = {'input_size', 'estimated_compute_time', 'memory_requirement', 'cpu_intensity'}
        filtered_defaults = {k: v for k, v in defaults.items() if k in task_profile_fields}
        
        return TaskProfile(
            task_id=task_id,
            function_name=function_name,
            **filtered_defaults
        )
    
    def _get_function_defaults(self, function_name: str) -> Dict:
        """Get default requirements based on function type"""
        
        # Define function-specific defaults
        function_defaults = {
            'recognizer__adult': {
                'memory_requirement': 512,  # MB
                'estimated_compute_time': 0.3,
                'cpu_intensity': 0.7
            },
            'recognizer__violence': {
                'memory_requirement': 512,  # MB
                'estimated_compute_time': 0.4,
                'cpu_intensity': 0.7
            },
            'recognizer__upload': {
                'memory_requirement': 256,  # MB
                'estimated_compute_time': 0.1,
                'cpu_intensity': 0.3
            },
            'recognizer__extract': {
                'memory_requirement': 384,  # MB
                'estimated_compute_time': 0.2,
                'cpu_intensity': 0.5
            }
        }
        
        # General defaults
        general_defaults = {
            'input_size': 1024 * 1024,  # 1MB
            'estimated_compute_time': 0.2,
            'memory_requirement': 256,  # MB
            'cpu_intensity': 0.3
        }
        
        # Merge function-specific with general defaults
        result = general_defaults.copy()
        if function_name in function_defaults:
            result.update(function_defaults[function_name])
        
        return result
    
    def get_cluster_status(self) -> Dict:
        """Get comprehensive cluster status"""
        container_status = self.container_manager.get_cluster_status()
        
        # Add cost model information
        cost_model_status = {
            'total_node_profiles': len(self.cost_models.node_profiles),
            'nodes_overloaded': 0,
            'total_warm_containers': 0,
            'average_cpu_utilization': 0.0,
            'average_memory_utilization': 0.0
        }
        
        total_cpu_util = 0.0
        total_mem_util = 0.0
        
        for node in self.cost_models.node_profiles.values():
            if node.is_overloaded():
                cost_model_status['nodes_overloaded'] += 1
            
            cost_model_status['total_warm_containers'] += len(node.container_cache)
            total_cpu_util += node.cpu_utilization
            total_mem_util += node.memory_utilization
        
        if self.cost_models.node_profiles:
            cost_model_status['average_cpu_utilization'] = total_cpu_util / len(self.cost_models.node_profiles)
            cost_model_status['average_memory_utilization'] = total_mem_util / len(self.cost_models.node_profiles)
        
        return {
            'container_manager': container_status,
            'cost_models': cost_model_status
        }


# Convenience function for easy integration
def create_integrated_cost_models(container_manager: FunctionContainerManager) -> ResourceIntegration:
    """Create integrated cost models with container manager"""
    return ResourceIntegration(container_manager)
