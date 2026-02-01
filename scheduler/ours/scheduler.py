from typing import Dict, List, Tuple, Optional, Literal
from dataclasses import dataclass
import heapq
from collections import defaultdict
import time
import random
from .config import get_config

@dataclass
class TaskAssignment:
    task_id: str
    node_id: str
    priority: float
    estimated_start_time: float

class Scheduler:
    def __init__(self, cost_models, mode_classifier, container_manager, function_manager):
        self.config = get_config()
        if not self.config:
            raise RuntimeError("Configuration not initialized. Call initialize_config() first.")
        
        self.cost_models = cost_models
        self.mode_classifier = mode_classifier
        self.container_manager = container_manager
        self.function_manager = function_manager

    def schedule_function(self, function_name, request_id):
        """
        Selects a worker/container for a function based on the algorithm
        specified in the configuration.
        """
        algorithm = self.config.scheduling.algorithm
        
        print(f"    - Scheduler using algorithm: '{algorithm}'")

        if algorithm == "random_worker":
            return self._schedule_random_worker(function_name, request_id)
        elif algorithm == "network_bound":
            # Placeholder for the more complex logic from the original class
            print("    - Warning: 'network_bound' scheduling not fully implemented in this refactor.")
            return self._schedule_random_worker(function_name, request_id) # Fallback
        else:
            print(f"    - Scheduler Error: Unknown scheduling algorithm '{algorithm}'. Falling back to random.")
            return self._schedule_random_worker(function_name, request_id)

    def _schedule_random_worker(self, function_name, request_id):
        """
        Selects a worker node to execute the function randomly.
        """
        available_workers = self.container_manager.get_workers()
        if not available_workers:
            print(f"    - Scheduler Error: No available workers to run function '{function_name}'.")
            return None
        
        selected_worker = random.choice(available_workers)
        print(f"    - Scheduler selected worker '{selected_worker['name']}' for function '{function_name}'.")
        
        return selected_worker

    # The old `select_container` method is now deprecated in favor of `select_worker`.
    # It is kept here for reference but should not be used in the new architecture.
    def select_container(self, function_name, request_id, incoming_time, parent_list):
        print("Warning: select_container is deprecated. Use select_worker instead.")
        
        workflow_name = self.function_manager.get_workflow_name(function_name)
        if workflow_name is None:
            return 'Workflow not found', 400

        available_containers = self.container_manager.list_functions(workflow_name)
        if not available_containers:
            return 'No available containers', 500

        # simple random selection
        selected_container = random.choice(available_containers)

        return selected_container

    def network_bound_scheduling(self, ready_tasks: List, available_nodes: List, 
                               dag_structure: Dict) -> List[TaskAssignment]:
        """
        Network-bound mode: prioritize locality and minimize transfers
        """
        assignments = []
        node_load = defaultdict(float)
        
        # Sort tasks by data transfer volume (descending)
        tasks_with_transfer = []
        for task in ready_tasks:
            transfer_volume = self._calculate_transfer_volume(task, dag_structure)
            tasks_with_transfer.append((transfer_volume, task))
        
        tasks_with_transfer.sort(reverse=True)  # Highest transfer first
        
        for transfer_volume, task in tasks_with_transfer:
            best_node = None
            best_score = float('inf')
            
            for node in available_nodes:
                # Calculate locality score
                locality_score = self._calculate_locality_score(task, node, dag_structure)
                
                # Calculate load balance score
                load_penalty = node_load[node.node_id] * 0.1
                
                # Combined score (lower is better)
                score = -locality_score + load_penalty
                
                if score < best_score:
                    best_score = score
                    best_node = node
            
            if best_node:
                assignment = TaskAssignment(
                    task_id=task.task_id,
                    node_id=best_node.node_id,
                    priority=transfer_volume,  # Higher transfer = higher priority
                    estimated_start_time=node_load[best_node.node_id]
                )
                assignments.append(assignment)
                node_load[best_node.node_id] += self.cost_models.estimate_compute_cost(task, best_node)
        
        return assignments
    
    def compute_bound_scheduling(self, ready_tasks: List, available_nodes: List) -> List[TaskAssignment]:
        """
        Compute-bound mode: prioritize parallelism and load balancing
        """
        assignments = []
        node_load = defaultdict(float)
        
        # Sort tasks by compute intensity (descending)
        tasks_with_compute = []
        for task in ready_tasks:
            compute_intensity = task.estimated_compute_time * task.cpu_intensity
            tasks_with_compute.append((compute_intensity, task))
        
        tasks_with_compute.sort(reverse=True)  # Highest compute first
        
        for compute_intensity, task in tasks_with_compute:
            # Find node with minimum load
            best_node = min(available_nodes, key=lambda n: node_load[n.node_id])
            
            assignment = TaskAssignment(
                task_id=task.task_id,
                node_id=best_node.node_id,
                priority=compute_intensity,
                estimated_start_time=node_load[best_node.node_id]
            )
            assignments.append(assignment)
            node_load[best_node.node_id] += self.cost_models.estimate_compute_cost(task, best_node)
        
        return assignments
    
    def _calculate_transfer_volume(self, task, dag_structure: Dict) -> float:
        """Calculate total data transfer volume for a task"""
        total_volume = 0.0
        for dependency in dag_structure.get(task.task_id, {}).get('dependencies', []):
            total_volume += dependency.get('data_size', 0)
        return total_volume
    
    def _calculate_locality_score(self, task, node, dag_structure: Dict) -> float:
        """Calculate locality score (higher = better locality)"""
        score = 0.0
        
        # Check if dependencies are on the same node
        for dependency in dag_structure.get(task.task_id, {}).get('dependencies', []):
            if dependency.get('node_id') == node.node_id:
                score += dependency.get('data_size', 0) * 0.1  # Reward for co-location
        
        # Check if warm container exists
        if node.container_cache.get(task.function_name, False):
            score += 100  # High reward for warm container
        
        return score
    
    def schedule_tasks(self, ready_tasks: List, available_nodes: List, 
                      mode: str, dag_structure: Dict) -> List[TaskAssignment]:
        """
        Main scheduling method that delegates to mode-specific strategies
        """
        if mode == "network-bound":
            return self.network_bound_scheduling(ready_tasks, available_nodes, dag_structure)
        else:  # compute-bound
            return self.compute_bound_scheduling(ready_tasks, available_nodes)

    # ===== MOCK FUNCTIONS FOR TESTING =====
    
    def mock_network_bound_scheduling(self, ready_tasks: List, available_nodes: List, 
                                    dag_structure: Dict) -> List[TaskAssignment]:
        """
        Mock function: Simple network-bound scheduling
        """
        assignments = []
        
        # Simple round-robin assignment with locality preference
        for i, task in enumerate(ready_tasks):
            # Prefer first node for locality
            node = available_nodes[i % len(available_nodes)]
            
            assignment = TaskAssignment(
                task_id=task.task_id,
                node_id=node.node_id,
                priority=1.0,  # Fixed priority
                estimated_start_time=i * 0.5  # Simple time estimation
            )
            assignments.append(assignment)
        
        return assignments
    
    def mock_compute_bound_scheduling(self, ready_tasks: List, available_nodes: List) -> List[TaskAssignment]:
        """
        Mock function: Simple compute-bound scheduling
        """
        assignments = []
        
        # Simple load balancing
        for i, task in enumerate(ready_tasks):
            # Round-robin assignment
            node = available_nodes[i % len(available_nodes)]
            
            assignment = TaskAssignment(
                task_id=task.task_id,
                node_id=node.node_id,
                priority=task.estimated_compute_time,  # Priority based on compute time
                estimated_start_time=i * 0.3  # Simple time estimation
            )
            assignments.append(assignment)
        
        return assignments
    
    def mock_schedule_tasks(self, ready_tasks: List, available_nodes: List, 
                           mode: str, dag_structure: Dict) -> List[TaskAssignment]:
        """
        Mock function: Simple task scheduling for testing
        """
        if mode == "network-bound":
            return self.mock_network_bound_scheduling(ready_tasks, available_nodes, dag_structure)
        else:
            return self.mock_compute_bound_scheduling(ready_tasks, available_nodes)
    
    @staticmethod
    def create_mock_assignments(task_count: int, node_count: int) -> List[TaskAssignment]:
        """
        Create mock task assignments for testing
        """
        assignments = []
        for i in range(task_count):
            assignment = TaskAssignment(
                task_id=f"task_{i}",
                node_id=f"node_{i % node_count}",
                priority=1.0 + i * 0.1,
                estimated_start_time=i * 0.5
            )
            assignments.append(assignment)
        return assignments