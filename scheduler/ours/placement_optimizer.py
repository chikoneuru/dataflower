import random
import copy
from typing import Dict, List, Tuple, Optional, Literal
from dataclasses import dataclass
import time

@dataclass
class OptimizationResult:
    assignments: Dict[str, str]  # task_id -> node_id
    total_cost: float
    execution_time: float
    iterations: int

class PlacementOptimizer:
    def __init__(self, cost_models, max_iterations: int = 100, time_limit: float = 5.0):
        self.cost_models = cost_models
        self.max_iterations = max_iterations
        self.time_limit = time_limit
    
    def local_search_optimization(self, initial_assignments: Dict[str, str], 
                                tasks: List, nodes: List, weights: Dict[str, float],
                                dag_structure: Dict) -> OptimizationResult:
        """
        Local search optimization starting from greedy solution
        """
        start_time = time.time()
        current_assignments = copy.deepcopy(initial_assignments)
        current_cost = self._calculate_total_cost(current_assignments, tasks, nodes, weights, dag_structure)
        
        best_assignments = copy.deepcopy(current_assignments)
        best_cost = current_cost
        iterations = 0
        
        while (iterations < self.max_iterations and 
               time.time() - start_time < self.time_limit):
            
            # Generate neighborhood by swapping task assignments
            improved = False
            
            for task_id, current_node_id in current_assignments.items():
                for node in nodes:
                    if node.node_id == current_node_id:
                        continue  # Skip current assignment
                    
                    # Try swapping this task to a different node
                    new_assignments = copy.deepcopy(current_assignments)
                    new_assignments[task_id] = node.node_id
                    
                    # Check if this is a valid assignment
                    if not self._is_valid_assignment(new_assignments, tasks, nodes):
                        continue
                    
                    new_cost = self._calculate_total_cost(new_assignments, tasks, nodes, weights, dag_structure)
                    
                    if new_cost < current_cost:
                        current_assignments = new_assignments
                        current_cost = new_cost
                        improved = True
                        
                        if current_cost < best_cost:
                            best_assignments = copy.deepcopy(current_assignments)
                            best_cost = current_cost
                        break
                
                if improved:
                    break
            
            if not improved:
                break  # Local optimum reached
            
            iterations += 1
        
        return OptimizationResult(
            assignments=best_assignments,
            total_cost=best_cost,
            execution_time=time.time() - start_time,
            iterations=iterations
        )
    
    def simulated_annealing_optimization(self, initial_assignments: Dict[str, str],
                                       tasks: List, nodes: List, weights: Dict[str, float],
                                       dag_structure: Dict) -> OptimizationResult:
        """
        Simulated annealing optimization
        """
        start_time = time.time()
        current_assignments = copy.deepcopy(initial_assignments)
        current_cost = self._calculate_total_cost(current_assignments, tasks, nodes, weights, dag_structure)
        
        best_assignments = copy.deepcopy(current_assignments)
        best_cost = current_cost
        
        # Annealing parameters
        initial_temp = 100.0
        final_temp = 0.1
        cooling_rate = 0.95
        
        temp = initial_temp
        iterations = 0
        
        while (temp > final_temp and iterations < self.max_iterations and 
               time.time() - start_time < self.time_limit):
            
            # Generate random neighbor
            task_id = random.choice(list(current_assignments.keys()))
            new_node = random.choice(nodes)
            
            new_assignments = copy.deepcopy(current_assignments)
            new_assignments[task_id] = new_node.node_id
            
            if not self._is_valid_assignment(new_assignments, tasks, nodes):
                continue
            
            new_cost = self._calculate_total_cost(new_assignments, tasks, nodes, weights, dag_structure)
            
            # Accept or reject based on temperature
            if new_cost < current_cost or random.random() < np.exp(-(new_cost - current_cost) / temp):
                current_assignments = new_assignments
                current_cost = new_cost
                
                if current_cost < best_cost:
                    best_assignments = copy.deepcopy(current_assignments)
                    best_cost = current_cost
            
            temp *= cooling_rate
            iterations += 1
        
        return OptimizationResult(
            assignments=best_assignments,
            total_cost=best_cost,
            execution_time=time.time() - start_time,
            iterations=iterations
        )
    
    def _calculate_total_cost(self, assignments: Dict[str, str], tasks: List, 
                            nodes: List, weights: Dict[str, float], 
                            dag_structure: Dict) -> float:
        """
        Calculate total cost: α⋅T_compute + β⋅T_transfer + γ⋅T_cold + δ⋅I_interference
        """
        total_cost = 0.0
        
        # Create node lookup
        node_lookup = {node.node_id: node for node in nodes}
        task_lookup = {task.task_id: task for task in tasks}
        
        for task_id, node_id in assignments.items():
            task = task_lookup[task_id]
            node = node_lookup[node_id]
            
            # Get data transfers for this task
            data_transfers = []
            for dependency in dag_structure.get(task_id, {}).get('dependencies', []):
                dep_node_id = assignments.get(dependency['task_id'], node_id)
                data_transfers.append((dep_node_id, dependency['data_size']))
            
            # Calculate all costs
            costs = self.cost_models.get_all_costs(task, node, data_transfers)
            
            # Apply weights
            weighted_cost = (weights['alpha'] * costs['compute'] +
                           weights['beta'] * costs['transfer'] +
                           weights['gamma'] * costs['cold_start'] +
                           weights['delta'] * costs['interference'])
            
            total_cost += weighted_cost
        
        return total_cost
    
    def _is_valid_assignment(self, assignments: Dict[str, str], tasks: List, nodes: List) -> bool:
        """
        Check if assignment satisfies constraints
        """
        # Check if all tasks are assigned
        if len(assignments) != len(tasks):
            return False
        
        # Check if all assigned nodes exist
        node_ids = {node.node_id for node in nodes}
        for node_id in assignments.values():
            if node_id not in node_ids:
                return False
        
        # TODO: Add more constraint checks (capacity, dependencies, etc.)
        return True
    
    def optimize_placement(self, initial_assignments: Dict[str, str], tasks: List, 
                          nodes: List, weights: Dict[str, float], 
                          dag_structure: Dict, method: str = "local_search") -> OptimizationResult:
        """
        Main optimization method
        """
        if method == "local_search":
            return self.local_search_optimization(initial_assignments, tasks, nodes, weights, dag_structure)
        elif method == "simulated_annealing":
            return self.simulated_annealing_optimization(initial_assignments, tasks, nodes, weights, dag_structure)
        else:
            raise ValueError(f"Unknown optimization method: {method}")

    # ===== MOCK FUNCTIONS FOR TESTING =====
    
    def mock_local_search_optimization(self, initial_assignments: Dict[str, str], 
                                     tasks: List, nodes: List, weights: Dict[str, float],
                                     dag_structure: Dict) -> OptimizationResult:
        """
        Mock function: Simple local search optimization
        """
        start_time = time.time()
        
        # Simple mock: just return initial assignments with slightly better cost
        mock_cost = self._mock_calculate_total_cost(initial_assignments, tasks, nodes, weights)
        
        return OptimizationResult(
            assignments=initial_assignments,
            total_cost=mock_cost * 0.9,  # 10% improvement
            execution_time=time.time() - start_time,
            iterations=5  # Mock iterations
        )
    
    def mock_simulated_annealing_optimization(self, initial_assignments: Dict[str, str],
                                            tasks: List, nodes: List, weights: Dict[str, float],
                                            dag_structure: Dict) -> OptimizationResult:
        """
        Mock function: Simple simulated annealing optimization
        """
        start_time = time.time()
        
        # Simple mock: return initial assignments with better cost
        mock_cost = self._mock_calculate_total_cost(initial_assignments, tasks, nodes, weights)
        
        return OptimizationResult(
            assignments=initial_assignments,
            total_cost=mock_cost * 0.8,  # 20% improvement
            execution_time=time.time() - start_time,
            iterations=10  # Mock iterations
        )
    
    def _mock_calculate_total_cost(self, assignments: Dict[str, str], tasks: List, 
                                 nodes: List, weights: Dict[str, float], 
                                 dag_structure: Dict = None) -> float:
        """
        Mock function: Simple total cost calculation
        """
        # Simple mock: base cost per task
        base_cost = len(assignments) * 1.0
        
        # Apply weights
        weighted_cost = (weights.get('alpha', 0.5) * base_cost +
                        weights.get('beta', 0.3) * base_cost * 0.5 +
                        weights.get('gamma', 0.1) * base_cost * 0.2 +
                        weights.get('delta', 0.1) * base_cost * 0.1)
        
        return weighted_cost
    
    def mock_optimize_placement(self, initial_assignments: Dict[str, str], tasks: List, 
                               nodes: List, weights: Dict[str, float], 
                               dag_structure: Dict, method: str = "local_search") -> OptimizationResult:
        """
        Mock function: Simple placement optimization for testing
        """
        if method == "local_search":
            return self.mock_local_search_optimization(initial_assignments, tasks, nodes, weights, dag_structure)
        elif method == "simulated_annealing":
            return self.mock_simulated_annealing_optimization(initial_assignments, tasks, nodes, weights, dag_structure)
        else:
            raise ValueError(f"Unknown optimization method: {method}")
    
    @staticmethod
    def create_mock_assignments(task_count: int, node_count: int) -> Dict[str, str]:
        """
        Create mock task-to-node assignments for testing
        """
        assignments = {}
        for i in range(task_count):
            assignments[f"task_{i}"] = f"node_{i % node_count}"
        return assignments