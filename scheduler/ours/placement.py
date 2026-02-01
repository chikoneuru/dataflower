"""
Function Placement Logic: Decides initial allocation of function containers to nodes.
Considers resource fit, locality, and cost optimization.
"""

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

from provider.function_container_manager import (FunctionContainer,
                                                 NodeResources)

from .cost_models import CostModels, NodeProfile, TaskProfile


class PlacementStrategy(Enum):
    """Available placement strategies"""
    RESOURCE_AWARE = "resource_aware"  # Based on available resources
    INITIAL_GREEDY = "initial_greedy"  # Greedy placement with DAG dependencies
    TARGETED_LNS = "targeted_lns"      # Targeted Local Neighborhood Search


@dataclass
class PlacementRequest:
    """Request for function container placement"""
    function_name: str
    cpu_requirement: float = 1.0
    memory_requirement: int = 512  # MB
    data_locality_hints: List[str] = None  # Data IDs that should be local
    preferred_nodes: List[str] = None  # Node preferences
    anti_affinity_functions: List[str] = None  # Functions to avoid co-location
    affinity_functions: List[str] = None  # Functions to prefer co-location
    priority: int = 1  # Higher number = higher priority
    dag_structure: Dict = None  # DAG structure for dependency-aware placement
    parent_tasks: List[str] = None  # Parent tasks in DAG
    time_budget_ms: int = 20  # Time budget for LNS optimization


@dataclass
class PlacementResult:
    """Result of placement decision"""
    target_node: str
    score: float
    reasoning: str
    alternative_nodes: List[Tuple[str, float]] = None  # (node_id, score)


class FunctionPlacement:
    """
    Handles placement decisions for function containers.
    Decides WHERE to initially deploy function containers based on:
    - Resource availability and fit
    - Data locality considerations  
    - Cost optimization
    - Load balancing
    - Affinity/anti-affinity rules
    """
    
    def __init__(self, cost_models: Optional[CostModels] = None, container_manager=None):
        self.cost_models = cost_models or CostModels()
        self.container_manager = container_manager
        self.logger = logging.getLogger(__name__)
        
        # Connect cost models with container manager for real-time resources
        if self.container_manager:
            self.cost_models.set_container_manager(container_manager)
        
        # Placement configuration
        self.placement_weights = {
            'resource_fit': 0.3,
            'data_locality': 0.25, 
            'cost_efficiency': 0.2,
            'load_balance': 0.15,
            'affinity': 0.1
        }
        
        # Cache for placement decisions
        self.placement_history: List[Dict] = []
        self.node_load_cache: Dict[str, float] = {}
    
    def find_best_placement(self, request: PlacementRequest, 
                           available_nodes: List[NodeResources],
                           existing_containers: Dict[str, List[FunctionContainer]],
                           strategy: PlacementStrategy = PlacementStrategy.RESOURCE_AWARE) -> Optional[PlacementResult]:
        """
        Find the best node for placing a function container.
        This is the main PLACEMENT decision logic.
        """
        if not available_nodes:
            self.logger.error("No available nodes for placement")
            return None
        
        self.logger.info(f"Finding placement for {request.function_name} using strategy {strategy.value}")
        
        # Filter nodes that can accommodate the request
        suitable_nodes = self._filter_suitable_nodes(request, available_nodes)
        if not suitable_nodes:
            self.logger.warning(f"No suitable nodes found for {request.function_name}")
            return None
        
        # Score nodes based on strategy
        if strategy == PlacementStrategy.RESOURCE_AWARE:
            return self._resource_aware_placement(request, suitable_nodes, existing_containers)
        elif strategy == PlacementStrategy.INITIAL_GREEDY:
            return self._initial_greedy_placement(request, suitable_nodes, existing_containers)
        elif strategy == PlacementStrategy.TARGETED_LNS:
            return self._targeted_lns_placement(request, suitable_nodes, existing_containers)
        else:
            self.logger.warning(f"Unknown placement strategy {strategy}, using resource_aware")
            return self._resource_aware_placement(request, suitable_nodes, existing_containers)
    
    def _filter_suitable_nodes(self, request: PlacementRequest, 
                              nodes: List[NodeResources]) -> List[NodeResources]:
        """Filter nodes that can accommodate the placement request with real-time resource check"""
        suitable = []
        
        # Update real-time resources first if container manager is available
        if self.container_manager:
            self.container_manager.update_real_time_resources()
        
        for node in nodes:
            # Get real-time resource availability
            if self.container_manager:
                real_resources = self.container_manager.get_node_available_resources(node.node_id)
                if 'error' in real_resources:
                    continue
                available_cpu = real_resources['available_cpu']
                available_memory = real_resources['available_memory']
            else:
                available_cpu = node.available_cpu
                available_memory = node.available_memory
            
            # Check resource requirements with real-time data
            if (available_cpu >= request.cpu_requirement and 
                available_memory >= request.memory_requirement / 1024):
                
                # Check preferred nodes
                if request.preferred_nodes and node.node_id not in request.preferred_nodes:
                    continue
                
                # Check anti-affinity
                if request.anti_affinity_functions:
                    has_conflict = any(
                        any(container.function_name in request.anti_affinity_functions 
                            for container in node.running_containers.values())
                    )
                    if has_conflict:
                        continue
                
                suitable.append(node)
        
        return suitable
    
    def _resource_aware_placement(self, request: PlacementRequest,
                                 nodes: List[NodeResources],
                                 existing_containers: Dict[str, List[FunctionContainer]]) -> PlacementResult:
        """Place based on available resources with real-time data"""
        node_scores = []
        
        for node in nodes:
            score = 0.0
            
            # Get real-time resource data if available
            if self.container_manager:
                real_resources = self.container_manager.get_node_available_resources(node.node_id)
                if 'error' not in real_resources:
                    available_cpu = real_resources['available_cpu']
                    available_memory = real_resources['available_memory']
                    total_cpu = real_resources['total_cpu']
                    total_memory = real_resources['total_memory']
                    cpu_utilization = real_resources['cpu_utilization']
                    memory_utilization = real_resources['memory_utilization']
                else:
                    available_cpu = node.available_cpu
                    available_memory = node.available_memory
                    total_cpu = node.cpu_cores
                    total_memory = node.memory_gb
                    cpu_utilization = (total_cpu - available_cpu) / total_cpu
                    memory_utilization = (total_memory - available_memory) / total_memory
            else:
                available_cpu = node.available_cpu
                available_memory = node.available_memory
                total_cpu = node.cpu_cores
                total_memory = node.memory_gb
                cpu_utilization = (total_cpu - available_cpu) / total_cpu
                memory_utilization = (total_memory - available_memory) / total_memory
            
            # CPU availability score (0-1) - prefer nodes with more available CPU
            cpu_ratio = available_cpu / total_cpu
            score += cpu_ratio * 0.4
            
            # Memory availability score (0-1) - prefer nodes with more available memory
            memory_ratio = available_memory / total_memory
            score += memory_ratio * 0.3
            
            # Utilization penalty - avoid highly utilized nodes
            utilization_penalty = (cpu_utilization + memory_utilization) / 2.0
            score -= utilization_penalty * 0.2
            
            # Container density penalty (prefer less crowded nodes)
            container_count = len(node.running_containers)
            density_penalty = container_count / 10.0  # Normalize by max expected containers
            score -= min(density_penalty, 0.1)
            
            # Resource fit bonus (prefer nodes that fit requirements well)
            cpu_fit = 1.0 - abs(available_cpu - request.cpu_requirement) / total_cpu
            memory_fit = 1.0 - abs(available_memory - request.memory_requirement/1024) / total_memory
            score += (cpu_fit + memory_fit) * 0.1
            
            node_scores.append((node.node_id, score))
        
        # Sort by score (highest first)
        node_scores.sort(key=lambda x: x[1], reverse=True)
        best_node_id, best_score = node_scores[0]
        
        return PlacementResult(
            target_node=best_node_id,
            score=best_score,
            reasoning=f"Resource-aware placement: {best_score:.3f} score based on real-time CPU/memory availability",
            alternative_nodes=node_scores[1:3]  # Top 2 alternatives
        )
    
    def _initial_greedy_placement(self, request: PlacementRequest,
                                 nodes: List[NodeResources],
                                 existing_containers: Dict[str, List[FunctionContainer]]) -> PlacementResult:
        """Initial greedy placement with DAG dependencies, cost optimization, and container locality"""
        node_scores = []
        
        # Check if current function has existing containers (for locality preference)
        current_function_containers = existing_containers.get(request.function_name, [])
        has_existing_containers = len(current_function_containers) > 0
        
        for node in nodes:
            total_cost = 0.0
            reasoning_parts = []
            
            # Execution cost (based on resource fit)
            cpu_ratio = node.available_cpu / node.cpu_cores
            memory_ratio = node.available_memory / node.memory_gb
            execution_cost = 1.0 - (cpu_ratio + memory_ratio) / 2.0
            total_cost += execution_cost * 0.4  # Reduced from 0.6 to make room for locality
            reasoning_parts.append(f"exec_cost={execution_cost:.3f}")
            
            # Transfer cost from parent tasks
            transfer_cost = 0.0
            if request.parent_tasks and request.dag_structure:
                for parent_task in request.parent_tasks:
                    # Find where parent is placed
                    parent_containers = existing_containers.get(parent_task, [])
                    if parent_containers:
                        # Calculate transfer cost based on network distance
                        parent_node = parent_containers[0].node_id
                        if parent_node != node.node_id:
                            # Inter-node transfer cost (simplified)
                            transfer_cost += 0.3
                        # else: intra-node transfer cost is negligible
            
            total_cost += transfer_cost * 0.3  # Reduced from 0.4
            if transfer_cost > 0:
                reasoning_parts.append(f"transfer_cost={transfer_cost:.3f}")
            
            # Container locality cost (NEW: strongly prefer nodes with existing containers)
            locality_cost = 0.0
            if has_existing_containers:
                # Check if this node has containers for the current function
                node_has_containers = any(c.node_id == node.node_id for c in current_function_containers)
                if not node_has_containers:
                    # High cost for nodes without existing containers (cold start penalty)
                    locality_cost = 0.5
                    reasoning_parts.append("cold_start_penalty")
                else:
                    # Bonus for nodes with existing containers (warm containers)
                    locality_cost = -0.2  # Negative cost = bonus
                    reasoning_parts.append("warm_container_bonus")
            
            total_cost += locality_cost * 0.3  # 30% weight for container locality
            
            # Convert cost to score (lower cost = higher score)
            score = 1.0 - min(total_cost, 1.0)
            node_scores.append((node.node_id, score, reasoning_parts))
        
        node_scores.sort(key=lambda x: x[1], reverse=True)
        best_node_id, best_score, best_reasoning = node_scores[0]
        
        reasoning = f"Initial greedy: {best_score:.3f} score ({', '.join(best_reasoning)})"
        
        return PlacementResult(
            target_node=best_node_id,
            score=best_score,
            reasoning=reasoning,
            alternative_nodes=[(node_id, score) for node_id, score, _ in node_scores[1:3]]
        )
    
    def _targeted_lns_placement(self, request: PlacementRequest,
                               nodes: List[NodeResources], 
                               existing_containers: Dict[str, List[FunctionContainer]]) -> PlacementResult:
        """Targeted Local Neighborhood Search for runtime optimization - only triggers when beneficial"""
        
        # Start with initial greedy solution
        current_result = self._initial_greedy_placement(request, nodes, existing_containers)
        
        # Check if LNS should trigger (selective triggering)
        should_trigger, trigger_reason = self._should_trigger_lns(request, current_result, nodes, existing_containers)
        
        if not should_trigger:
            # Return greedy result with explanation why LNS didn't trigger
            return PlacementResult(
                target_node=current_result.target_node,
                score=current_result.score,
                reasoning=f"LNS skipped: {trigger_reason}. {current_result.reasoning}",
                alternative_nodes=current_result.alternative_nodes
            )
        
        # LNS triggered - proceed with optimization
        start_time = time.time() * 1000  # Convert to milliseconds
        time_budget = request.time_budget_ms
        
        best_result = current_result
        best_cost = 1.0 - current_result.score  # Convert score back to cost
        
        # Build neighborhood (function + adjacent functions)
        neighborhood = self._build_neighborhood(request, existing_containers)
        
        iterations = 0
        while (time.time() * 1000 - start_time) < time_budget and iterations < 10:
            # Select high-cost edge/bottleneck
            target_node = self._select_problematic_node(neighborhood, nodes, existing_containers)
            
            if not target_node:
                break
            
            # Generate alternative moves
            alternative_moves = self._generate_alternative_moves(request, target_node, nodes)
            
            for move in alternative_moves:
                move_cost = self._estimate_move_cost(request, move, existing_containers, nodes)
                
                if move_cost < best_cost:
                    best_cost = move_cost
                    best_result = PlacementResult(
                        target_node=move,
                        score=1.0 - move_cost,
                        reasoning=f"LNS triggered ({trigger_reason}): {1.0 - move_cost:.3f} score after {iterations} iterations",
                        alternative_nodes=[(current_result.target_node, current_result.score)]
                    )
            
            iterations += 1
        
        # If LNS ran but found no improvement, update reasoning
        if best_result == current_result:
            best_result = PlacementResult(
                target_node=best_result.target_node,
                score=best_result.score,
                reasoning=f"LNS triggered ({trigger_reason}) but no improvement found. {current_result.reasoning}",
                alternative_nodes=best_result.alternative_nodes
            )
        
        return best_result
    
    def _should_trigger_lns(self, request: PlacementRequest, current_result: PlacementResult,
                           nodes: List[NodeResources], 
                           existing_containers: Dict[str, List[FunctionContainer]]) -> Tuple[bool, str]:
        """
        Determine if LNS should trigger based on selective criteria.
        Returns (should_trigger, reason)
        """
        
        # Criterion 1: Check if current placement is "good enough" (high score)
        if current_result.score > 0.85:  # 85% threshold
            return False, f"placement already optimal (score={current_result.score:.3f})"
        
        # Criterion 2: Check for high-cost edges (large transfers from parents)
        has_expensive_transfers = False
        if request.parent_tasks and request.dag_structure:
            for parent_task in request.parent_tasks:
                parent_containers = existing_containers.get(parent_task, [])
                if parent_containers:
                    parent_node = parent_containers[0].node_id
                    # Check if parent is on different node (cross-node transfer)
                    if parent_node != current_result.target_node:
                        # Check if transfer size is significant
                        if request.dag_structure:
                            deps = request.dag_structure.get(request.function_name, {}).get('dependencies', [])
                            for dep in deps:
                                if dep.get('task_id') == parent_task and dep.get('data_size', 0) > 1024*1024:  # >1MB
                                    has_expensive_transfers = True
                                    break
        
        # Criterion 3: Check for node overload (runtime metrics drift)
        target_node = None
        for node in nodes:
            if node.node_id == current_result.target_node:
                target_node = node
                break
        
        node_overloaded = False
        if target_node:
            cpu_util = 1.0 - (target_node.available_cpu / target_node.cpu_cores)
            memory_util = 1.0 - (target_node.available_memory / target_node.memory_gb)
            if cpu_util > 0.8 or memory_util > 0.8:  # 80% utilization threshold
                node_overloaded = True
        
        # Criterion 4: Check if function is at a bottleneck (simplified)
        # For now, assume functions with high compute requirements are more likely to benefit
        is_compute_intensive = request.cpu_requirement > 2.0 or request.memory_requirement > 1024
        
        # Decision logic
        if has_expensive_transfers:
            return True, "expensive cross-node transfers detected"
        
        if node_overloaded:
            return True, f"target node overloaded (CPU/mem >80%)"
        
        if is_compute_intensive and current_result.score < 0.7:  # 70% threshold for compute-intensive
            return True, "compute-intensive function with suboptimal placement"
        
        # Additional check: if score is very low, always try to optimize
        if current_result.score < 0.5:  # 50% threshold
            return True, f"placement score too low ({current_result.score:.3f})"
        
        # Default: don't trigger LNS
        return False, "conditions stable, no optimization needed"
    
    def _build_neighborhood(self, request: PlacementRequest, 
                           existing_containers: Dict[str, List[FunctionContainer]]) -> List[str]:
        """Build neighborhood of functions for LNS"""
        neighborhood = [request.function_name]
        
        # Add parent tasks
        if request.parent_tasks:
            neighborhood.extend(request.parent_tasks)
        
        # Add functions with data locality hints
        if request.data_locality_hints:
            for func_name, containers in existing_containers.items():
                for container in containers:
                    if any(hint in container.function_name for hint in request.data_locality_hints):
                        if func_name not in neighborhood:
                            neighborhood.append(func_name)
        
        return neighborhood
    
    def _select_problematic_node(self, neighborhood: List[str], 
                                nodes: List[NodeResources],
                                existing_containers: Dict[str, List[FunctionContainer]]) -> Optional[str]:
        """Select node with highest load/cost for optimization"""
        node_loads = {}
        
        for node in nodes:
            load = len(node.running_containers) / 10.0  # Normalize by expected max
            cpu_utilization = 1.0 - (node.available_cpu / node.cpu_cores)
            memory_utilization = 1.0 - (node.available_memory / node.memory_gb)
            
            total_load = (load + cpu_utilization + memory_utilization) / 3.0
            node_loads[node.node_id] = total_load
        
        if not node_loads:
            return None
        
        # Return node with highest load
        return max(node_loads.items(), key=lambda x: x[1])[0]
    
    def _generate_alternative_moves(self, request: PlacementRequest,
                                   problematic_node: str, 
                                   nodes: List[NodeResources]) -> List[str]:
        """Generate alternative node assignments"""
        alternatives = []
        
        for node in nodes:
            if (node.node_id != problematic_node and 
                node.available_cpu >= request.cpu_requirement and
                node.available_memory >= request.memory_requirement / 1024):
                alternatives.append(node.node_id)
        
        # Sort by available resources (prefer less loaded nodes)
        alternatives.sort(key=lambda node_id: next(
            (n.available_cpu + n.available_memory for n in nodes if n.node_id == node_id), 0
        ), reverse=True)
        
        return alternatives[:3]  # Return top 3 alternatives
    
    def _estimate_move_cost(self, request: PlacementRequest, target_node: str,
                           existing_containers: Dict[str, List[FunctionContainer]],
                           nodes: List[NodeResources]) -> float:
        """Estimate cost of moving function to target node"""
        target_node_obj = next((n for n in nodes if n.node_id == target_node), None)
        if not target_node_obj:
            return float('inf')
        
        # Resource cost
        cpu_cost = 1.0 - (target_node_obj.available_cpu / target_node_obj.cpu_cores)
        memory_cost = 1.0 - (target_node_obj.available_memory / target_node_obj.memory_gb)
        resource_cost = (cpu_cost + memory_cost) / 2.0
        
        # Transfer cost
        transfer_cost = 0.0
        if request.parent_tasks:
            for parent_task in request.parent_tasks:
                parent_containers = existing_containers.get(parent_task, [])
                if parent_containers and parent_containers[0].node_id != target_node:
                    transfer_cost += 0.2
        
        return resource_cost * 0.7 + transfer_cost * 0.3

    def record_placement_decision(self, request: PlacementRequest, result: PlacementResult):
        """Record placement decision for analysis and learning"""
        decision_record = {
            'timestamp': time.time(),
            'function_name': request.function_name,
            'target_node': result.target_node,
            'score': result.score,
            'reasoning': result.reasoning,
            'cpu_requirement': request.cpu_requirement,
            'memory_requirement': request.memory_requirement
        }
        
        self.placement_history.append(decision_record)
        
        # Keep only recent history
        if len(self.placement_history) > 1000:
            self.placement_history = self.placement_history[-500:]
        
        self.logger.info(f"Recorded placement: {request.function_name} -> {result.target_node} (score: {result.score:.3f})")
    
    def get_placement_statistics(self) -> Dict:
        """Get statistics about placement decisions"""
        if not self.placement_history:
            return {}
        
        # Node usage distribution
        node_usage = {}
        function_distribution = {}
        
        for record in self.placement_history:
            node = record['target_node']
            func = record['function_name']
            
            node_usage[node] = node_usage.get(node, 0) + 1
            function_distribution[func] = function_distribution.get(func, 0) + 1
        
        return {
            'total_placements': len(self.placement_history),
            'node_usage_distribution': node_usage,
            'function_distribution': function_distribution,
            'average_score': sum(r['score'] for r in self.placement_history) / len(self.placement_history),
            'recent_placements': self.placement_history[-10:]  # Last 10
        }
