import random
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Tuple


@dataclass
class BottleneckAnalysis:
    has_bottleneck: bool
    bottleneck_type: Optional[Literal["compute", "transfer", "none"]]
    bottleneck_severity: float
    critical_nodes: List[str]
    critical_edges: List[Tuple[str, str]]
    # Contribution-based analysis fields
    compute_contributions: Dict[str, float] = None  # task_id -> contribution
    network_contributions: Dict[Tuple[str, str], float] = None  # (src, dst) -> contribution
    max_contribution: float = 0.0
    total_cost: float = 0.0

@dataclass
class EdgeLoad:
    source_node: str
    dest_node: str
    data_size: int
    load_percentage: float

class BottleneckIdentifier:
    """
    Bottleneck Identifier using threshold-based analysis with DAG-aware network cost estimation.
    
    Features:
    - Threshold-based detection (default: 0.8 for both compute and transfer)  
    - Two bottleneck types: "compute" and "transfer"
    - Identifies critical resources (nodes/edges)
    - Returns w_comp, w_net weights for scheduler optimization
    - Proper no-bottleneck signaling to scheduler
    - DAG-aware network cost estimation using actual node resources
    
    Usage:
        analysis = identifier.identify_bottlenecks(tasks, nodes, dag_structure, cost_models)
        weights = identifier.get_bottleneck_weights(analysis)
        net_costs = cost_models.net_estimate(dag_structure, nodes)
        compute_costs = cost_models.compute_estimate(tasks, nodes)
    """
    def __init__(self, compute_threshold: float = 0.8, transfer_threshold: float = 0.8, config=None):
        self.compute_threshold = compute_threshold  # CPU utilization threshold
        self.transfer_threshold = transfer_threshold  # Network utilization threshold
        self.config = config

    def identify_bottlenecks(self, tasks: List, nodes: List, dag_structure: Dict,
                           cost_models) -> BottleneckAnalysis:
        """
        Identify bottlenecks using contribution-based analysis: compute vs network
        """
        # Get cost estimates
        compute_costs = cost_models.compute_estimate(tasks, nodes)
        net_costs = cost_models.net_estimate(dag_structure, nodes)
        
        # Calculate contributions for each component
        compute_contributions = self._calculate_compute_contributions(tasks, nodes, compute_costs)
        network_contributions = self._calculate_network_contributions(net_costs)
        
        # Calculate total system cost
        total_compute_cost = sum(compute_contributions.values())
        total_network_cost = sum(network_contributions.values())
        total_cost = total_compute_cost + total_network_cost
        
        # Find maximum contribution across compute and network
        all_contributions = {}
        all_contributions.update({f"compute_{k}": v for k, v in compute_contributions.items()})
        all_contributions.update({f"network_{k}": v for k, v in network_contributions.items()})
        
        max_contribution = max(all_contributions.values()) if all_contributions else 0.0
        max_contribution_share = max_contribution / total_cost if total_cost > 0 else 0.0
        
        # Determine bottleneck type: compute, network, or none
        bottleneck_type, severity, critical_nodes, critical_edges = self._determine_bottleneck_from_contributions(
            compute_contributions, network_contributions, max_contribution_share, nodes)
        
        has_bottleneck = bottleneck_type is not None and bottleneck_type != "none"

        return BottleneckAnalysis(
            has_bottleneck=has_bottleneck,
            bottleneck_type=bottleneck_type,
            bottleneck_severity=severity,
            critical_nodes=critical_nodes,
            critical_edges=critical_edges,
            compute_contributions=compute_contributions,
            network_contributions=network_contributions,
            max_contribution=max_contribution_share,
            total_cost=total_cost
        )

    # ---------------- Internal helpers to support dict or object nodes/tasks ----------------
    def _node_id(self, node) -> str:
        return getattr(node, 'node_id', None) or (node.get('node_id') if isinstance(node, dict) else None)

    def _cpu_cores(self, node) -> int:
        if hasattr(node, 'cpu_cores'):
            return node.cpu_cores
        if isinstance(node, dict):
            return int(node.get('cpu_cores', 4))
        return 4

    def _available_cpu(self, node) -> float:
        if hasattr(node, 'available_cpu'):
            return node.available_cpu
        if isinstance(node, dict):
            # Default: assume 80% available if not provided
            cores = self._cpu_cores(node)
            return float(node.get('available_cpu', cores * 0.8))
        return float(self._cpu_cores(node) * 0.8)

    def _task_id(self, task) -> str:
        return getattr(task, 'task_id', None) or (task.get('task_id') if isinstance(task, dict) else None)
    
    def _calculate_compute_contributions(self, tasks: List, nodes: List, compute_costs: Dict) -> Dict[str, float]:
        """
        Calculate compute contribution for each task: contr_i = t_comp(i,r_i) / total_cost
        """
        contributions = {}
        
        for task in tasks:
            task_id = self._task_id(task)
            if not task_id:
                continue
                
            # Find which node this task is assigned to
            assigned_node = self._get_task_node_assignment(task_id, nodes)
            if assigned_node and assigned_node in compute_costs:
                task_cost = compute_costs[assigned_node].get(task_id, 0.0)
                contributions[task_id] = task_cost
        
        return contributions
    
    def _calculate_network_contributions(self, net_costs: Dict) -> Dict[Tuple[str, str], float]:
        """
        Calculate network contribution for each edge: contr_ij = t_net_ij(r_i,r_j) / total_cost
        """
        contributions = {}
        
        for edge_key, cost in net_costs.items():
            contributions[edge_key] = cost
        
        return contributions
    
    
    def _determine_bottleneck_from_contributions(self, compute_contributions: Dict[str, float],
                                               network_contributions: Dict[Tuple[str, str], float],
                                               max_contribution_share: float,
                                               nodes: List) -> Tuple:
        """
        Determine bottleneck type: compute, network, or none
        """
        # Threshold for detecting dominant bottleneck (40% as suggested)
        bottleneck_threshold = 0.4
        
        if max_contribution_share < bottleneck_threshold:
            # No dominant bottleneck - balanced workload
            return "none", max_contribution_share, [], []
        
        # Find the component with maximum contribution
        max_compute = max(compute_contributions.values()) if compute_contributions else 0.0
        max_network = max(network_contributions.values()) if network_contributions else 0.0
        
        if max_compute >= max_network:
            # Compute bottleneck
            critical_nodes = self._find_critical_compute_nodes(compute_contributions, nodes)
            return "compute", max_contribution_share, critical_nodes, []
        
        else:
            # Network bottleneck
            critical_edges = self._find_critical_network_edges(network_contributions)
            return "transfer", max_contribution_share, [], critical_edges
    
    def _find_critical_compute_nodes(self, compute_contributions: Dict[str, float], nodes: List) -> List[str]:
        """Find nodes with high compute contributions"""
        # Group contributions by node
        node_contributions = {}
        for task_id, contribution in compute_contributions.items():
            node_id = self._get_task_node_assignment(task_id, nodes)
            if node_id:
                if node_id not in node_contributions:
                    node_contributions[node_id] = 0.0
                node_contributions[node_id] += contribution
        
        # Return nodes with high contributions
        threshold = max(node_contributions.values()) * 0.7 if node_contributions else 0.0
        return [node_id for node_id, contrib in node_contributions.items() if contrib >= threshold]
    
    def _find_critical_network_edges(self, network_contributions: Dict[Tuple[str, str], float]) -> List[Tuple[str, str]]:
        """Find edges with high network contributions"""
        if not network_contributions:
            return []
        
        threshold = max(network_contributions.values()) * 0.7
        return [edge for edge, contrib in network_contributions.items() if contrib >= threshold]
    

    def _analyze_compute_loads(self, tasks: List, nodes: List, cost_models) -> Dict[str, float]:
        """Analyze compute utilization per node"""
        compute_loads = {}

        # Option 1: Use bulk compute estimation for better performance
        if hasattr(cost_models, 'compute_estimate'):
            compute_costs = cost_models.compute_estimate(tasks, nodes)
            
            for node in nodes:
                # Calculate CPU utilization from available resources
                cores = self._cpu_cores(node)
                available = self._available_cpu(node)
                total_load = (cores - available) / cores if cores else 0.0
                
                # Add estimated load from all assigned tasks using pre-computed costs
                for task in tasks:
                    nid = self._node_id(node)
                    tid = self._task_id(task)
                    compute_cost = compute_costs.get(nid, {}).get(tid, 0.0)
                    # Convert cost to utilization percentage (simplified)
                    additional_load = min(0.1, compute_cost / 10.0)  # Assume 10s = 10% CPU
                    total_load += additional_load
                
                compute_loads[self._node_id(node)] = min(1.0, total_load)
        
        else:
            # Option 2: Fallback to individual cost calculations
            for node in nodes:
                # Calculate CPU utilization from available resources
                cores = self._cpu_cores(node)
                available = self._available_cpu(node)
                total_load = (cores - available) / cores if cores else 0.0

                # Add estimated load from assigned tasks
                for task in tasks:
                    compute_cost = cost_models.estimate_compute_cost(task, node)
                    # Convert cost to utilization percentage (simplified)
                    additional_load = min(0.1, compute_cost / 10.0)  # Assume 10s = 10% CPU
                    total_load += additional_load

                compute_loads[self._node_id(node)] = min(1.0, total_load)

        return compute_loads

    def _analyze_edge_loads(self, tasks: List, nodes: List, dag_structure: Dict,
                          cost_models) -> List[EdgeLoad]:
        """Analyze transfer loads on actual DAG edges between nodes"""
        edge_loads = []
        
        # If dag_structure is {'nodes': [...], 'edges': [...]}, skip (no per-task dependencies provided)
        if isinstance(dag_structure, dict) and 'nodes' in dag_structure and 'edges' in dag_structure:
            return edge_loads

        # Use cost models to get network costs for all DAG edges
        net_costs = cost_models.net_estimate(dag_structure, nodes)
        
        # Create node lookup for efficient access
        node_map = {self._node_id(node): node for node in nodes}
        
        # Analyze loads for each edge
        for task_id, task_info in dag_structure.items():
            for dependency in task_info.get('dependencies', []):
                dep_task_id = dependency['task_id']
                data_size = dependency['data_size']
                
                # Find actual source and destination nodes from task assignments
                source_node_id = self._get_task_node_assignment(dep_task_id, nodes)
                dest_node_id = self._get_task_node_assignment(task_id, nodes)
                
                if source_node_id and dest_node_id and source_node_id != dest_node_id:
                    edge_key = (source_node_id, dest_node_id)
                    
                    # Get transfer time from pre-computed network costs
                    transfer_time = net_costs.get(edge_key, 0.0)
                    
                    # Calculate load percentage based on network capacity
                    src_node = node_map.get(source_node_id)
                    dst_node = node_map.get(dest_node_id)
                    
                    # Convert bandwidth from Mbps to theoretical max load
                    theoretical_max_time = 1.0  # 1 second as baseline
                    load_percentage = min(1.0, transfer_time / theoretical_max_time)
                    
                    edge_loads.append(EdgeLoad(
                        source_node=source_node_id,
                        dest_node=dest_node_id,
                        data_size=data_size,
                        load_percentage=load_percentage
                    ))
        
        return edge_loads
    
    def _get_task_node_assignment(self, task_id: str, nodes: List) -> Optional[str]:
        """Get the node assignment for a task (simplified - can be enhanced with actual scheduler state)"""
        # For now, hash-based assignment for consistency
        # In practice, this should query the actual task-to-node mapping from scheduler
        if nodes:
            node_idx = hash(task_id) % len(nodes)
            return self._node_id(nodes[node_idx])
        return None

    def _determine_bottleneck_type(self, compute_loads: Dict[str, float],
                                 edge_loads: List[EdgeLoad], nodes: List) -> Tuple:
        """Determine the type and severity of bottleneck"""
        # Check compute bottlenecks
        overloaded_compute_nodes = [node_id for node_id, load in compute_loads.items()
                                  if load > self.compute_threshold]
        compute_severity = max(compute_loads.values()) if compute_loads else 0.0

        # Check transfer bottlenecks
        overloaded_edges = [edge for edge in edge_loads if edge.load_percentage > self.transfer_threshold]
        transfer_severity = max([edge.load_percentage for edge in edge_loads]) if edge_loads else 0.0

        # Determine bottleneck type
        if compute_severity > self.compute_threshold and compute_severity >= transfer_severity:
            return "compute", compute_severity, overloaded_compute_nodes, []
        elif transfer_severity > self.transfer_threshold:
            critical_edges = [(edge.source_node, edge.dest_node) for edge in overloaded_edges]
            return "transfer", transfer_severity, [], critical_edges
        else:
            return None, 0.0, [], []

    def get_bottleneck_weights(self, bottleneck_analysis: BottleneckAnalysis) -> Dict[str, float]:
        """
        Get weight factors based on bottleneck analysis using w_comp and w_net
        """
        if not bottleneck_analysis.has_bottleneck or bottleneck_analysis.bottleneck_type == "none":
            # No bottleneck: balanced weights, signify no bottleneck to scheduler
            return {
                'w_comp': 0.5,      # Balanced weight on compute
                'w_net': 0.5,       # Balanced weight on network transfer  
                'has_bottleneck': False,
                'bottleneck_type': None,
                'regime': 'balanced'
            }
        elif bottleneck_analysis.bottleneck_type == "compute":
            # Compute bottleneck: prioritize compute considerations
            return {
                'w_comp': 0.8,      # High weight on compute
                'w_net': 0.2,       # Lower weight on transfer
                'has_bottleneck': True,
                'bottleneck_type': 'compute',
                'regime': 'compute-heavy'
            }
        elif bottleneck_analysis.bottleneck_type == "transfer":
            # Network bottleneck: prioritize network considerations
            return {
                'w_comp': 0.2,      # Lower weight on compute
                'w_net': 0.8,       # High weight on transfer
                'has_bottleneck': True,
                'bottleneck_type': 'transfer', 
                'regime': 'data-heavy'
            }
        else:
            # Unknown bottleneck type: balanced weights
            return {
                'w_comp': 0.5,
                'w_net': 0.5,
                'has_bottleneck': True,
                'bottleneck_type': bottleneck_analysis.bottleneck_type,
                'regime': 'unknown'
            }

    # ===== MOCK FUNCTIONS FOR TESTING =====

    def mock_identify_bottlenecks(self, tasks: List, nodes: List, dag_structure: Dict,
                                cost_models) -> BottleneckAnalysis:
        """
        Mock function: Simple bottleneck identification for testing
        """
        # Random bottleneck detection for testing
        has_bottleneck = random.choice([True, False])

        if has_bottleneck:
            bottleneck_type = random.choice(["compute", "transfer"])
            severity = random.uniform(0.8, 0.95)

            if bottleneck_type == "compute":
                critical_nodes = random.sample([self._node_id(node) for node in nodes], k=min(2, len(nodes)))
                critical_edges = []
            else:
                critical_nodes = []
                # Create some random critical edges
                critical_edges = []
                for _ in range(min(3, len(nodes))):
                    src = random.choice([self._node_id(node) for node in nodes])
                    dst = random.choice([self._node_id(node) for node in nodes if self._node_id(node) != src])
                    critical_edges.append((src, dst))
        else:
            bottleneck_type = None
            severity = 0.0
            critical_nodes = []
            critical_edges = []

        return BottleneckAnalysis(
            has_bottleneck=has_bottleneck,
            bottleneck_type=bottleneck_type,
            bottleneck_severity=severity,
            critical_nodes=critical_nodes,
            critical_edges=critical_edges
        )

    def mock_get_bottleneck_weights(self, bottleneck_analysis: BottleneckAnalysis) -> Dict[str, float]:
        """
        Mock function: Simple weight generation for testing using w_comp and w_net
        """
        if not bottleneck_analysis.has_bottleneck:
            return {
                'w_comp': 0.5, 'w_net': 0.5,
                'has_bottleneck': False, 'bottleneck_type': None, 'regime': 'balanced'
            }
        elif bottleneck_analysis.bottleneck_type == "compute":
            return {
                'w_comp': 0.7, 'w_net': 0.3,
                'has_bottleneck': True, 'bottleneck_type': 'compute', 'regime': 'compute-heavy'
            }
        else:  # transfer
            return {
                'w_comp': 0.3, 'w_net': 0.7,
                'has_bottleneck': True, 'bottleneck_type': 'transfer', 'regime': 'data-heavy'
            }
