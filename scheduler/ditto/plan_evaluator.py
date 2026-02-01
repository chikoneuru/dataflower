"""
Ditto Plan Evaluation

Evaluates execution plans to compute Job Completion Time (JCT) and cost.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from .predictor import ExecutionTimePredictor, DoP, Placement


class PlanEvaluator:
    """Evaluates execution plans for JCT and cost estimation."""
    
    def __init__(self, predictor: ExecutionTimePredictor) -> None:
        self.predictor = predictor
    
    def evaluate_plan(
        self,
        dag: Dict[str, Any],
        placement: Dict[str, str],
        dop_ratios: Dict[str, DoP],
        models: Dict[str, Any],
        servers: List[str],
        resources: Dict[str, Any]
    ) -> Tuple[float, float]:
        """
        Evaluate a complete execution plan.
        
        Args:
            dag: Workflow DAG
            placement: Stage to server mapping
            dop_ratios: Stage to DoP mapping
            models: Execution time models for each stage
            servers: Available servers
            resources: Server resource information
            
        Returns:
            Tuple of (JCT_ms, cost)
        """
        # Compute critical path and estimate JCT
        jct_ms = self._compute_jct(dag, placement, dop_ratios, models)
        
        # Compute total cost
        cost = self._compute_cost(dag, placement, dop_ratios, servers, resources)
        
        return jct_ms, cost
    
    def _compute_jct(
        self,
        dag: Dict[str, Any],
        placement: Dict[str, str],
        dop_ratios: Dict[str, DoP],
        models: Dict[str, Any]
    ) -> float:
        """
        Compute Job Completion Time using critical path analysis.
        """
        # Build dependency graph - handle correct DAG structure
        if "workflow" in dag:
            nodes = [n for n in (dag["workflow"].get("nodes") or []) if isinstance(n, dict) and n.get("id")]
            edges = self._extract_edges(dag["workflow"])
        else:
            nodes = [n for n in (dag.get("nodes") or []) if isinstance(n, dict) and n.get("id")]
            edges = self._extract_edges(dag)
        
        # Compute earliest start times for each stage
        earliest_start = {}
        earliest_finish = {}
        
        # Initialize with source nodes (no dependencies)
        in_degree = {node["id"]: 0 for node in nodes}
        for u, v in edges:
            if v in in_degree:
                in_degree[v] += 1
                
        # Topological sort to compute start times
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        
        while queue:
            current = queue.pop(0)
            
            # Get execution time for current stage
            stage_server = placement.get(current, "unknown")
            dop = dop_ratios.get(current, 1)
            
            # Predict execution time
            execution_time = self._get_stage_execution_time(current, dop, stage_server, models)
            
            # Set start time
            if current not in earliest_start:
                earliest_start[current] = 0.0
                
            earliest_finish[current] = earliest_start[current] + execution_time
            
            # Update successors
            for u, v in edges:
                if u == current and v in in_degree:
                    in_degree[v] -= 1
                    if in_degree[v] == 0:
                        queue.append(v)
                        
                    # Communication delay (simplified)
                    comm_delay = 0.0
                    if placement.get(u) != placement.get(v):
                        comm_delay = 10.0  # 10ms cross-server communication
                        
                    # Update successor start time
                    if v not in earliest_start:
                        earliest_start[v] = 0.0
                    earliest_start[v] = max(
                        earliest_start[v], 
                        earliest_finish[current] + comm_delay
                    )
        
        # JCT is the maximum finish time
        if earliest_finish:
            return max(earliest_finish.values())
        return 0.0
    
    def _compute_cost(
        self,
        dag: Dict[str, Any],
        placement: Dict[str, str],
        dop_ratios: Dict[str, DoP],
        servers: List[str],
        resources: Dict[str, Any]
    ) -> float:
        """
        Compute total execution cost.
        """
        total_cost = 0.0
        
        # Cost per server (simplified model)
        server_costs = {}
        for server in servers:
            server_info = resources.get(server, {})
            # Base cost per CPU-hour (simplified)
            base_cost = server_info.get("cost_per_cpu_hour", 1.0)
            server_costs[server] = base_cost
        
        # Compute cost for each stage - handle correct DAG structure
        if "workflow" in dag:
            nodes = [n for n in (dag["workflow"].get("nodes") or []) if isinstance(n, dict) and n.get("id")]
        else:
            nodes = [n for n in (dag.get("nodes") or []) if isinstance(n, dict) and n.get("id")]
        for node in nodes:
            stage_id = node["id"]
            server = placement.get(stage_id, servers[0] if servers else "default")
            dop = dop_ratios.get(stage_id, 1)
            
            # Cost = (DoP * execution_time * server_cost) / 3600000 (convert ms to hours)
            execution_time = 1000.0  # Default 1 second execution time
            server_cost = server_costs.get(server, 1.0)
            stage_cost = (dop * execution_time * server_cost) / 3600000.0
            
            total_cost += stage_cost
            
        return total_cost
    
    def _extract_edges(self, dag: Dict[str, Any]) -> List[Tuple[str, str]]:
        """Extract edges from DAG structure."""
        edges = []
        dag_edges = dag.get("edges", [])
        
        for edge in dag_edges:
            if isinstance(edge, dict):
                # Handle different edge types
                if edge.get("type") == "sequential":
                    from_node = edge.get("from")
                    to_node = edge.get("to")
                    if from_node and to_node:
                        edges.append((from_node, to_node))
                elif edge.get("type") == "parallel":
                    from_node = edge.get("from")
                    to_nodes = edge.get("to", [])
                    if isinstance(to_nodes, list):
                        for to_node in to_nodes:
                            if from_node and to_node:
                                edges.append((from_node, to_node))
                elif edge.get("type") == "switch":
                    branches = edge.get("branches", {})
                    for branch_name, branch_edges in branches.items():
                        if isinstance(branch_edges, list):
                            for branch_edge in branch_edges:
                                if isinstance(branch_edge, dict):
                                    from_node = branch_edge.get("from")
                                    to_node = branch_edge.get("to")
                                    if from_node and to_node:
                                        edges.append((from_node, to_node))
                else:
                    # Fallback to old format
                    source = edge.get("source")
                    target = edge.get("target")
                    if source and target:
                        edges.append((source, target))
            elif isinstance(edge, (tuple, list)) and len(edge) == 2:
                edges.append((edge[0], edge[1]))
                
        return edges
    
    def _get_stage_execution_time(
        self,
        stage_id: str,
        dop: DoP,
        placement: str,
        models: Dict[str, Any]
    ) -> float:
        """
        Get execution time for a stage with given DoP and placement.
        """
        model = models.get(stage_id)
        if model:
            return model.predict(dop, placement)
        
        # Fallback: simple model based on DoP
        base_time = 1000.0  # 1 second base time
        return base_time / max(1, dop)
