"""
DoP Ratio Computing

Computes stage-wise degrees of parallelism (DoP) ratios across a DAG based on
time prediction models and an objective (minimize JCT or cost).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Tuple

from .predictor import ExecutionTimePredictor, DoP, Placement


Objective = str  # "min_jct" | "min_cost"


class DoPRatioComputer:
    def __init__(self, predictor: ExecutionTimePredictor) -> None:
        self.predictor = predictor
        self.logger = logging.getLogger('scheduler.ditto.dop_ratio')

    def compute(
        self,
        dag: Dict[str, Any],
        models_by_stage: Dict[str, Any],
        dop_candidates: Iterable[DoP],
        placement_by_stage: Dict[str, Placement],
        objective: Objective = "min_jct",
    ) -> Dict[str, DoP]:
        """
        Compute a DoP assignment for each stage.
        dag: expects keys {"nodes": [{"id": stage_id}, ...], "edges": [(u, v), ...]}
        """
        ratios: Dict[str, DoP] = {}
        
        # Debug: Check DAG structure
        if "workflow" in dag:
            workflow_nodes = dag["workflow"].get("nodes", [])
        else:
            workflow_nodes = dag.get("nodes", [])
        
        # Extract nodes from the correct location
        if "workflow" in dag:
            nodes = [n for n in (dag["workflow"].get("nodes") or []) if isinstance(n, dict) and n.get("id")]
        else:
            nodes = [n for n in (dag.get("nodes") or []) if isinstance(n, dict) and n.get("id")]
        self.logger.debug(f"   Available models: {list(models_by_stage.keys())}")
        self.logger.debug(f"   Placement by stage: {placement_by_stage}")
        
        # Balanced parallelism approach: consider DAG structure and aim for balanced load
        self.logger.debug(f"   Computing balanced DoP ratios for {len(nodes)} stages")
        
        # Step 1: Identify critical path and stage dependencies
        critical_stages = self._critical_path_nodes(dag)
        self.logger.debug(f"   Critical path stages: {critical_stages}")
        
        # Step 2: Compute balanced DoP ratios
        for node in nodes:
            stage_id = node["id"]
            placement = placement_by_stage.get(stage_id)
            
            # Determine base DoP based on stage characteristics
            base_dop = self._compute_base_dop(stage_id, placement, dop_candidates)
            
            # Adjust based on criticality and dependencies
            final_dop = self._adjust_dop_for_balance(
                stage_id, base_dop, critical_stages, ratios, dop_candidates
            )
            
            ratios[stage_id] = final_dop
            self.logger.debug(f"   ✅ {stage_id}: DoP = {final_dop} (base={base_dop}, critical={stage_id in critical_stages})")
        
        if objective == "min_cost":
            self._adjust_for_cost(ratios, dag)
        
        self.logger.debug(f"🔍 DoP Computer: Final balanced ratios = {ratios}")
        return ratios

    def _compute_base_dop(self, stage_id: str, placement: Placement, dop_candidates: Iterable[DoP]) -> DoP:
        """
        Compute base DoP for a stage based on its characteristics and placement.
        Uses a balanced approach rather than just minimizing execution time.
        """
        # Get execution times for different DoP values
        times = []
        for dop in dop_candidates:
            time_ms = self.predictor.predict_time(stage_id, dop, placement)
            times.append((dop, time_ms))
        
        # Find the "sweet spot" - DoP with good speedup but reasonable overhead
        best_dop = 1
        best_efficiency = 0.0
        
        for dop, time_ms in times:
            # Efficiency = speedup / overhead_penalty
            # Speedup = base_time / current_time
            # Overhead penalty = dop (more workers = more coordination)
            base_time = times[0][1]  # DoP=1 time
            speedup = base_time / time_ms if time_ms > 0 else 1.0
            efficiency = speedup / (1.0 + (dop - 1) * 0.1)  # Diminishing returns
            
            if efficiency > best_efficiency:
                best_efficiency = efficiency
                best_dop = dop
        
        return best_dop

    def _adjust_dop_for_balance(self, stage_id: str, base_dop: DoP, critical_stages: List[str], 
                              existing_ratios: Dict[str, DoP], dop_candidates: Iterable[DoP]) -> DoP:
        """
        Adjust DoP to maintain balance across the DAG.
        Critical stages get priority, but we avoid extreme imbalances.
        """
        # Critical stages can use higher DoP
        if stage_id in critical_stages:
            # Allow up to 1.5x base DoP for critical stages
            max_dop = min(int(base_dop * 1.5), max(dop_candidates))
            return min(max_dop, base_dop + 2)  # Cap the increase
        
        # Non-critical stages: use base DoP or slightly lower
        # Avoid having too many stages at maximum DoP
        max_dop_count = sum(1 for dop in existing_ratios.values() if dop >= max(dop_candidates) - 1)
        if max_dop_count >= 2:  # If already have 2+ stages at high DoP
            return max(1, base_dop - 1)  # Reduce by 1
        
        return base_dop

    def _adjust_for_cost(self, ratios: Dict[str, DoP], dag: Dict[str, Any]) -> None:
        # Placeholder: gently reduce DoP for non-critical stages to save cost
        critical = self._critical_path_nodes(dag)
        for stage_id in list(ratios.keys()):
            if stage_id not in critical and ratios[stage_id] > 1:
                ratios[stage_id] = max(1, ratios[stage_id] - 1)

    def _critical_path_nodes(self, dag: Dict[str, Any]) -> List[str]:
        # Very rough critical path approximation by topological order: last layer nodes
        # Extract nodes from the correct location
        if "workflow" in dag:
            nodes = [n["id"] for n in (dag["workflow"].get("nodes") or []) if isinstance(n, dict) and n.get("id")]
            edges = dag["workflow"].get("edges", [])
        else:
            nodes = [n["id"] for n in (dag.get("nodes") or []) if isinstance(n, dict) and n.get("id")]
            edges = dag.get("edges", [])
        
        # Convert edges to simple (u, v) format for critical path analysis
        edge_pairs = []
        for e in edges:
            if isinstance(e, dict):
                # Handle different edge types
                if e.get("type") == "sequential":
                    u, v = e.get("from"), e.get("to")
                    if u and v:
                        edge_pairs.append((u, v))
                elif e.get("type") == "parallel":
                    from_node = e.get("from")
                    to_nodes = e.get("to", [])
                    if isinstance(to_nodes, list):
                        for to_node in to_nodes:
                            if from_node and to_node:
                                edge_pairs.append((from_node, to_node))
                # Add other edge types as needed
            elif isinstance(e, (tuple, list)) and len(e) == 2:
                edge_pairs.append((e[0], e[1]))
        
        out_deg = {n: 0 for n in nodes}
        for u, v in edge_pairs:
            if u in out_deg:
                out_deg[u] += 1
        max_out = max(out_deg.values()) if out_deg else 0
        return [n for n, d in out_deg.items() if d == max_out]


