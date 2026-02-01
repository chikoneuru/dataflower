from typing import Any, Dict, List, Optional, Tuple
import random


class RoutingLP:
    """LP-based routing approximation.

    Implements a simplified multi-stage routing based on feasible paths and
    weighted random selection using pseudo LP outputs.
    """

    def __init__(self) -> None:
        pass

    # -------- LP plan computation (per workflow) -------------------------------
    def compute_workflow_lp(
        self,
        workflow_dag: Dict[str, Any],
        placement: Dict[str, str],
        nodes_capacity: Dict[str, float],
        durations: Dict[str, float],
        branch_freq: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Dict[str, float]]:
        """Return per-function node weights (proxy for LP solution oi per path).

        Output format: { function_name: { node_id: weight, ... }, ... }
        """
        plan: Dict[str, Dict[str, float]] = {}
        tasks = [n["id"] for n in (workflow_dag.get("workflow", {}).get("nodes", [])) if n.get("type") == "task"]
        for fn in tasks:
            # Feasible nodes: prefer placement node; allow any with capacity > 0
            weights: Dict[str, float] = {}
            for node_id, cap in nodes_capacity.items():
                base = 0.0
                if node_id == placement.get(fn):
                    base += 1.0  # prefer placed node
                # Capacity and duration effect: higher capacity/lower duration preferred
                dur = max(1e-6, float(durations.get(fn, 1.0)))
                base += float(cap) / (1.0 + dur)
                if base > 0:
                    weights[node_id] = base
            # Normalize
            s = sum(weights.values()) or 1.0
            for k in list(weights.keys()):
                weights[k] = weights[k] / s
            plan[fn] = weights
        return plan

    def get_feasible_nodes(self, function_name: str, lp_plan: Dict[str, Dict[str, float]]) -> List[str]:
        return [n for n, w in (lp_plan.get(function_name) or {}).items() if w > 0]

    def select_node_from_lp(
        self,
        function_name: str,
        candidate_nodes: List[str],
        lp_plan: Dict[str, Dict[str, float]],
        deterministic: bool = True,
    ) -> Tuple[Optional[str], Dict[str, Any]]:
        weights_map = lp_plan.get(function_name) or {}
        if not candidate_nodes:
            return None, {"strategy": "none", "candidates": 0}
        weights = [max(0.0, float(weights_map.get(n, 0.0))) for n in candidate_nodes]
        if sum(weights) == 0:
            # Fallback to equal weights
            weights = [1.0 for _ in candidate_nodes]
        if deterministic:
            idx = max(range(len(candidate_nodes)), key=lambda i: weights[i])
        else:
            # Weighted random
            total = sum(weights)
            r = random.random() * total
            acc = 0.0
            idx = 0
            for i, w in enumerate(weights):
                acc += w
                if r <= acc:
                    idx = i
                    break
        node_sel = candidate_nodes[idx]
        return node_sel, {"strategy": "lp-plan", "candidates": len(candidate_nodes), "weights": {n: w for n, w in zip(candidate_nodes, weights)}}

    # Backward-compatible simple route API
    def route(
        self,
        function_name: str,
        candidate_nodes: List[str],
        context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[str], Dict[str, Any]]:
        lp_plan = (context or {}).get("lp_plan") or {}
        return self.select_node_from_lp(function_name, candidate_nodes, lp_plan, deterministic=True)


