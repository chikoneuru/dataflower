"""
Ditto Placement Logic

Handles placement of grouped stages on available servers with resource constraints.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


class DittoPlacement:
    """Handles placement of stage groups on servers."""
    
    def __init__(self) -> None:
        pass
    
    def place_stages(
        self, 
        groups: List[List[str]], 
        servers: List[str], 
        resources: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Place stage groups on servers.
        
        Args:
            groups: List of stage groups, each group is a list of stage IDs
            servers: List of available server IDs
            resources: Server resource information
            
        Returns:
            Mapping from stage ID to server ID
        """
        placement: Dict[str, str] = {}
        
        if not servers:
            # No servers available, assign to a default
            default_server = "server-0"
            for group in groups:
                for stage_id in group:
                    placement[stage_id] = default_server
            return placement
        
        # Initialize mutable resource view: use provided 'capacity' per server when available
        remaining_capacity: Dict[str, int] = {}
        for server in servers:
            server_info = resources.get(server, {}) if isinstance(resources, dict) else {}
            remaining_capacity[server] = int(server_info.get('capacity', 10))

        # Greedy placement: for each group, pick best server by score, then update resources
        for group in groups:
            # Compute score per server; higher is better
            best_server: str | None = None
            best_score: float = float('-inf')
            for server in servers:
                score = self._score_server(group, server, resources, remaining_capacity)
                if score > best_score:
                    best_score = score
                    best_server = server

            # Fallback if scoring fails
            if best_server is None:
                best_server = servers[0]

            # Assign all stages in the group to the chosen server
            for stage_id in group:
                placement[stage_id] = best_server

            # Update mutable resources after placement
            self._update_resources_after_placement(group, best_server, resources, remaining_capacity)
            
        return placement
    
    def _evaluate_placement_cost(
        self,
        placement: Dict[str, str],
        groups: List[List[str]],
        servers: List[str],
        resources: Dict[str, Any]
    ) -> float:
        """
        Evaluate the cost of a placement considering:
        - Resource utilization
        - Network communication between stages
        - Load balancing
        """
        if not placement:
            return float('inf')
            
        # Calculate server load
        server_load = {server: 0 for server in servers}
        for stage_id, server in placement.items():
            server_load[server] += 1
            
        # Load balancing cost (variance in server load)
        loads = list(server_load.values())
        if loads:
            avg_load = sum(loads) / len(loads)
            load_variance = sum((load - avg_load) ** 2 for load in loads) / len(loads)
        else:
            load_variance = 0
            
        # Communication cost (simplified)
        communication_cost = 0
        for group in groups:
            if len(group) > 1:
                # Stages in same group should ideally be on same server
                group_servers = set(placement.get(stage_id) for stage_id in group)
                if len(group_servers) > 1:
                    communication_cost += len(group_servers) - 1
                    
        total_cost = load_variance * 0.5 + communication_cost * 0.5
        return total_cost

    def _score_server(
        self,
        group: List[str],
        server: str,
        resources: Dict[str, Any],
        remaining_capacity: Dict[str, int]
    ) -> float:
        """Score how good it is to place a group on a server.

        Current heuristic:
          - Prefer servers with more remaining_capacity
          - Slightly penalize groups that exceed apparent capacity
        """
        rem = remaining_capacity.get(server, 0)
        # Basic score: remaining capacity
        score = float(rem)
        # Penalize if this placement would exceed capacity
        if rem <= 0:
            score -= 1000.0
        return score

    def _update_resources_after_placement(
        self,
        group: List[str],
        server: str,
        resources: Dict[str, Any],
        remaining_capacity: Dict[str, int]
    ) -> None:
        """Update mutable resource state after placing a group on a server.

        Heuristic: each group consumes 1 unit of server 'capacity'.
        """
        remaining_capacity[server] = max(0, remaining_capacity.get(server, 0) - 1)
