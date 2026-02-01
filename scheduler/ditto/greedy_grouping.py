"""
Greedy Grouping

Groups consecutive stages when shuffle size exceeds a threshold to reduce
shuffle traffic and improve data locality.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Sequence, Tuple


class GreedyGrouper:
    def __init__(self, shuffle_threshold_bytes: int = 1 * 1024 * 1024) -> None:
        self.shuffle_threshold_bytes = shuffle_threshold_bytes
        self.logger = logging.getLogger('scheduler.ditto.greedy_grouping')

    def group(self, dag: Dict[str, Any]) -> List[List[str]]:
        """
        Return a list of groups; each group is a list of stage ids.
        dag: expects keys {"nodes": [{"id": ...}], "edges": [{type, from, to, shuffle_bytes}?]}
        """
        nodes = [n for n in (dag.get("nodes") or []) if isinstance(n, dict) and n.get("id")]
        edges = (dag.get("edges") or [])
        visited = set()
        groups: List[List[str]] = []

        # Extract all edges and their shuffle_bytes values
        edge_shuffle_map = self._extract_edge_shuffle_bytes(edges)
        
        self.logger.debug(f"🔍 Ditto: Extracted shuffle_bytes from {len(edge_shuffle_map)} edges")
        for edge_key, shuffle_mb in edge_shuffle_map.items():
            shuffle_bytes = int(shuffle_mb * 1024 * 1024)  # Convert MB to bytes
            self.logger.debug(f"   {edge_key}: {shuffle_mb} MB ({shuffle_bytes:,} bytes)")
            if shuffle_bytes > self.shuffle_threshold_bytes:
                self.logger.debug(f"   ⚠️  Large shuffle detected: {shuffle_bytes:,} bytes > {self.shuffle_threshold_bytes:,} threshold")

        # Group stages based on shuffle_bytes
        for edge_key, shuffle_mb in edge_shuffle_map.items():
            shuffle_bytes = int(shuffle_mb * 1024 * 1024)  # Convert MB to bytes
            
            # Parse edge key to get source and target
            if " -> " in edge_key:
                u, v = edge_key.split(" -> ", 1)
            else:
                continue
                
            if not u or not v or u == v:
                continue

            # Group stages if shuffle_bytes exceeds threshold
            if shuffle_bytes > self.shuffle_threshold_bytes and u not in visited and v not in visited:
                groups.append([u, v])
                visited.add(u)
                visited.add(v)
                self.logger.debug(f"   📦 Grouped {u} and {v} due to large shuffle: {shuffle_bytes:,} bytes")

        # Add remaining singleton stages
        for n in nodes:
            sid = n["id"]
            if sid not in visited:
                groups.append([sid])

        return groups

    def _extract_edge_shuffle_bytes(self, edges: List[Dict[str, Any]]) -> Dict[str, float]:
        """
        Extract shuffle_bytes from DAG edges in various formats.
        Returns a mapping of "source -> target" to shuffle_bytes in MB.
        """
        edge_shuffle_map = {}
        
        for edge in edges:
            if not isinstance(edge, dict):
                continue
                
            edge_type = edge.get("type", "")
            shuffle_bytes_mb = edge.get("shuffle_bytes", 0.0)
            
            if edge_type == "parallel":
                # Parallel edge: from -> [to1, to2, ...]
                from_node = edge.get("from")
                to_nodes = edge.get("to", [])
                
                if isinstance(to_nodes, list):
                    for to_node in to_nodes:
                        if from_node and to_node:
                            edge_key = f"{from_node} -> {to_node}"
                            edge_shuffle_map[edge_key] = shuffle_bytes_mb
                elif to_nodes:
                    edge_key = f"{from_node} -> {to_nodes}"
                    edge_shuffle_map[edge_key] = shuffle_bytes_mb
                    
            elif edge_type == "sequential":
                # Sequential edge: from -> to
                from_node = edge.get("from")
                to_node = edge.get("to")
                
                if from_node and to_node:
                    edge_key = f"{from_node} -> {to_node}"
                    edge_shuffle_map[edge_key] = shuffle_bytes_mb
                    
            elif edge_type == "switch":
                # Switch edge: check branches
                branches = edge.get("branches", {})
                
                # Handle 'then' branch
                then_branches = branches.get("then", [])
                if isinstance(then_branches, list):
                    for branch in then_branches:
                        if isinstance(branch, dict):
                            from_nodes = branch.get("from", [])
                            to_node = branch.get("to")
                            branch_shuffle = branch.get("shuffle_bytes", shuffle_bytes_mb)
                            
                            if isinstance(from_nodes, list):
                                for from_node in from_nodes:
                                    if from_node and to_node:
                                        edge_key = f"{from_node} -> {to_node}"
                                        edge_shuffle_map[edge_key] = branch_shuffle
                            elif from_nodes and to_node:
                                edge_key = f"{from_nodes} -> {to_node}"
                                edge_shuffle_map[edge_key] = branch_shuffle
                
                # Handle 'else' branch
                else_branches = branches.get("else", [])
                if isinstance(else_branches, list):
                    for branch in else_branches:
                        if isinstance(branch, dict):
                            from_nodes = branch.get("from", [])
                            to_node = branch.get("to")
                            branch_shuffle = branch.get("shuffle_bytes", shuffle_bytes_mb)
                            
                            if isinstance(from_nodes, list):
                                for from_node in from_nodes:
                                    if from_node and to_node:
                                        edge_key = f"{from_node} -> {to_node}"
                                        edge_shuffle_map[edge_key] = branch_shuffle
                            elif from_nodes and to_node:
                                edge_key = f"{from_nodes} -> {to_node}"
                                edge_shuffle_map[edge_key] = branch_shuffle
        
        return edge_shuffle_map


