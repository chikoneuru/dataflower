from typing import Any, Dict, List, Optional, Tuple


class Placement:
    """Placement engine implementing a simplified version of the FaaSPR pseudocode.

    Public API kept minimal; orchestrator uses these internally.
    """

    def __init__(self) -> None:
        pass

    # ---- Public light wrappers -------------------------------------------------
    def initial_placement(
        self,
        workflow_dag: Dict[str, Any],
        available_nodes: List[str],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        return self._horizontal_vertical_placement(
            workflow_dag=workflow_dag,
            last_P=context.get("last_P") if isinstance(context, dict) else {},
            server_list=available_nodes,
            resources=context.get("resources") if isinstance(context, dict) else {},
        )

    def update_placement(
        self,
        current_mapping: Dict[str, str],
        node_states: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        return dict(current_mapping)

    def compute_multi_workflow_placement(
        self,
        workflow_list: List[Dict[str, Any]],
        server_list: List[str],
        last_P: Dict[str, Dict[str, int]],
        resources: Dict[str, Any],
    ) -> Dict[str, Dict[str, int]]:
        # 1: Randomize list (deterministic noop here)
        P_total: Dict[str, Dict[str, int]] = {}
        for wf in workflow_list:
            P_W = self._horizontal_vertical_placement(wf, last_P, server_list, resources)
            # Update per-server resources (lightweight; not strictly enforced)
            for fn, srv in P_W.items():
                P_total.setdefault(srv, {})[fn] = P_total.get(srv, {}).get(fn, 0) + 1
        return P_total

    # ---- Core algorithms -------------------------------------------------------
    def _horizontal_vertical_placement(
        self,
        workflow_dag: Dict[str, Any],
        last_P: Dict[str, Dict[str, int]],
        server_list: List[str],
        resources: Dict[str, Any],
    ) -> Dict[str, str]:
        # Build initial vertical groups by task
        vG_plan: Dict[str, List[str]] = {}
        tasks = [n["id"] for n in (workflow_dag.get("workflow", {}).get("nodes", [])) if n.get("type") == "task"]
        for t in tasks:
            vG_plan[t] = [t]

        merge_flag = True
        while merge_flag:
            merge_flag = False
            critical_path = self._critical_path(workflow_dag, vG_plan)
            # Iterate edges along critical path in reverse
            edges = list(zip(critical_path, critical_path[1:]))[::-1]
            for u, v in edges:
                vga = self._vgroup_of(u, vG_plan)
                vgb = self._vgroup_of(v, vG_plan)
                if vga == vgb:
                    continue
                vG_temp = {k: list(val) for k, val in vG_plan.items()}
                self._merge_groups(vG_temp, vga, vgb)
                ok, placement = self._get_place_strategy(vG_temp, last_P, server_list, resources)
                if ok:
                    vG_plan = vG_temp
                    merge_flag = True
                    break

        # Final placement
        _, final_place = self._get_place_strategy(vG_plan, last_P, server_list, resources)
        return final_place

    def _critical_path(self, workflow_dag: Dict[str, Any], vG_plan: Dict[str, List[str]]) -> List[str]:
        # Topological order
        nodes = [n["id"] for n in (workflow_dag.get("workflow", {}).get("nodes", [])) if n.get("type") == "task"]
        edges = []
        for e in (workflow_dag.get("workflow", {}).get("edges", [])):
            if e.get("type") == "sequential":
                flow = e.get("flow", [])
                edges += list(zip(flow, flow[1:]))
            elif e.get("type") == "parallel":
                for to in e.get("to", []):
                    edges.append((e.get("from"), to))
            elif e.get("type") == "switch":
                for branch in (e.get("branches", {}).get("then", []) + e.get("branches", {}).get("else", [])):
                    to = branch.get("to")
                    if to != "END":
                        srcs = branch.get("from")
                        if isinstance(srcs, list):
                            for s in srcs:
                                edges.append((s, to))
                        else:
                            edges.append((srcs, to))

        succ = {u: [] for u in nodes}
        pred = {u: [] for u in nodes}
        for u, v in edges:
            if u in nodes and v in nodes:
                succ[u].append(v)
                pred[v].append(u)

        indeg = {u: len(pred[u]) for u in nodes}
        topo = [u for u in nodes if indeg[u] == 0]
        for u in topo:
            for v in succ.get(u, []):
                indeg[v] -= 1
                if indeg[v] == 0:
                    topo.append(v)

        earliest = {u: 0.0 for u in nodes}
        dur = {u: 1.0 for u in nodes}  # placeholder duration per node
        parent = {}
        for u in topo:
            for v in succ.get(u, []):
                comm = 0.1  # simplified communication cost
                total = earliest[u] + dur[u] + comm
                if total > earliest.get(v, 0.0):
                    earliest[v] = total
                    parent[v] = u

        end_node = max(nodes, key=lambda x: earliest.get(x, 0.0) + dur.get(x, 0.0)) if nodes else None
        path: List[str] = []
        cur = end_node
        while cur is not None:
            path.insert(0, cur)
            cur = parent.get(cur)
        return path

    def _get_place_strategy(
        self,
        vG_plan: Dict[str, List[str]],
        last_P: Dict[str, Dict[str, int]],
        server_list: List[str],
        resources: Dict[str, Any],
    ) -> Tuple[bool, Dict[str, str]]:
        # Initialize empty per-function placement
        P: Dict[str, str] = {}
        cap_used: Dict[str, int] = {s: 0 for s in server_list}
        cap = {s: int(resources.get(s, {}).get("capacity", 100)) for s in server_list}

        # Horizontal grouping approximation: single horizontal group per vertical group
        for vg_name, funcs in vG_plan.items():
            # Priority per server
            priorities = {s: self._get_priority(s, funcs, P, last_P) for s in server_list}
            s_best = max(priorities, key=lambda s: priorities[s]) if priorities else None
            if s_best is None or priorities[s_best] < 0:
                return True, P  # signal success per pseudocode branch (1)
            # Assign all functions in this merged group to s_best, respecting capacity
            for f in funcs:
                if cap_used[s_best] + 1 <= cap[s_best]:
                    P[f] = s_best
                    cap_used[s_best] += 1
        return True, P

    def _get_priority(
        self,
        server: str,
        group_funcs: List[str],
        P: Dict[str, str],
        last_P: Dict[str, Dict[str, int]],
    ) -> float:
        # Migration-minimizing heuristic: reward reuse of previous placements
        if server not in last_P:
            return 0.0
        score = 0.0
        prev = last_P.get(server, {})
        for f in group_funcs:
            reuse_cap = int(prev.get(f, 0))
            occ_num = sum(1 for x in P.values() if x == server and f in group_funcs)
            if reuse_cap > occ_num:
                score += 1.0  # cold-start saving
        # Simple locality/co-location boost
        score += 0.1 * sum(1 for x in P.values() if x == server)
        return score

    # ---- Helpers ---------------------------------------------------------------
    @staticmethod
    def _vgroup_of(fn: str, vG_plan: Dict[str, List[str]]) -> str:
        for g, members in vG_plan.items():
            if fn in members:
                return g
        return fn

    @staticmethod
    def _merge_groups(vG_plan: Dict[str, List[str]], g1: str, g2: str) -> None:
        if g1 == g2:
            return
        if g1 not in vG_plan or g2 not in vG_plan:
            return
        vG_plan[g1] = list(sorted(set(vG_plan[g1] + vG_plan[g2])))
        del vG_plan[g2]

