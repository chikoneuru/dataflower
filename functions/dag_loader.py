import os
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


# -------------------------------
# Public API
# -------------------------------

def load_workflow(path: str) -> Dict[str, Any]:
    """
    Load a workflow DAG/hierarchical pipeline from a YAML file and normalize it
    to a common internal representation.

    Returns dict:
      {
        "name": str,
        "version": str|None,
        "inputs": Dict[str, str],              # name -> type
        "tasks": Dict[str, Dict],              # id -> {type, module, inputs, outputs, extra}
        "edges": List[Dict],                   # [{"from": a, "to": b, "condition": str|None}]
        "meta": Dict[str, Any],                # raw fields
      }
    """
    if yaml is None:
        raise RuntimeError("PyYAML is required. Install with: pip install pyyaml")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    # Detect format
    if "pipeline" in data:
        return _parse_hierarchical(data, base_dir=os.path.dirname(path))
    if "workflow" in data and isinstance(data["workflow"], dict) and "nodes" in data["workflow"]:
        return _parse_graph(data, base_dir=os.path.dirname(path))

    # Fallback: attempt to infer
    if "nodes" in data and "edges" in data:
        return _parse_graph({"workflow": data}, base_dir=os.path.dirname(path))  # nest to reuse
    raise ValueError("Unrecognized workflow schema. Expect keys: 'pipeline' (hierarchical) or 'workflow.nodes/edges' (graph).")


# -------------------------------
# Hierarchical schema parsing
# -------------------------------

def _parse_hierarchical(doc: Dict[str, Any], base_dir: str) -> Dict[str, Any]:
    """
    Parse simplified hierarchical schema with keys like:
      - version, name, defaults, inputs, pipeline
    """
    version = str(doc.get("version", ""))
    name = str(doc.get("name", "workflow"))

    defaults = dict(doc.get("defaults", {}))
    module_resolution = str(defaults.get("module_resolution", "steps/{task}/main.py"))
    outputs_are_implicit = bool(defaults.get("outputs_are_implicit", True))

    # inputs: {name: type} | or {name: {type: ...}}
    inputs: Dict[str, str] = {}
    if isinstance(doc.get("inputs"), dict):
        for k, v in doc["inputs"].items():
            inputs[k] = v if isinstance(v, str) else str(v.get("type", "any"))
    elif isinstance(doc.get("inputs"), list):
        for item in doc["inputs"]:
            if isinstance(item, dict):
                k = list(item.keys())[0]
                v = item[k]
                if isinstance(v, str):
                    inputs[k] = v
                elif isinstance(v, dict):
                    inputs[k] = str(v.get("type", "any"))

    tasks: Dict[str, Dict[str, Any]] = {}
    edges: List[Dict[str, Any]] = []

    # State to resolve $prev and names inside nested blocks
    ctx = {
        "last_step": None,  # track last linear step id
        "fork_scope": [],   # stack of parallel/foreach scopes
        "foreach": None,    # current foreach symbol scope
    }

    def _ensure_task(task_id: str, node: Dict[str, Any]) -> str:
        if not isinstance(node, dict):
            node = {}
        # Resolve module
        module = node.get("module")
        if not module:
            module = module_resolution.format(task=task_id)
        # Inputs mapping
        ins = node.get("in", {}) or {}
        # Outputs mapping
        outs = node.get("out")
        outputs: List[str] = []
        if outs is None:
            outputs = []
        elif isinstance(outs, list):
            outputs = [str(x) for x in outs]
        elif isinstance(outs, str):
            # implicit single output
            outputs = [outs] if outputs_are_implicit else [outs]
        elif isinstance(outs, dict):
            # explicit mapping name: name or name: expr (we only keep keys)
            outputs = list(outs.keys())
        else:
            outputs = []

        tasks[task_id] = {
            "id": task_id,
            "type": "task",
            "module": module,
            "inputs": dict(ins),
            "outputs": outputs,
            "extra": {k: v for k, v in node.items() if k not in ("in", "out", "module")},
        }
        return task_id

    def _add_edge(src: str, dst: str, condition: Optional[str] = None):
        edges.append({"from": src, "to": dst, "condition": condition})

    def _seq(prev_id: Optional[str], node_entry: Any) -> Optional[str]:
        """
        Consume one entry in a linear sequence; return last task id in this segment.
        """
        # Form 5: explicit end marker
        if isinstance(node_entry, dict) and "end" in node_entry:
            end_val = node_entry.get("end")
            if end_val is True:
                end_id = _alloc_virtual(f"end_{len(edges)}")
                tasks[end_id] = {"id": end_id, "type": "end", "module": None, "inputs": {}, "outputs": [], "extra": {}}
                if prev_id:
                    _add_edge(prev_id, end_id, None)
                return end_id
            return prev_id
        
        # Form 1: {task_name: {in:, out:, module:, ...}}
        if isinstance(node_entry, dict) and len(node_entry) == 1 and "parallel" not in node_entry and "switch" not in node_entry and "foreach" not in node_entry:
            task_id = list(node_entry.keys())[0]
            spec = node_entry[task_id] or {}
            _ensure_task(task_id, spec)
            if prev_id:
                _add_edge(prev_id, task_id, None)
            return task_id

        # Form 2: parallel block: {"parallel": [ <task entries> ]}
        if isinstance(node_entry, dict) and "parallel" in node_entry:
            plist = node_entry["parallel"] or []
            last_ids: List[str] = []
            # fan-out from prev_id to each child
            for sub in plist:
                if not isinstance(sub, dict) or len(sub) != 1:
                    continue
                tid = list(sub.keys())[0]
                spec = sub[tid] or {}
                _ensure_task(tid, spec)
                if prev_id:
                    _add_edge(prev_id, tid, None)
                last_ids.append(tid)
            # the sequence continues after parallel; next item should depend on all?
            # We don't synthesize a join node; consumer steps will reference whichever outputs they need.
            # For linear flow, we return last item as None; next linear step must reference via explicit inputs.
            return None

        # Form 3: switch block: {"switch": <expr-or-when any>, "then": [...], "else": [...]}
        if isinstance(node_entry, dict) and ("switch" in node_entry or "when_any" in node_entry or "when_all" in node_entry):
            # collapse short forms
            if "switch" in node_entry:
                cond_expr = str(node_entry["switch"])
                then_list = node_entry.get("then", []) or []
                else_list = node_entry.get("else", []) or []
                # Model switch as a virtual node for edges clarity
                sw_id = _alloc_virtual(f"switch_{len(edges)}")
                tasks[sw_id] = {"id": sw_id, "type": "switch", "module": None, "inputs": {}, "outputs": [], "extra": {"expr": cond_expr}}
                if prev_id:
                    _add_edge(prev_id, sw_id, None)
                # then branch
                last_then = _walk_sequence(sw_id, then_list)
                # else branch
                last_else = _walk_sequence(sw_id, else_list)
                # Return None; subsequent steps should link explicitly
                return None

            # when_any/when_all forms (treated similarly)
            if "when_any" in node_entry or "when_all" in node_entry:
                key = "when_any" if "when_any" in node_entry else "when_all"
                cond_expr = f"{key}:{node_entry.get(key)}"
                then_list = node_entry.get("then", []) or []
                else_list = node_entry.get("else", []) or []
                sw_id = _alloc_virtual(f"{key}_{len(edges)}")
                tasks[sw_id] = {"id": sw_id, "type": "switch", "module": None, "inputs": {}, "outputs": [], "extra": {"expr": cond_expr}}
                if prev_id:
                    _add_edge(prev_id, sw_id, None)
                _walk_sequence(sw_id, then_list)
                _walk_sequence(sw_id, else_list)
                return None

        # Form 4: foreach block
        if isinstance(node_entry, dict) and ("foreach" in node_entry or "over" in node_entry):
            over_expr = node_entry.get("foreach") or node_entry.get("over")
            as_name = node_entry.get("as", "item")
            body = node_entry.get("body", []) or []
            join = node_entry.get("join", []) or []

            fe_id = _alloc_virtual(f"foreach_{len(edges)}")
            tasks[fe_id] = {"id": fe_id, "type": "foreach", "module": None, "inputs": {}, "outputs": [], "extra": {"over": str(over_expr), "as": as_name}}
            if prev_id:
                _add_edge(prev_id, fe_id, None)
            # body sequence (conceptual)
            _walk_sequence(fe_id, body)
            # join sequence
            _walk_sequence(fe_id, join)
            return None

        # Unknown form → ignore
        return prev_id

    def _walk_sequence(start_from: Optional[str], seq: List[Any]) -> Optional[str]:
        last = start_from
        for item in seq:
            last = _seq(last, item)
        return last

    # Walk pipeline
    _walk_sequence(None, list(doc.get("pipeline", [])))

    return {
        "name": name,
        "version": version,
        "inputs": inputs,
        "tasks": tasks,
        "edges": edges,
        "meta": {"schema": "hierarchical", "defaults": defaults},
    }


# -------------------------------
# Graph schema parsing
# -------------------------------

def _parse_graph(doc: Dict[str, Any], base_dir: str) -> Dict[str, Any]:
    """
    Parse graph-style schema with keys: workflow: {name, inputs, nodes, edges}
    """
    wf = dict(doc.get("workflow", {}))
    version = str(doc.get("version", wf.get("version", "")))
    name = str(wf.get("name", "workflow"))

    # inputs may be either list of {name/type} or dict
    inputs: Dict[str, str] = {}
    raw_inputs = wf.get("inputs") or wf.get("global_inputs") or {}
    if isinstance(raw_inputs, dict):
        for k, v in raw_inputs.items():
            inputs[k] = v if isinstance(v, str) else str(v.get("type", "any"))
    elif isinstance(raw_inputs, list):
        for item in raw_inputs:
            if isinstance(item, dict):
                k = list(item.keys())[0]
                v = item[k]
                if isinstance(v, str):
                    inputs[k] = v
                else:
                    inputs[k] = str(v.get("type", "any"))

    tasks: Dict[str, Dict[str, Any]] = {}
    edges: List[Dict[str, Any]] = []

    for node in wf.get("nodes", []):
        nid = str(node.get("id"))
        ntype = str(node.get("type", "task"))
        module = node.get("module")
        if module is None and ntype == "task":
            # best-effort inference: use id as task name
            module = f"steps/{nid}/main.py"
        ins = node.get("inputs", node.get("in", {})) or {}
        outs = node.get("outputs", node.get("out"))  # can be list or dict
        outputs: List[str] = []
        if isinstance(outs, list):
            outputs = [str(x) for x in outs]
        elif isinstance(outs, dict):
            outputs = list(outs.keys())
        tasks[nid] = {
            "id": nid,
            "type": ntype,
            "module": module,
            "inputs": dict(ins),
            "outputs": outputs,
            "extra": {k: v for k, v in node.items() if k not in ("id", "type", "module", "inputs", "in", "outputs", "out")},
        }

    for e in wf.get("edges", []):
        src = e.get("from")
        dsts = e.get("to")
        cond = e.get("condition")
        mapping = e.get("mapping")  # unused here, preserved in 'extra'
        if isinstance(dsts, list):
            for d in dsts:
                edges.append({"from": src, "to": d, "condition": cond, "extra": {"mapping": mapping}})
        else:
            edges.append({"from": src, "to": dsts, "condition": cond, "extra": {"mapping": mapping}})

    return {
        "name": name,
        "version": version,
        "inputs": inputs,
        "tasks": tasks,
        "edges": edges,
        "meta": {"schema": "graph"},
    }


# -------------------------------
# Utilities
# -------------------------------

_vcounter = 0
def _alloc_virtual(prefix: str) -> str:
    global _vcounter
    _vcounter += 1
    return f"__{prefix}_{_vcounter}__"


def topo_order(tasks: Dict[str, Dict[str, Any]], edges: List[Dict[str, Any]]) -> List[str]:
    """
    Compute a topological order of tasks ignoring switch/foreach semantics (best-effort).
    """
    indeg = {tid: 0 for tid in tasks.keys()}
    adj: Dict[str, List[str]] = {tid: [] for tid in tasks.keys()}

    for e in edges:
        src, dst = e.get("from"), e.get("to")
        if src in tasks and dst in tasks:
            adj[src].append(dst)
            indeg[dst] += 1

    q = [tid for tid, d in indeg.items() if d == 0]
    order: List[str] = []
    while q:
        u = q.pop(0)
        order.append(u)
        for v in adj.get(u, []):
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)
    # Append any residuals (cycles/virtuals)
    for tid in tasks:
        if tid not in order:
            order.append(tid)
    return order


def ready_tasks(tasks: Dict[str, Dict[str, Any]], edges: List[Dict[str, Any]], completed: List[str]) -> List[str]:
    """
    Return tasks whose predecessors are all in 'completed'.
    """
    preds: Dict[str, List[str]] = {tid: [] for tid in tasks}
    for e in edges:
        src, dst = e.get("from"), e.get("to")
        if src in tasks and dst in tasks:
            preds[dst].append(src)

    ready: List[str] = []
    completed_set = set(completed)
    for tid in tasks:
        if tid in completed_set:
            continue
        if all(p in completed_set for p in preds.get(tid, [])):
            ready.append(tid)
    return ready


def summarize(workflow: Dict[str, Any]) -> str:
    """
    Human-readable summary string.
    """
    lines = []
    lines.append(f"Workflow: {workflow.get('name')} (version: {workflow.get('version')})")
    lines.append(f"Inputs: {', '.join([f'{k}:{v}' for k,v in workflow.get('inputs', {}).items()]) or '-'}")
    lines.append(f"Tasks: {len(workflow.get('tasks', {}))}, Edges: {len(workflow.get('edges', []))}")
    lines.append("Topological order (best-effort):")
    order = topo_order(workflow["tasks"], workflow["edges"])
    lines.append("  " + " -> ".join(order))
    return "\n".join(lines)

if __name__ == "__main__":
    print(load_workflow("functions/recognizer/recognizer_dag.yaml"))
