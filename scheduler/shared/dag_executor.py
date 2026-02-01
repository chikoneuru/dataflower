"""
Shared DAG Execution Module

This module contains DAG-related functionality that can be reused across different schedulers.
It provides workflow execution, graph building, and input preparation capabilities.
"""

import time
import concurrent.futures
from typing import Dict, List, Optional, Any
from contextlib import contextmanager

from functions.dag_loader import get_parent_tasks, get_dag_analysis, _build_dependencies_from_dag
from scheduler.shared.minio_config import get_minio_storage_config

class DAGExecutor:
    """
    Shared DAG execution functionality for serverless schedulers.
    
    This class provides:
    - DAG workflow execution with parallel/sequential/switch logic
    - Graph building and dependency resolution
    - Input preparation for DAG nodes
    - Switch condition evaluation
    """
    
    def __init__(self, orchestrator):
        """
        Initialize DAG executor with reference to the orchestrator.
        
        Args:
            orchestrator: The orchestrator instance that will handle function execution
        """
        self.orchestrator = orchestrator
    
    def execute_dag(self, dag: Dict, context: Dict, request_id: str) -> Dict:
        """Execute a DAG workflow with proper parallel, sequential, and switch logic"""
        workflow = dag['workflow']
        nodes = workflow.get('nodes', [])
        edges = workflow.get('edges', [])
        
        
        # Initialize timeline
        tl_root = self.orchestrator._get_or_create_timeline(request_id)
        tl_root['timestamps'] = tl_root.get('timestamps', {})
        
        # Time the entire workflow orchestration
        with self._time_operation(tl_root['timestamps'], 'workflow_orchestration'):
            # Pre-initialize timeline with all function entries
            self._pre_initialize_dag_timeline(request_id, nodes)
            
            # Build execution graph
            execution_graph = self._build_execution_graph(nodes, edges)
            
            # Execute the workflow
            with self._time_operation(tl_root['timestamps'], 'workflow_execution'):
                final_context = self._execute_dag_graph(execution_graph, context, request_id)
        
        return final_context

    def _pre_initialize_dag_timeline(self, request_id: str, nodes: List) -> None:
        """Pre-initialize timeline with all DAG function entries"""
        tl_root = self.orchestrator._get_or_create_timeline(request_id)
        
        # Initialize functions array if it doesn't exist
        if 'functions' not in tl_root:
            tl_root['functions'] = []
        
        # Pre-create function timeline entries for all task nodes
        for node in nodes:
            if node.get('type') == 'task':
                function_name = node['id']
                function_request_id = f"{request_id}_{function_name}"
                
                # Check if this function entry already exists
                existing_entry = None
                for func_timeline in tl_root['functions']:
                    if (func_timeline.get('function') == function_name and 
                        func_timeline.get('request_id') == function_request_id):
                        existing_entry = func_timeline
                        break
                
                # If no existing entry, create a placeholder
                if not existing_entry:
                    placeholder_timeline = {
                        'function': function_name,
                        'request_id': function_request_id,
                        'timestamps': {},
                        'server_timestamps': {},
                        'derived': {}
                    }
                    tl_root['functions'].append(placeholder_timeline)
    
    def _build_execution_graph(self, nodes: List, edges: List) -> Dict:
        """Build execution graph from DAG nodes and edges"""
        graph = {
            'nodes': {node['id']: node for node in nodes},
            'edges': edges,
            'dependencies': {},
            'execution_order': []
        }

        # Reuse shared DAG dependency builder to avoid duplication
        try:
            dep_map = _build_dependencies_from_dag({'workflow': {'nodes': nodes, 'edges': edges}})
            graph['dependencies'] = dep_map
        except Exception as e:
            print(f"\033[31mERROR: Failed to build DAG dependencies: {e}\033[0m")
            raise

        # Find execution order using a local topological sort
        def topo_sort(nodes_map: Dict, dependencies: Dict[str, List[str]]) -> List[str]:
            in_degree = {node_id: 0 for node_id in nodes_map.keys()}
            for node_id, deps in dependencies.items():
                in_degree[node_id] = len(deps)
            queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
            result: List[str] = []
            while queue:
                current = queue.pop(0)
                result.append(current)
                for n_id, deps in dependencies.items():
                    if current in deps:
                        in_degree[n_id] -= 1
                        if in_degree[n_id] == 0:
                            queue.append(n_id)
            return result

        graph['execution_order'] = topo_sort(graph['nodes'], graph['dependencies'])

        return graph

    def _execute_dag_graph(self, graph: Dict, context: Dict, request_id: str) -> Dict:
        """Execute the DAG graph with proper parallel execution"""
        executed_nodes = set()
        node_results = {}
        
        # Continue until all executable nodes are processed
        max_iterations = 20  # Prevent infinite loops
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            
            # Find all nodes that are ready to execute (dependencies satisfied)
            ready_nodes = []
            for node_id in graph['nodes'].keys():
                if node_id in executed_nodes:
                    continue
                    
                node = graph['nodes'][node_id]
                if node.get('type') != 'task':
                    continue
                
                # Check if all dependencies are satisfied
                dependencies = graph['dependencies'].get(node_id, [])
                if all(dep in executed_nodes for dep in dependencies):
                    ready_nodes.append(node_id)
            
            if not ready_nodes:
                break
            
            
            # Group ready nodes by parallel execution groups
            parallel_groups = self._group_parallel_nodes(ready_nodes, graph)
            
            # Execute each group
            for group in parallel_groups:
                if len(group) == 1:
                    # Sequential execution
                    node_id = group[0]
                    result = self._execute_single_node(node_id, context, request_id, node_results, graph)
                    node_results[node_id] = result
                    executed_nodes.add(node_id)
                    
                    # Handle switch conditions
                    self._handle_switch_conditions(graph, node_id, result, executed_nodes, node_results)
                else:
                    # Parallel execution
                    parallel_results = self._execute_parallel_nodes(group, context, request_id, node_results, graph)
                    
                    # Store results
                    for node_id, result in parallel_results.items():
                        node_results[node_id] = result
                        executed_nodes.add(node_id)
                        
                        # Handle switch conditions
                        self._handle_switch_conditions(graph, node_id, result, executed_nodes, node_results)
        
        return context
    
    def _group_parallel_nodes(self, ready_nodes: List[str], graph: Dict) -> List[List[str]]:
        """Group nodes that can be executed in parallel"""
        groups = []
        remaining_nodes = set(ready_nodes)
        
        def has_dependency(from_node: str, to_node: str) -> bool:
            deps = graph['dependencies'].get(to_node, [])
            return from_node in deps

        while remaining_nodes:
            # Find nodes that can be executed together
            current_group = []
            for node_id in list(remaining_nodes):
                # Check if this node can be added to current group
                can_add = True
                for existing_node in current_group:
                    # Check if there's a dependency between them
                    if has_dependency(node_id, existing_node) or \
                       has_dependency(existing_node, node_id):
                        can_add = False
                        break
                
                if can_add:
                    current_group.append(node_id)
                    remaining_nodes.remove(node_id)
            
            if current_group:
                groups.append(current_group)
            else:
                # If no nodes can be grouped, execute them one by one
                groups.append([remaining_nodes.pop()])
        
        return groups

    def _execute_single_node(self, node_id: str, context: Dict, request_id: str, node_results: Dict, graph: Dict) -> Dict:
        """Execute a single DAG node"""
        function_name = node_id
        function_request_id = f"{request_id}_{function_name}"
        
        # Prepare the input payload using collected dependency results or initial context
        input_data = self.prepare_input_for_node(function_name, graph, context, node_results)
        
        # Debug logging for visibility into how inputs are prepared
        dependencies = graph.get('dependencies', {}).get(function_name, [])
        if not dependencies:
            self._debug(f"\033[93m{function_name}: first function, direct input provided\033[0m")
        else:
            self._debug(f"\033[96m{function_name}: dependencies {dependencies}, prepared input type: {type(input_data)}\033[0m")
        
        # Execute function using the orchestrator's invoke_function method
        result = self.orchestrator.invoke_function(function_name, function_request_id, input_data)
        
        # Update timeline with execution results
        self._update_function_timeline(request_id, function_name, function_request_id, result)
        
        return result
    
    def _execute_parallel_nodes(self, node_ids: List[str], context: Dict, request_id: str, node_results: Dict, graph: Dict) -> Dict:
        """Execute multiple DAG nodes in parallel"""
        results = {}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(node_ids)) as executor:
            # Submit all functions for parallel execution
            future_to_node = {}
            for node_id in node_ids:
                function_name = node_id
                function_request_id = f"{request_id}_{function_name}"
                
                # Prepare per-node input using available dependency outputs/context
                input_data = self.prepare_input_for_node(function_name, graph, context, node_results)
                
                future = executor.submit(self.orchestrator.invoke_function, function_name, function_request_id, input_data)
                future_to_node[future] = node_id
            
            # Collect results
            for future in concurrent.futures.as_completed(future_to_node):
                node_id = future_to_node[future]
                try:
                    result = future.result()
                    results[node_id] = result
                    
                    # Update timeline with execution results
                    function_request_id = f"{request_id}_{node_id}"
                    self._update_function_timeline(request_id, node_id, function_request_id, result)
                except Exception as e:
                    print(f"\033[31mERROR: Parallel execution failed for {node_id}: {e}\033[0m")
                    error_result = {'status': 'error', 'message': str(e)}
                    results[node_id] = error_result
                    
                    # Update timeline with error results
                    function_request_id = f"{request_id}_{node_id}"
                    self._update_function_timeline(request_id, node_id, function_request_id, error_result)
        
        return results
    
    def _update_function_timeline(self, request_id: str, function_name: str, function_request_id: str, result: Dict) -> None:
        """Update timeline with function execution results"""
        try:
            tl_root = self.orchestrator._get_or_create_timeline(request_id)
            if 'functions' not in tl_root:
                return
            
            # Find the existing timeline entry for this function
            for func_timeline in tl_root['functions']:
                if (func_timeline.get('function') == function_name and 
                    func_timeline.get('request_id') == function_request_id):
                    
                    # Update the timeline entry with execution results
                    func_timeline.update({
                        'status': result.get('status', 'ok'),
                        'message': result.get('message'),
                        'node_id': result.get('node_id'),
                        'container_id': result.get('container_id'),
                        'exec_ms': result.get('exec_ms', 0.0),
                        'derived': result.get('derived', {}),
                        'timestamps': {
                            'invocation_start': result.get('client_timestamps', {}).get('sent'),
                            'invocation_end': result.get('client_timestamps', {}).get('received'),
                            # Preserve all other timestamps from the response
                            **result.get('timestamps', {})
                        }
                    })
                    break
        except Exception as e:
            pass
    
    def prepare_input_for_node(self, node_id: str, graph: Dict, context: Dict, node_results: Dict) -> Any:
        """Prepare input payload for a DAG node based on graph dependencies and prior results.

        Strategy:
        - If dependencies have results with a standard 'result' key, merge them.
        - If no dependencies or no usable outputs, fallback to original workflow input (bytes image).
        - Keep payload simple: if only one dependency has a single key, pass that value directly; otherwise pass a dict.
        """
        dependencies = graph.get('dependencies', {}).get(node_id, [])
        collected: Dict[str, Any] = {}

        for dep in dependencies:
            dep_output = node_results.get(dep)
            if isinstance(dep_output, dict):
                # Prefer structured result payloads
                result_payload = dep_output.get('result') if 'result' in dep_output else dep_output
                if isinstance(result_payload, dict):
                    for k, v in result_payload.items():
                        # Do not overwrite existing keys blindly
                        if k not in collected:
                            collected[k] = v
                else:
                    # Store under the dependency name if non-dict
                    if dep not in collected:
                        collected[dep] = result_payload

        # If we collected nothing, fallback to original input for compatibility
        # If we collected nothing, fallback to original input for compatibility
        if not collected:
            # Special handling for recognizer__upload - it needs the actual image bytes
            if node_id == 'recognizer__upload':
                return context['inputs']['img']
            
            # Check if we have an initial input key for MinIO retrieval
            initial_input_key = context.get('inputs', {}).get('initial_input_key')
            if initial_input_key:
                storage_config = get_minio_storage_config()
                
                # Return a MinIO-based input reference for the first function
                return {
                    'src': 'remote',
                    'input_key': initial_input_key,
                    'storage_config': storage_config
                }
            return context['inputs']['img']

        # Normalize payloads per target function expectations
        image_functions = {
            'recognizer__adult',
            'recognizer__violence',
            'recognizer__extract',
            'recognizer__mosaic',
        }
        text_functions = {
            'recognizer__translate',
            'recognizer__censor',
        }

        # Helpers to extract best candidate values
        def pick_image_b64(from_dict: Dict[str, Any]) -> Optional[str]:
            # Preferred keys in order
            preferred_keys = ['img_b64', 'img_clean_b64', 'image_b64']
            for key in preferred_keys:
                val = from_dict.get(key)
                if isinstance(val, str) and val:
                    return val
            # Fallback: any key that looks like image and ends with _b64
            for k, v in from_dict.items():
                if isinstance(v, str) and v and (k.endswith('_b64') or 'img' in k.lower()):
                    return v
            return None

        def pick_text(from_dict: Dict[str, Any]) -> Optional[str]:
            preferred_keys = ['text', 'extracted_text', 'filtered_text', 'raw_text']
            for key in preferred_keys:
                val = from_dict.get(key)
                if isinstance(val, str):
                    return val
            # If a nested result contains text, try one level deep
            for v in from_dict.values():
                if isinstance(v, dict):
                    inner = pick_text(v)
                    if inner is not None:
                        return inner
            return None

        # Target-specific shaping
        if node_id in image_functions:
            # Try to supply a JSON payload with img_b64 when possible
            if isinstance(collected, dict):
                img_b64 = pick_image_b64(collected)
                if img_b64:
                    return {'img_b64': img_b64}
                # If bytes present under common keys, return as bytes
                for k, v in collected.items():
                    if isinstance(v, (bytes, bytearray)) and ('img' in k.lower() or 'image' in k.lower()):
                        return bytes(v)
            # If a single non-dict value was collected earlier, avoid returning raw strings
            # and instead fall through to original input as bytes
            return context['inputs']['img']

        if node_id in text_functions:
            if isinstance(collected, dict):
                text_val = pick_text(collected)
                if text_val is None:
                    text_val = ''  # Safe default to avoid handler errors
                return {'text': text_val}
            # If single value was a string, wrap it
            if isinstance(collected, str):
                return {'text': collected}
            return {'text': ''}

        # Default behavior: if only one value, return it; else pass merged dict
        if len(collected) == 1:
            return next(iter(collected.values()))

        return collected
    
    def _handle_switch_conditions(self, graph: Dict, node_id: str, result: Dict, executed_nodes: set, node_results: Dict) -> None:
        """Handle switch conditions and mark additional nodes for execution"""
        
        # Helper function: evaluate switch expressions to avoid exposing class-wide eval helpers
        def evaluate_switch_condition(condition: str, eval_context: Dict[str, Any], branch_name: str) -> bool:
            """Evaluate a simple boolean condition against a flattened context of node outputs.

            Supported operators: 'and', 'or', 'not' (case-insensitive), parentheses, dotted keys.
            Truthiness: non-empty/non-zero values are True; booleans pass through.
            Unknown keys default to False.
            """
            expr = (condition or '').replace('{{', '').replace('}}', '').strip()
            if not expr:
                return True

            # Tokenize by replacing operators with Python equivalents and map identifiers
            safe = expr
            replacements = {
                ' AND ': ' and ', ' and ': ' and ',
                ' OR ': ' or ',  ' or ': ' or ',
                ' NOT ': ' not ', ' not ': ' not ',
            }
            for k, v in replacements.items():
                safe = safe.replace(k, v)

            # Replace dotted identifiers with lookup into eval_context via a helper
            # We avoid eval on raw identifiers by converting them to helper('key') calls
            import re
            identifier = r"[A-Za-z0-9_]+\.[A-Za-z0-9_\.]+"  # e.g., recognizer__adult.is_adult

            def repl(match):
                key = match.group(0)
                return f"__get__('{key}')"

            safe = re.sub(identifier, repl, safe)

            def __get__(key: str) -> bool:
                val = eval_context.get(key)
                if isinstance(val, bool):
                    return val
                return bool(val)

            try:
                result_val = eval(safe, {'__builtins__': {}}, {'__get__': __get__})
            except Exception:
                # On any parsing/eval error, fail safe: execute else branch only
                return branch_name != 'then'

            return bool(result_val) if branch_name == 'then' else not bool(result_val)

        # Find switch edges that involve this node
        for edge in graph['edges']:
            if edge.get('type') == 'switch':
                branches = edge.get('branches', {})
                
                # Check if this node affects any switch conditions
                for branch_name, branch_edges in branches.items():
                    for branch_edge in branch_edges:
                        from_nodes = branch_edge.get('from', [])
                        if node_id in from_nodes:
                            # Evaluate condition using a safe evaluator over available node results
                            condition = edge.get('condition', '')

                            # Collect context from all executed nodes for flexible expressions
                            eval_context: Dict[str, Any] = {}
                            for n_id in executed_nodes:
                                if n_id in node_results and isinstance(node_results[n_id], dict):
                                    # flatten node outputs under '<node>.' prefix
                                    for k, v in node_results[n_id].items():
                                        eval_context[f"{n_id}.{k}"] = v

                            should_execute = evaluate_switch_condition(condition, eval_context, branch_name)
                            
                            if should_execute:
                                to_node = branch_edge.get('to')
                                if to_node and to_node != 'END':
                                    # Mark the target node for execution
                                    if to_node not in executed_nodes:
                                        # Add to execution order if not already there
                                        if to_node not in graph['execution_order']:
                                            graph['execution_order'].append(to_node)


    @contextmanager
    def _time_operation(self, timeline: Dict, operation_name: str):
        """Context manager to automatically time operations and store in timeline"""
        start_time = time.time()
        try:
            yield
        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000.0
            timeline[f'{operation_name}_start'] = start_time
            timeline[f'{operation_name}_end'] = end_time
            timeline[f'{operation_name}_duration_ms'] = duration_ms

    def _debug(self, message: str) -> None:
        """Delegate debug logging to the orchestrator"""
        if hasattr(self.orchestrator, '_debug'):
            self.orchestrator._debug(message)
