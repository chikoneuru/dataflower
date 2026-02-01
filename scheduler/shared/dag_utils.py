"""
Shared DAG Utilities Module

This module contains common DAG processing functionality that can be reused
across different schedulers. It provides:
- Topological ordering
- Edge processing (sequential, parallel, switch)
- Input data resolution
- Switch condition evaluation
"""

from typing import Dict, List, Any, Optional, Tuple


class DAGUtils:
    """Shared DAG processing utilities for all schedulers"""
    
    @staticmethod
    def build_topological_order(dag: Dict[str, Any], tasks: List[str]) -> List[str]:
        """
        Build topological order from DAG edges.
        Handles sequential, parallel, and switch edge types.
        """
        edges = []
        workflow = dag.get("workflow", {})
        
        for edge in workflow.get("edges", []):
            if edge.get("type") == "sequential":
                # Handle both 'flow' format and 'from'/'to' format
                if 'flow' in edge:
                    flow = edge.get("flow", [])
                    edges += list(zip(flow, flow[1:]))
                elif 'from' in edge and 'to' in edge:
                    from_node = edge['from']
                    to_node = edge['to']
                    if to_node != 'END':
                        edges.append((from_node, to_node))
            elif edge.get("type") == "parallel":
                for to in edge.get("to", []):
                    edges.append((edge.get("from"), to))
            elif edge.get("type") == "switch":
                branches = edge.get("branches", {})
                for branch in branches.get("then", []) + branches.get("else", []):
                    to = branch.get("to")
                    if to != "END":
                        srcs = branch.get("from")
                        if isinstance(srcs, list):
                            for s in srcs:
                                edges.append((s, to))
                        else:
                            edges.append((srcs, to))
        
        # Build adjacency list and in-degree count
        succ = {u: [] for u in tasks}
        indeg = {u: 0 for u in tasks}
        
        for u, v in edges:
            if u in tasks and v in tasks:
                succ[u].append(v)
                indeg[v] += 1
        
        # Kahn's algorithm for topological sort
        order = [u for u in tasks if indeg[u] == 0]
        i = 0
        while i < len(order):
            u = order[i]
            for v in succ.get(u, []):
                indeg[v] -= 1
                if indeg[v] == 0:
                    order.append(v)
            i += 1
        
        return order
    
    @staticmethod
    def resolve_input_data(task_id: str, context: Dict, dag: Dict) -> Any:
        """
        Resolve input data for a task based on DAG dependencies.
        Handles sequential, parallel, and switch edge types.
        """
        workflow = dag.get('workflow', {})
        edges = workflow.get('edges', [])
        
        # Find incoming edges to this task
        incoming_data = []
        
        for edge in edges:
            if edge.get('type') == 'sequential':
                # Handle both 'flow' format and 'from'/'to' format
                if 'flow' in edge:
                    flow = edge.get('flow', [])
                    if task_id in flow and flow.index(task_id) > 0:
                        # Get data from previous task in sequence
                        prev_task = flow[flow.index(task_id) - 1]
                        if prev_task in context:
                            incoming_data.append(context[prev_task])
                elif 'from' in edge and 'to' in edge:
                    if edge['to'] == task_id:
                        from_task = edge['from']
                        if from_task in context:
                            incoming_data.append(context[from_task])
                        
            elif edge.get('type') == 'parallel':
                if task_id in edge.get('to', []):
                    from_task = edge.get('from')
                    if from_task in context:
                        incoming_data.append(context[from_task])
                        
            elif edge.get('type') == 'switch':
                branches = edge.get('branches', {})
                for branch in branches.get('then', []) + branches.get('else', []):
                    if branch.get('to') == task_id:
                        from_tasks = branch.get('from', [])
                        if isinstance(from_tasks, list):
                            for from_task in from_tasks:
                                if from_task in context:
                                    incoming_data.append(context[from_task])
                        elif from_tasks in context:
                            incoming_data.append(context[from_tasks])
        
        # Return the most recent input data, or original input if no dependencies
        if incoming_data:
            return incoming_data[-1].get('result') or incoming_data[-1]
        else:
            return context.get('inputs', {}).get('img')
    
    @staticmethod
    def evaluate_switch_condition(condition: str, eval_context: Dict[str, Any], branch_name: str) -> bool:
        """
        Evaluate switch condition expressions safely.
        Supports basic boolean expressions and function result checks.
        """
        try:
            # Create a safe evaluation context
            safe_context = {
                'result': eval_context.get('result', {}),
                'status': eval_context.get('status', 'ok'),
                'data': eval_context.get('data', {}),
                'img': eval_context.get('img', {}),
                'text': eval_context.get('text', ''),
                'adult': eval_context.get('adult', False),
                'violence': eval_context.get('violence', False),
                'censor': eval_context.get('censor', False),
                'mosaic': eval_context.get('mosaic', False),
                'translate': eval_context.get('translate', False),
                'extract': eval_context.get('extract', False),
                'upload': eval_context.get('upload', False),
            }
            
            # Handle common condition patterns
            if condition == 'adult_detected':
                return safe_context.get('adult', False)
            elif condition == 'violence_detected':
                return safe_context.get('violence', False)
            elif condition == 'censor_required':
                return safe_context.get('censor', False)
            elif condition == 'mosaic_applied':
                return safe_context.get('mosaic', False)
            elif condition == 'translate_needed':
                return safe_context.get('translate', False)
            elif condition == 'extract_text':
                return safe_context.get('extract', False)
            elif condition == 'upload_ready':
                return safe_context.get('upload', False)
            elif condition == 'success':
                return safe_context.get('status') == 'ok'
            elif condition == 'error':
                return safe_context.get('status') == 'error'
            
            # For more complex expressions, use safe evaluation
            # Replace common patterns with safe alternatives
            safe_condition = condition.replace('result.', 'result.get("', 1)
            safe_condition = safe_condition.replace('.', '", {}).get("')
            safe_condition = safe_condition.replace(' == ', ' == ')
            safe_condition = safe_condition.replace(' != ', ' != ')
            safe_condition = safe_condition.replace(' and ', ' and ')
            safe_condition = safe_condition.replace(' or ', ' or ')
            safe_condition = safe_condition.replace(' not ', ' not ')
            
            # Add closing parentheses for get() calls
            if 'result.get("' in safe_condition:
                safe_condition += '")'
            
            # Evaluate the condition safely
            return bool(eval(safe_condition, {"__builtins__": {}}, safe_context))
            
        except Exception:
            # Default to False for any evaluation errors
            return False
    
    @staticmethod
    def extract_task_nodes(dag: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract task nodes from DAG workflow"""
        workflow = dag.get('workflow', {})
        nodes = workflow.get('nodes', [])
        return [n for n in nodes if n.get('type') == 'task']
    
    @staticmethod
    def extract_task_ids(dag: Dict[str, Any]) -> List[str]:
        """Extract task IDs from DAG workflow"""
        task_nodes = DAGUtils.extract_task_nodes(dag)
        return [n.get('id') for n in task_nodes if n.get('id')]
    
    @staticmethod
    def find_task_node_by_id(dag: Dict[str, Any], task_id: str) -> Optional[Dict[str, Any]]:
        """Find a task node by its ID"""
        task_nodes = DAGUtils.extract_task_nodes(dag)
        for node in task_nodes:
            if node.get('id') == task_id:
                return node
        return None
    
    @staticmethod
    def get_function_name_from_task(task_node: Dict[str, Any]) -> str:
        """Get function name from task node"""
        return task_node.get('function') or task_node.get('id', '')
    
    @staticmethod
    def build_dependency_graph(dag: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Build a dependency graph from DAG edges.
        Returns a mapping of task_id -> list of dependent task_ids
        """
        dependencies = {}
        workflow = dag.get('workflow', {})
        task_ids = DAGUtils.extract_task_ids(dag)
        
        # Initialize all tasks
        for task_id in task_ids:
            dependencies[task_id] = []
        
        # Process edges to build dependencies
        for edge in workflow.get('edges', []):
            if edge.get("type") == "sequential":
                # Handle both 'flow' format and 'from'/'to' format
                if 'flow' in edge:
                    flow = edge.get("flow", [])
                    for i in range(len(flow) - 1):
                        if flow[i] in task_ids and flow[i + 1] in task_ids:
                            dependencies[flow[i]].append(flow[i + 1])
                elif 'from' in edge and 'to' in edge:
                    from_task = edge['from']
                    to_task = edge['to']
                    if from_task in task_ids and to_task in task_ids and to_task != 'END':
                        dependencies[from_task].append(to_task)
                        
            elif edge.get("type") == "parallel":
                from_task = edge.get("from")
                for to_task in edge.get("to", []):
                    if from_task in task_ids and to_task in task_ids:
                        dependencies[from_task].append(to_task)
                        
            elif edge.get("type") == "switch":
                branches = edge.get("branches", {})
                for branch in branches.get("then", []) + branches.get("else", []):
                    to_task = branch.get("to")
                    if to_task != "END":
                        from_tasks = branch.get("from", [])
                        if not isinstance(from_tasks, list):
                            from_tasks = [from_tasks]
                        for from_task in from_tasks:
                            if from_task in task_ids and to_task in task_ids:
                                dependencies[from_task].append(to_task)
        
        return dependencies
