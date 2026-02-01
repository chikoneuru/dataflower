#!/usr/bin/env python3
"""
Clean DAG loader and analysis functions for the orchestrator
"""

from typing import Any, Dict, List, Optional

import yaml


def parse_workflow(dag_file_path: str):
    """Parse workflow"""
    with open(dag_file_path, "r") as f:
        dag = yaml.safe_load(f)
    
    workflow = dag['workflow']
    all_functions = [node['id'] for node in workflow['nodes'] if node['type'] == 'task']
    edges = workflow.get('edges', [])
    
    # Build dependency map
    dependencies = {}
    
    for edge in edges:
        if edge.get('type') == 'parallel':
            for to_node in edge['to']:
                if to_node not in dependencies:
                    dependencies[to_node] = []
                dependencies[to_node].append(edge['from'])
                
        elif edge.get('type') == 'sequential':
            # Handle both 'flow' format and 'from'/'to' format
            if 'flow' in edge:
                flow = edge['flow']
                for i in range(1, len(flow)):
                    if flow[i] not in dependencies:
                        dependencies[flow[i]] = []
                    dependencies[flow[i]].append(flow[i-1])
            elif 'from' in edge and 'to' in edge:
                from_node = edge['from']
                to_node = edge['to']
                if to_node != 'END' and to_node not in dependencies:
                    dependencies[to_node] = []
                if to_node != 'END':
                    dependencies[to_node].append(from_node)
                
        elif edge.get('type') == 'switch':
            branches = edge.get('branches', {})
            for branch_name, branch_list in branches.items():
                for branch in branch_list:
                    to_node = branch['to']
                    if to_node != 'END' and to_node not in dependencies:
                        dependencies[to_node] = []
                    if to_node != 'END':
                        from_nodes = branch['from']
                        if isinstance(from_nodes, list):
                            dependencies[to_node].extend(from_nodes)
                        else:
                            dependencies[to_node].append(from_nodes)
    
    return dependencies, dag, all_functions, edges

def analyze_workflow_simple():
    """Display workflow analysis with pretty printing"""
    dependencies, dag, all_functions, edges = parse_workflow("functions/recognizer/recognizer_dag.yaml")
    
    print("=== Simple Workflow Analysis ===")
    print(f"Workflow: {dag['name']}")
    
    print(f"\nAll Functions ({len(all_functions)}):")
    for i, func in enumerate(all_functions, 1):
        print(f"  {i}. {func}")
    
    print("\n=== Workflow Logic Patterns ===")
    
    for edge in edges:
        edge_type = edge.get('type', 'unknown')
        
        if edge_type == 'parallel':
            from_node = edge['from']
            to_nodes = edge['to']
            print(f"🔄 PARALLEL: {from_node} → {to_nodes}")
            
        elif edge_type == 'sequential':
            flow = edge['flow']
            print(f"➡️  SEQUENTIAL: {' → '.join(flow)}")
            
        elif edge_type == 'switch':
            condition = edge['condition']
            print(f"🔀 SWITCH: {condition}")
            
            branches = edge.get('branches', {})
            if 'then' in branches:
                for branch in branches['then']:
                    from_nodes = branch['from']
                    to_node = branch['to']
                    print(f"   THEN: {from_nodes} → {to_node}")
                    
            if 'else' in branches:
                for branch in branches['else']:
                    from_nodes = branch['from']
                    to_node = branch['to']
                    print(f"   ELSE: {from_nodes} → {to_node}")
    
    print("\n=== Function Dependencies (Simplified) ===")
    for func_name in all_functions:
        deps = dependencies.get(func_name, [])
        print(f"📋 {func_name}: {deps if deps else 'ENTRY POINT'}")
    
    return dependencies, dag

def get_parent_tasks(function_name: str, dag: Dict[str, Any]) -> Optional[List[str]]:
    """Get parent tasks for LNS placement - orchestrator interface"""
    if not dag or 'workflow' not in dag:
        return None
    
    try:
        dependencies = _build_dependencies_from_dag(dag)
        parents = dependencies.get(function_name, [])
        return parents if parents else None
    except Exception:
        return None

def get_dag_analysis(function_name: str, dag: Dict[str, Any], workflow_name: str = None) -> Optional[Dict]:
    """Get DAG structure for LNS placement - orchestrator interface"""
    if not dag or 'workflow' not in dag:
        return None
    
    try:
        workflow = dag['workflow']
        all_functions = [node['id'] for node in workflow.get('nodes', []) if node.get('type') == 'task']
        
        if function_name not in all_functions:
            return None
        
        dependencies = _build_dependencies_from_dag(dag)
        
        return {
            'workflow_name': workflow_name or dag.get('name', 'unknown'),
            'functions': all_functions,
            'dag': dag,
            'function_dependencies': dependencies,
            'execution_order': all_functions.index(function_name) if function_name in all_functions else None
        }
    except Exception:
        return None

def _build_dependencies_from_dag(dag: Dict[str, Any]) -> Dict[str, List[str]]:
    """Build dependency map from DAG structure"""
    workflow = dag['workflow']
    edges = workflow.get('edges', [])
    nodes = workflow.get('nodes', [])
    dependencies = {}
    
    # First, build dependencies from edges
    for edge in edges:
        if edge.get('type') == 'parallel':
            for to_node in edge['to']:
                if to_node not in dependencies:
                    dependencies[to_node] = []
                dependencies[to_node].append(edge['from'])
                
        elif edge.get('type') == 'sequential':
            # Handle both 'flow' format and 'from'/'to' format
            if 'flow' in edge:
                flow = edge['flow']
                for i in range(1, len(flow)):
                    if flow[i] not in dependencies:
                        dependencies[flow[i]] = []
                    dependencies[flow[i]].append(flow[i-1])
            elif 'from' in edge and 'to' in edge:
                from_node = edge['from']
                to_node = edge['to']
                if to_node != 'END' and to_node not in dependencies:
                    dependencies[to_node] = []
                if to_node != 'END':
                    dependencies[to_node].append(from_node)
                
        elif edge.get('type') == 'switch':
            branches = edge.get('branches', {})
            for branch_name, branch_list in branches.items():
                for branch in branch_list:
                    to_node = branch['to']
                    if to_node != 'END' and to_node not in dependencies:
                        dependencies[to_node] = []
                    if to_node != 'END':
                        from_nodes = branch['from']
                        if isinstance(from_nodes, list):
                            dependencies[to_node].extend(from_nodes)
                        else:
                            dependencies[to_node].append(from_nodes)
    
    # Second, add dependencies from input specifications
    for node in nodes:
        if node.get('type') == 'task':
            node_id = node['id']
            node_inputs = node.get('in', {})
            
            # Extract dependencies from input specifications like $recognizer__upload.img_b64
            for input_key, input_value in node_inputs.items():
                if isinstance(input_value, str) and input_value.startswith('$'):
                    # Parse $recognizer__upload.img_b64 -> recognizer__upload
                    parts = input_value[1:].split('.')
                    if len(parts) >= 1:
                        source_node = parts[0]
                        if source_node != 'inputs' and source_node != node_id:
                            if node_id not in dependencies:
                                dependencies[node_id] = []
                            if source_node not in dependencies[node_id]:
                                dependencies[node_id].append(source_node)
    
    return dependencies

# Legacy function for backward compatibility
def get_dag_structure(function_name: str, dag: dict) -> dict:
    """Legacy function - use get_dag_analysis instead"""
    return get_dag_analysis(function_name, dag)

def test_lns_functions():
    """Test the LNS placement functions"""
    print("\n=== LNS Placement Function Test ===")
    
    with open("functions/recognizer/recognizer_dag.yaml", "r") as f:
        dag = yaml.safe_load(f)
    
    test_functions = ["recognizer__upload", "recognizer__extract", "recognizer__translate", "recognizer__mosaic"]
    
    for func_name in test_functions:
        print(f"\n🎯 Testing LNS for: {func_name}")
        
        # Test parent tasks function
        parents = get_parent_tasks(func_name, dag)
        print(f"   Parent tasks: {parents}")
        
        # Test DAG structure function
        dag_structure = get_dag_analysis(func_name, dag)
        if dag_structure:
            print(f"   Workflow: {dag_structure['workflow_name']}")
            print(f"   DAG type: {dag_structure['dag_type']}")
            print(f"   Execution order: {dag_structure['execution_order']}")
            print(f"   Total functions: {len(dag_structure['functions'])}")
        else:
            print("   ❌ No DAG structure found")

if __name__ == "__main__":
    analyze_workflow_simple()
    test_lns_functions()
