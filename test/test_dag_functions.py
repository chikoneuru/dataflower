#!/usr/bin/env python3
"""
Simple test script to verify the DAG function implementations.
"""

import os
import sys

import yaml

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(__file__))

from scheduler.ours.config import load_config
from scheduler.ours.function_orchestrator import FunctionOrchestrator


def test_dag_functions():
    """Test the DAG parsing functions with real workflow data"""
    
    # Initialize the orchestrator
    load_config('configs/mock.json')
    orchestrator = FunctionOrchestrator()
    
    # Load and register the recognizer workflow
    with open('functions/recognizer/recognizer_dag.yaml', 'r') as f:
        recognizer_dag = yaml.safe_load(f)
    
    orchestrator.register_workflow('recognizer', recognizer_dag)
    
    print("=== Testing DAG Function Implementations ===\n")
    
    # Test _get_parent_tasks for various functions
    test_functions = [
        'recognizer__upload',
        'recognizer__adult', 
        'recognizer__violence',
        'recognizer__extract',
        'recognizer__translate',
        'recognizer__censor',
        'recognizer__mosaic'
    ]
    
    print("1. Testing _get_parent_tasks:")
    for func_name in test_functions:
        parents = orchestrator._get_parent_tasks(func_name)
        print(f"   {func_name}: {parents}")
    
    print("\n2. Testing _get_dag_structure:")
    for func_name in test_functions[:3]:  # Test a few functions
        dag_structure = orchestrator._get_dag_structure(func_name)
        if dag_structure:
            print(f"   {func_name}:")
            print(f"     - Workflow: {dag_structure['workflow_name']}")
            print(f"     - DAG Type: {dag_structure['dag_type']}")
            print(f"     - Execution Order: {dag_structure['execution_order']}")
            print(f"     - Dependencies: {dag_structure['function_dependencies'].get(func_name, [])}")
        else:
            print(f"   {func_name}: No DAG structure found")
    
    print("\n3. Testing dependency map:")
    dag_structure = orchestrator._get_dag_structure('recognizer__upload')
    if dag_structure and 'function_dependencies' in dag_structure:
        print("   Full dependency map:")
        for func, deps in dag_structure['function_dependencies'].items():
            if deps:  # Only show functions with dependencies
                print(f"     {func} <- {deps}")
    
    print("\n=== Test Complete ===")


if __name__ == "__main__":
    test_dag_functions()
