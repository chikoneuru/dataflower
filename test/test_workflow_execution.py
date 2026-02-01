import argparse
import os
import sys
import uuid
from pathlib import Path

import yaml

from provider.container_manager import ContainerManager
from provider.function_manager import FunctionManager
from scheduler.ours.config import load_config
from scheduler.ours.cost_models import CostModels
from scheduler.ours.orchestrator import Orchestrator


def main(args):
    """
    Initializes the orchestration components and runs the specified workflow.
    """
    print(f"🚀 Starting workflow execution using '{args.config}' configuration...")

    # 0. Initialize configuration
    try:
        load_config(preset=args.config)
    except (ValueError, FileNotFoundError) as e:
        print(f"❌ CRITICAL: Could not load configuration. {e}")
        return

    # 1. Initialize provider components
    print("  - Initializing provider components...")
    cost_models = CostModels()
    container_manager = ContainerManager() # Will auto-discover workers on init

    # Check if any workers were found
    if not container_manager.get_workers():
        print("\n❌ CRITICAL: No running worker containers were found.")
        print("   Please ensure the Docker Compose environment is running:")
        print("   docker compose -f docker-compose-multi-node.yml up -d --build")
        return

    function_manager = FunctionManager()
    orchestrator = Orchestrator(function_manager, container_manager)

    # 2. Load workflow definition
    print(f"  - Loading workflow DAG from: {args.dag}")
    dag_path = Path(args.dag)
    try:
        with open(dag_path, 'r') as f:
            workflow_dag = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"❌ ERROR: Workflow DAG file not found at {dag_path}")
        return
    
    workflow_name = workflow_dag.get("name", "recognizer_workflow")
    orchestrator.add_workflow(workflow_name, workflow_dag)

    # 3. Prepare initial input
    print(f"  - Preparing initial input data from: {args.input}")
    input_path = Path(args.input)
    try:
        # For this workflow, the initial input is expected to be an image file (bytes)
        with open(input_path, 'rb') as f:
            initial_data = f.read()
    except FileNotFoundError:
        print(f"❌ ERROR: Input file not found at {input_path}")
        return
    except Exception as e:
        print(f"❌ ERROR: Could not read input file: {e}")
        return

    # 4. Run the workflow
    request_id = str(uuid.uuid4())
    print(f"  - Executing workflow '{workflow_name}' with Request ID: {request_id}\n")
    
    final_result = orchestrator.run_workflow(workflow_name, request_id, initial_data)

    # Sanitize the result for printing: replace raw bytes with a placeholder string
    if final_result and 'inputs' in final_result and isinstance(final_result.get('inputs', {}).get('img'), bytes):
        image_size = len(final_result['inputs']['img'])
        final_result['inputs']['img'] = f"<image content: {image_size} bytes>"

    # Sanitize the base64 string from the upload step for cleaner printing
    if final_result and 'upload' in final_result and isinstance(final_result.get('upload', {}).get('img_b64'), str):
        final_result['upload']['img_b64'] = "<base64 image content>"

    # 5. Print final result
    print("\n✅ Workflow execution finished.")
    print("  - Final Result:")
    # Pretty print the final dictionary
    import json
    print(json.dumps(final_result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a serverless workflow via the orchestrator.")
    parser.add_argument(
        '--dag',
        type=str,
        default='functions/recognizer/recognizer_dag.yaml',
        help='Path to the workflow DAG file, relative to the project root.'
    )
    parser.add_argument(
        '--input',
        type=str,
        default='data/test.png',
        help='Path to the initial input file for the workflow, relative to the project root.'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='mock',
        choices=['mock', 'simple', 'production'],
        help='The configuration preset to use.'
    )
    
    # Proper module execution - no sys.path manipulation needed

    args = parser.parse_args()
    main(args)
