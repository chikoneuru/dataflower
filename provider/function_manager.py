import importlib
from typing import Dict, Optional, Callable


class FunctionManager:
    def __init__(self):
        # Stores a mapping of {function_name: workflow_name}
        self.function_to_workflow = {}

    def register_workflow(self, workflow_name, dag):
        """
        Processes a workflow DAG to map its functions to the workflow name.
        """
        if 'functions' not in dag:
            return
        
        for function_name in dag['functions']:
            self.function_to_workflow[function_name] = workflow_name
        print(f"    - FunctionManager: Registered {len(dag['functions'])} functions for workflow '{workflow_name}'.")

    def get_workflow_name(self, function_name):
        """
        Retrieves the workflow name for a given function.
        """
        return self.function_to_workflow.get(function_name)

    def get_function(self, fn_name: str) -> Callable:
        """
        Dynamically imports and returns the 'run' function from a module.
        It also reloads the module on every call to support live code editing.
        """
        try:
            parent_dir = fn_name.split('__')[0]
            module_path = f'functions.{parent_dir}.{fn_name}.main'
            
            # Import the module
            fn_module = importlib.import_module(module_path)
            
            # Force a reload of the module to pick up any code changes.
            importlib.reload(fn_module)

            return getattr(fn_module, 'run')
        except (ModuleNotFoundError, AttributeError) as e:
            print(f"Error loading function '{fn_name}': {e}")
            print("Please ensure the function directory structure is correct (e.g., functions/parent/parent__func/main.py) and all necessary __init__.py files exist.")
            raise
