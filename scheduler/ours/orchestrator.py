"""
Unified Orchestrator: Complete serverless orchestration with Kubernetes integration,
configurable algorithms, data locality management, and remote storage support.
"""

import os
import json
import time
import logging
import requests
import subprocess
import concurrent.futures
from typing import Dict, List, Optional, Tuple, Any, Literal
from dataclasses import dataclass

# Core components
from .cost_models import CostModels, TaskProfile, NodeProfile
from .mode_classifier import ModeClassifier
from .scheduler import Scheduler
from .placement_optimizer import PlacementOptimizer
from .config import SystemConfig, ConfigPresets, get_config, load_config
from provider.data_manager import DataManager, DataMetadata, DataLocation, DataAccessRequest
from provider.remote_storage_adapter import create_storage_adapter, RemoteStorageAdapter


@dataclass
class WorkflowStep:
    """Represents a workflow step with data dependencies"""
    step_name: str
    function_name: str
    input_data_ids: List[str]
    output_data_id: str
    node_preference: Optional[str] = None
    dependencies: List[str] = None  # Step dependencies


class Orchestrator:
    """
    Unified orchestrator with complete serverless orchestration capabilities:
    - Basic function and workflow execution
    - Kubernetes integration for multi-node clusters
    - Configurable algorithms (cost models, scheduling, optimization)
    - Data transfer management
    - Performance metrics and logging
    """
    
    def __init__(self, scheduler=None, function_manager=None, config: Optional[SystemConfig] = None):
        # Basic orchestration
        self.scheduler = scheduler
        self.function_manager = function_manager
        self.workflows = {}
        self.workflow_results = {}
        
        # Configuration
        if config:
            self.config = config
        else:
            if not get_config():
                load_config()  # Defaults to 'development'
            self.config = get_config()
        
        # Kubernetes settings
        self.namespace = "serverless"
        
        # System state
        self.current_nodes: List[NodeProfile] = []
        self.current_tasks: List[TaskProfile] = []
        self.dag_structure: Dict = {}
        
        # Metrics and history
        self.scheduling_history: List[Dict] = []
        self.performance_metrics: Dict = {}
        
        # Setup logging and components
        self._setup_logging()
        self._initialize_components()
        self._setup_k8s_logging()
        self._initialize_data_locality()
    
    def _setup_logging(self):
        """Setup logging based on configuration"""
        if hasattr(self.config, 'enable_logging') and self.config.enable_logging:
            logging.basicConfig(
                level=getattr(logging, getattr(self.config, 'log_level', 'INFO')),
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logging.getLogger(__name__)
            self.logger.disabled = True
    
    def _setup_k8s_logging(self):
        """Setup logging for Kubernetes operations"""
        self.k8s_logger = logging.getLogger("scheduler.ours.orchestrator.k8s")
        self.locality_logger = logging.getLogger("scheduler.ours.orchestrator.data_locality")
    
    def _initialize_components(self):
        """Initialize orchestration components based on configuration"""
        try:
            # Initialize cost models
            self.cost_models = CostModels()
            if hasattr(self.config, 'cost_models'):
                self.cost_models.config = self.config.cost_models
            
            # Initialize mode classifier
            threshold = getattr(getattr(self.config, 'mode_classifier', None), 'threshold', 1.0)
            self.mode_classifier = ModeClassifier(threshold=threshold)
            if hasattr(self.config, 'mode_classifier'):
                self.mode_classifier.config = self.config.mode_classifier
            
            # Initialize placement optimizer
            self.placement_optimizer = PlacementOptimizer(self.cost_models)
            if hasattr(self.config, 'optimization'):
                self.placement_optimizer.config = self.config.optimization
                
        except Exception as e:
            self.logger.warning(f"Could not fully initialize components: {e}")
            # Use minimal defaults
            self.cost_models = CostModels()
            self.mode_classifier = ModeClassifier()
            self.placement_optimizer = PlacementOptimizer(self.cost_models)
    
    def _initialize_data_locality(self):
        """Initialize data locality management components"""
        try:
            data_locality_config = getattr(self.config, 'data_locality', {}) if hasattr(self.config, 'data_locality') else {}
            
            # Initialize remote storage using flexible configuration
            if 'config_object' in data_locality_config:
                # Use DataLocalitySystemConfig object
                system_config = data_locality_config['config_object']
                effective_storage_config = system_config.get_effective_storage_config()
                storage_type = effective_storage_config.type
                storage_config_dict = effective_storage_config.__dict__
            else:
                # Fallback to default configuration
                storage_config = data_locality_config.get('remote_storage', {})
                storage_type = storage_config.get('type', 'minio')  # Default to MinIO
                
                # Use MinIO with default setup if not specified
                if storage_type == 'minio' and not storage_config.get('minio_host'):
                    storage_config.update({
                        'minio_host': 'minio',
                        'minio_port': 9000,
                        'access_key': 'dataflower',
                        'secret_key': 'dataflower123',
                        'bucket_name': 'serverless-data'
                    })
                storage_config_dict = storage_config
            
            # Create remote storage adapter
            self.remote_storage_adapter = create_storage_adapter(storage_type, storage_config_dict)
            
            # Initialize data locality manager
            node_config = data_locality_config.get('node_config', {
                'node_id': self._get_node_id(),
                'cache_dir': '/tmp/serverless_cache',
                'max_cache_items': 1000,
                'max_disk_size_gb': 10,
                'max_memory_size_mb': 1024
            })
            
            self.data_manager = DataManager(node_config)
            self.data_manager.set_remote_storage_adapter(self.remote_storage_adapter)
            
            self.locality_logger.info("Data locality management initialized")
            
        except Exception as e:
            self.locality_logger.warning(f"Could not initialize data locality: {e}")
            self.data_manager = None
            self.remote_storage_adapter = None
    
    def _get_node_id(self) -> str:
        """Get current node ID"""
        try:
            # Try to get from K8s environment
            pod_name = os.environ.get('HOSTNAME', '')
            if pod_name:
                return pod_name
            
            # Fallback to hostname
            import socket
            return socket.gethostname()
        except:
            return 'default-node'
    
    # =================================================================
    # Basic Orchestration Methods
    # =================================================================

    def add_workflow(self, name, dag):
        """Register a workflow DAG"""
        self.workflows[name] = dag
        if self.function_manager:
            self.function_manager.register_workflow(name, dag)
        print(f"  - Workflow '{name}' registered with the orchestrator.")

    def run_function(self, function_name, request_id, body):
        """Execute a single function"""
        print(f"  - Orchestrator: Running function '{function_name}'...")
        if self.scheduler:
            worker = self.scheduler.schedule_function(function_name, request_id)
            if worker is None:
                return {'status': 'error', 'message': 'No available worker found by scheduler'}
            return self.send_request(worker, function_name, request_id, body)
        else:
            return {'status': 'error', 'message': 'No scheduler configured'}
    
    
    def send_request(self, worker: Dict, function_name: str, request_id: str, body: Any) -> Dict:
        host_port = worker['host_port']
        url = f'http://127.0.0.1:{host_port}/run?fn={function_name}'
        headers, data, json_payload = {}, None, None

        if isinstance(body, bytes):
            headers['Content-Type'] = 'application/octet-stream'
            data = body
        elif isinstance(body, dict):
            headers['Content-Type'] = 'application/json'
            json_payload = body
        else:
            return {'status': 'error', 'message': f'Unsupported payload type: {type(body)}'}

        try:
            print(f"    - Sending request to {url}...")
            response = requests.post(url, headers=headers, data=data, json=json_payload, timeout=300)
            response.raise_for_status()
            print(f"    - Received response from worker '{worker['name']}'.")
            return response.json()
        except requests.exceptions.RequestException as e:
            error_message = f"Failed to invoke function '{function_name}' on worker '{worker['name']}': {e}"
            print(f"    - Error: {error_message}")
            return {'status': 'error', 'message': error_message}

    def run_workflow(self, workflow_name, request_id, body):
        if workflow_name not in self.workflows:
            return 'Workflow not found', 400

        self.workflow_results[request_id] = {}
        dag = self.workflows[workflow_name]
        
        context = {'inputs': {'img': body}}
        
        final_context = self.execute_pipeline(dag.get('pipeline', []), context, dag.get('function_mapping', {}))

        # The execute_pipeline now returns the final context, so we just return it.
        # No need to clean up 'inputs' here as it's part of the complete record.
        return final_context

    def execute_pipeline(self, pipeline, context, function_mapping):
        """
        Executes a pipeline of functions defined in the DAG.
        """
        for step in pipeline:
            step_name = list(step.keys())[0]
            step_details = step.get(step_name)

            # Correctly handle the 'end' instruction
            if step_name == 'end' and step_details is True:
                print("  - Pipeline finished by 'end' instruction.")
                return context

            if step_name == 'parallel' and isinstance(step_details, list):
                print(f"  - Executing parallel block with {len(step_details)} steps concurrently...")
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    # It's crucial to pass a snapshot (copy) of the context to each thread
                    # to prevent race conditions during input resolution.
                    context_snapshot = context.copy()
                    future_to_step = {
                        executor.submit(self._execute_parallel_step, p_step, context_snapshot, function_mapping): p_step 
                        for p_step in step_details
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_step):
                        try:
                            step_name_completed, output = future.result()
                            # The 'output' is now the correctly processed dictionary
                            if output is not None:
                                # Update the main context in the main thread to ensure thread safety.
                                context[step_name_completed] = output
                                print(f"    - ✅ Result received for parallel step: '{step_name_completed}'")
                        except Exception as exc:
                            step_info = future_to_step[future]
                            print(f"    - ❌ Error in parallel step '{list(step_info.keys())[0]}': {exc}")
                
                continue # Finished with the parallel block, move to the next step in the pipeline

            if step_name == 'switch':
                print(f"  - Executing switch block on condition: \"{step_details.get('condition')}\"...")
                condition_str = step_details['condition'].replace('{{', '').replace('}}', '').strip()
                variable_names = [v.strip() for v in condition_str.split(' or ')]
                
                # Prepend '$' to treat condition parts as variables
                condition_result = any(self.resolve_variable(f'${var_name}', context) for var_name in variable_names)
                
                branch_to_execute = 'then' if condition_result else 'else'
                print(f"    - Condition evaluated to '{condition_result}', executing '{branch_to_execute}' branch.")
                
                self.execute_pipeline(step_details.get(branch_to_execute, []), context, function_mapping)
                continue

            # This is a normal function call
            print(f"  - Executing sequential step: '{step_name}'...")
            function_name = function_mapping.get(step_name, step_name)
            input_payload = self.prepare_function_input(step_details, context)
            
            # Run the function
            result = self.run_function(function_name, "request_id_placeholder", input_payload)
            
            # Handle 'out' mapping
            output_spec = step_details.get("out", {})
            
            if not output_spec:
                context[step_name] = result
            else:
                context[step_name] = {}
                # The worker's response has a 'result' key containing the actual function output.
                function_result_data = result.get("result", {})
                
                for out_key, out_path in output_spec.items():
                    # The out_path (e.g., "result.text") refers to keys within the function's own return value.
                    print(f"    - DEBUG: Resolving output '{out_key}' from path '{out_path}'")
                    
                    # Sanitize function_result_data for debug printing
                    debug_data = {}
                    for key, value in function_result_data.items():
                        if isinstance(value, bytes):
                            debug_data[key] = f"<bytes: {len(value)} bytes>"
                        elif isinstance(value, str) and len(value) > 100:
                            debug_data[key] = f"<string: {len(value)} chars>"
                        else:
                            debug_data[key] = value
                    print(f"    - DEBUG: function_result_data keys = {list(function_result_data.keys())}")
                    print(f"    - DEBUG: function_result_data = {debug_data}")
                    
                    resolved_value = self.resolve_variable(f"${out_path}", {"result": function_result_data})
                    
                    # Sanitize resolved value for debug printing
                    if isinstance(resolved_value, bytes):
                        print(f"    - DEBUG: resolved_value = <bytes: {len(resolved_value)} bytes>")
                    elif isinstance(resolved_value, str) and len(resolved_value) > 100:
                        print(f"    - DEBUG: resolved_value = <string: {len(resolved_value)} chars>")
                    else:
                        print(f"    - DEBUG: resolved_value = {resolved_value}")
                    
                    # Store the resolved value, e.g., context['extract_text']['text'] = "the extracted text"
                    context[step_name][out_key] = resolved_value

            print(f"    - ✅ Result received for sequential step: '{step_name}'")

        return context
    
    def _execute_parallel_step(self, step: Dict, context_snapshot: Dict, function_mapping: Dict) -> Tuple[str, Any]:
        """
        Executes a single step within a parallel block. Designed to be called from a thread.
        """
        step_name = list(step.keys())[0]
        step_details = step.get(step_name)

        function_name = function_mapping.get(step_name, step_name)
        # We use the context_snapshot to ensure thread-safe read operations for input preparation.
        input_payload = self.prepare_function_input(step_details, context_snapshot)
        
        # Using a placeholder request_id for now. This could be improved.
        output = self.run_function(function_name, "parallel_step_placeholder", input_payload)
        
        # Apply the 'out' mapping logic to parallel steps
        output_spec = step_details.get("out", {})
        
        if not output_spec:
            final_output = output
        else:
            final_output = {}
            function_result_data = output.get("result", {})
            for out_key, out_path in output_spec.items():
                resolved_value = self.resolve_variable(f"${out_path}", {"result": function_result_data})
                final_output[out_key] = resolved_value

        return step_name, final_output

    def resolve_variable(self, var_path: str, context: dict):
        """
        Resolves a variable path like '$inputs.img' or '$extract_text.result.text' from the context.
        """
        if not isinstance(var_path, str) or not var_path.startswith('$'):
            return var_path # Return literal values immediately

        clean_path = var_path.lstrip('$')
        parts = clean_path.split('.')
        
        # Start resolution from the correct object
        # The first part of the path is the key in the top-level context.
        current_val = context.get(parts[0])
        
        # Now, iterate through the rest of the path parts
        for part in parts[1:]:
            if isinstance(current_val, dict):
                current_val = current_val.get(part)
            else:
                # Cannot traverse further
                return None
        
        return current_val

    def prepare_function_input(self, step_details: dict, context: dict):
        """
        Prepares the input payload for a function based on the DAG definition.
        """
        if not step_details:
            return {} # Return empty dict if no details, not the whole context

        input_data = step_details.get('in')

        if input_data is None:
            return {} # Return empty dict if no input specified

        # SPECIAL CASE: If the input is a dictionary with a single value pointing
        # to the raw input bytes, we should send the bytes directly as the payload
        # with an 'application/octet-stream' content type.
        if isinstance(input_data, dict) and len(input_data) == 1:
            first_value = next(iter(input_data.values()))
            if first_value == '$inputs.img':
                return self.resolve_variable(first_value, context)

        if isinstance(input_data, str):
            # Case: "$prev.some_key" or "$inputs.some_key" or "step_name.key"
            return self.resolve_variable(input_data, context)
        elif isinstance(input_data, dict):
            # Case: a dictionary of variables to resolve
            payload = {}
            for key, value_path in input_data.items():
                payload[key] = self.resolve_variable(value_path, context)
            return payload

        # Fallback to an empty dictionary
        return {}

    
    # =================================================================
    # Kubernetes Integration Methods  
    # =================================================================
    
    def get_cluster_nodes(self) -> List[NodeProfile]:
        """Get current cluster nodes with resource information"""
        try:
            # Get nodes from kubectl
            result = subprocess.run(
                ['kubectl', 'get', 'nodes', '-o', 'json'],
                capture_output=True, text=True, check=True
            )
            
            nodes_data = json.loads(result.stdout)
            node_profiles = []
            
            for node in nodes_data['items']:
                node_name = node['metadata']['name']
                labels = self._get_node_labels(node_name)
                resources = self._get_node_resources(node_name)
                
                # Create node profile
                profile = NodeProfile(
                    node_id=node_name,
                    cpu_cores=resources.get('cpu', 0),
                    memory_gb=resources.get('memory', 0),
                    network_bandwidth_mbps=resources.get('network', 1000),  # Default
                    labels=labels
                )
                node_profiles.append(profile)
                
            self.k8s_logger.info(f"Retrieved {len(node_profiles)} cluster nodes")
            return node_profiles
            
        except subprocess.CalledProcessError as e:
            self.k8s_logger.error(f"Failed to get cluster nodes: {e}")
            return []
        except Exception as e:
            self.k8s_logger.error(f"Error parsing cluster nodes: {e}")
            return []
    
    def _get_node_labels(self, node_name: str) -> Dict[str, str]:
        """Get labels for a specific node"""
        try:
            result = subprocess.run(
                ['kubectl', 'get', 'node', node_name, '-o', 'json'],
                capture_output=True, text=True, check=True
            )
            node_data = json.loads(result.stdout)
            return node_data.get('metadata', {}).get('labels', {})
        except:
            return {}
    
    def _get_node_resources(self, node_name: str) -> Dict[str, float]:
        """Get resource capacity for a specific node"""
        try:
            result = subprocess.run(
                ['kubectl', 'get', 'node', node_name, '-o', 'json'],
                capture_output=True, text=True, check=True
            )
            node_data = json.loads(result.stdout)
            
            capacity = node_data.get('status', {}).get('capacity', {})
            
            return {
                'cpu': self._parse_cpu(capacity.get('cpu', '0')),
                'memory': self._parse_memory(capacity.get('memory', '0')),
                'storage': self._parse_memory(capacity.get('ephemeral-storage', '0'))
            }
        except:
            return {'cpu': 0, 'memory': 0, 'storage': 0}
    
    def _parse_cpu(self, cpu_str: str) -> float:
        """Parse CPU string to float cores"""
        if cpu_str.endswith('m'):
            return float(cpu_str[:-1]) / 1000
        return float(cpu_str)
    
    def _parse_memory(self, mem_str: str) -> float:
        """Parse memory string to GB"""
        if mem_str.endswith('Ki'):
            return float(mem_str[:-2]) / (1024 * 1024)
        elif mem_str.endswith('Mi'):
            return float(mem_str[:-2]) / 1024
        elif mem_str.endswith('Gi'):
            return float(mem_str[:-2])
        return float(mem_str) / (1024**3)
    
    def deploy_function(self, function_name: str, function_type: str, 
                       target_node: str, image_name: str = None) -> bool:
        """Deploy a function to a specific Kubernetes node"""
        try:
            image = image_name or f"dataflower/{function_name}:latest"
            
            # Apply function deployment
            cmd = ['kubectl', 'apply', '-f', '-']
            deployment_yaml = f"""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {function_name}
  namespace: {self.namespace}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: {function_name}
  template:
    metadata:
      labels:
        app: {function_name}
        function-type: {function_type}
    spec:
      nodeSelector:
        kubernetes.io/hostname: {target_node}
      containers:
      - name: {function_name}
        image: {image}
        ports:
        - containerPort: 8080
"""
            
            result = subprocess.run(cmd, input=deployment_yaml, text=True, capture_output=True)
            if result.returncode == 0:
                self.k8s_logger.info(f"Deployed function {function_name} to node {target_node}")
                return True
            else:
                self.k8s_logger.error(f"Failed to deploy function: {result.stderr}")
                return False
                
        except Exception as e:
            self.k8s_logger.error(f"Error deploying function {function_name}: {e}")
            return False
    
    def get_running_pods(self) -> List[Dict]:
        """Get currently running pods in the serverless namespace"""
        try:
            result = subprocess.run(
                ['kubectl', 'get', 'pods', '-n', self.namespace, '-o', 'json'],
                capture_output=True, text=True, check=True
            )
            return json.loads(result.stdout).get('items', [])
        except:
            return []
    
    def scale_function(self, function_name: str, replicas: int) -> bool:
        """Scale a function to specified number of replicas"""
        try:
            result = subprocess.run([
                'kubectl', 'scale', 'deployment', function_name, 
                f'--replicas={replicas}', '-n', self.namespace
            ], capture_output=True, text=True)
            
            return result.returncode == 0
        except:
            return False
    
    def cleanup_function(self, function_name: str) -> bool:
        """Remove a deployed function from Kubernetes"""
        try:
            result = subprocess.run([
                'kubectl', 'delete', 'deployment', function_name, '-n', self.namespace
            ], capture_output=True, text=True)
            
            return result.returncode == 0
        except:
            return False
    
    # =================================================================
    # Data Locality Methods (without CacheManager and DataLocalityScheduler)
    # =================================================================
    
    def execute_workflow(self, workflow_steps: List[WorkflowStep],
                        available_nodes: List[NodeProfile] = None) -> Dict[str, str]:
        """Execute workflow with automatic data locality handling (local first, remote fallback)"""
        if not self.data_manager:
            self.locality_logger.warning("Data locality not initialized")
            return {}
        
        assignments = {}
        step_outputs = {}
        
        try:
            # Get available nodes
            if not available_nodes:
                available_nodes = self.get_cluster_nodes()
            
            if not available_nodes:
                self.locality_logger.warning("No nodes available")
                return {}
            
            # Execute workflow steps in dependency order
            for step in workflow_steps:
                # Simple node selection (user can override this logic)
                target_node = step.node_preference or available_nodes[0].node_id
                
                # Execute the step
                success, output_data_id = self.execute_workflow_step(step, target_node, step_outputs)
                
                if success and output_data_id:
                    assignments[step.step_name] = target_node
                    step_outputs[step.output_data_id] = output_data_id
                    self.locality_logger.info(f"Step {step.step_name} executed on {target_node}")
                else:
                    self.locality_logger.error(f"Step {step.step_name} failed")
                    return {}
            
            return assignments
            
        except Exception as e:
            self.locality_logger.error(f"Error in workflow execution: {e}")
            return {}
    
    def execute_workflow_step(self, step: WorkflowStep, target_node: str,
                            previous_outputs: Dict[str, str] = None) -> Tuple[bool, str]:
        """Execute a single workflow step on target node"""
        try:
            self.locality_logger.info(f"Executing step {step.step_name} on node {target_node}")
            
            # Ensure input data is available (local first, remote fallback)
            input_ready = self._ensure_input_data_available(step, target_node)
            if not input_ready:
                self.locality_logger.error(f"Failed to ensure input data availability for {step.step_name}")
                return False, None
            
            # Execute the function
            result = self._execute_function_on_node(step, target_node, previous_outputs)
            if result is None:
                return False, None
            
            # Store the output
            output_data_id = self._store_step_output(step, result, target_node)
            if not output_data_id:
                return False, None
            
            return True, output_data_id
            
        except Exception as e:
            self.locality_logger.error(f"Error executing step {step.step_name}: {e}")
            return False, None
    
    def _ensure_input_data_available(self, step: WorkflowStep, target_node: str) -> bool:
        """Ensure input data is available - prioritize local storage, fallback to remote"""
        if not step.input_data_ids or not self.data_manager:
            return True
        
        try:
            for data_id in step.input_data_ids:
                # Create data access request
                request = DataAccessRequest(
                    data_id=data_id,
                    requesting_function=step.function_name,
                    requesting_node=target_node,
                    access_type="read",
                    preferred_location=DataLocation.LOCAL_ONLY  # Try local first
                )
                
                # Try to get data locally first
                data, location = self.data_manager.get_data(request)
                
                if data is not None:
                    self.locality_logger.info(f"Data {data_id} found locally at {location}")
                    continue
                
                # If not found locally, try remote storage
                self.locality_logger.info(f"Data {data_id} not found locally, checking remote storage")
                request.preferred_location = DataLocation.REMOTE_ONLY
                
                data, location = self.data_manager.get_data(request)
                
                if data is not None:
                    self.locality_logger.info(f"Data {data_id} found remotely at {location}")
                    continue
                
                # Data not found anywhere
                self.locality_logger.warning(f"Data {data_id} not found in local or remote storage for step {step.step_name}")
                return False
            
            return True
            
        except Exception as e:
            self.locality_logger.error(f"Error ensuring data availability: {e}")
            return False
    
    def _execute_function_on_node(self, step: WorkflowStep, target_node: str, 
                                previous_outputs: Dict[str, str] = None) -> Optional[Any]:
        """Execute function on specified node"""
        try:
            # Prepare function input
            function_input = {}
            if step.input_data_ids and self.data_manager:
                for data_id in step.input_data_ids:
                    # Try local first, then remote
                    request = DataAccessRequest(
                        data_id=data_id,
                        requesting_function=step.function_name,
                        requesting_node=target_node,
                        access_type="read",
                        preferred_location=DataLocation.LOCAL_ONLY
                    )
                    
                    data, location = self.data_manager.get_data(request)
                    
                    # If not found locally, try remote
                    if data is None:
                        request.preferred_location = DataLocation.REMOTE_ONLY
                        data, location = self.data_manager.get_data(request)
                    
                    if data is not None:
                        function_input[data_id] = data
                        self.locality_logger.debug(f"Loaded data {data_id} from {location} for function execution")
            
            # Add previous outputs if available
            if previous_outputs:
                function_input.update(previous_outputs)
            
            # Execute function (simplified - can be extended with actual K8s execution)
            result = self.run_function(step.function_name, f"workflow_{step.step_name}", function_input)
            
            return result
            
        except Exception as e:
            self.locality_logger.error(f"Error executing function {step.function_name}: {e}")
            return None
    
    def _store_step_output(self, step: WorkflowStep, execution_result: Any, node_id: str) -> str:
        """Store step output locally (with automatic remote backup if configured)"""
        try:
            if not self.data_manager or not step.output_data_id:
                return None
            
            # Create metadata for the output
            metadata = DataMetadata(
                data_id=step.output_data_id,
                node_id=node_id,
                function_name=step.function_name,
                step_name=step.step_name,
                size_bytes=len(str(execution_result)),  # Rough estimate
                created_time=time.time(),
                last_accessed=time.time(),
                access_count=0,
                data_type="output",
                dependencies=step.input_data_ids or [],
                cache_priority=3
            )
            
            # Store the data (DataLocalityManager handles local storage first, remote backup automatically)
            success = self.data_manager.store_data(
                step.output_data_id, 
                execution_result, 
                metadata
            )
            
            if success:
                self.locality_logger.info(f"Stored output {step.output_data_id} locally for step {step.step_name}")
                return step.output_data_id
            else:
                self.locality_logger.error(f"Failed to store output {step.output_data_id}")
                return None
                
        except Exception as e:
            self.locality_logger.error(f"Error storing step output: {e}")
            return None
    
    def get_cluster_data_locality_status(self) -> Dict[str, Any]:
        """Get data locality status across the cluster"""
        if not self.data_manager:
            return {'error': 'Data locality not initialized'}
        
        return {
            'node_id': self._get_node_id(),
            'data_locality_info': self.data_manager.get_data_locality_info(),
            'remote_storage_type': type(self.remote_storage_adapter).__name__ if self.remote_storage_adapter else 'None',
            'timestamp': time.time()
        }
    
    def cleanup_data_locality(self):
        """Cleanup data locality resources"""
        try:
            if self.data_manager:
                self.data_manager.cleanup()
                self.locality_logger.info("Data locality resources cleaned up")
        except Exception as e:
            self.locality_logger.error(f"Error cleaning up data locality: {e}")


# =================================================================
# Utility Functions
# =================================================================

def create_mock_workflow(task_count: int = 3) -> List[TaskProfile]:
    """Create mock workflow tasks for testing"""
    tasks = []
    for i in range(task_count):
        task = TaskProfile(
            task_id=f"task_{i}",
            cpu_requirement=1.0 + i * 0.5,
            memory_requirement_gb=2.0 + i * 1.0,
            estimated_duration_sec=10.0 + i * 5.0,
            dependencies=[f"task_{j}" for j in range(i) if j < i]
        )
        tasks.append(task)
    return tasks


def create_mock_nodes(node_count: int = 2) -> List[NodeProfile]:
    """Create mock nodes for testing"""
    nodes = []
    for i in range(node_count):
        node = NodeProfile(
            node_id=f"node_{i}",
            cpu_cores=4.0 + i * 2.0,
            memory_gb=8.0 + i * 4.0,
            network_bandwidth_mbps=1000 + i * 500
        )
        nodes.append(node)
    return nodes
