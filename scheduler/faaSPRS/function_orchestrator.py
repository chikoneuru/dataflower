import logging
import traceback
import time
from typing import Any, Dict, Optional, List, Tuple
from contextlib import contextmanager
import requests

from functions.dag_loader import get_parent_tasks, get_dag_analysis, _build_dependencies_from_dag
from provider.remote_storage_adapter import create_storage_adapter
from provider.function_container_manager import FunctionContainerManager
from scheduler.faaSPRS import config as faasprs_config
from scheduler.faaSPRS.placement import Placement
from scheduler.faaSPRS.routing_lp import RoutingLP
from scheduler.shared import DAGUtils, OrchestratorUtils
from scheduler.shared.dag_executor import DAGExecutor
from scheduler.shared import ContainerStorageService, UnifiedInputHandler, create_storage_service, create_input_handler
from scheduler.shared.api import (
    Workflow, PlacementPlan, RoutingPlan,
    WorkflowContext, InvocationResult,
    build_invocation_result_from_dict,
)


class FunctionOrchestrator:
    """Minimal FaasPRS orchestrator interface used by the experiment runner.

    Exposes:
    - register_workflow(name, dag)
    - submit_workflow(name, request_id, input_data)
    - invoke_function(function_name, request_id, payload)
    """

    def __init__(self, container_manager=None) -> None:
        # Config and logging
        if faasprs_config.get_config() is None:
            faasprs_config.initialize_config()
        self.config = faasprs_config.get_config()
        self._setup_logging()

        # Core state
        self._workflows: Dict[str, Dict[str, Any]] = {}
        self.workflows: Dict[str, Dict[str, Any]] = {}
        self.workflow_results: Dict[str, Any] = {}
        self.active_workflows: Dict[str, Dict[str, Any]] = {}
        self.request_timelines: Dict[str, Dict[str, Any]] = {}
        self._lp_plan_by_workflow: Dict[str, Dict[str, Dict[str, float]]] = {}
        self._placement_by_workflow: Dict[str, Dict[str, str]] = {}
        self._workflow_for_request: Dict[str, str] = {}
        self._init_overhead_by_workflow: Dict[str, Dict[str, float]] = {}

        # Misc settings (not strategy-specific)
        self.namespace = "serverless"
        self.auto_scaling_enabled = True
        self.min_containers_per_function = 1
        self.max_containers_per_function = 5

        # Use shared container manager if provided, otherwise create new one
        if container_manager is not None:
            self.function_container_manager = container_manager
        else:
            self.function_container_manager = FunctionContainerManager()

        # Data locality / storage adapter (generic IO)
        self._initialize_data_locality()

        # Internal placement and routing engines (not exposed publicly)
        self._placement_engine = Placement()
        self._routing_engine = RoutingLP()

        # Create DAGExecutor for input preparation (but not for execution)
        self._dag_executor = DAGExecutor(self)

        # Initialize storage service for remote data transfer
        self.storage_service = create_storage_service('faaSPRS')
        self.input_handler = create_input_handler('faaSPRS')

        # Cross-scheduler API state: global plans computed once
        self._global_placement_plan: Dict[str, Any] = {}
        self._global_routing_plan: Dict[str, Any] = {}

    @contextmanager
    def _time_operation(self, timeline: Dict, operation_name: str):
        """Context manager to automatically time operations and store in timeline"""
        start_time = time.time()
        try:
            yield
        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000.0
            # Ensure timeline is a dictionary before assignment
            if timeline is not None and isinstance(timeline, dict):
                timeline[f'{operation_name}_start'] = start_time
                timeline[f'{operation_name}_end'] = end_time
                timeline[f'{operation_name}_ms'] = duration_ms
        # FaasPRS doesn't use the shared DAGExecutor - it has its own execution strategy

    @contextmanager
    def _time_init_operation(self, operation_name: str):
        """Context manager to time initialization operations and return duration"""
        start_time = time.time()
        try:
            yield
        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000.0
            # Store the duration for later use
            if not hasattr(self, '_current_init_timings'):
                self._init_timings = {}
            self._init_timings[operation_name] = duration_ms

    @contextmanager
    def _time_scheduling_operation(self, operation_name: str):
        """Context manager to time scheduling operations and return duration"""
        start_time = time.time()
        try:
            yield
        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000.0
            # Store the duration for later use
            if not hasattr(self, '_current_scheduling_timings'):
                self._scheduling_timings = {}
            self._scheduling_timings[operation_name] = duration_ms

    @contextmanager
    def _time_overhead_operation(self, timeline: Dict, operation_name: str):
        """Context manager to time orchestrator overhead operations outside main wrappers."""
        start_time = time.time()
        try:
            yield
        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000.0
            timeline[f'{operation_name}_start'] = start_time
            timeline[f'{operation_name}_end'] = end_time
            timeline[f'{operation_name}_ms'] = duration_ms

    # Utilities -----------------------------------------------------------
    def _get_or_create_timeline(self, request_id: str) -> Dict[str, Any]:
        tl = self.request_timelines.get(request_id)
        if tl is None:
            tl = {}
            self.request_timelines[request_id] = tl
        return tl

    def _setup_logging(self) -> None:
        cfg = faasprs_config.get_config()
        enable = getattr(cfg, 'enable_logging', True)
        level_name = getattr(cfg, 'log_level', 'INFO')
        level = getattr(logging, level_name, logging.INFO)
        logging.basicConfig(level=level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        if not enable:
            self.logger.disabled = True

    def _initialize_data_locality(self) -> None:
        # Use centralized MinIO configuration
        from scheduler.shared.minio_config import get_minio_config
        storage_config = get_minio_config()
        storage_config['type'] = 'minio'
        storage_config['bucket_name'] = 'dataflower-storage'  # Use consistent bucket name
        
        try:
            self.remote_storage_adapter = create_storage_adapter('minio', storage_config)
        except Exception:
            self.remote_storage_adapter = None

    def _debug(self, message: str) -> None:
        try:
            self.logger.debug(message)
        except Exception as e:
            print(f"\033[31m ERROR: {e}\033[0m")
            traceback.print_exc()


    # Workflow management -------------------------------------------------
    def register_workflow(self, workflow_name: str, workflow_dag: Dict[str, Any]) -> None:
        self._workflows[workflow_name] = workflow_dag
        self.workflows[workflow_name] = workflow_dag

        # Initialize timing storage for this workflow registration
        self._init_timings = {}

        # Compute placement and LP plan ONCE at setup time
        with self._time_init_operation('placement'):
            # Use shared node resource discovery across schedulers
            nodes_resources = self.function_container_manager.get_all_nodes_resources()
            server_list = list(nodes_resources.keys())
            resources = {s: {"capacity": max(1, int(nodes_resources[s].get("available_cpu", 1)))} for s in server_list}
            last_P: Dict[str, Dict[str, int]] = {}
            placement = self._placement_engine.initial_placement(
                workflow_dag=workflow_dag,
                available_nodes=server_list,
                context={"last_P": last_P, "resources": resources},
            )
            self._placement_by_workflow[workflow_name] = placement

        # Pre-compute routing LP plan
        with self._time_init_operation('routing'):
            durations = {n.get("id"): 1.0 for n in (workflow_dag.get("workflow", {}).get("nodes", [])) if n.get("type") == "task"}
            nodes_capacity = {s: float(nodes_resources.get(s, {}).get("available_cpu", 1.0)) for s in server_list}
            lp_plan = self._routing_engine.compute_workflow_lp(
                workflow_dag=workflow_dag,
                placement=placement,
                nodes_capacity=nodes_capacity,
                durations=durations,
            )
            self._lp_plan_by_workflow[workflow_name] = lp_plan
        
        # Record init overhead with timing from context managers
        self._init_overhead_by_workflow[workflow_name] = {
            "lp_precompute_ms": self._init_timings.get('placement', 0.0),
            "route_ms": self._init_timings.get('routing', 0.0),
        }
    
    def submit_workflow(self, workflow_name: str, request_id: str, body: Any) -> Dict:
        """Execute a complete workflow"""
        self._debug(f"run_workflow called for {workflow_name} with request_id {request_id}")
        if workflow_name not in self._workflows:
            return {'status': 'error', 'message': 'Workflow not found'}

        self.logger.info(f"Running workflow '{workflow_name}' (request: {request_id})")

        # Store current workflow request ID for function calls
        self._current_workflow_request_id = request_id

        dag = self._workflows[workflow_name]
        context = {'inputs': {'img': body}}
        
        # Store workflow in active_workflows for dependency resolution
        self.active_workflows[workflow_name] = {
            'request_id': request_id,
            'dag': dag,
            'start_time': time.time()
        }

        # Workflow-level timing
        tl = self._get_or_create_timeline(request_id)
        tl.setdefault('workflow', {})['start'] = time.time()

        try:
            if 'workflow' not in dag:
                raise Exception('Invalid DAG structure: missing workflow')
            self._debug("Executing DAG workflow")
            final_context = self.schedule_workflow(dag, context, request_id)

            # Attach any submission-time overheads if FaasPRS precomputes them
            try:
                lp_ms = float(self._init_overhead_by_workflow.get(workflow_name, {}).get('lp_precompute_ms', 0.0))
                if lp_ms > 0:
                    sched_block = tl.setdefault('scheduler', {})
                    sched_block['lp_precompute_ms'] = sched_block.get('lp_precompute_ms', 0.0) + lp_ms
            except Exception as e:
                print(f"\033[31m ERROR: {e}\033[0m")
                traceback.print_exc()

            tl['workflow']['end'] = time.time()
            tl['workflow']['duration_ms'] = (tl['workflow']['end'] - tl['workflow']['start']) * 1000.0
            final_context = final_context or {}
            final_context['timeline'] = tl
            self.logger.info(f"Workflow '{workflow_name}' completed successfully")
            
            # Cleanup: remove workflow from active_workflows
            if workflow_name in self.active_workflows:
                del self.active_workflows[workflow_name]
            
            return final_context

        except Exception as e:
            error_message = f"Workflow '{workflow_name}' failed: {e}"
            self.logger.error(error_message)
            
            # Cleanup: remove workflow from active_workflows even on error
            if workflow_name in self.active_workflows:
                del self.active_workflows[workflow_name]
            
            return {'status': 'error', 'message': error_message}

    def schedule_workflow(self, dag: Dict, context: Dict, request_id: str) -> Dict:
        """
        FaasPRS DAG scheduling: Walk through nodes in topological order.
        At each node: look up precomputed LP routing plan, choose container (weighted random by o[i]), 
        and call invoke_function.
        """
        workflow = dag['workflow']
        nodes = workflow.get('nodes', [])
        edges = workflow.get('edges', [])
        
        self._debug(f"FaasPRS DAG workflow has {len(nodes)} nodes and {len(edges)} edges")
        
        # Initialize timeline
        tl_root = self._get_or_create_timeline(request_id)
        tl_root['timestamps'] = tl_root.get('timestamps', {})
        
        # Debug timeline state
        self._debug(f"Timeline root type: {type(tl_root)}, timestamps type: {type(tl_root.get('timestamps'))}")
        
        # Get workflow name for LP plan lookup
        workflow_name = None
        for name, wf_dag in self._workflows.items():
            if wf_dag == dag:
                workflow_name = name
                break
        
        if not workflow_name:
            workflow_name = f"workflow_{request_id}"
        
        # Get precomputed LP plan for this workflow
        # lp_plan = self._lp_plan_by_workflow.get(workflow_name, {})
        # placement = self._placement_by_workflow.get(workflow_name, {})
        
        # Extract task nodes and build topological order using shared utilities
        task_ids = DAGUtils.extract_task_ids(dag)
        
        if not task_ids:
            self.logger.warning("No task nodes found in workflow")
            return context
        
        # Build topological order using shared DAG utilities
        topological_order = DAGUtils.build_topological_order(dag, task_ids)
        self._debug(f"Topological order: {topological_order}")
        
        # Ensure topological_order is not None
        if topological_order is None:
            self.logger.error("Topological order is None, using task_ids as fallback")
            topological_order = task_ids
        
        # Time the entire workflow orchestration
        timestamps_dict = tl_root.get('timestamps', {})
        if timestamps_dict is None:
            timestamps_dict = {}
            tl_root['timestamps'] = timestamps_dict
        
        with self._time_operation(timestamps_dict, 'workflow_orchestration'):
            # Build execution graph for input preparation
            execution_graph = self._dag_executor._build_execution_graph(nodes, edges)
            
            # Walk through nodes in topological order
            final_context = context.copy()
            node_results = {}  # Track results for input preparation
            
            for task_id in topological_order:
                # Find the node definition using shared DAG utilities
                task_node = DAGUtils.find_task_node_by_id(dag, task_id)
                
                if not task_node:
                    self.logger.warning(f"Task node {task_id} not found")
                    continue
                
                function_name = DAGUtils.get_function_name_from_task(task_node)
                
                # Prepare input data using DAGExecutor's sophisticated method
                input_data = self._dag_executor.prepare_input_for_node(task_id, execution_graph, final_context, node_results)
                
                # Debug: Log what input_data we're getting
                self.logger.debug(f"Function {task_id} input_data type: {type(input_data)}")
                if isinstance(input_data, dict):
                    self.logger.debug(f"Function {task_id} input_data keys: {list(input_data.keys())}")
                
                # Check if this function has dependencies and needs remote storage
                dependencies = execution_graph.get('dependencies', {}).get(task_id, [])
                self.logger.debug(f"Function {task_id} dependencies: {dependencies}")
                
                if dependencies and input_data is not None:
                    # Function has dependencies - it should retrieve input from previous function's output
                    # The input_data contains the output from dependency functions
                    # We need to create a storage context for remote input retrieval
                    try:
                        # The storage key should be based on the dependency function's output
                        # We need to determine which dependency provides the data this function needs
                        if dependencies:
                            # For now, use the first dependency to determine the storage key
                            # TODO: This should be improved to match the specific input requirements
                            dependency = dependencies[0]  # Use first dependency
                            storage_key = f"{request_id}_{dependency}_output"
                            
                            # Special handling for specific functions based on their input requirements
                            if task_id == 'recognizer__extract':
                                # recognizer__extract needs image data from recognizer__upload
                                if 'recognizer__upload' in dependencies:
                                    dependency = 'recognizer__upload'
                                    storage_key = f"{request_id}_{dependency}_output"
                            elif task_id == 'recognizer__translate':
                                # recognizer__translate needs text data from recognizer__extract
                                if 'recognizer__extract' in dependencies:
                                    dependency = 'recognizer__extract'
                                    storage_key = f"{request_id}_{dependency}_output"
                            elif task_id == 'recognizer__censor':
                                # recognizer__censor needs translated text from recognizer__translate
                                if 'recognizer__translate' in dependencies:
                                    dependency = 'recognizer__translate'
                                    storage_key = f"{request_id}_{dependency}_output"
                            elif task_id == 'recognizer__mosaic':
                                # recognizer__mosaic needs image data from recognizer__upload
                                if 'recognizer__upload' in dependencies:
                                    dependency = 'recognizer__upload'
                                    storage_key = f"{request_id}_{dependency}_output"
                            
                            # Set input_data to None - the function will retrieve from storage
                            input_data = None
                            
                            self.logger.debug(f"Function {task_id} will retrieve input from storage key: {storage_key}")
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to create storage context for {task_id}: {e}")
                        # Continue with original input_data
                
                # Bind workflow mapping for this request so invoke_function uses LP plan
                self._workflow_for_request[request_id] = workflow_name
                
                # Invoke function using precomputed LP routing
                try:
                    result = self.invoke_function(function_name, request_id, input_data)
                    
                    # Store result in context for downstream functions
                    final_context[task_id] = result
                    # Also store in node_results for input preparation
                    node_results[task_id] = result
                    
                    # Check for errors
                    if result.get('status') == 'error':
                        self.logger.error(f"Function {function_name} failed: {result.get('message')}")
                        # Continue execution but mark error in context
                        final_context[f"{task_id}_error"] = result.get('message')
                    
                except Exception as e:
                    error_msg = f"Failed to invoke function {function_name}: {e}"
                    self.logger.error(error_msg)
                    final_context[f"{task_id}_error"] = error_msg
                    # Store error result in node_results as well
                    node_results[task_id] = {'status': 'error', 'message': error_msg}
        
        return final_context or context

    def _determine_storage_context_for_function(self, function_name: str, request_id: str) -> Optional[Dict]:
        """Determine storage context for function invocation based on DAG dependencies"""
        try:
            # Get the workflow request ID
            workflow_request_id = getattr(self, '_current_workflow_request_id', None)
            if not workflow_request_id:
                self.logger.warning(f"No active workflow found for function {function_name}")
                return None
            
            # Find the active workflow
            active_workflow = None
            workflow_name = None
            
            # First try to find by exact request_id match
            for wf_name, wf_info in self.active_workflows.items():
                if wf_info.get('request_id') == workflow_request_id:
                    active_workflow = wf_info
                    workflow_name = wf_name
                    break
            
            # If not found by exact match, try prefix matching
            if not active_workflow:
                for wf_name, wf_info in self.active_workflows.items():
                    if workflow_request_id.startswith(f"{wf_name}_"):
                        active_workflow = wf_info
                        workflow_name = wf_name
                        break
            
            if not active_workflow:
                self.logger.warning(f"No active workflow found for request {workflow_request_id}")
                return None
            
            # Get the workflow DAG
            workflow_dag = active_workflow.get('dag', {})
            if not workflow_dag:
                self.logger.warning(f"No DAG found in active workflow {workflow_name}")
                return None
            
            # Analyze dependencies using dag_loader
            dependencies = get_parent_tasks(function_name, workflow_dag)
            
            # If function has dependencies, determine input key from previous function outputs
            if dependencies:
                # Find the input key from the most recent dependency
                input_key = self._determine_input_key_for_function(function_name, workflow_request_id, dependencies)
                if input_key:
                    # Create storage context for remote retrieval
                    from scheduler.shared.minio_config import get_minio_storage_config
                    storage_config = get_minio_storage_config()
                    
                    storage_context = {
                        'src': 'remote',
                        'input_key': input_key,
                        'storage_config': storage_config
                    }
                    
                    self.logger.info(f"Function {function_name} will retrieve input from storage: {input_key}")
                    return storage_context
            
            # No dependencies or no input key found - use direct data passing
            self.logger.info(f"Function {function_name} receiving input via direct data passing")
            return None
            
        except Exception as e:
            self.logger.error(f"Error determining storage context for {function_name}: {e}")
            return None

    def _determine_input_key_for_function(self, function_name: str, workflow_request_id: str, dependencies: List[str]) -> Optional[str]:
        """Determine the input key for a function based on its dependencies"""
        try:
            if dependencies:
                # Use the first dependency as default
                dependency = dependencies[0]
                input_key = f"{workflow_request_id}_{dependency}_output"
                
                # Special handling for specific functions based on their input requirements
                if function_name == 'recognizer__extract':
                    # recognizer__extract needs image data from recognizer__upload
                    if 'recognizer__upload' in dependencies:
                        dependency = 'recognizer__upload'
                        input_key = f"{workflow_request_id}_{dependency}_output"
                elif function_name == 'recognizer__translate':
                    # recognizer__translate needs text data from recognizer__extract
                    if 'recognizer__extract' in dependencies:
                        dependency = 'recognizer__extract'
                        input_key = f"{workflow_request_id}_{dependency}_output"
                elif function_name == 'recognizer__censor':
                    # recognizer__censor needs translated text from recognizer__translate
                    if 'recognizer__translate' in dependencies:
                        dependency = 'recognizer__translate'
                        input_key = f"{workflow_request_id}_{dependency}_output"
                elif function_name == 'recognizer__mosaic':
                    # recognizer__mosaic needs image data from recognizer__upload
                    if 'recognizer__upload' in dependencies:
                        dependency = 'recognizer__upload'
                        input_key = f"{workflow_request_id}_{dependency}_output"
                
                return input_key
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error determining input key for {function_name}: {e}")
            return None
    
    def invoke_function(self, function_name: str, request_id: str, body: bytes, node: Optional[str] = None, storage_context: Optional[Dict] = None) -> Dict[str, Any]:
        # Create per-function timeline and attach to workflow timeline root
        func_tl: Dict[str, Any] = {"function": function_name, "request_id": request_id, "timestamps": {}}

        # OVERHEAD 1: Timeline setup and context propagation
        with self._time_overhead_operation(func_tl['timestamps'], 'timeline_setup'):
            # Attach into workflow-level container list early
            try:
                tl_root = self._get_or_create_timeline(request_id)
                tl_root.setdefault('functions', []).append(func_tl)
            except Exception as e:
                self._debug(f"Error adding function to timeline: {e}")

        # OVERHEAD 2: Container discovery and filtering with capacity enforcement
        with self._time_overhead_operation(func_tl['timestamps'], 'container_discovery'):
            # Determine workflow context
            workflow_name = self._workflow_for_request.get(request_id)
            
            # Select FaasPRS function-specific container for routing
            container = None
            try:
                available = self.function_container_manager.get_running_containers(function_name)
            except Exception:
                available = []
            function_containers = []
            for c in available:
                try:
                    host_port = getattr(c, 'host_port', None)
                    node_id = getattr(c, 'node_id', 'unknown')
                    if host_port:
                        function_containers.append({
                            'id': getattr(c, 'container_id', None),
                            'name': getattr(c, 'name', None),
                            'function_name': function_name,
                            'node_id': node_id,
                            'host_port': int(host_port),
                            'container': c,
                            'type': 'function'
                        })
                except Exception as e:
                    print(f"\033[31m ERROR: {e}\033[0m")
                    continue
            
            # CAPACITY ENFORCEMENT: If no containers available, try to deploy new one
            if not function_containers:
                self.logger.info(f"No running containers found for {function_name}, attempting to deploy new container")
                
                # Get function resource requirements (use defaults if not available)
                cpu_requirement = self.function_container_manager.default_cpu_limit
                memory_requirement = self.function_container_manager.default_memory_limit
                
                # Find best node for deployment
                available_nodes = self.function_container_manager._find_nodes_with_capacity(cpu_requirement, memory_requirement)
                if available_nodes:
                    best_node = available_nodes[0]  # Use first available node
                    new_container = self.function_container_manager.deploy_function_container(
                        function_name, best_node, cpu_limit=cpu_requirement, memory_limit=memory_requirement
                    )
                    if new_container:
                        # Add new container to function_containers list
                        function_containers.append({
                            'id': new_container.container_id,
                            'name': getattr(new_container, 'name', None),
                            'function_name': function_name,
                            'node_id': new_container.node_id,
                            'host_port': new_container.port,
                            'container': new_container,
                            'type': 'function'
                        })
                        self.logger.info(f"Successfully deployed new container for {function_name} on {best_node}")
                    else:
                        self.logger.warning(f"Failed to deploy container for {function_name} - insufficient capacity")
                else:
                    self.logger.warning(f"No nodes available with sufficient capacity for {function_name}")

        # Select node via LP plan if not provided
        with self._time_scheduling_operation('routing'):
            if node is None:
            
                lp_plan = self._lp_plan_by_workflow.get(workflow_name or "", {})
                placement = self._placement_by_workflow.get(workflow_name or "", {})
                candidates = self._routing_engine.get_feasible_nodes(function_name, lp_plan) or ([placement.get(function_name)] if function_name in placement else [])
                candidates = [c for c in candidates if c]
                node, routing_diag = self._routing_engine.select_node_from_lp(function_name, candidates, lp_plan, deterministic=True)
            else:
                routing_diag = {"strategy": "preselected"}
            
        # OVERHEAD 3: Container selection logic and routing preparation
        with self._time_overhead_operation(func_tl['timestamps'], 'container_selection'):
            if node and function_containers:
                node_candidates = [c for c in function_containers if c['node_id'] == node]
                if node_candidates:
                    container = node_candidates[0]  # Use first available
                    node = container['node_id']
            
            # If none on specific node, use any available function container
            if container is None and function_containers:
                container = function_containers[0]  # Use first available
                node = container['node_id']

        # OVERHEAD 4: Request preparation and resource monitoring setup
        with self._time_overhead_operation(func_tl['timestamps'], 'request_preparation'):
            # Send request to container and record timings
            if not container:
                func_tl['status'] = 'error'
                func_tl['message'] = f"No running container available for {function_name} on node {node} (candidates={candidates})"
                func_tl['error_message'] = func_tl['message']
                func_tl['node'] = node
                func_tl['routing'] = routing_diag
                # Record into root timeline once
                root_id = getattr(self, "_current_workflow_request_id", request_id)
                fn_req_id = f"{root_id}_{function_name}"
                self._record_function_timeline(root_id, function_name, fn_req_id, func_tl)
                return {
                    "status": "error",
                    "message": func_tl['message'],
                    "function": function_name,
                    "request_id": request_id,
                    "node": node,
                    "container_id": None,
                    "timeline": func_tl,
                }

        # OVERHEAD 5: Network client send/receive (timed within _send_request_to_container)
        start_time = time.time()
        try:
            # Determine storage context for function invocation
            storage_context = self._determine_storage_context_for_function(function_name, request_id)
            response = self._send_request_to_container(container, function_name, body, func_tl, storage_context)
            
            # OVERHEAD 6: Result processing and metrics recording
            with self._time_overhead_operation(func_tl['timestamps'], 'result_processing'):
                # Record routing result for metrics
                response_time = time.time() - start_time
                success = response.get('status') != 'error'
                
                # Attach client-side timeline to response for downstream consumers
                response.setdefault('timeline', func_tl)

            # Propagate status/message into the per-function timeline so ExperimentRunner can persist errors
            status_text = response.get('status', 'ok')
            if isinstance(func_tl, dict):
                func_tl['status'] = status_text
                if status_text == 'error':
                    func_tl['message'] = response.get('message') or response.get('error') or response.get('detail')
            
            # OVERHEAD 7: Timeline merging and context propagation
            with self._time_overhead_operation(func_tl['timestamps'], 'timeline_merging'):
                # Store derived metrics in workflow timeline for experiment runner
                try:
                    if hasattr(self, '_current_workflow_request_id'):
                        workflow_request_id = self._current_workflow_request_id
                        tl_root = self._get_or_create_timeline(workflow_request_id)
                        self._debug(f"Merging derived metrics for {function_name} into workflow timeline")
                        self._debug(f"Workflow timeline has {len(tl_root.get('functions', []))} functions")
                        
                        # Find the corresponding function timeline in the workflow
                        # The timeline was added earlier with the same request_id
                        if 'functions' in tl_root:
                            for func_timeline in tl_root['functions']:
                                if (func_timeline.get('function') == function_name and 
                                    func_timeline.get('request_id') == request_id):
                                    # Update with all timing data
                                    func_timeline['timestamps'] = func_tl.get('timestamps', {})
                                    func_timeline['server_timestamps'] = func_tl.get('server_timestamps', {})
                                    func_timeline['derived'] = func_tl.get('derived', {})
                                    # Also persist status and any error/message so ExperimentRunner can reflect failures
                                    if 'status' in func_tl:
                                        func_timeline['status'] = func_tl.get('status')
                                    if 'message' in func_tl:
                                        func_timeline['message'] = func_tl.get('message')
                                    if 'error_message' in func_tl:
                                        func_timeline['error_message'] = func_tl.get('error_message')
                                    self._debug(f"Updated function timeline with all timing data")
                                    break
                            else:
                                self._debug(f"Could not find matching function timeline for {function_name} with request_id {request_id}")
                                # Add the timeline if not found
                                tl_root['functions'].append(func_tl)
                                self._debug(f"Added missing function timeline to workflow")
                        else:
                            # Create functions array if it doesn't exist
                            tl_root['functions'] = [func_tl]
                            self._debug(f"Created functions array with timeline")
                    else:
                        self._debug(f"Skipping timeline merge - workflow_id: {hasattr(self, '_current_workflow_request_id')}")
                except Exception as e:
                    self._debug(f"Error updating workflow timeline: {e}")
            
            # Persist function output to storage
            if hasattr(self, '_current_workflow_request_id') and response.get('status') != 'error':
                workflow_request_id = self._current_workflow_request_id
                self._persist_function_output(workflow_request_id, function_name, node or 'unknown', response)
            
        except Exception as e:
            # OVERHEAD 8: Error handling and timeline merging
            with self._time_overhead_operation(func_tl['timestamps'], 'error_handling'):
                func_tl['timestamps']['client_error_time'] = time.time()
                error_message = f"Failed to execute {function_name}: {e}"
                self.logger.error(f"\033[31m{error_message}\033[0m")
            
            func_tl['status'] = 'error'
            func_tl['message'] = str(e)
            func_tl['error_message'] = func_tl['message']
            func_tl['node'] = node
            func_tl['routing'] = routing_diag
            # Record into root timeline once
            root_id = getattr(self, "_current_workflow_request_id", request_id)
            fn_req_id = f"{root_id}_{function_name}"
            self._record_function_timeline(root_id, function_name, fn_req_id, func_tl)
            return {
                "status": "error",
                "message": func_tl['message'],
                "function": function_name,
                "request_id": request_id,
                "node": node,
                "container_id": container.get('id') if isinstance(container, dict) else getattr(container, 'container_id', None),
                "timeline": func_tl,
            }

        # Attach routing timing if available
        func_tl.setdefault('timestamps', {})
        # Correct check: use _scheduling_timings populated by _time_scheduling_operation
        routing_duration_ms = 0.0
        try:
            if hasattr(self, '_scheduling_timings') and isinstance(self._scheduling_timings, dict):
                routing_duration_ms = float(self._scheduling_timings.get('routing', 0.0))
        except Exception:
            routing_duration_ms = 0.0
        # Provide both keys for downstream compatibility
        func_tl['timestamps']['route_duration_ms'] = routing_duration_ms
        func_tl['timestamps']['route_ms'] = routing_duration_ms
        # Aggregate per-function scheduling overhead for portability (no LNS here)
        try:
            ts = func_tl.get('timestamps', {})
            lns_ms = ts.get('lns_ms') or ts.get('lns_duration_ms') or 0.0
            route_ms = ts.get('route_ms') or ts.get('route_duration_ms') or 0.0
            ts['scheduling_overhead_ms'] = float(lns_ms) + float(route_ms)
        except Exception as e:
            print(f"\033[31m ERROR: {e}\033[0m")
            traceback.print_exc()
        func_tl['node'] = node
        func_tl['container_id'] = container.get('id') if isinstance(container, dict) else getattr(container, 'container_id', None)
        func_tl['routing'] = routing_diag

        # Record into root timeline once
        root_id = getattr(self, "_current_workflow_request_id", request_id)
        fn_req_id = f"{root_id}_{function_name}"
        self._record_function_timeline(root_id, function_name, fn_req_id, func_tl)
        
        # Log based on function result status
        try:
            status_text = response.get('status', 'ok')
            if status_text == 'error':
                self.logger.error(f"Function {function_name} returned error on {node}: {response.get('message') or response.get('error')}")
            else:
                self.logger.info(f"Function {function_name} executed successfully on {node}")
        except Exception as e:
            print(f"\033[31m ERROR: {e}\033[0m")
            
        return {
            "status": response.get('status', 'ok'),
            "function": function_name,
            "request_id": request_id,
            "node": node,
            "container_id": container.get('id') if isinstance(container, dict) else getattr(container, 'container_id', None),
            "timestamps": func_tl.get('timestamps', {}),
            "server_timestamps": func_tl.get('server_timestamps', {}),
            "derived": func_tl.get('derived', {}),
            "message": func_tl.get('message'),
            "error_message": func_tl.get('error_message'),
            "routing": routing_diag,
        }

    # ------------------------------------------------------------------
    # Timeline recording helper: write a single per-function entry once
    # ------------------------------------------------------------------
    def _record_function_timeline(self, root_request_id: str, function_name: str, function_request_id: str, tl: Dict[str, Any]) -> None:
        try:
            root = self._get_or_create_timeline(root_request_id)
            funcs = root.setdefault('functions', [])
            entry = None
            for e in funcs:
                if e.get('function') == function_name and e.get('request_id') == function_request_id:
                    entry = e
                    break
            if entry is None:
                entry = {"function": function_name, "request_id": function_request_id}
                funcs.append(entry)
            # Copy selected fields from tl
            for k in ("timestamps", "server_timestamps", "derived", "status", "message", "node", "container_id"):
                if k in tl and tl.get(k) is not None:
                    entry[k] = tl.get(k)
        except Exception as e:
            print(f"\033[31m ERROR: {e}\033[0m")
            traceback.print_exc()


    # Single function execution ------------------------------------------
 
    def _send_request_to_container(self, container, function_name: str, body: Any, timeline: Optional[Dict] = None, storage_context: Optional[Dict] = None) -> Dict:
        """Delegates to shared HTTP request utility with storage support (aligned with ours)."""
        if storage_context:
            # Use new storage-aware method
            from scheduler.shared.container_storage_service import StorageContext
            storage_ctx = StorageContext(
                src=storage_context.get('src', 'remote'),
                input_key=storage_context.get('input_key'),
                storage_config=storage_context.get('storage_config'),
                scheduler_type='faaSPRS'
            )
            return OrchestratorUtils.send_request_with_storage(
                container, function_name, body, storage_ctx, timeline, timeout=300
            )
        else:
            # Use legacy method for backward compatibility
            return OrchestratorUtils.send_request_to_container(container, function_name, body, timeline, timeout=300)

    def _wait_container_ready(self, container, timeout: float = 5.0, interval: float = 0.1) -> None:
        """Wait briefly for a newly deployed container port to accept connections."""
        import socket
        host = '127.0.0.1'
        port = int(container.host_port)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with socket.create_connection((host, port), timeout=interval):
                    return
            except Exception:
                time.sleep(interval)

    def _list_function_containers(self) -> List[Dict[str, Any]]:
        """Discover all function containers (shared with ours scheduler)."""
        containers = []
        try:
            client = getattr(self.function_container_manager, 'client', None)
            if client is None:
                self.logger.debug("No Docker client available")
                return containers
            
            # Find containers labeled as function containers regardless of scheduler label
            faaspr_containers = client.containers.list(
                filters={'label': 'dataflower-type=function'}
            )
            self.logger.debug(f"Found {len(faaspr_containers)} function containers")
            
            for container in faaspr_containers:
                try:
                    labels = container.attrs.get('Config', {}).get('Labels', {})
                    function_name = labels.get('dataflower-function')
                    node_id = labels.get('dataflower-node', 'unknown')
                    
                    # self.logger.debug(f"Processing container {container.name}: function={function_name}, node={node_id}")
                    
                    if not function_name:
                        self.logger.debug(f"Skipping container {container.name}: no function name")
                        continue
                    
                    # Extract port information
                    ports = container.attrs.get('HostConfig', {}).get('PortBindings', {})
                    host_port = None
                    for port_spec, port_bindings in ports.items():
                        if port_bindings:
                            host_port = int(port_bindings[0]['HostPort'])
                            break
                    
                    if host_port:
                        container_info = {
                            'id': container.id,
                            'name': container.name,
                            'function_name': function_name,
                            'node_id': node_id,
                            'host_port': host_port,
                            'container': container,
                            'type': 'function'
                        }
                        containers.append(container_info)
                        # self.logger.debug(f"Added container {container.name} for function {function_name} on node {node_id}")
                    else:
                        self.logger.debug(f"Skipping container {container.name}: no host port")
                        
                except Exception as e:
                    self.logger.debug(f"Failed to process FaasPRS container {container.name}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.debug(f"Failed to discover FaasPRS function containers: {e}")
            
        self.logger.debug(f"Total FaasPRS function containers discovered: {len(containers)}")
        return containers

    def _persist_function_output(self, workflow_request_id: str, function_name: str, node_id: str, result_payload: Any) -> None:
        """Persist function output to local storage and MinIO via remote adapter."""
        self.logger.info(f"DEBUG: _persist_function_output called for {function_name} with workflow_request_id: {workflow_request_id}")
        try:
            # Ensure shared volume dir - use host-accessible path
            import pathlib, json
            shared_volume_root = '/tmp/dataflower_shared'
            exp_dir = pathlib.Path(shared_volume_root) / workflow_request_id
            exp_dir.mkdir(parents=True, exist_ok=True)

            # Build filenames
            local_filename = f"{workflow_request_id}_{function_name}_{node_id}_output.json"
            local_path = exp_dir / local_filename
            remote_data_id = f"{workflow_request_id}_{function_name}_output"

            # Normalize result to JSON serializable
            serializable = result_payload
            if not isinstance(serializable, (dict, list, str, int, float, bool)) and serializable is not None:
                try:
                    serializable = json.loads(serializable)
                except Exception:
                    serializable = str(serializable)

            # Write local file
            try:
                with open(local_path, 'w', encoding='utf-8') as f:
                    if isinstance(serializable, (dict, list)):
                        json.dump(serializable, f, ensure_ascii=False)
                    else:
                        f.write(str(serializable))
                # GREEN for local storage
                self.logger.info(f"\033[92mSaved local output (local storage): {local_path}\033[0m")
            except Exception as e:
                self.logger.warning(f"Failed to write local output {local_path}: {e}")

            # Upload to remote storage (MinIO) - primary storage method
            if hasattr(self, 'remote_storage_adapter') and self.remote_storage_adapter is not None:
                try:
                    # Prepare DataMetadata
                    size_bytes = 0
                    try:
                        payload_bytes = json.dumps(serializable).encode('utf-8') if isinstance(serializable, (dict, list)) else str(serializable).encode('utf-8')
                        size_bytes = len(payload_bytes)
                        data_to_store = serializable  # adapter handles json/str/bytes
                    except Exception:
                        data_to_store = serializable

                    from provider.data_manager import DataMetadata
                    metadata = DataMetadata(
                        data_id=remote_data_id,
                        node_id=node_id,
                        function_name=function_name,
                        step_name=function_name,
                        size_bytes=size_bytes,
                        created_time=time.time(),
                        last_accessed=time.time(),
                        access_count=0,
                        data_type='output',
                        dependencies=[],
                        cache_priority=1,
                    )
                    success = self.remote_storage_adapter.store_data(remote_data_id, data_to_store, metadata)
                    if success:
                        # CYAN for MinIO/remote storage
                        self.logger.info(f"\033[96mUploaded to remote storage (MinIO): {remote_data_id}\033[0m")
                    else:
                        self.logger.warning(f"Remote storage upload returned False for {remote_data_id}")
                        self.logger.warning(f"Data to store: {type(data_to_store)}, size: {len(str(data_to_store)) if data_to_store else 'None'}")
                except Exception as e:
                    self.logger.error(f"Remote upload failed for {remote_data_id}: {e}")
                    self.logger.error(f"Remote storage adapter type: {type(self.remote_storage_adapter)}")
                    self.logger.error(f"Exception details: {e.__class__.__name__}: {str(e)}")
            else:
                self.logger.warning("No remote storage adapter available")
        except Exception as e:
            self.logger.warning(f"Output persistence encountered error: {e}")

    # =================================================================
    # DAG-RELATED & METRICS METHODS (Workflow Execution, Graph Building)
    # =================================================================
    
    def get_resource_metrics(self) -> Dict:
        """Get resource utilization metrics for all function invocations"""
        return OrchestratorUtils.get_resource_metrics()
    
    def get_cluster_resource_status(self) -> Dict:
        """Get comprehensive cluster resource status"""
        return OrchestratorUtils.get_cluster_resource_status(self.function_container_manager)
    
    def get_function_resource_summary(self, function_name: str) -> Dict:
        """Get resource utilization summary for a specific function"""
        return OrchestratorUtils.get_function_resource_summary(function_name, self.function_container_manager)
    
    def extract_resource_utilization(self) -> Dict[str, Any]:
        """
        Extract comprehensive resource utilization metrics for containers and nodes.
        Returns aggregated metrics suitable for experiment reporting.
        """
        return OrchestratorUtils.extract_resource_utilization(self.function_container_manager)