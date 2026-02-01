"""
Palette Scheduler - Function Orchestrator
Implements bucket-based hash (BH) scheduling for deterministic data locality.
"""
import logging
import time
import traceback
from typing import Any, Dict, Optional, List, Callable
from contextlib import contextmanager

from functions.dag_loader import get_dag_analysis
from provider.remote_storage_adapter import create_storage_adapter
from provider.function_container_manager import FunctionContainerManager
from scheduler.palette import config as palette_config
from scheduler.palette.bucket_manager import BucketManager
from scheduler.palette.container_queue import ContainerQueueManager
from scheduler.shared import DAGUtils, OrchestratorUtils
from scheduler.shared.dag_executor import DAGExecutor


class FunctionOrchestrator:
    """
    Palette orchestrator using bucket-based hash scheduling.
    
    Key features:
    - Deterministic routing via color hints hashed to buckets
    - Pre-assigned bucket-to-container mapping
    - Per-function bucket spaces for isolation
    - Data locality through consistent request routing
    """
    
    def __init__(self, container_manager: Optional[FunctionContainerManager] = None) -> None:
        """
        Initialize Palette orchestrator.
        
        Args:
            num_buckets: Number of hash buckets for color mapping
            container_manager: Optional shared container manager
        """
        # Initialize configuration
        if palette_config.get_config() is None:
            palette_config.initialize_config()
        self.config = palette_config.get_config()
        
        self.num_buckets = self.config.num_buckets
        
        # Setup logging
        self._setup_logging()
        
        # Core state
        self.workflows: Dict[str, Dict[str, Any]] = {}
        self.workflow_results: Dict[str, Any] = {}
        self.active_workflows: Dict[str, Dict[str, Any]] = {}
        self.request_timelines: Dict[str, Dict[str, Any]] = {}
        
        # Palette-specific: bucket managers per function
        self.bucket_managers: Dict[str, BucketManager] = {}  # function_name → BucketManager
        
        # Container queue manager for preventing routing conflicts (optional)
        self.container_queue_manager = None
        if self.config.enable_container_queue:
            self.container_queue_manager = ContainerQueueManager(
                max_queue_depth=self.config.queue_max_depth,
                request_timeout=self.config.queue_request_timeout,
                process_interval=self.config.queue_process_interval
            )
            self.container_queue_manager.start()
            self.logger.info("Container queue system enabled")
        else:
            self.logger.info("Container queue system disabled")
        
        # Track workflow-to-request mapping for color hint propagation
        self._workflow_for_request: Dict[str, str] = {}  # request_id → workflow_name
        
        # Initialization overhead tracking
        self._init_overhead_by_workflow: Dict[str, Dict[str, float]] = {}
        self._init_timings: Dict[str, float] = {}
        self._scheduling_timings: Dict[str, float] = {}
        
        # Settings
        self.namespace = "serverless"
        self.auto_scaling_enabled = True
        self.min_containers_per_function = 1
        self.max_containers_per_function = 5
        
        # Use shared container manager if provided
        if container_manager is not None:
            self.function_container_manager = container_manager
        else:
            self.function_container_manager = FunctionContainerManager()
        
        # Data locality / storage adapter
        self._initialize_data_locality()
        
        # Create DAGExecutor for input preparation
        self._dag_executor = DAGExecutor(self)
        
        self.logger.info(f"Palette orchestrator initialized with {self.num_buckets} buckets")
    
    @contextmanager
    def _time_operation(self, timeline: Dict, operation_name: str):
        """Context manager to automatically time operations and store in timeline"""
        start_time = time.time()
        try:
            yield
        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000.0
            if timeline is not None and isinstance(timeline, dict):
                timeline[f'{operation_name}_start'] = start_time
                timeline[f'{operation_name}_end'] = end_time
                timeline[f'{operation_name}_ms'] = duration_ms
    
    @contextmanager
    def _time_init_operation(self, operation_name: str):
        """Context manager to time initialization operations"""
        start_time = time.time()
        try:
            yield
        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000.0
            if not hasattr(self, '_init_timings'):
                self._init_timings = {}
            self._init_timings[operation_name] = duration_ms
    
    @contextmanager
    def _time_scheduling_operation(self, operation_name: str):
        """Context manager to time scheduling operations"""
        start_time = time.time()
        try:
            yield
        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000.0
            if not hasattr(self, '_scheduling_timings'):
                self._scheduling_timings = {}
            self._scheduling_timings[operation_name] = duration_ms
    
    @contextmanager
    def _time_overhead_operation(self, timeline: Dict, operation_name: str):
        """Context manager to time orchestrator overhead operations"""
        start_time = time.time()
        try:
            yield
        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000.0
            if timeline is not None and isinstance(timeline, dict):
                timeline[f'{operation_name}_start'] = start_time
                timeline[f'{operation_name}_end'] = end_time
                timeline[f'{operation_name}_ms'] = duration_ms
    
    def _get_or_create_timeline(self, request_id: str) -> Dict[str, Any]:
        """Get or create timeline for a request"""
        tl = self.request_timelines.get(request_id)
        if tl is None:
            tl = {}
            self.request_timelines[request_id] = tl
        return tl
    
    def _setup_logging(self) -> None:
        """Setup logging configuration"""
        cfg = palette_config.get_config()
        enable = getattr(cfg, 'enable_logging', True)
        level_name = getattr(cfg, 'log_level', 'INFO')
        level = getattr(logging, level_name, logging.INFO)
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        if not enable:
            self.logger.disabled = True
    
    def _initialize_data_locality(self) -> None:
        """Initialize storage adapter for data locality"""
        storage_config = {
            'type': 'minio',
            'minio_host': 'minio',
            'minio_port': 9000,
            'access_key': 'dataflower',
            'secret_key': 'dataflower123',
            'bucket_name': 'serverless-data',
        }
        try:
            self.remote_storage_adapter = create_storage_adapter('minio', storage_config)
        except Exception:
            self.remote_storage_adapter = None
    
    def _debug(self, message: str) -> None:
        """Debug logging helper"""
        try:
            self.logger.debug(message)
        except Exception as e:
            print(f"\033[31m ERROR: {e}\033[0m")
            traceback.print_exc()
    
    # ========================================================================
    # WORKFLOW MANAGEMENT
    # ========================================================================
    
    def register_workflow(self, workflow_name: str, workflow_dag: Dict[str, Any]) -> None:
        """
        Register a workflow and set up bucket-to-container mappings.
        
        This is the SETUP PHASE where we:
        1. Extract all function names from the DAG
        2. Get/deploy containers for each function
        3. Create BucketManager for each function
        4. Pre-assign buckets to containers (round-robin or capacity-weighted)
        
        Args:
            workflow_name: Name of the workflow
            workflow_dag: DAG definition dictionary
        """
        self.logger.info(f"Registering workflow '{workflow_name}' with Palette scheduler")
        self.workflows[workflow_name] = workflow_dag
        
        # Initialize timing storage
        self._init_timings = {}
        
        # Force container discovery to ensure timeseries sampler can find containers
        # This populates self.function_container_manager.function_containers
        with self._time_init_operation('container_discovery'):
            self.logger.info(f"Before discovery: {len(self.function_container_manager.function_containers)} function containers")
            self.function_container_manager.discover_existing_containers()
            total_containers = sum(len(containers) for containers in self.function_container_manager.function_containers.values())
            self.logger.info(f"After discovery: {len(self.function_container_manager.function_containers)} functions, {total_containers} containers total")
        
        # Extract task IDs from DAG
        with self._time_init_operation('task_extraction'):
            task_ids = DAGUtils.extract_task_ids(workflow_dag)
            self.logger.info(f"Extracted {len(task_ids)} tasks from workflow")
        
        # Get node capacity info for weighted assignment
        with self._time_init_operation('node_discovery'):
            nodes_resources = self.function_container_manager.get_all_nodes_resources()
            node_capacity = {
                node_id: {
                    'cpu_cores': res.get('available_cpu', 1.0),
                    'memory_gb': res.get('available_memory', 1.0)
                }
                for node_id, res in nodes_resources.items()
            }
        
        # For each function, create bucket manager and assign buckets
        with self._time_init_operation('bucket_assignment'):
            for task_id in task_ids:
                # Find the task node
                task_node = DAGUtils.find_task_node_by_id(workflow_dag, task_id)
                if not task_node:
                    continue
                
                function_name = DAGUtils.get_function_name_from_task(task_node)
                
                # Create bucket manager for this function if not exists
                if function_name not in self.bucket_managers:
                    self.logger.info(f"Creating bucket manager for function '{function_name}'")
                    bucket_manager = BucketManager(
                        function_name=function_name,
                        num_buckets=self.num_buckets,
                        hash_algorithm=self.config.hash_algorithm,
                        assignment_strategy=self.config.bucket_assignment_strategy
                    )
                    self.bucket_managers[function_name] = bucket_manager
                    
                    # Get available containers for this function
                    containers = self.function_container_manager.get_running_containers(function_name)
                    
                    # Debug: Check what containers are available
                    all_function_containers = self.function_container_manager.function_containers
                    # self.logger.debug(f"All discovered functions: {list(all_function_containers.keys())}")
                    self.logger.debug(f"Containers for '{function_name}': {len(containers)}")
                    
                    # If no containers exist yet, that's okay - they'll be deployed on-demand during execution
                    # We still create the bucket manager so it's ready when containers are deployed
                    if not containers:
                        self.logger.info(
                            f"No pre-existing containers for '{function_name}'. "
                            f"Bucket manager created, containers will be deployed on-demand during execution."
                        )
                    else:
                        # Assign buckets to existing containers
                        if self.config.capacity_weighted_assignment:
                            bucket_manager.assign_buckets(containers, node_capacity)
                        else:
                            bucket_manager.assign_buckets(containers)
                        
                        self.logger.info(
                            f"Assigned {self.num_buckets} buckets to {len(containers)} "
                            f"existing containers for '{function_name}'"
                        )
        
        # Record initialization overhead
        self._init_overhead_by_workflow[workflow_name] = {
            "task_extraction_ms": self._init_timings.get('task_extraction', 0.0),
            "node_discovery_ms": self._init_timings.get('node_discovery', 0.0),
            "bucket_assignment_ms": self._init_timings.get('bucket_assignment', 0.0),
            "total_init_ms": sum(self._init_timings.values()),
        }
        
        self.logger.info(
            f"Workflow '{workflow_name}' registered successfully. "
            f"Init overhead: {self._init_overhead_by_workflow[workflow_name]['total_init_ms']:.2f} ms"
        )
    
    def submit_workflow(self, workflow_name: str, request_id: str, body: Any) -> Dict:
        """
        Execute a complete workflow.
        
        This is the EXECUTION PHASE where we:
        1. Generate/extract color hint from request_id
        2. Walk through DAG in topological order
        3. For each function, invoke with color hint
        4. Propagate color hint through entire workflow
        
        Args:
            workflow_name: Name of the workflow to execute
            request_id: Unique request identifier
            body: Input data for the workflow
            
        Returns:
            Execution results with timeline
        """
        self._debug(f"submit_workflow called for '{workflow_name}' with request_id {request_id}")
        
        if workflow_name not in self.workflows:
            return {'status': 'error', 'message': 'Workflow not found'}
        
        self.logger.info(f"Executing workflow '{workflow_name}' (request: {request_id})")
        
        # Store current workflow request ID for color hint propagation
        self._current_workflow_request_id = request_id
        self._workflow_for_request[request_id] = workflow_name
        
        # Generate color hint from request_id (Palette's key feature)
        color_hint = self._generate_color_hint(request_id)
        self.logger.debug(f"Generated color hint '{color_hint}' for request {request_id}")
        
        dag = self.workflows[workflow_name]
        context = {'inputs': {'img': body}}
        
        # Workflow-level timing
        tl = self._get_or_create_timeline(request_id)
        tl.setdefault('workflow', {})['start'] = time.time()
        
        try:
            if 'workflow' not in dag:
                raise Exception('Invalid DAG structure: missing workflow')
            
            self._debug("Executing DAG workflow with Palette scheduler")
            final_context = self.schedule_workflow(dag, context, request_id, color_hint)
            
            # Attach initialization overhead if available
            try:
                init_ms = self._init_overhead_by_workflow.get(workflow_name, {}).get('total_init_ms', 0.0)
                if init_ms > 0:
                    sched_block = tl.setdefault('scheduler', {})
                    sched_block['init_overhead_ms'] = init_ms
            except Exception as e:
                self._debug(f"Error attaching init overhead: {e}")
            
            tl['workflow']['end'] = time.time()
            tl['workflow']['duration_ms'] = (tl['workflow']['end'] - tl['workflow']['start']) * 1000.0
            
            final_context = final_context or {}
            final_context['timeline'] = tl
            
            self.logger.info(f"Workflow '{workflow_name}' completed successfully")
            return final_context
        
        except Exception as e:
            error_message = f"Workflow '{workflow_name}' failed: {e}"
            self.logger.error(error_message)
            traceback.print_exc()
            return {'status': 'error', 'message': error_message}
    
    def _generate_color_hint(self, request_id: str) -> str:
        """
        Generate color hint based on configuration.
        
        Args:
            request_id: Request identifier
            
        Returns:
            Color hint string for hashing
        """
        if self.config.color_hint_source == "request_id":
            # Use request_id directly as color
            return request_id
        elif self.config.color_hint_source == "auto_generated":
            # Generate a random color (for testing load distribution)
            import random
            return f"color_{random.randint(0, 1000)}"
        else:
            # Default to request_id
            return request_id
    
    def schedule_workflow(self, dag: Dict, context: Dict, request_id: str, color_hint: str) -> Dict:
        """
        Palette DAG scheduling: Walk through nodes in topological order.
        At each node, use color hint to determine target container via bucket hash.
        
        Args:
            dag: DAG definition
            context: Execution context with inputs
            request_id: Request identifier
            color_hint: Color hint for bucket-based routing
            
        Returns:
            Final execution context with results
        """
        workflow = dag['workflow']
        nodes = workflow.get('nodes', [])
        edges = workflow.get('edges', [])
        
        self._debug(f"Palette DAG workflow has {len(nodes)} nodes and {len(edges)} edges")
        
        # Initialize timeline
        tl_root = self._get_or_create_timeline(request_id)
        tl_root['timestamps'] = tl_root.get('timestamps', {})
        
        # Extract task nodes and build topological order
        task_ids = DAGUtils.extract_task_ids(dag)
        
        if not task_ids:
            self.logger.warning("No task nodes found in workflow")
            return context
        
        # Build topological order using shared DAG utilities
        topological_order = DAGUtils.build_topological_order(dag, task_ids)
        self._debug(f"Topological order: {topological_order}")
        
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
                # Find the node definition
                task_node = DAGUtils.find_task_node_by_id(dag, task_id)
                
                if not task_node:
                    self.logger.warning(f"Task node {task_id} not found")
                    continue
                
                function_name = DAGUtils.get_function_name_from_task(task_node)
                
                # Prepare input data using DAGExecutor's method
                input_data = self._dag_executor.prepare_input_for_node(
                    task_id, execution_graph, final_context, node_results
                )
                
                # Invoke function using Palette's bucket-based routing with color hint
                try:
                    result = self.invoke_function(function_name, request_id, input_data, color_hint)
                    
                    # Store result in context for downstream functions
                    final_context[task_id] = result
                    node_results[task_id] = result
                    
                    # Check for errors
                    if result.get('status') == 'error':
                        self.logger.error(f"Function {function_name} failed: {result.get('message')}")
                        final_context[f"{task_id}_error"] = result.get('message')
                
                except Exception as e:
                    error_msg = f"Failed to invoke function {function_name}: {e}"
                    self.logger.error(error_msg)
                    final_context[f"{task_id}_error"] = error_msg
                    node_results[task_id] = {'status': 'error', 'message': error_msg}
        
        return final_context or context
    
    def invoke_function(self, function_name: str, request_id: str, body: bytes, 
                       color_hint: Optional[str] = None) -> Dict[str, Any]:
        """
        Invoke a function using Palette's bucket-based hash scheduling.
        
        This is the CORE ROUTING LOGIC:
        1. Hash color_hint → bucket_id
        2. Lookup bucket_id → target_container (from BucketManager)
        3. Send request to that specific container
        4. Handle container unavailability with fallback
        
        Args:
            function_name: Name of the function to invoke
            request_id: Request identifier
            body: Input data
            color_hint: Color hint for bucket-based routing (required for Palette)
            
        Returns:
            Invocation result with timeline
        """
        # Create per-function timeline
        func_tl: Dict[str, Any] = {
            "function": function_name,
            "request_id": request_id,
            "timestamps": {}
        }
        
        # OVERHEAD 1: Timeline setup
        with self._time_overhead_operation(func_tl['timestamps'], 'timeline_setup'):
            try:
                tl_root = self._get_or_create_timeline(request_id)
                tl_root.setdefault('functions', []).append(func_tl)
            except Exception as e:
                self._debug(f"Error adding function to timeline: {e}")
        
        # Validate color hint
        if color_hint is None:
            # Auto-generate from request_id if not provided
            color_hint = self._generate_color_hint(request_id)
            self.logger.debug(f"Auto-generated color hint: {color_hint}")
        
        # OVERHEAD 2: Bucket-based container selection (Palette's key operation)
        with self._time_scheduling_operation('bucket_routing'):
            # Get bucket manager for this function
            bucket_manager = self.bucket_managers.get(function_name)
            
            if not bucket_manager:
                error_msg = f"No bucket manager found for function '{function_name}'"
                self.logger.error(error_msg)
                func_tl['status'] = 'error'
                func_tl['message'] = error_msg
                return self._build_error_response(function_name, request_id, error_msg, func_tl)
            
            # Hash color to bucket, then bucket to container
            target_container = bucket_manager.get_container_for_color(color_hint)
            bucket_id = bucket_manager.hash_color_to_bucket(color_hint)
            
            try:
                cid = target_container.container_id if target_container else 'None'
                node = target_container.node_id if target_container else 'None'
                self.logger.info(
                    f"[palette-route] fn={function_name} color={color_hint} bucket={bucket_id} cid={cid} node={node}"
                )
            except Exception as e:
                print(f"\033[31m ERROR: {e}\033[0m")
        
        # OVERHEAD 3: Container availability check and fallback
        with self._time_overhead_operation(func_tl['timestamps'], 'container_selection'):
            container = None
            routing_info = {
                'strategy': 'palette_bucket_hash',
                'color_hint': color_hint,
                'bucket_id': bucket_id,
                'fallback_used': False
            }
            
            if target_container:
                # Verify container is still running
                available_containers = self.function_container_manager.get_running_containers(function_name)
                
                if target_container in available_containers:
                    container = target_container
                    routing_info['target_node'] = container.node_id
                elif self.config.enable_fallback:
                    # Primary container unavailable, use fallback
                    self.logger.warning(
                        f"Primary container {target_container.container_id} unavailable, using fallback"
                    )
                    container = bucket_manager.get_fallback_container(
                        color_hint,
                        available_containers,
                        self.config.fallback_strategy
                    )
                    routing_info['fallback_used'] = True
                    if container:
                        routing_info['fallback_node'] = container.node_id
            
            # If still no container, try to deploy one
            if not container:
                self.logger.info(f"No available container for '{function_name}', attempting to deploy")
                container = self._deploy_container_if_needed(function_name)
                
                if container:
                    routing_info['container_deployed'] = True
                    routing_info['deployed_node'] = container.node_id
                    
                    # If this is the first container for this function, assign buckets now
                    if bucket_manager and len(bucket_manager.bucket_to_container) == 0:
                        self.logger.info(f"First container deployed for '{function_name}', assigning buckets")
                        all_containers = self.function_container_manager.get_running_containers(function_name)
                        if all_containers:
                            bucket_manager.assign_buckets(all_containers)
                            # Re-get the target container after assignment
                            target_container = bucket_manager.get_container_for_color(color_hint)
                            if target_container and target_container in all_containers:
                                container = target_container
        
        # OVERHEAD 4: Request preparation and queue management
        with self._time_overhead_operation(func_tl['timestamps'], 'request_preparation'):
            if not container:
                error_msg = (
                    f"No running container available for {function_name} "
                    f"(bucket={bucket_id}, color={color_hint})"
                )
                func_tl['status'] = 'error'
                func_tl['message'] = error_msg
                func_tl['routing'] = routing_info
                return self._build_error_response(function_name, request_id, error_msg, func_tl, routing_info)
            
            # Check if queue system is enabled and function requires queuing
            if self.container_queue_manager:
                queue_result = self.container_queue_manager.queue_request(
                    request_id=request_id,
                    function_name=function_name,
                    container=container,
                    body=body,
                    color_hint=color_hint,
                    callback=self._create_queue_callback(request_id, function_name, func_tl),
                    priority=0
                )
                
                # Handle queue result
                if queue_result['status'] == 'rejected':
                    error_msg = queue_result['message']
                    func_tl['status'] = 'error'
                    func_tl['message'] = error_msg
                    func_tl['routing'] = routing_info
                    func_tl['queue_info'] = queue_result
                    return self._build_error_response(function_name, request_id, error_msg, func_tl, routing_info)
                
                elif queue_result['status'] == 'queued':
                    # Request is queued, return queued status
                    func_tl['status'] = 'queued'
                    func_tl['message'] = queue_result['message']
                    func_tl['routing'] = routing_info
                    func_tl['queue_info'] = queue_result
                    
                    # Store the request for later processing
                    self._store_queued_request(request_id, function_name, container, body, func_tl, routing_info)
                    
                    return {
                        "status": "queued",
                        "message": queue_result['message'],
                        "function": function_name,
                        "request_id": request_id,
                        "node": container.node_id,
                        "container_id": container.container_id,
                        "timeline": func_tl,
                        "routing": routing_info,
                        "queue_info": queue_result
                    }
                
                # Request can proceed immediately (container was marked busy)
                func_tl['queue_info'] = queue_result
            else:
                # Queue system disabled, proceed directly
                func_tl['queue_info'] = {'status': 'disabled', 'message': 'Queue system disabled'}
        
        # OVERHEAD 5: Network request
        start_time = time.time()
        try:
            response = self._send_request_to_container(container, function_name, body, func_tl)
            
            # OVERHEAD 6: Result processing
            with self._time_overhead_operation(func_tl['timestamps'], 'result_processing'):
                response_time = time.time() - start_time
                response.setdefault('timeline', func_tl)
                
                # Propagate status
                status_text = response.get('status', 'ok')
                if isinstance(func_tl, dict):
                    func_tl['status'] = status_text
                    if status_text == 'error':
                        func_tl['message'] = (
                            response.get('message') or 
                            response.get('error') or 
                            response.get('detail')
                        )
            
            # OVERHEAD 7: Timeline merging
            with self._time_overhead_operation(func_tl['timestamps'], 'timeline_merging'):
                self._merge_function_timeline(func_tl)
            
            # Release the container after successful processing (if queue system enabled)
            if self.container_queue_manager:
                self.container_queue_manager.release_container(container.container_id)
        
        except Exception as e:
            # No reroute: just record detailed error
            try:
                exc_name = type(e).__name__
                cid = getattr(container, 'container_id', None)
                node = getattr(container, 'node_id', None)
                self.logger.error(f"[palette-error] fn={function_name} cid={cid} node={node} bucket={bucket_id} err={exc_name} msg={e}")
                
                # Release container on error (if queue system enabled)
                if cid and self.container_queue_manager:
                    self.container_queue_manager.release_container(cid)
            except Exception:
                pass
            # OVERHEAD 8: Error handling
            with self._time_overhead_operation(func_tl['timestamps'], 'error_handling'):
                func_tl['timestamps']['client_error_time'] = time.time()
                error_message = f"Failed to execute {function_name}: {e}"
                self.logger.error(f"\033[31m{error_message}\033[0m")
                traceback.print_exc()
            func_tl['status'] = 'error'
            func_tl['message'] = str(e)
            func_tl['routing'] = routing_info
            return self._build_error_response(function_name, request_id, str(e), func_tl, routing_info)
        
        # Attach routing timing
        func_tl.setdefault('timestamps', {})
        routing_duration_ms = 0.0
        try:
            if hasattr(self, '_scheduling_timings') and isinstance(self._scheduling_timings, dict):
                routing_duration_ms = float(self._scheduling_timings.get('bucket_routing', 0.0))
        except Exception:
            routing_duration_ms = 0.0
        
        func_tl['timestamps']['route_duration_ms'] = routing_duration_ms
        func_tl['timestamps']['route_ms'] = routing_duration_ms
        
        # Aggregate scheduling overhead
        try:
            ts = func_tl.get('timestamps', {})
            route_ms = ts.get('route_ms', 0.0)
            ts['scheduling_overhead_ms'] = float(route_ms)
        except Exception as e:
            self._debug(f"Error calculating scheduling overhead: {e}")
        
        func_tl['node'] = container.node_id
        func_tl['container_id'] = container.container_id
        func_tl['routing'] = routing_info
        
        # Record into root timeline
        root_id = getattr(self, "_current_workflow_request_id", request_id)
        fn_req_id = f"{root_id}_{function_name}"
        self._record_function_timeline(root_id, function_name, fn_req_id, func_tl)
        
        # Log result
        try:
            status_text = response.get('status', 'ok')
            if status_text == 'error':
                self.logger.error(
                    f"Function {function_name} returned error on {container.node_id}: "
                    f"{response.get('message') or response.get('error')}"
                )
            else:
                self.logger.info(
                    f"Function {function_name} executed successfully on {container.node_id} (bucket={bucket_id})"
                )
        except Exception as e:
            self._debug(f"Error logging result: {e}")
        
        return {
            "status": response.get('status', 'ok'),
            "function": function_name,
            "request_id": request_id,
            "node": container.node_id,
            "container_id": container.container_id,
            "timestamps": func_tl.get('timestamps', {}),
            "server_timestamps": func_tl.get('server_timestamps', {}),
            "derived": func_tl.get('derived', {}),
            "message": func_tl.get('message'),
            "error_message": func_tl.get('error_message'),
            "routing": routing_info,
        }
    
    # ========================================================================
    # HELPER METHODS
    # ========================================================================
    
    def _deploy_container_if_needed(self, function_name: str) -> Optional[Any]:
        """Deploy a new container if needed and capacity allows"""
        try:
            cpu_requirement = self.function_container_manager.default_cpu_limit
            memory_requirement = self.function_container_manager.default_memory_limit
            
            available_nodes = self.function_container_manager._find_nodes_with_capacity(
                cpu_requirement, memory_requirement
            )
            
            if available_nodes:
                best_node = available_nodes[0]
                new_container = self.function_container_manager.deploy_function_container(
                    function_name,
                    best_node,
                    cpu_limit=cpu_requirement,
                    memory_limit=memory_requirement
                )
                
                if new_container:
                    self.logger.info(f"Deployed new container for '{function_name}' on {best_node}")
                    
                    # Update bucket assignments to include new container
                    if self.config.enable_rebalancing:
                        bucket_manager = self.bucket_managers.get(function_name)
                        if bucket_manager:
                            containers = self.function_container_manager.get_running_containers(function_name)
                            bucket_manager.rebalance(containers)
                    
                    return new_container
                else:
                    self.logger.warning(f"Failed to deploy container for '{function_name}'")
            else:
                self.logger.warning(f"No nodes available with sufficient capacity for '{function_name}'")
        except Exception as e:
            self.logger.error(f"Error deploying container: {e}")
        
        return None
    
    def _build_error_response(self, function_name: str, request_id: str, 
                             error_msg: str, func_tl: Dict,
                             routing_info: Optional[Dict] = None) -> Dict[str, Any]:
        """Build standardized error response"""
        func_tl['error_message'] = error_msg
        
        return {
            "status": "error",
            "message": error_msg,
            "function": function_name,
            "request_id": request_id,
            "node": None,
            "container_id": None,
            "timeline": func_tl,
            "routing": routing_info or {},
        }
    
    def _merge_function_timeline(self, func_tl: Dict) -> None:
        """Merge function timeline into workflow timeline"""
        try:
            if hasattr(self, '_current_workflow_request_id'):
                workflow_request_id = self._current_workflow_request_id
                tl_root = self._get_or_create_timeline(workflow_request_id)
                
                if 'functions' in tl_root:
                    function_name = func_tl.get('function')
                    request_id = func_tl.get('request_id')
                    
                    for func_timeline in tl_root['functions']:
                        if (func_timeline.get('function') == function_name and
                            func_timeline.get('request_id') == request_id):
                            # Update with all timing data
                            func_timeline['timestamps'] = func_tl.get('timestamps', {})
                            func_timeline['server_timestamps'] = func_tl.get('server_timestamps', {})
                            func_timeline['derived'] = func_tl.get('derived', {})
                            if 'status' in func_tl:
                                func_timeline['status'] = func_tl.get('status')
                            if 'message' in func_tl:
                                func_timeline['message'] = func_tl.get('message')
                            if 'error_message' in func_tl:
                                func_timeline['error_message'] = func_tl.get('error_message')
                            break
        except Exception as e:
            self._debug(f"Error merging timeline: {e}")
    
    def _record_function_timeline(self, root_request_id: str, function_name: str,
                                 function_request_id: str, tl: Dict[str, Any]) -> None:
        """Write a single per-function entry to timeline"""
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
            self._debug(f"Error recording timeline: {e}")
    
    def _send_request_to_container(self, container, function_name: str, 
                                  body: Any, timeline: Optional[Dict] = None) -> Dict:
        """Delegate to shared HTTP request utility"""
        return OrchestratorUtils.send_request_to_container(
            container, function_name, body, timeline, timeout=300
        )
    
    # ========================================================================
    # RESOURCE METRICS (Shared interface)
    # ========================================================================
    
    def get_resource_metrics(self) -> Dict:
        """Get resource utilization metrics for all function invocations"""
        return OrchestratorUtils.get_resource_metrics()
    
    def get_cluster_resource_status(self) -> Dict:
        """Get comprehensive cluster resource status"""
        return OrchestratorUtils.get_cluster_resource_status(self.function_container_manager)
    
    def get_function_resource_summary(self, function_name: str) -> Dict:
        """Get resource utilization summary for a specific function"""
        return OrchestratorUtils.get_function_resource_summary(
            function_name, self.function_container_manager
        )
    
    def extract_resource_utilization(self) -> Dict[str, Any]:
        """Extract comprehensive resource utilization metrics"""
        return OrchestratorUtils.extract_resource_utilization(self.function_container_manager)
    
    # ========================================================================
    # PALETTE-SPECIFIC MONITORING
    # ========================================================================
    
    def get_bucket_statistics(self) -> Dict[str, Any]:
        """Get statistics about bucket assignments and load distribution"""
        stats = {
            'num_buckets': self.num_buckets,
            'functions': {}
        }
        
        for function_name, bucket_manager in self.bucket_managers.items():
            stats['functions'][function_name] = bucket_manager.get_statistics()
        
        return stats
    
    def get_queue_statistics(self) -> Dict[str, Any]:
        """Get statistics about container queues"""
        if self.container_queue_manager:
            return self.container_queue_manager.get_statistics()
        else:
            return {
                'status': 'disabled',
                'message': 'Container queue system is disabled',
                'config': {
                    'enable_container_queue': self.config.enable_container_queue,
                    'queue_max_depth': self.config.queue_max_depth,
                    'queue_request_timeout': self.config.queue_request_timeout,
                    'queue_process_interval': self.config.queue_process_interval
                }
            }
    
    def _create_queue_callback(self, request_id: str, function_name: str, func_tl: Dict) -> Callable:
        """Create a callback function for queued requests"""
        def queue_callback(result: Dict[str, Any]):
            """Callback executed when a queued request is ready to be processed"""
            try:
                if result['status'] == 'ready_to_process':
                    # The request is ready to be processed - this would trigger the actual function call
                    self.logger.info(f"Queued request {request_id} is ready to be processed")
                    # In a full implementation, this would re-invoke the function
                elif result['status'] == 'error':
                    # Handle error from queue processing
                    func_tl['status'] = 'error'
                    func_tl['message'] = result.get('message', 'Unknown queue error')
                    self.logger.error(f"Queue error for request {request_id}: {result.get('message')}")
            except Exception as e:
                self.logger.error(f"Error in queue callback for request {request_id}: {e}")
        
        return queue_callback
    
    def _store_queued_request(self, request_id: str, function_name: str, container: Any, 
                             body: Any, func_tl: Dict, routing_info: Dict):
        """Store queued request information for later processing"""
        # Store request details for when it gets dequeued
        if not hasattr(self, '_queued_requests'):
            self._queued_requests = {}
        
        self._queued_requests[request_id] = {
            'function_name': function_name,
            'container': container,
            'body': body,
            'func_tl': func_tl,
            'routing_info': routing_info,
            'queued_at': time.time()
        }
    
    def cleanup(self):
        """Cleanup resources when orchestrator is destroyed"""
        if hasattr(self, 'container_queue_manager') and self.container_queue_manager:
            self.container_queue_manager.stop()

