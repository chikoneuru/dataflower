"""
Ours Scheduler
Function-based Orchestrator: Manages per-function containers with placement and routing.
Replaces the worker-centric orchestration with function-specific container management.
"""

import os
import time
import logging
import requests
import concurrent.futures
from typing import Dict, List, Optional, Tuple, Any
from contextlib import contextmanager

import requests

# Removed unused imports - now using shared utilities
from provider.data_manager import (DataAccessRequest, DataLocation,
                                   DataManager, DataMetadata)
from provider.function_container_manager import (ContainerStatus,
                                                 FunctionContainer,
                                                 FunctionContainerManager)
from provider.remote_storage_adapter import (RemoteStorageAdapter,
                                             create_storage_adapter)
from provider.resource_monitor import resource_monitor  # Still needed for monitoring context

from .bottleneck_identifier import BottleneckIdentifier
from .config import SystemConfig, get_config, load_config
from .cost_models import CostModels
from .placement import FunctionPlacement, PlacementRequest, PlacementStrategy
from .routing import FunctionRouting, RoutingRequest, RoutingStrategy
from .cost_models import CostModels
from .bottleneck_identifier import BottleneckIdentifier
from .static_profiling import CalibrationRunner
from scheduler.shared import DAGExecutor, DAGUtils, OrchestratorUtils
from scheduler.shared.safe_printing import safe_print_value
from scheduler.shared.minio_config import get_minio_config, get_minio_storage_config
from scheduler.shared import ContainerStorageService, UnifiedInputHandler, create_storage_service, create_input_handler


class FunctionOrchestrator:
    """
    Function-based orchestrator that manages per-function containers.
    
    Key differences from worker-based orchestrator:
    1. Uses FunctionContainerManager instead of ContainerManager
    2. Makes PLACEMENT decisions for new containers
    3. Makes ROUTING decisions for existing containers
    4. Handles function lifecycle (deploy, scale, cleanup)
    """
    
    def __init__(self, config: Optional[SystemConfig] = None, container_manager: Optional[FunctionContainerManager] = None):
        # Configuration
        if config:
            self.config = config
        else:
            if not get_config():
                load_config()
            self.config = get_config()
        
        # Core components
        self.function_container_manager = container_manager if container_manager is not None else FunctionContainerManager()
        self.placement_engine = FunctionPlacement()
        self.routing_engine = FunctionRouting()
        self.cost_models = CostModels()
        self.bottleneck_identifier = BottleneckIdentifier()
        self.dag_executor = DAGExecutor(self)
        
        # Static profiling system
        self.calibration_runner = CalibrationRunner(self.function_container_manager)
        
        # Workflow tracking
        self.workflows = {}
        self.workflow_results = {}
        self.active_workflows: Dict[str, Dict] = {}
        # In-memory per-request timelines (kept in same process/file)
        self.request_timelines: Dict[str, Dict] = {}

        # Settings
        self.namespace = "serverless"
        self.auto_scaling_enabled = True
        self.min_containers_per_function = 1
        self.max_containers_per_function = 5
        
        # Initialize scheduling timings dictionary
        self._scheduling_timings = {}
        
        # Setup logging and components
        self._setup_logging()
        self._initialize_data_locality()

    def _debug(self, message: str) -> None:
        # Also log to logger for debugging
        if hasattr(self, 'logger'):
            self.logger.debug(message)

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
            timeline[f'{operation_name}_ms'] = duration_ms

    @contextmanager
    def _time_scheduling_operation(self, operation_name: str):
        """Context manager to time scheduling-only operations (e.g., LNS, routing)."""
        start_time = time.time()
        try:
            yield
        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000.0
            if not hasattr(self, '_scheduling_timings') or not isinstance(getattr(self, '_scheduling_timings'), dict):
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

    def _get_or_create_timeline(self, request_id: str) -> Dict[str, Any]:
        tl = self.request_timelines.get(request_id)
        if tl is None:
            tl = {}
            self.request_timelines[request_id] = tl
        return tl
    
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
    
    def _initialize_data_locality(self):
        """Initialize data locality management components"""
        try:
            data_locality_config = getattr(self.config, 'data_locality', {}) if hasattr(self.config, 'data_locality') else {}
            
            # Initialize remote storage
            storage_config = data_locality_config.get('remote_storage', {})
            storage_type = storage_config.get('type', 'minio')
            
            if storage_type == 'minio' and not storage_config.get('minio_host'):
                # Use centralized MinIO configuration
                minio_config = get_minio_config()
                storage_config.update(get_minio_storage_config())
                self.logger.info(f"Using centralized MinIO config: {minio_config['context']} - {minio_config['host']}:{minio_config['port']}")
            
            self.remote_storage_adapter = create_storage_adapter(storage_type, storage_config)
            
            # Initialize data locality manager
            node_config = data_locality_config.get('node_config', {
                'node_id': self._get_node_id(),
                'cache_dir': '/tmp/serverless_cache',
                'max_cache_items': 1000,
            })
            
            self.data_manager = DataManager(node_config)
            self.data_manager.set_remote_storage_adapter(self.remote_storage_adapter)
            
            # Initialize storage service for remote data transfer (using shared infrastructure)
            self.storage_service = create_storage_service('ours')
            self.input_handler = create_input_handler('ours')
            
            self.logger.info("Data locality management initialized")
            
        except Exception as e:
            self.logger.warning(f"Could not initialize data locality: {e}")
            self.data_manager = None
            self.remote_storage_adapter = None
    
    def _get_node_id(self) -> str:
        """Get current node ID"""
        try:
            pod_name = os.environ.get('HOSTNAME', '')
            if pod_name:
                return pod_name
            
            import socket
            return socket.gethostname()
        except:
            return 'default-node'
    
    # =================================================================
    # SCHEDULER-SPECIFIC METHODS (Placement, Routing, Cost Models)
    # =================================================================
    
    def register_workflow(self, name: str, dag: Dict):
        """Register a workflow DAG"""
        self.workflows[name] = dag
        
        # Pre-analyze workflow to determine function requirements
        try:
            # DAG-only: extract function IDs from DAG nodes
            workflow_section = dag.get('workflow', {}) if isinstance(dag, dict) else {}
            nodes = workflow_section.get('nodes', []) if isinstance(workflow_section, dict) else []
            workflow_functions = [n['id'] for n in nodes if isinstance(n, dict) and n.get('type') == 'task' and 'id' in n]
            edges = workflow_section.get('edges', []) if isinstance(workflow_section, dict) else []
            self.logger.info(f"Workflow '{name}' registered with functions: {workflow_functions}")
            
        except Exception as e:
            self.logger.error(f"Error registering workflow '{name}': {e}")
            self.active_workflows[name] = {'dag': dag, 'status': 'registration_failed'} 
    
    def submit_workflow(self, workflow_name: str, request_id: str, body: Any) -> Dict:
        """Execute a complete workflow"""
        self._debug(f"submit_workflow called for {workflow_name} with request_id {request_id}")
        if workflow_name not in self.workflows:
            return {'status': 'error', 'message': 'Workflow not found'}
        
        self.logger.info(f"Running workflow '{workflow_name}' (request: {request_id})")
        
        # Store current workflow request ID for function calls
        self._current_workflow_request_id = request_id
        
        self.workflow_results[request_id] = {}
        dag = self.workflows[workflow_name]
        
        # Store workflow in active_workflows for dependency analysis
        self.active_workflows[workflow_name] = {
            'dag': dag,
            'status': 'running',
            'request_id': request_id,
            'start_time': time.time()
        }
        
        # Store initial input data in MinIO for first function
        initial_input_key = f"{request_id}_initial_input"
        if self.remote_storage_adapter and body:
            try:
                from provider.data_manager import DataMetadata
                initial_metadata = DataMetadata(
                    data_id=initial_input_key,
                    node_id=self._get_node_id(),
                    function_name="initial_input",
                    step_name="input",
                    size_bytes=len(body) if hasattr(body, '__len__') else 0,
                    created_time=time.time(),
                    last_accessed=time.time(),
                    access_count=1,
                    data_type="input",
                    dependencies=[]
                )
                success = self.remote_storage_adapter.store_data(initial_input_key, body, initial_metadata)
                if success:
                    self.logger.debug(f"Stored initial input data in MinIO with key: {initial_input_key}")
                else:
                    self.logger.warning(f"Failed to store initial input data in MinIO")
            except Exception as e:
                self.logger.warning(f"Could not store initial input data in MinIO: {e}")
        
        context = {'inputs': {'img': body, 'initial_input_key': initial_input_key}}
        # Workflow-level timing
        tl = self._get_or_create_timeline(request_id)
        tl.setdefault('workflow', {})['start'] = time.time()
        
        try:
            # DAG-only execution
            if 'workflow' not in dag:
                raise Exception('Invalid DAG structure: missing workflow')
            self._debug(f"Executing DAG workflow")
            final_context = self.schedule_workflow(dag, context, request_id)

            tl['workflow']['end'] = time.time()
            tl['workflow']['duration_ms'] = (tl['workflow']['end'] - tl['workflow']['start']) * 1000.0
            # Attach timeline into the returned context
            final_context = final_context or {}
            final_context['timeline'] = tl
            
            # Clean up active workflow
            if workflow_name in self.active_workflows:
                self.active_workflows[workflow_name]['status'] = 'completed'
                self.active_workflows[workflow_name]['end_time'] = time.time()
            
            self.logger.info(f"Workflow '{workflow_name}' completed successfully")
            return final_context
            
        except Exception as e:
            error_message = f"Workflow '{workflow_name}' failed: {e}"
            self.logger.error(error_message)
            
            # Clean up active workflow on error
            if workflow_name in self.active_workflows:
                self.active_workflows[workflow_name]['status'] = 'failed'
                self.active_workflows[workflow_name]['end_time'] = time.time()
                self.active_workflows[workflow_name]['error'] = error_message
            
            return {'status': 'error', 'message': error_message}

    def schedule_workflow(self, dag: Dict, context: Dict, request_id: str) -> Dict:
        """Execute a DAG workflow with proper parallel, sequential, and switch logic"""
        workflow = dag['workflow']
        nodes = workflow.get('nodes', [])
        edges = workflow.get('edges', [])
        
        self._debug(f"DAG workflow has {len(nodes)} nodes and {len(edges)} edges")
        
        # Initialize timeline
        tl_root = self._get_or_create_timeline(request_id)
        tl_root['timestamps'] = tl_root.get('timestamps', {})
        
        # Cost Model Analysis
        with self._time_scheduling_operation('cost_model'):
            cost_model = self._perform_workflow_cost_analysis(nodes, context)
        
        # Bottleneck Analysis
        with self._time_scheduling_operation('bottleneck_analysis'):
            bottleneck_analysis = self._analyze_workflow_bottlenecks(nodes, edges, context, profiling_suggestions=None)
            if bottleneck_analysis.has_bottleneck:
                self._debug(f"Bottleneck detected: {bottleneck_analysis.bottleneck_type} (severity: {bottleneck_analysis.bottleneck_severity:.2f})")
                self._debug(f"Critical nodes: {bottleneck_analysis.critical_nodes}")
                self._debug(f"Critical edges: {bottleneck_analysis.critical_edges}")
        
        # Write workflow-level scheduling timings into timeline (profiling removed)
        try:
            cost_model_ms = float(self._scheduling_timings.get('cost_model', 0.0)) if hasattr(self, '_scheduling_timings') else 0.0
            bottleneck_ms = float(self._scheduling_timings.get('bottleneck_analysis', 0.0)) if hasattr(self, '_scheduling_timings') else 0.0
            tl_root['timestamps']['cost_model_duration_ms'] = cost_model_ms
            tl_root['timestamps']['bottleneck_analysis_duration_ms'] = bottleneck_ms
            tl_root['timestamps']['scheduling_overhead_ms'] = cost_model_ms + bottleneck_ms
        except Exception as e:
            print(f"\033[31m ERROR: {e}\033[0m")
        
        # Execute the workflow using shared DAG executor
        with self._time_operation(tl_root['timestamps'], 'workflow_execution'):
            final_context = self.dag_executor.execute_dag(dag, context, request_id)
        
        return final_context

    def invoke_function(self, function_name: str, request_id: str, body: Any) -> Dict:
        """
        Execute a function using ROUTING logic to select the best container.
        """
        self.logger.info(f"Running function '{function_name}' (request: {request_id})")
        # Prepare client-side timeline container
        timeline: Dict[str, Any] = {
            'function': function_name,
            'request_id': request_id,
            'timestamps': {}
        }
        # OVERHEAD 1: Timeline setup and context propagation
        with self._time_overhead_operation(timeline['timestamps'], 'timeline_setup'):
            # Attach into workflow-level container list early
            try:
                tl_root = self._get_or_create_timeline(request_id)
                tl_root.setdefault('functions', []).append(timeline)
            except Exception as e:
                self._debug(f"Error adding function to timeline: {e}")
        
        # OVERHEAD 2: Container discovery and filtering with capacity enforcement
        with self._time_overhead_operation(timeline['timestamps'], 'container_discovery'):
            # Get available containers
            available_containers = self.function_container_manager.get_running_containers(function_name)
            # If none found, force a discovery and retry once
            if not available_containers:
                try:
                    self.function_container_manager.discover_existing_containers()
                    available_containers = self.function_container_manager.get_running_containers(function_name)
                except Exception:
                    pass
            # Proactively filter out stale containers by checking /health
            try:
                if available_containers:
                    def _is_container_ready(c):
                        try:
                            import requests as _req
                            host_port = getattr(c, 'host_port', None)
                            if host_port is None:
                                self.logger.debug(f"Container {c.container_id} has no host_port")
                                return False
                            try:
                                url = f"http://127.0.0.1:{host_port}/"
                                resp = _req.get(url, timeout=1.0)  # Increased timeout
                                if resp.status_code == 200:
                                    self.logger.debug(f"Container {c.container_id} is ready on port {host_port}")
                                    return True
                            except Exception as e:
                                self.logger.debug(f"Container {c.container_id} health check failed: {e}")
                            self.logger.debug(f"Container {c.container_id} failed all health checks")
                            return False
                        except Exception as e:
                            self.logger.debug(f"Container {c.container_id} readiness check error: {e}")
                            return False
                    ready = [c for c in available_containers if _is_container_ready(c)]
                    if not ready:
                        self.logger.info(f"No containers passed health check for {function_name}, trying discovery refresh")
                        # Refresh discovery once and retry readiness check
                        self.function_container_manager.discover_existing_containers()
                        discovered = self.function_container_manager.get_running_containers(function_name)
                        ready = [c for c in discovered if _is_container_ready(c)]
                    
                    # If still no ready containers, use all available containers as fallback
                    if not ready and available_containers:
                        self.logger.warning(f"No containers passed health check for {function_name}, using all {len(available_containers)} containers as fallback")
                        ready = available_containers
                    
                    available_containers = ready
            except Exception as e:
                self.logger.error(f"Container readiness check failed: {e}")
                # Keep original containers if readiness check fails completely
            
            # Exclude generic workers explicitly for ours scheduler
            available_containers = [c for c in available_containers if 'generic-worker' not in (c.image_name or '')]
            
            if not available_containers:
                # DEPLOYMENT DISABLED: No container deployment logic
                self.logger.warning(f"No ready containers found for {function_name} - deployment feature disabled")
                return {
                    'status': 'error',
                    'message': f'No containers available for function {function_name}'
                }
                
                # # CAPACITY ENFORCEMENT: Try to deploy new container if none available (DISABLED)
                # self.logger.info(f"No ready containers found for {function_name}, attempting to deploy new container")
                # 
                # # Get function resource requirements
                # task_profile = self.cost_models.get_task_profile(function_name)
                # cpu_requirement = task_profile.cpu_intensity if task_profile else self.function_container_manager.default_cpu_limit
                # memory_requirement = task_profile.memory_requirement if task_profile else self.function_container_manager.default_memory_limit
                # 
                # # Find best node for deployment
                # best_node = self._find_best_deployment_node(function_name, cpu_requirement, memory_requirement)
                # if best_node:
                #     # Deploy new container with capacity enforcement
                #     new_container = self.function_container_manager.deploy_function_container(
                #         function_name, best_node, cpu_limit=cpu_requirement, memory_limit=memory_requirement
                #     )
                #     if new_container:
                #         available_containers = [new_container]
                #         self.logger.info(f"Successfully deployed new container for {function_name} on {best_node}")
                #     else:
                #         return {
                #             'status': 'error',
                #             'message': f'Failed to deploy container for function {function_name} - insufficient capacity'
                #         }
                # else:
                #     return {
                #         'status': 'error',
                #         'message': f'No nodes available with sufficient capacity for function {function_name}'
                #     }
        
        # PHASE 2: Targeted LNS refinement at function invocation
        with self._time_scheduling_operation('lns'):
            refined_placement = self._apply_targeted_lns_refinement(
                function_name, request_id, available_containers
            )
        
        # OVERHEAD 3: Container selection logic and routing preparation
        with self._time_scheduling_operation('route'):
            # Use refined containers if LNS provided optimization
            containers_to_route = refined_placement if refined_placement else available_containers
            
            # Routing preparation
            routing_strategy = RoutingStrategy.LEAST_LOADED
            routing_request = RoutingRequest(
                function_name=function_name,
                request_id=request_id,
                latency_priority=True
            )
            routing_result = self.routing_engine.route_request(
                routing_request,
                containers_to_route,
                routing_strategy
            )

            container_id = routing_result.target_container.container_id
            node_id = routing_result.target_container.node_id
            input_size = len(str(body)) if body else 0
        
        if not routing_result:
            return {
                'status': 'error',
                'message': f'No suitable container found for routing {function_name}'
            }
                
        # OVERHEAD 5: Network client send/receive (timed within _send_request_to_container)
        start_time = time.time()
        try:
            with resource_monitor.monitor_function_invocation(
                function_name=function_name,
                container_id=container_id,
                node_id=node_id,
                input_size=input_size
            ) as metrics:
                response = self._send_request_to_container(
                    routing_result.target_container,
                    function_name,
                    body,
                    timeline
                )
                
                
                # Update output size in metrics and persist output
                if isinstance(response, dict):
                    # Prefer 'result' for size if available; otherwise use whole payload
                    payload_for_size = response.get('result', response)
                    try:
                        output_size = len(str(payload_for_size))
                        metrics.output_size_bytes = output_size
                    except Exception:
                        pass
                    # Persist output for any non-error status
                    status_val = response.get('status', 'ok')
                    if status_val != 'error':
                        try:
                            # Extract workflow request ID properly
                            workflow_req_id = getattr(self, '_current_workflow_request_id', None)
                            if not workflow_req_id:
                                # Extract from request_id if not stored
                                if '_recognizer__' in request_id:
                                    workflow_req_id = request_id.rsplit('_recognizer__', 1)[0]
                                elif '_' in request_id:
                                    workflow_req_id = request_id.rsplit('_', 1)[0]
                                else:
                                    workflow_req_id = request_id
                            
                            self._persist_function_output(
                                workflow_request_id=workflow_req_id,
                                function_name=function_name,
                                node_id=node_id,
                                result_payload=payload_for_size
                            )
                        except Exception as persist_err:
                            self.logger.warning(f"Output persistence failed for {function_name}: {persist_err}")
                
            response_time = time.time() - start_time
            
            # Add client timestamps to response for DAG executor compatibility
            if isinstance(response, dict):
                response.setdefault('client_timestamps', {})
                response['client_timestamps'].update({
                    'sent': start_time, 
                    'received': start_time + response_time
                })
            
            # OVERHEAD 6: Result processing and metrics recording
            with self._time_overhead_operation(timeline['timestamps'], 'result_processing'):
                # Record routing result for metrics
                success = response.get('status') != 'error'
                self.routing_engine.record_request_result(
                    routing_result.target_container.container_id,
                    response_time,
                    success
                )
                
                # Profiling removed
                
                # Attach client-side timeline to response for downstream consumers
                response.setdefault('timeline', timeline)
                
                # Ensure derived metrics are included in the response
                if 'derived' in timeline:
                    response.setdefault('derived', timeline['derived'])
                # Propagate status/message into the per-function timeline so ExperimentRunner can persist errors
                status_text = response.get('status', 'ok')
                if isinstance(timeline, dict):
                    timeline['status'] = status_text
                    if status_text == 'error':
                        timeline['message'] = response.get('message') or response.get('error') or response.get('detail')
            
            # OVERHEAD 7: Attach scheduling timings BEFORE timeline merging
            with self._time_overhead_operation(timeline['timestamps'], 'scheduling_overhead_calculation'):
                # Attach scheduling timings (lns/route) into timeline for reporting BEFORE merging
                try:
                    timeline.setdefault('timestamps', {})
                    lns_duration_ms = 0.0
                    route_duration_ms = 0.0
                    if hasattr(self, '_scheduling_timings') and isinstance(self._scheduling_timings, dict):
                        lns_duration_ms = float(self._scheduling_timings.get('lns', 0.0))
                        route_duration_ms = float(self._scheduling_timings.get('route', 0.0))
                    # Record explicit duration keys only
                    if lns_duration_ms > 0.0:
                        timeline['timestamps']['lns_duration_ms'] = lns_duration_ms
                    timeline['timestamps']['route_duration_ms'] = route_duration_ms
                    # Aggregate per-function scheduling overhead for portability
                    total_scheduling_ms = float(lns_duration_ms) + float(route_duration_ms)
                    timeline['timestamps']['scheduling_overhead_ms'] = total_scheduling_ms
                except Exception as e:
                    print(f"\033[31m ERROR: {e}\033[0m")

            # OVERHEAD 8: Timeline merging and context propagation
            with self._time_overhead_operation(timeline['timestamps'], 'timeline_merging'):
                # Store derived metrics in workflow timeline for experiment runner
                try:
                    if 'derived' in timeline and hasattr(self, '_current_workflow_request_id'):
                        workflow_request_id = self._current_workflow_request_id
                        tl_root = self._get_or_create_timeline(workflow_request_id)
                        
                        # Find the corresponding function timeline in the workflow
                        # The timeline was added earlier with the same request_id
                        if 'functions' in tl_root:
                            for func_timeline in tl_root['functions']:
                                if (func_timeline.get('function') == function_name and 
                                    func_timeline.get('request_id') == request_id):
                                    # Merge all timing data to preserve previously recorded client-side overheads
                                    try:
                                        # timestamps
                                        func_ts = func_timeline.setdefault('timestamps', {})
                                        src_ts = timeline.get('timestamps', {}) or {}
                                        if isinstance(func_ts, dict) and isinstance(src_ts, dict):
                                            func_ts.update(src_ts)
                                        else:
                                            func_timeline['timestamps'] = src_ts
                                        # server_timestamps
                                        func_srv = func_timeline.setdefault('server_timestamps', {})
                                        src_srv = timeline.get('server_timestamps', {}) or {}
                                        if isinstance(func_srv, dict) and isinstance(src_srv, dict):
                                            func_srv.update(src_srv)
                                        else:
                                            func_timeline['server_timestamps'] = src_srv
                                        # derived
                                        func_drv = func_timeline.setdefault('derived', {})
                                        src_drv = timeline.get('derived', {}) or {}
                                        if isinstance(func_drv, dict) and isinstance(src_drv, dict):
                                            func_drv.update(src_drv)
                                        else:
                                            func_timeline['derived'] = src_drv
                                    except Exception:
                                        # Fallback to overwrite if any issue occurs
                                        func_timeline['timestamps'] = timeline.get('timestamps', {})
                                        func_timeline['server_timestamps'] = timeline.get('server_timestamps', {})
                                        func_timeline['derived'] = timeline.get('derived', {})
                                    
                                    # Also persist status and any error/message so ExperimentRunner can reflect failures
                                    if 'status' in timeline:
                                        func_timeline['status'] = timeline.get('status')
                                    if 'message' in timeline:
                                        func_timeline['message'] = timeline.get('message')
                                    if 'error_message' in timeline:
                                        func_timeline['error_message'] = timeline.get('error_message')
                                    break
                            else:
                                # Add the timeline if not found
                                tl_root['functions'].append(timeline)
                        else:
                            # Create functions array if it doesn't exist
                            tl_root['functions'] = [timeline]
                except Exception as e:
                    pass
                
                # CRITICAL: Also ensure scheduling overhead is included in the response for DAG executor
                # The DAG executor needs this data to pass to the experiment runner
                if 'timestamps' in timeline and 'scheduling_overhead_ms' in timeline['timestamps']:
                    response.setdefault('timestamps', {})
                    response['timestamps']['scheduling_overhead_ms'] = timeline['timestamps']['scheduling_overhead_ms']

            # Store function result in workflow_results for dependency tracking
            try:
                if hasattr(self, '_current_workflow_request_id') and self._current_workflow_request_id:
                    if self._current_workflow_request_id not in self.workflow_results:
                        self.workflow_results[self._current_workflow_request_id] = {}
                    
                    # Store the function result for dependency resolution
                    self.workflow_results[self._current_workflow_request_id][function_name] = response
                    # Log only function names and status, not full response data
                    self.logger.debug(f"Stored result for {function_name}.")
            except Exception as e:
                self.logger.warning(f"Failed to store function result in workflow_results: {e}")

            # Log based on function result status
            try:
                status_text = response.get('status', 'ok')
                if status_text == 'error':
                    self.logger.error(f"Function {function_name} returned error on {routing_result.target_container.node_id}: {response.get('message') or response.get('error')}")
                else:
                    self.logger.info(f"Function {function_name} executed successfully on {routing_result.target_container.node_id}")
            except Exception as e:
                print(f"\033[31m ERROR: {e}\033[0m")
            return response
            
        except Exception as e:
            response_time = time.time() - start_time
            
            # OVERHEAD 8: Error handling and timeline merging
            with self._time_overhead_operation(timeline['timestamps'], 'error_handling'):
                self.routing_engine.record_request_result(
                    routing_result.target_container.container_id,
                    response_time,
                    False
                )
                
                timeline['timestamps']['client_error_time'] = time.time()
                error_message = f"Failed to execute {function_name}: {e}"
                self.logger.error(f"\033[31m{error_message}\033[0m")
            
                # Add client timestamps to error response for DAG executor compatibility
                error_response = {'status': 'error', 'message': error_message, 'timeline': timeline}
                error_response.setdefault('client_timestamps', {})
                error_response['client_timestamps'].update({
                    'sent': start_time, 
                    'received': start_time + response_time
                })
                
                # Ensure derived metrics are included in error response too
                if 'derived' in timeline:
                    error_response.setdefault('derived', timeline['derived'])
            
            return error_response
    
        # Note: scheduling timings already recorded before success return

    def _send_request_to_container(self, container: FunctionContainer, 
                                  function_name: str, body: Any,
                                  timeline: Optional[Dict] = None) -> Dict:
        """Send request to container with storage context for input retrieval."""
        # Update access tracking before/after as needed
        try:
            container.last_accessed = time.time()
            container.request_count += 1
        except Exception as e:
            print(f"\033[31m ERROR: {e}\033[0m")
        
        # Use direct data passing for all functions
        # The DAG executor now handles input preparation, so we just pass the data directly
        self.logger.info(f"\033[93mFunction {function_name} receiving input via direct data passing\033[0m")
        return OrchestratorUtils.send_request_to_container(container, function_name, body, timeline, timeout=300)
    
    def _determine_input_key_for_function(self, function_name: str, timeline: Optional[Dict] = None) -> Optional[str]:
        """Determine the input key for a function based on DAG dependencies"""
        try:
            # Get the workflow request ID from timeline
            request_id = timeline.get('request_id', 'unknown') if timeline else 'unknown'
            
            # Get the current workflow request ID to find the active workflow
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
                self.logger.warning(f"Available workflows: {list(self.active_workflows.keys())}")
                return None
            
            dag = active_workflow.get('dag', {})
            if not dag:
                self.logger.warning(f"No DAG found in active workflow")
                return None
            
            # Analyze DAG structure to find dependencies for this function
            workflow_section = dag.get('workflow', {})
            nodes = workflow_section.get('nodes', [])
            
            # Find the node definition for this function
            function_node = None
            for node in nodes:
                if node.get('id') == function_name and node.get('type') == 'task':
                    function_node = node
                    break
            
            if not function_node:
                self.logger.warning(f"No node definition found for function {function_name}")
                return None
            
            # Check if this function has input dependencies
            input_spec = function_node.get('in', {})
            if not input_spec:
                # No input specification means this is a first function
                return None
            
            # Check if any input references other functions (not just $inputs)
            dependency_functions = []
            for input_key, input_value in input_spec.items():
                if isinstance(input_value, str) and input_value.startswith('$') and not input_value.startswith('$inputs'):
                    # Parse $recognizer__upload.img_b64 -> recognizer__upload
                    parts = input_value[1:].split('.')
                    if len(parts) >= 1:
                        source_function = parts[0]
                        if source_function != function_name:  # Avoid self-dependency
                            dependency_functions.append(source_function)
            
            if not dependency_functions:
                # Only depends on workflow inputs, not other functions
                return None
            
            # Find the most recently completed dependency function
            if hasattr(self, 'workflow_results') and workflow_request_id in self.workflow_results:
                completed_functions = list(self.workflow_results[workflow_request_id].keys())
                
                # Find the most recent dependency that has completed
                for completed_function in reversed(completed_functions):
                    if completed_function in dependency_functions:
                        input_key = f"{workflow_request_id}_{completed_function}"
                        return input_key
            
            # If no dependency functions have completed yet, this function should wait
            # Return None to indicate this function has dependencies but they're not ready
            self.logger.warning(f"Function {function_name} has dependencies {dependency_functions} but none have completed yet")
            return None
            
        except Exception as e:
            self.logger.warning(f"Error determining input key for {function_name}: {e}")
            return None
    
    def _find_best_deployment_node(self, function_name: str, cpu_requirement: float, memory_requirement: float) -> Optional[str]:
        """Find the best node for deploying a new container based on capacity and cost"""
        try:
            # Get all nodes with sufficient capacity
            available_nodes = self.function_container_manager._find_nodes_with_capacity(cpu_requirement, memory_requirement)
            
            if not available_nodes:
                return None
            
            # If we have cost models, use them to find the best node
            if hasattr(self, 'cost_models') and self.cost_models:
                task_profile = self.cost_models.get_task_profile(function_name)
                if task_profile:
                    # Find node with lowest estimated cost
                    best_node = None
                    best_cost = float('inf')
                    
                    for node_id in available_nodes:
                        if node_id in self.cost_models.node_profiles:
                            node_profile = self.cost_models.node_profiles[node_id]
                            cost_estimate = self.cost_models.get_all_costs(task_profile, node_profile, [])
                            total_cost = cost_estimate.get('total', float('inf'))
                            
                            if total_cost < best_cost:
                                best_cost = total_cost
                                best_node = node_id
                    
                    return best_node
            
            # Fallback: return first available node (already sorted by capacity)
            return available_nodes[0] if available_nodes else None
            
        except Exception as e:
            self.logger.error(f"Error finding best deployment node for {function_name}: {e}")
            return None
    
    def _apply_targeted_lns_refinement(self, function_name: str, request_id: str,
                                     available_containers: List[FunctionContainer]) -> Optional[List[FunctionContainer]]:
        """
        PHASE 2: Apply Targeted LNS refinement at function invocation
        Refines placement decisions made during workflow submission
        """
        try:
            # Check if targeted LNS is enabled in configuration
            placement_config = getattr(self.config, 'placement', {})
            if placement_config.get('default_strategy') != 'targeted_lns':
                return None  # Skip LNS if not configured
            
            # Get current runtime state
            available_nodes = self.function_container_manager.get_nodes()
            existing_containers = self._get_existing_containers_map()
            
            # Create placement request for LNS
            placement_request = PlacementRequest(
                function_name=function_name,
                cpu_requirement=1.0,
                memory_requirement=512,
                time_budget_ms=placement_config.get('targeted_lns', {}).get('time_budget_ms', 20),
                # Add DAG context if available
                parent_tasks=self._get_parent_tasks(function_name),
                dag_structure=self._get_dag_structure(function_name)
            )
            
            # Apply targeted LNS placement
            lns_result = self.placement_engine.find_best_placement(
                placement_request,
                available_nodes,
                existing_containers,
                PlacementStrategy.TARGETED_LNS
            )
            
            if lns_result:
                # Filter containers to prefer the LNS-optimized node
                optimized_containers = [
                    container for container in available_containers 
                    if container.node_id == lns_result.target_node
                ]
                
                if optimized_containers:
                    self.logger.info(f"LNS refinement: routing {function_name} to optimized node {lns_result.target_node}")
                    return optimized_containers
            
            return None  # No optimization found, use original containers
            
        except Exception as e:
            self.logger.warning(f"Error in targeted LNS refinement: {e}")
            return None  # Fallback to original containers on error

    def _persist_function_output(self, workflow_request_id: str, function_name: str, node_id: str, result_payload: Any) -> None:
        """Persist function output to /mnt/node_data/<request_id>/ and optionally to MinIO via remote adapter.

        Local filename (node-aware):
          /mnt/node_data/<request_id>/<request_id>_<function_name>_<node_id>.json

        Remote data_id (no node):
          <request_id>_<function_name>
        """
        self.logger.info(f"DEBUG: _persist_function_output called for {function_name} with workflow_request_id: {workflow_request_id}")
        try:
            # Ensure shared volume dir - use host-accessible path
            import pathlib, json
            shared_volume_root = '/tmp/dataflower_shared'
            exp_dir = pathlib.Path(shared_volume_root) / workflow_request_id
            exp_dir.mkdir(parents=True, exist_ok=True)

            # Build filenames
            local_filename = f"{workflow_request_id}_{function_name}_{node_id}.json"
            local_path = exp_dir / local_filename
            remote_data_id = f"{workflow_request_id}_{function_name}"

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
            self.logger.info(f"DEBUG: remote_storage_adapter is {type(self.remote_storage_adapter)}")
            if self.remote_storage_adapter is not None:
                try:
                    # Prepare DataMetadata
                    size_bytes = 0
                    try:
                        payload_bytes = json.dumps(serializable).encode('utf-8') if isinstance(serializable, (dict, list)) else str(serializable).encode('utf-8')
                        size_bytes = len(payload_bytes)
                        data_to_store = serializable  # adapter handles json/str/bytes
                    except Exception:
                        data_to_store = serializable

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
    
    def greedy_placement(self, nodes: List) -> None:
        """Initial greedy placement"""
        available_nodes = self.function_container_manager.get_nodes()
        existing_containers_all = self._get_existing_containers_map()

        for node in nodes:
            if node.get('type') != 'task':
                continue
            function_name = node.get('id')
            if not function_name:
                continue

            # Skip if there is already at least one running container
            if self.function_container_manager.get_running_containers(function_name):
                continue

            # Build placement request with basic profile and DAG context if available
            task_profile = self.cost_models.get_task_profile(function_name)
            cpu_req = task_profile.cpu_intensity if task_profile else 1.0
            mem_req = task_profile.memory_requirement if task_profile else 512

            placement_request = PlacementRequest(
                function_name=function_name,
                cpu_requirement=cpu_req,
                memory_requirement=mem_req,
                priority=1,
                parent_tasks=self._get_parent_tasks(function_name),
                dag_structure=self._get_dag_structure(function_name)
            )

            placement_result = self.placement_engine.find_best_placement(
                placement_request,
                available_nodes,
                existing_containers_all,
                PlacementStrategy.INITIAL_GREEDY
            )

            # DEPLOYMENT DISABLED: Skip container deployment in greedy placement
            # if placement_result:
            #     container = self.function_container_manager.deploy_function_container(
            #         function_name,
            #         placement_result.target_node
            #     )
            #     if container:
            #         self.placement_engine.record_placement_decision(placement_request, placement_result)
            #         existing_containers_all = self._get_existing_containers_map()
    
    def _perform_workflow_cost_analysis(self, nodes: List, context: Dict) -> None:
        """
        Perform workflow-level cost model analysis.
        This includes analyzing all functions in the workflow and preparing cost-aware decisions.
        """
        self._debug(f"Performing workflow-level cost model analysis for {len(nodes)} nodes")
        
        # Analyze each function in the workflow
        for node in nodes:
            if node.get('type') == 'task':
                function_name = node.get('function')
                if not function_name:
                    continue
                
                # Get task profile for this function (this will cache it)
                task_profile = self.cost_models.get_task_profile(function_name)
                self._debug(f"Analyzed function {function_name}: CPU={task_profile.cpu_intensity:.2f}, Memory={task_profile.memory_requirement}MB")
        
        self._debug(f"Workflow cost model analysis completed")
    
    def _analyze_workflow_bottlenecks(self, nodes: List, edges: List, context: Dict, profiling_suggestions: Dict = None) -> Any:
        """Analyze workflow bottlenecks using the bottleneck identifier"""
        try:
            # TODO: Use profiling_suggestions to inform bottleneck analysis
            # For now, we'll use the standard bottleneck analysis
            # In the future, we can use profiling data to predict which nodes/edges are likely bottlenecks
            
            # Get node resources for bottleneck analysis
            available_nodes = self.function_container_manager.get_nodes()
            
            node_resources = {}
            for node in available_nodes:
                # Handle both NodeResources objects and dictionaries
                if hasattr(node, 'node_id'):
                    node_id = node.node_id
                    cpu_cores = node.cpu_cores
                    memory_gb = node.memory_gb
                elif isinstance(node, dict):
                    node_id = node.get('node_id')
                    cpu_cores = node.get('cpu_cores', 4)
                    memory_gb = node.get('memory_gb', 8.0)
                else:
                    continue
                
                node_resources[node_id] = {
                    'cpu_cores': cpu_cores,
                    'memory_gb': memory_gb,
                    'bandwidth_mbps': getattr(node, 'bandwidth_mbps', 1000) if hasattr(node, 'bandwidth_mbps') else 1000
                }
            
            # Convert nodes to the format expected by bottleneck identifier
            bottleneck_nodes = []
            for node in available_nodes:
                # Handle both NodeResources objects and dictionaries
                if hasattr(node, 'node_id'):
                    node_id = node.node_id
                    cpu_cores = node.cpu_cores
                    memory_gb = node.memory_gb
                elif isinstance(node, dict):
                    node_id = node.get('node_id')
                    cpu_cores = node.get('cpu_cores', 4)
                    memory_gb = node.get('memory_gb', 8.0)
                else:
                    continue
                
                bottleneck_nodes.append({
                    'node_id': node_id,
                    'cpu_cores': cpu_cores,
                    'memory_gb': memory_gb,
                    'bandwidth_mbps': getattr(node, 'bandwidth_mbps', 1000) if hasattr(node, 'bandwidth_mbps') else 1000
                })
            
            # Convert workflow nodes to tasks format
            tasks = []
            for node in nodes:
                # Handle both dict and object formats for DAG nodes
                if isinstance(node, dict):
                    node_type = node.get('type')
                    function_name = node.get('function') or node.get('id')
                    task_id = node.get('id')
                else:
                    # Handle object format
                    node_type = getattr(node, 'type', None)
                    function_name = getattr(node, 'function', None) or getattr(node, 'id', None)
                    task_id = getattr(node, 'id', None)
                
                if node_type == 'task':
                    if not function_name:  # Skip nodes without function names
                        continue
                        
                    task_profile = self.cost_models.get_task_profile(function_name)
                    
                    tasks.append({
                        'task_id': task_id,
                        'function_name': function_name,
                        'cpu_intensity': task_profile.cpu_intensity if task_profile else 0.5,
                        'memory_requirement': task_profile.memory_requirement if task_profile else 512,
                        'input_size': len(context.get('inputs', {}).get('img', b''))
                    })
            
            # Create DAG structure in the format expected by bottleneck identifier
            # Convert to task-centric format: {task_id: {'dependencies': [...]}}
            dag_structure = {}
            for node in nodes:
                # Handle both dict and object formats for DAG nodes
                if isinstance(node, dict):
                    task_id = node.get('id')
                    node_type = node.get('type')
                else:
                    task_id = getattr(node, 'id', None)
                    node_type = getattr(node, 'type', None)
                
                if node_type == 'task' and task_id:
                    # Find dependencies for this task
                    dependencies = []
                    for edge in edges:
                        # Handle both dict and object formats for edges
                        if isinstance(edge, dict):
                            source = edge.get('source')
                            target = edge.get('target')
                            data_size = edge.get('data_size', 1024)  # Default 1KB
                        else:
                            source = getattr(edge, 'source', None)
                            target = getattr(edge, 'target', None)
                            data_size = getattr(edge, 'data_size', 1024)
                        
                        if target == task_id:  # This edge points to our task
                            dependencies.append({
                                'task_id': source,
                                'data_size': data_size
                            })
                    
                    dag_structure[task_id] = {
                        'dependencies': dependencies
                    }
            
            # Convert NodeResources to NodeProfile objects for bottleneck analysis
            node_profiles = []
            for node in available_nodes:
                if hasattr(node, 'node_id'):
                    # Convert NodeResources to NodeProfile
                    from .cost_models import NodeProfile
                    node_profile = NodeProfile(
                        node_id=node.node_id,
                        cpu_cores=node.cpu_cores,
                        cpu_frequency=2.4,  # Default frequency
                        memory_total=node.memory_gb,
                        memory_available=node.available_memory,
                        disk_total=100.0,  # Default disk size
                        disk_available=80.0,  # Default available disk
                        cpu_utilization=0.0,  # Default utilization
                        memory_utilization=0.0,  # Default utilization
                        disk_utilization=0.2,  # Default utilization
                        network_bandwidth=1000.0,  # Default bandwidth
                        labels=node.labels,
                        running_containers=len(node.running_containers)
                    )
                    node_profiles.append(node_profile)
            
            # Perform bottleneck analysis
            bottleneck_analysis = self.bottleneck_identifier.identify_bottlenecks(
                tasks=tasks,
                nodes=node_profiles,
                dag_structure=dag_structure,
                cost_models=self.cost_models
            )
            
            return bottleneck_analysis
            
        except Exception as e:
            print(f"\033[31m ERROR: {e}\033[0m")
            # Return no bottleneck if analysis fails
            from .bottleneck_identifier import BottleneckAnalysis
            return BottleneckAnalysis(
                has_bottleneck=False,
                bottleneck_type=None,
                bottleneck_severity=0.0,
                critical_nodes=[],
                critical_edges=[]
            )

    # =================================================================
    # DAG-RELATED & METRICS METHODS (Workflow Execution, Graph Building)
    # =================================================================
    
    def _get_parent_tasks(self, function_name: str) -> Optional[List[str]]:
        """Get parent tasks from active workflow DAG"""
        return OrchestratorUtils.get_parent_tasks_from_workflows(function_name, self.active_workflows)
    
    def _get_dag_structure(self, function_name: str) -> Optional[Dict]:
        """Get DAG structure from active workflows"""
        return OrchestratorUtils.get_dag_structure_from_workflows(function_name, self.active_workflows)

    def _get_existing_containers_map(self) -> Dict[str, List[FunctionContainer]]:
        """Build a mapping function_name -> List[FunctionContainer] from manager registry."""
        return OrchestratorUtils.build_existing_containers_map(self.function_container_manager)
    
    def get_resource_metrics(self) -> Dict:
        """Get resource utilization metrics for all function invocations"""
        return OrchestratorUtils.get_resource_metrics()
    
    def get_function_resource_summary(self, function_name: str) -> Dict:
        """Get resource utilization summary for a specific function"""
        return OrchestratorUtils.get_function_resource_summary(function_name, self.function_container_manager)
    
    def get_cluster_resource_status(self) -> Dict:
        """Get comprehensive cluster resource status"""
        return OrchestratorUtils.get_cluster_resource_status(self.function_container_manager)
    
    def extract_resource_utilization(self) -> Dict[str, Any]:
        """
        Extract comprehensive resource utilization metrics for containers and nodes.
        Returns aggregated metrics suitable for experiment reporting.
        """
        return OrchestratorUtils.extract_resource_utilization(self.function_container_manager)
    
    # Profiling analysis removed
    
    def _estimate_input_size(self, func_name: str, context: Dict) -> float:
        """
        Estimate input size for a function based on context.
        This is a simple heuristic - can be improved with more sophisticated analysis.
        """
        # Try to get actual input size from context
        if 'inputs' in context:
            inputs = context['inputs']
            if isinstance(inputs, dict):
                # Look for image data
                if 'img' in inputs:
                    img_data = inputs['img']
                    if isinstance(img_data, bytes):
                        return len(img_data) / (1024 * 1024)  # Convert to MB
                    elif isinstance(img_data, str):
                        return len(img_data.encode()) / (1024 * 1024)
        
        # Fallback: estimate based on function type
        if 'recognizer' in func_name.lower():
            return 1.0  # Default image size
        elif 'extract' in func_name.lower():
            return 0.5  # Smaller for text extraction
        else:
            return 0.1  # Default small size
    
    # Execution profiling removed
    
    # Network profiling removed
    
    def run_calibration(self, functions: List[str] = None, input_sizes_mb: List[float] = None) -> bool:
        """
        Run calibration phase to create static profiles
        
        Args:
            functions: List of function names to calibrate (default: all registered functions)
            input_sizes_mb: List of input sizes to test (default: [0.1, 0.5, 1.0, 2.0, 5.0])
        
        Returns:
            True if calibration succeeded, False otherwise
        """
        try:
            # Default functions if not provided
            if functions is None:
                functions = list(self.workflows.keys()) if self.workflows else ['recognizer__upload', 'recognizer__adult', 'recognizer__violence', 'recognizer__extract']
            
            # Default input sizes if not provided
            if input_sizes_mb is None:
                input_sizes_mb = [0.1, 0.5, 1.0, 2.0, 5.0]
            
            self.logger.info(f"Starting calibration for functions: {functions}")
            self.logger.info(f"Input sizes: {input_sizes_mb} MB")
            
            # Run calibration
            profiles = self.calibration_runner.run_calibration(functions, input_sizes_mb)
            
            # Save profiles
            success = self.calibration_runner.static_profiler.save_profiles(profiles)
            
            if success:
                # Reload profiles in cost models
                self.cost_models.static_profiler.load_profiles()
                self.logger.info("Calibration completed successfully")
                return True
            else:
                self.logger.error("Failed to save calibration profiles")
                return False
                
        except Exception as e:
            self.logger.error(f"Calibration failed: {e}")
            return False