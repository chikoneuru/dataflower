import json
import logging
import time
from typing import Any, Dict, List, Optional
from contextlib import contextmanager

import requests

from provider.container_manager import ContainerManager
from scheduler.shared.dag_executor import DAGExecutor
from scheduler.shared.safe_printing import safe_print_value
from scheduler.shared.orchestrator_utils import OrchestratorUtils
from scheduler.shared import ContainerStorageService, UnifiedInputHandler, create_storage_service, create_input_handler

from .greedy_grouping import GreedyGrouper
from .predictor import ExecutionTimePredictor, StageProfiler
from .dop_ratio import DoPRatioComputer
from .placement import DittoPlacement
from .plan_evaluator import PlanEvaluator
from .config import get_ditto_config
from .debug_utils import create_debug_context


class FunctionOrchestrator:
    """A minimal Ditto-style orchestrator compatible with ExperimentRunner.

    Implements:
      - register_workflow(workflow_name, dag)
      - submit_workflow(workflow_name, request_id, input_data)
      - schedule_workflow(workflow_name, request_id, input_data)
      - invoke_function(function_name, request_id, payload, worker)
    """

    def __init__(self, container_manager: Optional[ContainerManager] = None) -> None:
        self.container_manager = container_manager or ContainerManager()
        self.workflows: Dict[str, Dict[str, Any]] = {}
        self.workflow_results: Dict[str, Dict[str, Any]] = {}
        
        # Initialize logger
        self.logger = logging.getLogger('scheduler.ditto.function_orchestrator')
        
        # Initialize Ditto components
        self.config = get_ditto_config()
        self.grouper = GreedyGrouper()
        self.predictor = ExecutionTimePredictor()
        self.profiler = StageProfiler()
        self.dop_computer = DoPRatioComputer(self.predictor)
        self.placement = DittoPlacement()
        self.plan_evaluator = PlanEvaluator(self.predictor)

        # Core state
        self._workflows: Dict[str, Dict[str, Any]] = {}
        self.workflows: Dict[str, Dict[str, Any]] = {}
        self.workflow_results: Dict[str, Any] = {}
        self.active_workflows: Dict[str, Dict[str, Any]] = {}
        self.request_timelines: Dict[str, Dict[str, Any]] = {}
        self._placement_by_workflow: Dict[str, Dict[str, str]] = {}
        self._workflow_for_request: Dict[str, str] = {}
        self._init_overhead_by_workflow: Dict[str, Dict[str, float]] = {}
        
        # Initialize debug components
        self.debug_logger, self.debug_metrics, self.debug_plan_saver = create_debug_context(self.config)
        
        # Initialize storage service for remote data transfer
        self.storage_service = create_storage_service('ditto')
        self.input_handler = create_input_handler('ditto')
        
        if self.config.debug_mode:
            self.debug_logger.info("Ditto debug mode enabled", 
                                 log_level=self.config.debug_log_level,
                                 save_plans=self.config.debug_save_plans)

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
            if not hasattr(self, '_init_timings'):
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
            if not hasattr(self, '_scheduling_timings'):
                self._scheduling_timings = {}
            self._scheduling_timings[operation_name] = duration_ms


    # --- Public API ---
    def register_workflow(self, workflow_name: str, workflow_dag: Dict[str, Any]) -> None:
        """Register a workflow DAG using Ditto's integrated planning approach.
        
        Implements the DittoRegisterWorkflow pseudocode:
        1. Grouping: Groups consecutive stages when shuffle size exceeds threshold
        2. Build execution time models for each group
        3. Compute optimal DoP ratios
        4. Place groups on servers
        5. Predict end-to-end job time/cost
        """
        # Reset/init per-registration timing breakdown
        self._init_timings = {}

        # Start timing the entire registration process
        with self._time_init_operation("setup"):
            self.debug_metrics.start_timer(f"workflow_registration_{workflow_name}")
            
            self.debug_logger.info(f"Registering workflow '{workflow_name}' with integrated planning")
            self.logger.debug(f"🔧 Ditto: Registering workflow '{workflow_name}' with integrated planning")
            
            # Get available servers and resources
            servers = self._get_available_servers()
            resources = self._get_server_resources()
            
            self.debug_logger.debug("Server discovery completed", 
                                servers=servers, 
                                resources=resources)
            
            if not servers:
                error_msg = "No servers available for Ditto workflow registration"
                self.debug_logger.error(error_msg)
                self.logger.debug(f"❌ {error_msg}")
                self.debug_metrics.end_timer(f"workflow_registration_{workflow_name}", 
                                        {"status": "error", "reason": "no_servers"})
                raise RuntimeError(error_msg)
        
        try:
            with self._time_init_operation("grouping"):
                # Step 1: Grouping
                self.debug_logger.info("Step 1: Grouping stages based on shuffle threshold")
                self.logger.debug("📊 Step 1: Grouping stages based on shuffle threshold")
                
                self.debug_metrics.start_timer("grouping_operation")
                # Extract the inner workflow structure for grouping
                inner_dag = workflow_dag.get('workflow', workflow_dag)
                
                # Show shuffle threshold info
                threshold_mb = self.grouper.shuffle_threshold_bytes / (1024 * 1024)
                threshold_kb = self.grouper.shuffle_threshold_bytes / 1024
                self.logger.debug(f"   Shuffle threshold: {threshold_mb:.1f} MB ({threshold_kb:.1f} KB)")
                
                groups = self.grouper.group(inner_dag)
                self.debug_metrics.end_timer("grouping_operation", 
                                        {"num_groups": len(groups), "groups": groups})
                
                self.logger.debug(f"   Created {len(groups)} groups: {groups}")
                self.debug_logger.debug("Stage grouping completed", groups=groups)
            
            with self._time_init_operation("building_models"):
                # Step 2: Build execution time models
                self.debug_logger.info("Step 2: Building execution time models")
                self.logger.debug("⏱️  Step 2: Building execution time models")
            
                models = {}
                dop_candidates = range(self.config.dop_min, self.config.dop_max + 1)
                
                self.debug_logger.debug("DoP candidates range", 
                                    dop_min=self.config.dop_min, 
                                    dop_max=self.config.dop_max,
                                    dop_candidates=list(dop_candidates))
                
                for group in groups:
                    for stage_id in group:
                        self.debug_logger.debug(f"Building model for stage: {stage_id}")
                        self.logger.debug(f"   Building model for stage: {stage_id}")
                        
                        model = self.predictor.build_time_model(
                            stage_id=stage_id,
                            dop_candidates=dop_candidates,
                            placement_candidates=servers,
                            profiler=self.profiler
                        )
                        models[stage_id] = model
                
                self.debug_logger.debug("Execution time models built", 
                                    num_models=len(models),
                                    model_stages=list(models.keys()))
            
            # Step 3: Compute optimal DoP ratios
            with self._time_init_operation("dop_computation"):
                self.debug_logger.info("Step 3: Computing optimal DoP ratios")
                self.logger.debug("🎯 Step 3: Computing optimal DoP ratios")
                
                self.debug_metrics.start_timer(f"dop_computation_{workflow_name}_initial")
                dop_ratios = self.dop_computer.compute(
                    dag=workflow_dag,
                    models_by_stage=models,
                    dop_candidates=dop_candidates,
                    placement_by_stage={},  # Will be filled by placement
                    objective=self.config.objective
                )
                self.debug_metrics.end_timer(f"dop_computation_{workflow_name}_initial", 
                                        {"dop_ratios": dop_ratios})
                
                self.logger.debug(f"   DoP ratios: {dop_ratios}")
                self.debug_logger.debug("Initial DoP ratios computed", dop_ratios=dop_ratios)
            
            # Step 4: Place groups on servers
            with self._time_init_operation("placement"):
                self.debug_logger.info("Step 4: Placing groups on servers")
                self.logger.debug("📍 Step 4: Placing groups on servers")
                
                self.debug_metrics.start_timer(f"placement_{workflow_name}")
                placement = self.placement.place_stages(groups, servers, resources)
                self.debug_metrics.end_timer(f"placement_{workflow_name}", 
                                        {"placement": placement})
                
                self.logger.debug(f"   Placement: {placement}")
                self.debug_logger.debug("Stage placement completed", placement=placement)
            
                # Update DoP computation with actual placement
                self.debug_metrics.start_timer(f"dop_computation_{workflow_name}_final")
                dop_ratios = self.dop_computer.compute(
                    dag=workflow_dag,
                    models_by_stage=models,
                    dop_candidates=dop_candidates,
                    placement_by_stage=placement,
                    objective=self.config.objective
                )
                self.debug_metrics.end_timer(f"dop_computation_{workflow_name}_final", 
                                        {"dop_ratios": dop_ratios, "placement": placement})
                
                self.debug_logger.debug("Final DoP ratios computed with placement", 
                                    dop_ratios=dop_ratios)
            
            # Step 5: Predict end-to-end job time/cost
            with self._time_init_operation("plan_evaluation"):
                self.debug_logger.info("Step 5: Evaluating execution plan")
                self.logger.debug("📈 Step 5: Evaluating execution plan")
                
                self.debug_metrics.start_timer(f"plan_evaluation_{workflow_name}")
                jct_ms, cost = self.plan_evaluator.evaluate_plan(
                    dag=workflow_dag,
                    placement=placement,
                    dop_ratios=dop_ratios,
                    models=models,
                    servers=servers,
                    resources=resources
                )
                self.debug_metrics.end_timer(f"plan_evaluation_{workflow_name}", 
                                        {"jct_ms": jct_ms, "cost": cost})
            
                self.debug_metrics.start_timer(f"plan_evaluation_{workflow_name}")
                jct_ms, cost = self.plan_evaluator.evaluate_plan(
                    dag=workflow_dag,
                    placement=placement,
                    dop_ratios=dop_ratios,
                    models=models,
                    servers=servers,
                    resources=resources
                )
                self.debug_metrics.end_timer(f"plan_evaluation_{workflow_name}", 
                                        {"jct_ms": jct_ms, "cost": cost})
                
                # Store the computed plan, but exclude parallel functions from placement
                # This ensures parallel functions use dynamic worker selection instead
                filtered_placement = {}
                for fn, server in placement.items():
                    fn_dop = dop_ratios.get(fn, 1)
                    # Only include sequential functions in placement plan
                    if fn_dop == 1:
                        filtered_placement[fn] = server
                    
                plan = {
                    "placement": filtered_placement,
                    "DoP_ratios": dop_ratios,
                    "JCT": jct_ms,
                    "Cost": cost,
                    "groups": groups,
                    "models": models,
                    "servers": servers,
                    "resources": resources
                }
                
                self.logger.debug(f"✅ Ditto plan computed - JCT: {jct_ms:.2f}ms, Cost: {cost:.4f}")
                self.debug_logger.info("Ditto plan computed successfully", 
                                    jct_ms=jct_ms, cost=cost)
            
            # Validate that we have a meaningful plan; if not, continue with minimal registration
            with self._time_init_operation("validation"):
                if not groups or not placement or not dop_ratios:
                    warn_msg = (
                        f"Ditto plan is empty - groups: {len(groups)}, placement: {len(placement)}, "
                        f"dop_ratios: {len(dop_ratios)}. Proceeding with minimal registration."
                    )
                    self.debug_logger.warning(warn_msg)
                    self.logger.debug(f"⚠️  {warn_msg}")
                
                # Save plan if debug mode is enabled
                if self.config.debug_save_plans:
                    self.debug_plan_saver.save_plan(workflow_name, plan, "execution")
            
            # Extract task list for compatibility
            with self._time_init_operation("record_registration"):
                tasks = self._extract_tasks_from_dag(workflow_dag)
                
                self.workflows[workflow_name] = {
                    'dag': workflow_dag,
                    'tasks': tasks,
                    # Store plan only if it has content; else keep minimal to allow fallback execution
                    **({'ditto_plan': plan} if groups and placement and dop_ratios else {})
                }
                
                # Record successful completion metrics
                self.debug_metrics.record_metric("workflow_registration", "success", True, {
                    "workflow_name": workflow_name,
                    "num_tasks": len(tasks),
                    "num_groups": len(groups),
                    "jct_ms": jct_ms,
                    "cost": cost
                })
                
                # End timing for successful registration
                self.debug_metrics.end_timer(f"workflow_registration_{workflow_name}", {
                    "status": "success",
                    "num_groups": len(groups),
                    "jct_ms": jct_ms,
                    "cost": cost
                })

            # Store initialization overhead breakdown for external consumers
            try:
                total_ms = 0.0
                if isinstance(getattr(self, "_init_timings", None), dict):
                    # Sum only numeric values
                    total_ms = sum(float(v) for v in self._init_timings.values() if isinstance(v, (int, float)))
                self._init_overhead_by_workflow[workflow_name] = {
                    "init_overhead_ms": total_ms,
                    "components": dict(self._init_timings),
                }
            except Exception:
                pass
            
        except Exception as e:
            # On error, attempt minimal registration so execution can still proceed
            self.debug_logger.error(f"Error in Ditto workflow registration, proceeding minimally: {e}")
            self.logger.warning(f"❌Error in Ditto workflow registration, proceeding minimally: {e}")
            try:
                tasks = self._extract_tasks_from_dag(workflow_dag)
                self.workflows[workflow_name] = {
                    'dag': workflow_dag,
                    'tasks': tasks,
                }
            except Exception:
                pass
            
            # Record error metrics
            self.debug_metrics.record_metric("workflow_registration", "error", str(e), {
                "workflow_name": workflow_name,
                "error_type": type(e).__name__
            })
            
            # End timing for failed registration
            self.debug_metrics.end_timer(f"workflow_registration_{workflow_name}", {
                "status": "error",
                "error": str(e)
            })
                
    
    def _extract_tasks_from_dag(self, workflow_dag: Dict[str, Any]) -> List[str]:
        """Extract task list from workflow DAG."""
        tasks: List[str] = []
        try:
            if isinstance(workflow_dag, dict) and 'workflow' in workflow_dag:
                nodes = (workflow_dag.get('workflow') or {}).get('nodes', [])
                tasks = [n.get('id') for n in nodes if isinstance(n, dict) and n.get('type') == 'task']
            else:
                # Fallback to old format: pipeline is a list of steps
                pipeline = workflow_dag.get('pipeline', []) if isinstance(workflow_dag, dict) else []
                for step in pipeline:
                    if isinstance(step, dict):
                        fn = step.get('function') or step.get('id')
                        if isinstance(fn, str):
                            tasks.append(fn)
                    elif isinstance(step, str):
                        tasks.append(step)
        except Exception:
            tasks = []
        return tasks
    
    def _get_available_servers(self) -> List[str]:
        """Get list of available server IDs."""
        try:
            workers = self._get_workers()
            return [w.get('name') or w.get('id', f'worker-{i}') for i, w in enumerate(workers)]
        except Exception:
            return ['server-0', 'server-1']  # Fallback servers
    
    def _get_node_id(self) -> str:
        """Get current node ID"""
        try:
            pod_name = os.environ.get('HOSTNAME', '')
            if pod_name:
                return pod_name
            
            import socket
            return socket.gethostname()
        except Exception:
            return 'unknown-node'
    
    def _get_server_resources(self) -> Dict[str, Any]:
        """Get server resource information from Ditto workers."""
        try:
            workers = self._get_workers()
            resources = {}
            for worker in workers:
                server_id = worker.get('name') or worker.get('id', 'unknown')
                resources[server_id] = {
                    'cpu_cores': worker.get('cpu_cores', 4.0),
                    'memory_gb': worker.get('memory_gb', 8.0),
                    'cost_per_cpu_hour': 1.0,  # Default cost
                    'capacity': 10,  # Default capacity
                    'node_type': worker.get('node_type', 'ditto-worker')
                }
            return resources
        except Exception as e:
            self.debug_logger.error(f"Failed to get server resources: {e}")
            raise RuntimeError(f"Failed to get server resources: {e}")

    def submit_workflow(self, workflow_name: str, request_id: str, body: bytes) -> Dict[str, Any]:
        """Execute a complete workflow"""
        self._debug(f"run_workflow called for {workflow_name} with request_id {request_id}")
        if workflow_name not in self.workflows:
            return {'status': 'error', 'message': 'Workflow not found'}

        # Reset scheduling timings for this request
        self._scheduling_timings = {}

        # Store current workflow request ID for function calls
        self._current_workflow_request_id = request_id

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
                    self.debug_logger.debug(f"Stored initial input data in MinIO with key: {initial_input_key}")
                else:
                    self.debug_logger.warning(f"Failed to store initial input data in MinIO")
            except Exception as e:
                self.debug_logger.warning(f"Could not store initial input data in MinIO: {e}")
        
        dag = (self.workflows.get(workflow_name) or {}).get('dag') or {}
        context = {'inputs': {'img': body, 'initial_input_key': initial_input_key}}

        tl = self._get_or_create_timeline(request_id)
        tl.setdefault('workflow', {})['start'] = time.time()

        # Attach init overhead
        overhead = self._init_overhead_by_workflow.get(workflow_name, {})
        if "init_overhead_ms" in overhead:
            sched_block = tl.setdefault("scheduler", {})
            sched_block["init_overhead_ms"] = overhead["init_overhead_ms"]
        
        self.debug_logger.info(f"Submitting workflow '{workflow_name}'")
        try:
            if 'workflow' not in dag:
                raise Exception('Invalid DAG structure: missing workflow')
            self._debug("Executing DAG workflow")
            
            # Correct signature: (workflow_name, request_id, input_data)
            final_context = self.schedule_workflow(workflow_name, request_id, body)

            tl['workflow']['end'] = time.time()
            tl['workflow']['duration_ms'] = (tl['workflow']['end'] - tl['workflow']['start']) * 1000.0
            
            # Add scheduling overhead to timeline
            if hasattr(self, '_scheduling_timings') and self._scheduling_timings:
                total_scheduling_ms = sum(self._scheduling_timings.values())
                tl.setdefault('timestamps', {})['scheduling_overhead_ms'] = total_scheduling_ms
            
            # Ensure timeline includes functions for experiment runner extraction
            if hasattr(self, '_function_execution_results') and request_id in self._function_execution_results:
                tl['functions'] = self._function_execution_results[request_id]
            
            # If final_context has a timeline with functions (from Ditto plan execution), use that instead
            if isinstance(final_context, dict) and 'timeline' in final_context:
                ditto_timeline = final_context['timeline']
                if isinstance(ditto_timeline, dict) and 'functions' in ditto_timeline:
                    tl['functions'] = ditto_timeline['functions']
                    # Merge other timeline data
                    for key, value in ditto_timeline.items():
                        if key not in tl:
                            tl[key] = value
            
            final_context = final_context or {}
            final_context['timeline'] = tl
            self.debug_logger.info(f"Workflow '{workflow_name}' completed successfully")
            return final_context
        except Exception as e:
            error_message = f"\033[91mWorkflow '{workflow_name}' failed: {e}\033[0m"
            self.debug_logger.error(error_message)
            return {'status': 'error', 'message': error_message}

    def schedule_workflow(self, workflow_name: str, request_id: str, input_data: bytes) -> Dict[str, Any]:
        """Schedule workflow execution using Ditto's precomputed plan.
        
        📌 At Workflow Submission: The precomputed plan is applied to the submitted workflow.
        """
        workflow = self.workflows.get(workflow_name)
        if not workflow:
            raise ValueError(f"Workflow '{workflow_name}' is not registered")

        tasks: List[str] = workflow.get('tasks', [])
        if not tasks:
            return {
                'status': 'error',
                'message': 'No tasks to execute',
            }

        # Build simple context for DAG execution
        context: Dict[str, Any] = {
            'inputs': {
                'img': input_data
            }
        }

        # Time worker discovery and placement operations
        with self._time_scheduling_operation("worker_discovery"):
            # Prepare placement → worker mapping (random fallback if missing)
            workers = self._get_workers()
            if not workers:
                return {
                    'status': 'error',
                    'message': 'No worker nodes available',
                }

        with self._time_scheduling_operation("placement_mapping"):
            server_to_worker: Dict[str, Dict[str, Any]] = {}
            for w in workers:
                sid = w.get('name') or w.get('id')
                if sid and sid not in server_to_worker:
                    server_to_worker[sid] = w

            # Use precomputed placement when available; otherwise synthesize a simple one
            ditto_plan = workflow.get('ditto_plan') or {}
            plan_placement = ditto_plan.get('placement') or {}
            self.logger.debug(f"PLACEMENT (plan): {plan_placement}")

            # Build effective function->worker mapping used by invoke_function
            # KEY INSIGHT: For parallel functions (DoP > 1), don't pre-assign workers!
            # Only pre-assign sequential functions (DoP = 1) to avoid conflicts
            self._function_to_worker_for_current_run: Dict[str, Dict[str, Any]] = {}

            # Get DoP information for each function
            dop_ratios = {}
            if 'dop' in workflow:
                dop_ratios = workflow['dop']
            elif 'ditto_plan' in workflow and 'DoP_ratios' in workflow['ditto_plan']:
                dop_ratios = workflow['ditto_plan']['DoP_ratios']

            if plan_placement:
                for fn, server in plan_placement.items():
                    fn_dop = dop_ratios.get(fn, 1)
                    # Only pre-assign sequential functions
                    if fn_dop == 1:
                        self._function_to_worker_for_current_run[fn] = server_to_worker.get(server, self._select_worker_node_based(workers, ""))
                    # Skip pre-assignment for parallel functions - let them use dynamic selection
            else:
                # Simple round-robin placement across discovered workers, but ONLY for sequential functions
                rr = 0
                for fn in tasks:
                    fn_dop = dop_ratios.get(fn, 1)
                    if fn_dop == 1:
                        worker = workers[rr % len(workers)]
                        self._function_to_worker_for_current_run[fn] = worker
                        rr += 1
                    # Skip parallel functions - they'll use dynamic selection

            # Log effective placement for this run
            effective = {fn: (w.get('name') or w.get('id')) for fn, w in self._function_to_worker_for_current_run.items()}
            self.logger.debug(f"PLACEMENT (effective): {effective}")

        # Time DAG execution setup (but not the actual execution)
        with self._time_scheduling_operation("dag_setup"):
            # Setup DAG executor but don't execute yet
            dag = workflow.get('dag') or {}
            executor = DAGExecutor(self)
            # Track whether any function was invoked; fallback to sequential if not
            self._invocations_count = 0
            self.logger.debug(f"🎯 Ditto: Setting up DAG execution with {len(tasks)} tasks")
        
        # Execute DAG (outside timing context)
        try:
            # Check if we have a Ditto plan to use
            ditto_plan = workflow.get('ditto_plan')
            if ditto_plan:
                self.logger.debug(f"🎯 Ditto: Using precomputed plan for execution")
                result = self._execute_with_ditto_plan(request_id, input_data, tasks, ditto_plan)
                self.logger.debug(f"✅ Ditto: Plan execution completed")
                return result
            else:
                self.logger.debug(f"⚠️  Ditto: No precomputed plan available, using DAG executor")
            executor.execute_dag(dag, context, request_id)
            self.logger.debug(f"✅ Ditto: DAG execution completed, {getattr(self, '_invocations_count', 0)} functions invoked")
            return {
                'status': 'success',
                'request_id': request_id,
            }
        except Exception as e:
            self.logger.warning(f"❌ Ditto: DAG execution error, falling back to sequential: {e}")
            self.debug_logger.error(f"Ditto DAG execution error, falling back to sequential: {e}")

        # Time sequential execution setup (but not individual function calls)
        with self._time_scheduling_operation("sequential_setup"):
            if getattr(self, '_invocations_count', 0) == 0:
                # Setup sequential fallback but don't execute yet
                self.logger.debug(f"🔄 Ditto: Setting up sequential fallback for {len(tasks)} tasks")
                function_timelines = []
                
                # Initialize function execution results tracking
                if not hasattr(self, '_function_execution_results'):
                    self._function_execution_results = {}
                self._function_execution_results[request_id] = []
    
        # Execute sequential functions (outside timing context)
        if getattr(self, '_invocations_count', 0) == 0:
            for fn in tasks:
                try:
                    self.logger.debug(f"   📞 Invoking {fn}...")
                    # Note: invoke_function timing is not included in scheduling overhead
                    response = self.invoke_function(fn, f"{request_id}_{fn}", input_data)
                    
                    # Capture function execution result in timeline format
                    func_timeline = {
                        'function': fn,
                        'status': response.get('status', 'ok'),
                        'message': response.get('message'),
                        'node_id': response.get('node_id'),
                        'container_id': response.get('container_id'),
                        'exec_ms': response.get('exec_ms', 0.0),
                        'derived': response.get('derived', {}),
                        'timestamps': {
                            'invocation_start': response.get('client_timestamps', {}).get('sent'),
                            'invocation_end': response.get('client_timestamps', {}).get('received'),
                        }
                    }
                    function_timelines.append(func_timeline)
                    self._function_execution_results[request_id].append(func_timeline)
                    
                    # Safely print response with truncation for long values
                    safe_response = safe_print_value(response)
                    self.logger.debug(f"   ✅ {fn} result: {safe_response}")
                    self.debug_logger.debug(f"Sequential fallback {fn}: {safe_response}")                 
                except Exception as se:
                    self.logger.warning(f"❌ Sequential fallback invocation failed for {fn}: {se}")
                    # Add error timeline entry
                    error_timeline = {
                        'function': fn,
                        'status': 'error',
                        'message': str(se),
                        'exec_ms': 0.0,
                        'derived': {'exec_ms': 0.0, 'network_delay_ms': 0.0},
                    }
                    function_timelines.append(error_timeline)
                    self._function_execution_results[request_id].append(error_timeline)
                    return {
                        'status': 'error',
                        'message': f"Sequential fallback invocation failed for {fn}: {se}",
                    }
        # Create timeline for sequential execution
        sequential_timeline = {
            'workflow': {
                'status': 'success',
                'duration_ms': 0.0,  # Will be calculated by experiment runner
            },
            'functions': function_timelines,
        }
        
        return {
            'status': 'success',
            'request_id': request_id,
            'timeline': sequential_timeline,
        }

        
    def _execute_with_ditto_plan(
        self, 
        request_id: str, 
        input_data: bytes, 
        tasks: List[str], 
        ditto_plan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute workflow using Ditto's precomputed plan."""
        placement = ditto_plan.get('placement', {})
        dop_ratios = ditto_plan.get('DoP_ratios', {})
        groups = ditto_plan.get('groups', [])
        
        self.logger.debug(f"   Using placement: {placement}")
        self.logger.debug(f"   Using DoP ratios: {dop_ratios}")
        
        t0 = time.time()
        fn_timelines: List[Dict[str, Any]] = []
        workers = self._get_workers()
        
        if not workers:
            return {
                'status': 'error',
                'message': 'No worker nodes available',
                'timeline': {
                    'workflow': {'status': 'error'},
                    'functions': [],
                },
            }
        
        # Create server to worker mapping
        server_to_worker = {}
        for worker in workers:
            server_id = worker.get('name') or worker.get('id')
            server_to_worker[server_id] = worker
        
        # Build execution context for proper data flow
        context = {
            'inputs': {
                'img': input_data
            }
        }
        
        # Track all stage outputs for data flow
        all_stage_results = {}
        
        # Execute according to groups and placement with proper DoP allocation
        for group_idx, group in enumerate(groups):
            # Filter out END nodes (workflow termination markers, not real functions)
            group = [stage_id for stage_id in group if stage_id != 'END']
            if not group:
                self.logger.debug(f"🎯 Skipping group {group_idx + 1}/{len(groups)}: {group} (contains only END node)")
                continue
                
            self.logger.debug(f"🎯 Executing group {group_idx + 1}/{len(groups)}: {group}")
            
            # Setup shared memory for grouped stages (if applicable)
            if len(group) > 1:
                self.logger.debug(f"   📦 Setting up shared memory for grouped stages: {group}")
                # TODO: Implement actual shared memory setup
                # For now, we'll use a simple shared context approach
            
            # Execute stages in parallel within the group
            group_start_time = time.time()
            stage_results = {}
            
            for stage_id in group:
                # Find worker for this stage
                target_server = placement.get(stage_id, 'server-0')
                worker = server_to_worker.get(target_server, self._select_worker_node_based(workers, ""))
                
                # Get DoP ratio for this stage
                dop = dop_ratios.get(stage_id, 1)
                
                # Prepare proper input data for this stage based on DAG dependencies
                stage_input_data = self._prepare_stage_input(stage_id, context, all_stage_results)
                
                # Execute stage with proper DoP allocation
                stage_results[stage_id] = self._execute_stage_with_dop(
                    stage_id=stage_id,
                    dop=dop, 
                    worker=worker,
                    target_server=target_server,
                    request_id=request_id,
                    input_data=stage_input_data,
                    group=group
                )
            
            # Store results for data flow to subsequent groups
            for stage_id, result in stage_results.items():
                # For parallel execution, use the merged result for data flow
                if 'merged_result' in result and result['merged_result']:
                    # Create a data flow result that includes the merged output
                    data_flow_result = {
                        'function': stage_id,
                        'status': result['status'],
                        'result': result['merged_result'],
                        'exec_ms': result['exec_ms'],
                        'derived': result['derived'],
                        'timestamps': result['timestamps']
                    }
                    all_stage_results[stage_id] = data_flow_result
                else:
                    all_stage_results[stage_id] = result
                
                fn_timelines.append(result)
            
            group_end_time = time.time()
            group_duration = (group_end_time - group_start_time) * 1000.0
            self.logger.debug(f"   ✅ Group {group_idx + 1} completed in {group_duration:.3f}ms")

        t1 = time.time()
        total_duration = (t1 - t0) * 1000.0
        
        # Debug logging for execution summary
        self.debug_logger.info("Workflow execution completed with Ditto plan", 
                             total_duration_ms=total_duration,
                             num_functions=len(fn_timelines),
                             predicted_jct=ditto_plan.get('JCT', 0),
                             predicted_cost=ditto_plan.get('Cost', 0))
        
        # Ensure fn_timelines have proper structure for experiment runner
        formatted_timelines = []
        for timeline_entry in fn_timelines:
            if isinstance(timeline_entry, dict):
                # Ensure required fields are present
                formatted_entry = {
                    'function': timeline_entry.get('function', 'unknown'),
                    'status': timeline_entry.get('status', 'ok'),
                    'message': timeline_entry.get('message', ''),
                    'node_id': timeline_entry.get('node_id'),
                    'container_id': timeline_entry.get('container_id'),
                    'duration_ms': timeline_entry.get('duration_ms', 0.0),
                    'exec_ms': timeline_entry.get('exec_ms', 0.0),
                    'derived': timeline_entry.get('derived', {}),
                    'timestamps': timeline_entry.get('timestamps', {}),
                    'result_preview': timeline_entry.get('result_preview'),
                    'ditto_info': timeline_entry.get('ditto_info', {})
                }
                formatted_timelines.append(formatted_entry)
                
                # Debug: print timing info for each function
                func_name = formatted_entry['function']
                exec_ms = formatted_entry['exec_ms']
                derived = formatted_entry['derived']
                self.logger.debug(f"DEBUG: Timeline {func_name} - exec_ms: {exec_ms}, derived: {derived}")
        
        result: Dict[str, Any] = {
            'status': 'success',
            'request_id': request_id,
            'timeline': {
                'workflow': {
                    'status': 'success',
                    'duration_ms': total_duration,
                    'timestamps': {'start': t0, 'end': t1},
                },
                'functions': formatted_timelines,
            },
            'ditto_plan_applied': True,
            'predicted_jct': ditto_plan.get('JCT', 0),
            'predicted_cost': ditto_plan.get('Cost', 0)
        }
        
        self.workflow_results[request_id] = {
            'status': 'success',
            'context': {
                'functions': {t.get('function'): t for t in fn_timelines},
            },
        }
        return result
    
    def _prepare_stage_input(self, stage_id: str, context: Dict[str, Any], all_stage_results: Dict[str, Any]) -> Any:
        """Prepare input data for a stage based on DAG dependencies and previous stage outputs.
        
        This method dynamically determines dependencies from the actual DAG structure
        and prepares appropriate input data for each stage.
        """
        # Get the workflow DAG from the registered workflows
        workflow_name = None
        dag = None
        
        # Find the workflow that contains this stage
        for wf_name, wf_data in self.workflows.items():
            if 'dag' in wf_data:
                dag = wf_data['dag']
                workflow = dag.get('workflow', {})
                nodes = workflow.get('nodes', [])
                
                # Check if this stage exists in this workflow
                stage_exists = any(node.get('id') == stage_id for node in nodes if node.get('type') == 'task')
                if stage_exists:
                    workflow_name = wf_name
                    break
        
        if not dag or not workflow_name:
            # Fallback to original input if we can't find the DAG
            self.logger.warning(f"⚠️  Warning: Could not find DAG for stage {stage_id}, using original input")
            return context['inputs']['img']
        
        # Build dependencies from the actual DAG structure
        try:
            from functions.dag_loader import _build_dependencies_from_dag
            dependencies = _build_dependencies_from_dag(dag)
        except Exception as e:
            self.logger.warning(f"⚠️  Warning: Failed to build dependencies from DAG: {e}")
            return context['inputs']['img']
        
        # Get dependencies for this specific stage
        stage_dependencies = dependencies.get(stage_id, [])
        collected: Dict[str, Any] = {}

        # Collect outputs from dependency stages
        for dep in stage_dependencies:
            dep_output = all_stage_results.get(dep)
            if isinstance(dep_output, dict):
                # Extract the actual result from the stage output
                result_payload = dep_output.get('result') if 'result' in dep_output else dep_output
                if isinstance(result_payload, dict):
                    for k, v in result_payload.items():
                        if k not in collected:
                            collected[k] = v
                else:
                    if dep not in collected:
                        collected[dep] = result_payload
                
                # Also check if the dep_output itself contains useful data
                if isinstance(dep_output, dict):
                    for k, v in dep_output.items():
                        if k not in collected and k not in ['function', 'status', 'message', 'node_id', 'container_id', 'duration_ms', 'exec_ms', 'derived', 'timestamps', 'result_preview', 'ditto_info']:
                            collected[k] = v
        
        # Debug output for extract stage
        if 'extract' in stage_id.lower():
            self.logger.debug(f"      🔍 DEBUG: Dependencies for {stage_id}: {stage_dependencies}")
            self.logger.debug(f"      🔍 DEBUG: Available stage results: {list(all_stage_results.keys())}")
            for dep in stage_dependencies:
                dep_result = all_stage_results.get(dep)
                if dep_result:
                    if isinstance(dep_result, dict) and 'result' in dep_result:
                        result_data = dep_result['result']
                        if not isinstance(result_data, dict):
                            self.logger.debug(f"      🔍 DEBUG: {dep} result data type: {type(result_data)}")

        # If we collected nothing, fallback to original input for compatibility
        if not collected:
            return context['inputs']['img']

        # Determine input type based on stage name patterns (more flexible than hardcoded lists)
        # This is a heuristic approach that can be improved with DAG node metadata
        def is_text_processing_stage(stage_id: str) -> bool:
            """Determine if a stage processes text based on common naming patterns"""
            text_keywords = ['translate', 'censor', 'filter', 'text', 'extract_text', 'ocr']
            return any(keyword in stage_id.lower() for keyword in text_keywords)
        
        def is_image_processing_stage(stage_id: str) -> bool:
            """Determine if a stage processes images based on common naming patterns"""
            image_keywords = ['upload', 'adult', 'violence', 'mosaic', 'blur', 'resize', 'crop']
            return any(keyword in stage_id.lower() for keyword in image_keywords)

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
            preferred_keys = ['text', 'extracted_text', 'filtered_text', 'raw_text', 'translated_text']
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

        # Target-specific shaping based on stage type detection
        if is_image_processing_stage(stage_id):
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
        
        # Special case for extract stage - it needs image data from upload stage
        elif 'extract' in stage_id.lower():
            self.logger.debug(f"      🔍 DEBUG: Extract stage input preparation for {stage_id}")
            self.logger.debug(f"         - Collected data keys: {list(collected.keys()) if isinstance(collected, dict) else 'not dict'}")
            self.logger.debug(f"         - Collected data type: {type(collected)}")
            # Extract stage should get image data from upload stage
            if isinstance(collected, dict):
                img_b64 = pick_image_b64(collected)
                if img_b64:
                    self.logger.debug(f"         - Found img_b64 data, length: {len(img_b64)}")
                    return {'img_b64': img_b64}
                # If bytes present under common keys, return as bytes
                for k, v in collected.items():
                    if isinstance(v, (bytes, bytearray)) and ('img' in k.lower() or 'image' in k.lower()):
                        self.logger.debug(f"         - Found image bytes in key '{k}', length: {len(v)}")
                        return bytes(v)
            # Fallback to original input if no image data found
            print(f"         - No image data found, using original input")
            return context['inputs']['img']

        elif is_text_processing_stage(stage_id):
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
    
    def _calculate_input_size_kb(self, input_data: Any) -> float:
        """Calculate input data size in KB for various data types."""
        if isinstance(input_data, bytes):
            return len(input_data) / 1024
        elif isinstance(input_data, str):
            return len(input_data.encode('utf-8')) / 1024
        elif isinstance(input_data, dict):
            # For dictionary input, estimate size by serializing to JSON
            try:
                import json
                json_str = json.dumps(input_data, separators=(',', ':'))
                return len(json_str.encode('utf-8')) / 1024
            except Exception:
                # Fallback: estimate based on key-value pairs
                total_chars = 0
                for key, value in input_data.items():
                    total_chars += len(str(key)) + len(str(value))
                return total_chars / 1024
        elif isinstance(input_data, list):
            # For list input, sum up the sizes of all elements
            total_size = 0
            for item in input_data:
                total_size += self._calculate_input_size_kb(item)
            return total_size
        else:
            # For other types, estimate based on string representation
            try:
                return len(str(input_data).encode('utf-8')) / 1024
            except Exception:
                return 0.0
    
    def _divide_image_input(self, input_data: Any, dop: int, stage_id: str) -> List[Any]:
        """Divide image input into ``dop`` equal parts.

        - bytes: returns list[bytes] for each slice
        - dict with 'img' (bytes) or 'img_b64' (base64): returns list[dict] preserving other fields
        - fallback: duplicates if splitting fails
        """
        if dop <= 1:
            return [input_data]

        # Lazy imports to avoid hard dependency at module import time
        try:
            from PIL import Image  # type: ignore
            import io
            import base64
        except Exception as import_err:  # pragma: no cover
            self.logger.debug(f"      PIL unavailable ({import_err}); duplicating input {dop}x")
            return [input_data] * dop

        def _bytes_to_image(img_bytes):
            return Image.open(io.BytesIO(img_bytes))

        def _image_to_bytes(img, fmt):
            buffer = io.BytesIO()
            save_format = fmt or 'PNG'
            img.save(buffer, format=save_format)
            return buffer.getvalue()

        def _slice_image_vertically(img, parts):
            width, height = img.size
            parts = max(1, min(parts, max(1, width)))
            slice_width = width // parts
            slices = []
            for i in range(parts):
                left = i * slice_width
                right = (i + 1) * slice_width if i < parts - 1 else width
                box = (left, 0, right, height)
                slices.append(img.crop(box))
            return slices

        try:
            if isinstance(input_data, bytes):
                img = _bytes_to_image(input_data)
                original_format = img.format
                pieces = _slice_image_vertically(img, dop)
                return [_image_to_bytes(p, original_format) for p in pieces]

            if isinstance(input_data, dict):
                if 'img_b64' in input_data:
                    img_bytes = base64.b64decode(input_data['img_b64'])
                    img = _bytes_to_image(img_bytes)
                    original_format = img.format
                    pieces = _slice_image_vertically(img, dop)
                    outputs: List[Any] = []
                    for piece in pieces:
                        out_dict = dict(input_data)
                        out_dict['img_b64'] = base64.b64encode(_image_to_bytes(piece, original_format)).decode('utf-8')
                        outputs.append(out_dict)
                    return outputs
                if 'img' in input_data and isinstance(input_data['img'], (bytes, bytearray)):
                    img = _bytes_to_image(bytes(input_data['img']))
                    original_format = img.format
                    pieces = _slice_image_vertically(img, dop)
                    outputs = []
                    for piece in pieces:
                        out_dict = dict(input_data)
                        out_dict['img'] = _image_to_bytes(piece, original_format)
                        outputs.append(out_dict)
                    return outputs

            # Fallback for unsupported structures
            return [input_data] * dop
        except Exception as e:
            self.logger.debug(f"      Image slicing failed ({e}); duplicating input {dop}x")
            return [input_data] * dop
    
    def _divide_input_by_dop(self, input_data: Any, dop: int, stage_id: str = None) -> List[Any]:
        """Divide input data into equal chunks based on DoP for parallel processing."""
        if dop <= 1:
            return [input_data]
        
        # For most stages, parallel instances should process the SAME input data
        # Only divide input for specific stages where it makes sense
        
        if stage_id and any(keyword in stage_id.lower() for keyword in ['upload', 'adult', 'violence', 'mosaic']):
            # For image processing stages, use different strategies
            return self._divide_image_input(input_data, dop, stage_id)
        elif stage_id and 'extract' in stage_id.lower():
            # For text extraction, we could potentially split text, but for now use same input
            # This could be enhanced later to split text into chunks
            return [input_data] * dop
        elif stage_id and any(keyword in stage_id.lower() for keyword in ['translate', 'censor']):
            # For text processing stages, we could split text, but for now use same input
            # This could be enhanced later to split text into chunks
            return [input_data] * dop
        else:
            # For all other stages, duplicate the input data
            # Parallel instances should process the same input for redundancy/load balancing
            return [input_data] * dop
    
    def _is_image_chunking_stage(self, stage_id: str) -> bool:
        """Check if this stage processes image chunks in parallel."""
        return stage_id and any(keyword in stage_id.lower() for keyword in ['upload', 'adult', 'violence', 'mosaic'])
    
    def _merge_parallel_results(self, successful_instances: List[Dict[str, Any]], stage_id: str) -> Dict[str, Any]:
        """Merge results from parallel instances back into a single result."""
        if not successful_instances:
            return {}
        
        if len(successful_instances) == 1:
            # Single instance, return its result directly
            response = successful_instances[0].get('response', {})
            return response.get('result', response) if isinstance(response, dict) else response
        
        # Multiple instances - merge their results

        def _merge_numeric(a, b):
            """Attempt to merge two values by averaging numeric content.
            Returns (merged_value, merged=True) if a numeric merge was applied, otherwise (a, False) to keep existing.
            """
            try:
                # Scalars
                if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                    return ((float(a) + float(b)) / 2.0, True)
                # Dicts: field-wise merge
                if isinstance(a, dict) and isinstance(b, dict):
                    merged = {}
                    keys = set(a.keys()) | set(b.keys())
                    any_merged = False
                    for k in keys:
                        va = a.get(k)
                        vb = b.get(k)
                        if va is None:
                            merged[k] = vb
                            continue
                        if vb is None:
                            merged[k] = va
                            continue
                        mv, did_merge = _merge_numeric(va, vb)
                        merged[k] = mv if did_merge else va
                        any_merged = any_merged or did_merge
                    return (merged, any_merged)
                # Lists: element-wise merge when same length
                if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
                    merged_list = []
                    any_merged = False
                    for va, vb in zip(a, b):
                        mv, did_merge = _merge_numeric(va, vb)
                        merged_list.append(mv if did_merge else va)
                        any_merged = any_merged or did_merge
                    return (merged_list, any_merged)
            except Exception:
                pass
            return (a, False)

        merged_result = {}
        all_responses = []
        
        for instance in successful_instances:
            response = instance.get('response', {})
            if isinstance(response, dict):
                result = response.get('result', response)
                all_responses.append(result)
                
        # Merge dictionary results properly
        if isinstance(result, dict):
            for key, value in result.items():
                if key not in merged_result:
                    merged_result[key] = value  # Initialize with first value
                else:
                    # Check if this is an image processing stage with expected different results
                    if self._is_image_chunking_stage(stage_id) and key in ['img_clean_b64', 'message', 'redis_used']:
                        # For image chunking, we expect different results per instance
                        # Combine img_clean_b64 from all instances, keep other values from first instance
                        if key == 'img_clean_b64':
                            # Store all image chunks - these represent different pieces of the image
                            if '_image_chunks' not in merged_result:
                                merged_result['_image_chunks'] = []
                                merged_result['img_clean_b64'] = value  # Keep first chunk as primary
                            merged_result['_image_chunks'].append(value)
                        elif key == 'message':
                            # Combine messages to show total processing
                            if merged_result[key] != value:
                                merged_result[f'{key}_messages'] = [merged_result[key], value]
                                merged_result[key] = f"Combined from {len(successful_instances)} parallel instances"
                    else:
                        # For non-image stages or non-chunked content, use original logic
                        if merged_result[key] != value:
                            merged_val, did_merge = _merge_numeric(merged_result[key], value)
                            if did_merge:
                                merged_result[key] = merged_val
                            else:
                                # Only warn for unexpected differences (not image chunking)
                                print(f"        ⚠️  Warning: Different results for key '{key}' in parallel instances")
            else:
                # Non-dict result, store in a generic list
                if 'results' not in merged_result:
                    merged_result['results'] = []
                merged_result['results'].append(result)
        
        # For parallel processing with identical inputs, results should be identical
        # The merged_result already contains the correct single result from the first instance
        # No additional stage-specific merging is needed since all instances process the same input
        
        # Add metadata about the merge
        merged_result['_merge_info'] = {
            'num_instances': len(successful_instances),
            'stage_id': stage_id,
            'merge_strategy': 'parallel_chunks'
        }
        
        return merged_result
    
    def _setup_shared_memory_for_group(self, group: List[str], group_idx: int) -> None:
        """
        Setup shared memory for grouped stages to enable efficient data sharing.
        
        Args:
            group: List of stage IDs in the group
            group_idx: Index of the group for unique identification
        """
        self.logger.debug(f"      🔧 Configuring shared memory for group {group_idx + 1}")
        
        # Create a shared memory identifier for this group
        shared_memory_id = f"ditto_group_{group_idx}_{hash(tuple(group)) % 10000}"
        
        # In a real implementation, this would:
        # 1. Create shared memory segments for intermediate data
        # 2. Configure memory mapping between stages
        # 3. Set up data serialization/deserialization protocols
        # 4. Configure memory cleanup policies
        
        # For now, we'll simulate shared memory setup
        shared_memory_config = {
            'shared_memory_id': shared_memory_id,
            'stages': group,
            'group_index': group_idx,
            'memory_size_mb': 100,  # Default 100MB shared memory
            'access_pattern': 'read_write',  # Stages can read/write to shared memory
            'cleanup_policy': 'auto',  # Automatic cleanup after group completion
        }
        
        self.logger.debug(f"      📋 Shared memory config: {shared_memory_config}")
        
        # Store shared memory configuration for this group
        if not hasattr(self, '_shared_memory_configs'):
            self._shared_memory_configs = {}
        self._shared_memory_configs[shared_memory_id] = shared_memory_config
        
        # Log shared memory setup
        self.debug_logger.info(f"Shared memory setup for group {group_idx + 1}", 
                              group=group,
                              shared_memory_id=shared_memory_id,
                              memory_size_mb=shared_memory_config['memory_size_mb'])
    
    def _execute_stage_with_dop(
        self,
        stage_id: str,
        dop: int,
        worker: Dict[str, Any],
        target_server: str,
        request_id: str,
        input_data: Any,
        group: List[str]
    ) -> Dict[str, Any]:
        """
        Execute a stage with proper DoP (Degree of Parallelism) allocation.
        
        Args:
            stage_id: The stage/function to execute
            dop: Degree of parallelism (number of parallel instances)
            worker: Target worker for execution
            target_server: Target server name
            request_id: Request identifier
            input_data: Input data for the stage (bytes or dict)
            group: Group this stage belongs to
            
        Returns:
            Timeline entry for this stage execution
        """
        self.logger.debug(f"      🚀 Executing {stage_id} with DoP={dop}")
        
        stage_start_time = time.time()
        
        if dop == 1:
            # Single instance execution
            try:
                response = self.invoke_function(stage_id, request_id, input_data, worker)
                stage_end_time = time.time()
                    
                message = None
                if isinstance(response, dict):
                    message = response.get('message') or response.get('error') or response.get('detail')
                    
                return {
                        'function': stage_id,
                        'status': response.get('status', 'ok'),
                        'message': message if message is not None else "No message",
                        'node_id': worker.get('name') or worker.get('id'),
                        'container_id': worker.get('id'),
                        'duration_ms': (stage_end_time - stage_start_time) * 1000.0,
                        'exec_ms': response.get('exec_ms', (stage_end_time - stage_start_time) * 1000.0),
                        'derived': response.get('derived', {
                            'exec_ms': response.get('exec_ms', (stage_end_time - stage_start_time) * 1000.0),
                            'network_delay_ms': response.get('network_delay_ms', 0.0),
                            'processing_delay_ms': response.get('processing_delay_ms', 0.0),
                        }),
                        'timestamps': {
                        'invocation_start': stage_start_time,
                        'invocation_end': stage_end_time,
                        },
                        'result_preview': self._summarize_result(response),
                        'ditto_info': {
                            'placement': target_server,
                            'dop': dop,
                        'group': group,
                        'execution_type': 'single'
                        }
                }
            except Exception as e:
                stage_end_time = time.time()
                return {
                        'function': stage_id,
                        'status': 'error',
                        'message': str(e),
                        'node_id': worker.get('name') or worker.get('id'),
                        'container_id': worker.get('id'),
                        'duration_ms': (stage_end_time - stage_start_time) * 1000.0,
                        'exec_ms': (stage_end_time - stage_start_time) * 1000.0,
                        'derived': {
                            'exec_ms': (stage_end_time - stage_start_time) * 1000.0,
                            'network_delay_ms': 0.0,
                            'processing_delay_ms': 0.0,
                        },
                        'timestamps': {
                        'invocation_start': stage_start_time,
                        'invocation_end': stage_end_time,
                        },
                        'ditto_info': {
                            'placement': target_server,
                            'dop': dop,
                        'group': group,
                        'execution_type': 'single'
                    }
                }
        else:
            # Multi-instance parallel execution using concurrent.futures
            import concurrent.futures
            import threading
            
            self.logger.debug(f"      🔄 Executing {dop} parallel instances of {stage_id} concurrently")
            
            # Divide input data into equal chunks based on DoP
            input_chunks = self._divide_input_by_dop(input_data, dop, stage_id)
            self.logger.debug(f"      📊 Divided input into {len(input_chunks)} chunks for parallel processing")
            
            instance_results = []
            instance_start_times = []
            instance_end_times = []
            
            def execute_instance(instance_id: int) -> Dict[str, Any]:
                """Execute a single instance of the stage with its input chunk"""
                instance_start = time.time()
                instance_start_times.append(instance_start)
                
                try:
                    # Create unique request ID for each instance
                    # Include target_server in request_id so worker selection can use it to select from same node
                    instance_request_id = f"{request_id}_{stage_id}_instance_{instance_id}_node_{target_server}"
                    
                    # Get the input chunk for this instance
                    instance_input = input_chunks[instance_id] if instance_id < len(input_chunks) else input_data
                    
                    # Execute the instance with its chunk
                    # Pass None as worker to force dynamic selection for each parallel instance
                    response = self.invoke_function(stage_id, instance_request_id, instance_input, None)
                    
                    instance_end = time.time()
                    instance_end_times.append(instance_end)
                    
                    # Extract only JSON-serializable fields from response
                    response_summary = {
                        'status': response.get('status', 'ok'),
                        'message': response.get('message'),
                        'exec_ms': response.get('exec_ms'),
                    }
                    # Include derived if it exists and is serializable
                    if 'derived' in response and isinstance(response.get('derived'), dict):
                        response_summary['derived'] = response.get('derived')
                    
                    result = {
                        'instance_id': instance_id,
                        'status': response.get('status', 'ok'),
                        'duration_ms': (instance_end - instance_start) * 1000.0,
                        'response': response,  # Keep full response for merge logic
                        'response_summary': response_summary,  # JSON-safe summary for timeline
                        'input_chunk_size': self._calculate_input_size_kb(instance_input)
                    }
                    
                    chunk_size_kb = result['input_chunk_size']
                    if isinstance(chunk_size_kb, (int, float)):
                        self.logger.debug(f"        ✅ Instance {instance_id + 1}/{dop} completed (chunk size: {chunk_size_kb:.1f} KB)")
                    else:
                        self.logger.debug(f"        ✅ Instance {instance_id + 1}/{dop} completed (chunk size: {chunk_size_kb})")
                    return result
                    
                except Exception as e:
                    instance_end = time.time()
                    instance_end_times.append(instance_end)
                    
                    result = {
                        'instance_id': instance_id,
                        'status': 'error',
                        'duration_ms': (instance_end - instance_start) * 1000.0,
                        'error': str(e)
                    }
                    
                    print(f"\033[91m        ❌ Instance {instance_id + 1}/{dop} failed: {e}\033[0m")
                    return result
            
            # Execute all instances concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=dop) as executor:
                # Submit all instances
                future_to_instance = {
                    executor.submit(execute_instance, instance_id): instance_id 
                    for instance_id in range(dop)
                }
                
                # Collect results as they complete
                for future in concurrent.futures.as_completed(future_to_instance):
                    instance_id = future_to_instance[future]
                    try:
                        result = future.result()
                        instance_results.append(result)
                    except Exception as e:
                        print(f"\033[91m        ❌ Instance {instance_id + 1}/{dop} executor error: {e}\033[0m")
                        instance_results.append({
                            'instance_id': instance_id,
                            'status': 'error',
                            'duration_ms': 0.0,
                            'error': str(e)
                        })
            
            stage_end_time = time.time()
            
            # Aggregate results from all instances
            successful_instances = [r for r in instance_results if r['status'] != 'error']
            failed_instances = [r for r in instance_results if r['status'] == 'error']
            
            # Merge results from all successful instances
            merged_result = self._merge_parallel_results(successful_instances, stage_id)
            
            # Calculate aggregate metrics
            total_duration = (stage_end_time - stage_start_time) * 1000.0
            
            # Calculate average execution time from successful instances' exec_ms
            avg_instance_duration = 0.0
            successful_with_exec = [r for r in successful_instances if 'response' in r and isinstance(r['response'], dict)]
            if successful_with_exec:
                exec_times = []
                for r in successful_with_exec:
                    response = r['response']
                    exec_ms = response.get('exec_ms', 0.0)
                    if isinstance(exec_ms, (int, float)) and exec_ms > 0:
                        exec_times.append(float(exec_ms))
                if exec_times:
                    avg_instance_duration = sum(exec_times) / len(exec_times)
                else:
                    # Fallback to duration_ms if no exec_ms available
                    avg_instance_duration = sum(r['duration_ms'] for r in instance_results) / len(instance_results)
            else:
                # Fallback to duration_ms if no successful instances
                avg_instance_duration = sum(r['duration_ms'] for r in instance_results) / len(instance_results)
            
            max_instance_duration = max(r['duration_ms'] for r in instance_results)
            
            # Calculate average network delay from successful instances
            avg_network_delay = 0.0
            successful_with_network = [r for r in successful_instances if 'response' in r and isinstance(r['response'], dict)]
            if successful_with_network:
                network_delays = []
                for r in successful_with_network:
                    response = r['response']
                    derived = response.get('derived', {})
                    if 'network_delay_ms' in derived:
                        network_delays.append(derived['network_delay_ms'])
                if network_delays:
                    avg_network_delay = sum(network_delays) / len(network_delays)
            
            # Determine overall status
            overall_status = 'ok' if len(successful_instances) > 0 else 'error'
            
            # Debug: print instance success/failure details
            self.logger.debug(f"      📊 {stage_id} parallel execution summary:")
            self.logger.debug(f"         - Total instances: {dop}")
            self.logger.debug(f"         - Successful instances: {len(successful_instances)}")
            if len(failed_instances)>0:
                print(f"\033[91m         - Failed instances: {len(failed_instances)}\033[0m")
            self.logger.debug(f"         - Failed instances: {len(failed_instances)}")
            self.logger.debug(f"         - Overall status: {overall_status}")
            
            return {
                'function': stage_id,
                'status': overall_status,
                'message': f"Executed {len(successful_instances)}/{dop} instances successfully",
                'node_id': worker.get('name') or worker.get('id'),
                'container_id': worker.get('id'),
                'duration_ms': total_duration,
                'exec_ms': avg_instance_duration,  # Use average instance execution time
                'derived': {
                    'exec_ms': avg_instance_duration,
                    'network_delay_ms': avg_network_delay,
                    'processing_delay_ms': 0.0,
                },
                'timestamps': {
                    'invocation_start': stage_start_time,
                    'invocation_end': stage_end_time,
                },
                'ditto_info': {
                    'placement': target_server,
                    'dop': dop,
                    'group': group,
                    'execution_type': 'parallel',
                    'instances': {
                        'total': dop,
                        'successful': len(successful_instances),
                        'failed': len(failed_instances),
                        'avg_duration_ms': avg_instance_duration,
                        'max_duration_ms': max_instance_duration
                    }
                },
                'instance_results': [
                    {
                        'instance_id': r['instance_id'],
                        'status': r['status'],
                        'duration_ms': r['duration_ms'],
                        'response_summary': r.get('response_summary', {}),
                        'input_chunk_size': r.get('input_chunk_size'),
                        'error': r.get('error')
                    } for r in instance_results
                ],
                'merged_result': merged_result
            }

    def invoke_function(self, function_name: str, request_id: str, payload: bytes, worker: Optional[Dict[str, Any]] = None, storage_context: Optional[Dict] = None) -> Dict[str, Any]:
        """Invoke a function by forwarding to a Ditto worker HTTP endpoint with capacity enforcement.

        This uses discovered Ditto workers (via ContainerManager) and sends
        the request to one worker, which then runs the function.
        """
        # Discover or reuse provided worker with capacity enforcement
        if worker is None:
            # For parallel execution, always use dynamic node-based selection
            # For sequential functions, we can use pre-assigned workers from placement plan
            # Check if this is a parallel execution by looking for instance suffix in request_id
            is_parallel_execution = '_instance_' in request_id
            
            if is_parallel_execution:
                # Always use dynamic worker selection for parallel instances
                workers = self._get_workers()
                if not workers:
                    # CAPACITY ENFORCEMENT: Try to deploy new worker if none available
                    self.debug_logger.warning("No Ditto workers available, attempting to deploy new worker")
                    
                    # Try to deploy a new worker container
                    new_worker = self._deploy_new_worker()
                    if new_worker:
                        workers = [new_worker]
                        self.debug_logger.info(f"Successfully deployed new Ditto worker: {new_worker['id']}")
                    else:
                        raise RuntimeError('No Ditto workers available and failed to deploy new worker')
                
                # Use instance-specific context for node-based worker distribution
                instance_context = f"{function_name}_{request_id}_{time.time()}"
                worker = self._select_worker_node_based(workers, instance_context, function_name)
            else:
                # Use pre-assigned worker for sequential execution
                mapped = getattr(self, '_function_to_worker_for_current_run', {}).get(function_name)
                if mapped is not None:
                    worker = mapped
                else:
                    workers = self._get_workers()
                    if not workers:
                        # CAPACITY ENFORCEMENT: Try to deploy new worker if none available
                        self.debug_logger.warning("No Ditto workers available, attempting to deploy new worker")
                        
                        # Try to deploy a new worker container
                        new_worker = self._deploy_new_worker()
                        if new_worker:
                            workers = [new_worker]
                            self.debug_logger.info(f"Successfully deployed new Ditto worker: {new_worker['id']}")
                        else:
                            raise RuntimeError('No Ditto workers available and failed to deploy new worker')
                    
                    # Use basic worker selection for sequential execution
                    context = f"{function_name}_{request_id}"
                    worker = self._select_worker_node_based(workers, context)
                
                # CAPACITY ENFORCEMENT: Check if selected worker is overloaded
                if worker and worker.get('capacity_status') == 'overloaded':
                    self.debug_logger.warning(f"Selected worker {worker['id']} is overloaded, trying to find alternative")
                    # Try to find a less loaded worker
                    available_workers = [w for w in workers if w.get('capacity_status') != 'overloaded']
                    if available_workers:
                        worker = self._select_worker_node_based(available_workers, "")
                    else:
                        self.debug_logger.warning("All workers are overloaded, proceeding with least loaded worker")

        # Use Ditto's custom method (workers use /run endpoint, not /invoke)
        client_send = time.time()
        response = self._send_request_to_worker_with_storage(worker, function_name, request_id, payload, storage_context)
        response_time = time.time() - client_send
        round_trip_ms = response_time * 1000.0

        safe_response_debug = safe_print_value(response)
        self.debug_logger.debug(safe_response_debug)

        # Mark that an invocation happened
        try:
            self._invocations_count = getattr(self, '_invocations_count', 0) + 1
        except Exception:
            pass

        if isinstance(response, dict):
            response.setdefault('status', 'ok')
            response.setdefault('client_timestamps', {})
            response['client_timestamps'].update({'sent': client_send, 'received': client_send + response_time})
            response['node_id'] = worker.get('name') or worker.get('id')
            response['container_id'] = worker.get('id')
            
            # Add capacity information to response
            response['worker_capacity'] = {
                'cpu_utilization': worker.get('cpu_utilization', 0.0),
                'memory_utilization': worker.get('memory_utilization', 0.0),
                'capacity_status': worker.get('capacity_status', 'unknown')
            }
            
            # Extract server-side timestamps if available
            server_timestamps = response.get('server_timestamps', {})
            server_start = server_timestamps.get('exec_start')
            server_end = server_timestamps.get('exec_end')
            
            # Debug: check if we're getting server timestamps
            self.logger.debug(f"DEBUG: Ditto {function_name} - server_timestamps: {server_timestamps}")
            self.logger.debug(f"DEBUG: Ditto {function_name} - round_trip_ms: {round_trip_ms:.1f}ms")
            
            # Calculate separate network delay and function execution time
            network_delay_ms = 0.0
            function_exec_ms = 0.0
            
            if server_start and server_end:
                # Function execution time from server-side timestamps
                function_exec_ms = (server_end - server_start) * 1000.0
                # Network delay = time from client send to server start (same as Ours & FaasPRS)
                network_delay_ms = (server_start - client_send) * 1000.0
            else:
                # Fallback: if no server timestamps, use fixed small network delay
                # For local workers, network should be minimal (~1-5ms)
                # Most of the time is likely function execution
                function_exec_ms = round_trip_ms - 15.0  # Assume 10ms total network overhead
                network_delay_ms = 15.0  # Fixed small network delay for local workers
            
            # Add execution timing for experiment runner extraction
            response['exec_ms'] = function_exec_ms  # Use actual function execution time, not total
            response['derived'] = {
                'exec_ms': function_exec_ms,
                'network_delay_ms': network_delay_ms,
                'total_time_ms': round_trip_ms,
            }
            
            # Store server timestamps for detailed analysis
            response['server_timestamps'] = server_timestamps
            
            # Debug: print function result summary with detailed timing
            safe_message = safe_print_value(response.get('message', ''), 30)
            self.logger.debug(f"{safe_message}")
            # print(f"📊 {function_name} → {response['status']}, exec: {function_exec_ms:.1f}ms, network: {network_delay_ms:.1f}ms, total: {round_trip_ms:.1f}ms")
        
        return response

    # Timeline management for function execution tracking
    def _get_or_create_timeline(self, request_id: str) -> Dict[str, Any]:
        """Get or create timeline for request"""
        if not hasattr(self, 'request_timelines'):
            self.request_timelines = {}
        
        if request_id not in self.request_timelines:
            self.request_timelines[request_id] = {
                'workflow': {},
                'functions': [],
                'timestamps': {}
            }
        
        return self.request_timelines[request_id]

    def _debug(self, message: str) -> None:
        try:
            self.debug_logger.debug(message)
            # Also print to stdout so tee can capture it
            self.logger.debug(f"DEBUG: {message}")
        except Exception as e:
            self.logger.error(f"\033[91mERROR: Can't print debug message\033[0m")

    # --- Helpers & minimal metrics ---
    def _get_workers(self) -> List[Dict[str, Any]]:
        """Get Ditto worker containers by discovering them via container manager with capacity enforcement."""
        try:
            # Discover worker containers using container manager
            worker_containers: List[Dict[str, Any]] = []
            if hasattr(self.container_manager, 'discover_worker_containers'):
                worker_containers = self.container_manager.discover_worker_containers() or []
            else:
                # Fallback to generic discover/get API
                if hasattr(self.container_manager, 'discover_workers'):
                    self.container_manager.discover_workers()
                if hasattr(self.container_manager, 'get_workers'):
                    generic = self.container_manager.get_workers() or []
                    worker_containers = [
                        {
                            'container_id': w.get('id') or w.get('container_id') or w.get('name'),
                            'name': w.get('name') or w.get('worker_id') or 'unknown',
                            'worker_id': w.get('name') or w.get('worker_id') or 'unknown',
                            'port': w.get('port') or 5000,
                            'status': w.get('status', 'running'),
                        }
                        for w in generic
                    ]
            
            if not worker_containers:
                self.debug_logger.warning("No worker containers discovered, using fallback")
                # Fallback to hardcoded workers if discovery fails
                return [
                    {
                        'id': 'ditto-worker-1',
                        'name': 'ditto-worker-1',
                        'host_port': 5001,
                        'cpu_cores': 4.0,
                        'memory_gb': 8.0,
                        'node_type': 'ditto-worker'
                    },
                    {
                        'id': 'ditto-worker-2', 
                        'name': 'ditto-worker-2',
                        'host_port': 5002,
                        'cpu_cores': 4.0,
                        'memory_gb': 8.0,
                        'node_type': 'ditto-worker'
                    },
                    {
                        'id': 'ditto-worker-3',
                        'name': 'ditto-worker-3', 
                        'host_port': 5003,
                        'cpu_cores': 4.0,
                        'memory_gb': 8.0,
                        'node_type': 'ditto-worker'
                    }
                ]
            
            # Convert discovered worker containers to Ditto format with capacity information
            ditto_workers = []
            for worker in worker_containers:
                # Get real-time capacity information if available
                worker_id = worker.get('container_id') or worker.get('id') or worker.get('name')
                capacity_info = self._get_worker_capacity_info(worker_id)
                
                ditto_worker = {
                    'id': worker_id,
                    'name': worker.get('name'),
                    'host_port': worker.get('port') or worker.get('host_port') or 5000,
                    'cpu_cores': capacity_info.get('cpu_cores', 4.0),
                    'memory_gb': capacity_info.get('memory_gb', 8.0),
                    'available_cpu': capacity_info.get('available_cpu', 4.0),
                    'available_memory': capacity_info.get('available_memory', 8.0),
                    'cpu_utilization': capacity_info.get('cpu_utilization', 0.0),
                    'memory_utilization': capacity_info.get('memory_utilization', 0.0),
                    'node_type': 'ditto-worker',
                    'worker_id': worker.get('worker_id') or worker.get('name'),
                    'status': worker.get('status', 'running'),
                    'capacity_status': capacity_info.get('capacity_status', 'available')
                }
                ditto_workers.append(ditto_worker)
            
            # Filter out overloaded workers
            available_workers = [w for w in ditto_workers if w['capacity_status'] == 'available']
            
            if not available_workers:
                self.debug_logger.warning("All Ditto workers are overloaded, using least loaded worker")
                # If all workers are overloaded, use the least loaded one
                available_workers = [min(ditto_workers, key=lambda w: w['cpu_utilization'] + w['memory_utilization'])]
            
            self.debug_logger.debug(f"Discovered {len(ditto_workers)} Ditto worker containers, {len(available_workers)} available")
            return available_workers
            
        except Exception as e:
            self.debug_logger.error(f"Failed to get Ditto workers: {e}")
            raise RuntimeError(f"Failed to initialize Ditto workers: {e}")

    def _get_worker_capacity_info(self, worker_id: str) -> Dict[str, Any]:
        """Get capacity information for a Ditto worker"""
        try:
            # Try to get capacity info from function container manager if available
            if hasattr(self.container_manager, 'get_node_available_resources'):
                # Map worker to node (simplified mapping)
                node_id = self._map_worker_to_node(worker_id)
                if node_id:
                    resources = self.container_manager.get_node_available_resources(node_id)
                    if 'error' not in resources:
                        cpu_utilization = resources.get('cpu_utilization', 0.0)
                        memory_utilization = resources.get('memory_utilization', 0.0)
                        
                        # Determine capacity status
                        capacity_status = 'available'
                        if cpu_utilization > 0.8 or memory_utilization > 0.8:
                            capacity_status = 'overloaded'
                        elif cpu_utilization > 0.6 or memory_utilization > 0.6:
                            capacity_status = 'high_load'
                        
                        return {
                            'cpu_cores': resources.get('total_cpu', 4.0),
                            'memory_gb': resources.get('total_memory', 8.0),
                            'available_cpu': resources.get('available_cpu', 4.0),
                            'available_memory': resources.get('available_memory', 8.0),
                            'cpu_utilization': cpu_utilization,
                            'memory_utilization': memory_utilization,
                            'capacity_status': capacity_status
                        }
            
            # Fallback to default capacity info
            return {
                'cpu_cores': 4.0,
                'memory_gb': 8.0,
                'available_cpu': 4.0,
                'available_memory': 8.0,
                'cpu_utilization': 0.0,
                'memory_utilization': 0.0,
                'capacity_status': 'available'
            }
            
        except Exception as e:
            self.debug_logger.warning(f"Failed to get capacity info for worker {worker_id}: {e}")
            return {
                'cpu_cores': 4.0,
                'memory_gb': 8.0,
                'available_cpu': 4.0,
                'available_memory': 8.0,
                'cpu_utilization': 0.0,
                'memory_utilization': 0.0,
                'capacity_status': 'available'
            }
    
    def _map_worker_to_node(self, worker_id: str) -> Optional[str]:
        """Map a Ditto worker ID to a node ID using regex pattern matching"""
        import re
        
        # Extract node number from worker ID using regex
        # Matches patterns like: worker-1, worker1, ditto-worker-1, worker_1, etc.
        match = re.search(r'worker[-_]?(\d+)', worker_id.lower())
        if match:
            node_num = match.group(1)
            return f'node{node_num}'
        
        # Default to node1 if no clear mapping found
        return 'node1'
    
    def _deploy_new_worker(self) -> Optional[Dict[str, Any]]:
        """Deploy a new Ditto worker container with capacity enforcement"""
        try:
            # Get available nodes with sufficient capacity
            if hasattr(self.container_manager, '_find_nodes_with_capacity'):
                # Use default worker resource requirements
                cpu_requirement = 2.0  # Ditto workers typically need more resources
                memory_requirement = 1024  # 1GB memory
                
                available_nodes = self.container_manager._find_nodes_with_capacity(cpu_requirement, memory_requirement)
                if not available_nodes:
                    self.debug_logger.warning("No nodes available with sufficient capacity for new Ditto worker")
                    return None
                
                # Use first available node
                target_node = available_nodes[0]
                
                # Generate unique worker ID
                worker_id = f"ditto-worker-{int(time.time())}"
                
                # Deploy worker container using function container manager
                if hasattr(self.container_manager, 'deploy_function_container'):
                    # Deploy as a generic worker container
                    container = self.container_manager.deploy_function_container(
                        'ditto-worker', target_node, 
                        cpu_limit=cpu_requirement, memory_limit=memory_requirement
                    )
                    
                    if container:
                        # Create worker info in Ditto format
                        worker_info = {
                            'id': container.container_id,
                            'name': worker_id,
                            'host_port': container.port,
                            'cpu_cores': cpu_requirement,
                            'memory_gb': memory_requirement / 1024,
                            'available_cpu': cpu_requirement,
                            'available_memory': memory_requirement / 1024,
                            'cpu_utilization': 0.0,
                            'memory_utilization': 0.0,
                            'node_type': 'ditto-worker',
                            'worker_id': worker_id,
                            'status': 'running',
                            'capacity_status': 'available'
                        }
                        
                        self.debug_logger.info(f"Deployed new Ditto worker {worker_id} on {target_node}")
                        return worker_info
            
            self.debug_logger.warning("Failed to deploy new Ditto worker - container manager not available")
            return None
            
        except Exception as e:
            self.debug_logger.error(f"Failed to deploy new Ditto worker: {e}")
            return None
    
    def get_cluster_capacity_status(self) -> Dict[str, Any]:
        """Get cluster capacity status for Ditto workers"""
        try:
            workers = self._get_workers()
            
            total_workers = len(workers)
            available_workers = len([w for w in workers if w.get('capacity_status') == 'available'])
            overloaded_workers = len([w for w in workers if w.get('capacity_status') == 'overloaded'])
            
            # Calculate average utilization
            avg_cpu_utilization = sum(w.get('cpu_utilization', 0) for w in workers) / total_workers if total_workers > 0 else 0
            avg_memory_utilization = sum(w.get('memory_utilization', 0) for w in workers) / total_workers if total_workers > 0 else 0
            
            return {
                'timestamp': time.time(),
                'ditto_cluster_summary': {
                    'total_workers': total_workers,
                    'available_workers': available_workers,
                    'overloaded_workers': overloaded_workers,
                    'avg_cpu_utilization': avg_cpu_utilization,
                    'avg_memory_utilization': avg_memory_utilization
                },
                'worker_details': [
                    {
                        'worker_id': w.get('id'),
                        'cpu_utilization': w.get('cpu_utilization', 0),
                        'memory_utilization': w.get('memory_utilization', 0),
                        'capacity_status': w.get('capacity_status', 'unknown')
                    }
                    for w in workers
                ],
                'capacity_warnings': {
                    'high_utilization': avg_cpu_utilization > 0.8 or avg_memory_utilization > 0.8,
                    'overloaded_workers_count': overloaded_workers,
                    'low_availability': available_workers < total_workers * 0.5
                }
            }
            
        except Exception as e:
            self.debug_logger.error(f"Failed to get Ditto cluster capacity status: {e}")
            return {'error': str(e), 'timestamp': time.time()}
    
    def _group_workers_by_node(self, workers: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group workers by their node identifier for node-based distribution."""
        import re
        node_groups = {}
        
        for worker in workers:
            # Extract node from worker name (e.g., 'ditto-worker-node2-3' -> 'node2')
            name = worker.get('name', worker.get('id', ''))
            match = re.search(r'node(\d+)', name)
            if match:
                node_id = f"node{match.group(1)}"
                if node_id not in node_groups:
                    node_groups[node_id] = []
                node_groups[node_id].append(worker)
            else:
                # Fallback for workers without node pattern
                default_node = "default"
                if default_node not in node_groups:
                    node_groups[default_node] = []
                node_groups[default_node].append(worker)
        
        return node_groups
    
    def _select_worker_node_based(self, workers: List[Dict[str, Any]], request_context: str = "", function_name: str = None) -> Optional[Dict[str, Any]]:
        """Select worker using node-based distribution strategy with random selection for parallel execution"""
        if not workers:
            return None
        
        import random
        import time
        
        # Group workers by node first
        node_groups = self._group_workers_by_node(workers)
        if not node_groups:
            # Fallback to original selection if grouping fails
            return workers[0] if workers else None
        
        # Check if this is a parallel execution instance by checking if context contains '_instance_'
        # This works because parallel instances have request_context like "function_name_request_id_stage_instance_X_node_Y"
        is_parallel_instance = '_instance_' in request_context
        
        # Debug logging
        self.debug_logger.debug(f"🔍 Worker selection - context: '{request_context}', is_parallel: {is_parallel_instance}")
        
        if is_parallel_instance:
            # For parallel execution, extract the target node from context and randomly select from workers on that node
            # Context format: "...instance_X_node_ditto-worker-nodeY-Z"
            import re
            
            # Extract target server/node from context
            target_node = None
            node_match = re.search(r'_node_(ditto-worker-node\d+)', request_context)
            if node_match:
                target_server_name = node_match.group(1)
                # Extract just the node part (e.g., "node4" from "ditto-worker-node4")
                node_num_match = re.search(r'node(\d+)', target_server_name)
                if node_num_match:
                    target_node = f"node{node_num_match.group(1)}"
            
            # If we found a target node in the context, select workers only from that node
            if target_node and target_node in node_groups:
                node_workers = node_groups[target_node]
                selected_worker = random.choice(node_workers)
                
                # Debug logging with node information
                worker_name = selected_worker.get('name', 'unknown')
                worker_port = selected_worker.get('host_port') or selected_worker.get('port', 'unknown')
                self.debug_logger.debug(f"🎲 Randomly selected worker '{worker_name}' on port {worker_port} "
                                      f"from {len(node_workers)} workers on {target_node}")
            else:
                # Fallback: select from all workers if node not found
                all_workers_flat = []
                for node_workers in node_groups.values():
                    all_workers_flat.extend(node_workers)
                
                selected_worker = random.choice(all_workers_flat)
                
                worker_name = selected_worker.get('name', 'unknown')
                worker_port = selected_worker.get('host_port') or selected_worker.get('port', 'unknown')
                self.debug_logger.debug(f"🎲 Randomly selected worker '{worker_name}' on port {worker_port} "
                                      f"from {len(all_workers_flat)} workers (all nodes, target node not found)")
            
            return selected_worker
        else:
            # For non-parallel execution, use the original node-based strategy
            # Step 1: Select target node based on context
            available_nodes = list(node_groups.keys())
            
            # Use context to hash to node selection for deterministic but distributed assignment
            context_hash = hash(request_context + str(time.time_ns())) % len(available_nodes)
            selected_node = available_nodes[context_hash]
            
            # Step 2: Select worker within the selected node
            node_workers = node_groups[selected_node]
            
            # Random selection within the selected node
            selected_worker = random.choice(node_workers)
            
            return selected_worker

    def _send_request_to_worker_with_storage(self, worker: Dict, function_name: str, request_id: str, body: Any, storage_context: Optional[Dict] = None) -> Dict:
        """Send request with data payload to a discovered worker container with storage support."""
        host_port = worker.get('host_port') or worker.get('port') or 5000
        url = f"http://127.0.0.1:{host_port}/run?fn={function_name}"
        headers: Dict[str, str] = {}
        data = None
        json_payload = None

        if storage_context:
            # Function has dependencies - send storage context
            headers['Content-Type'] = 'application/json'
            json_payload = {
                'input_ref': storage_context
            }
        elif isinstance(body, bytes):
            headers['Content-Type'] = 'application/octet-stream'
            data = body
        elif isinstance(body, dict):
            headers['Content-Type'] = 'application/json'
            json_payload = body
        else:
            return {'status': 'error', 'message': f'Unsupported payload type: {type(body)}'}

        try:
            self.logger.debug(f"    - Sending request to {url} with storage_context: {storage_context is not None}")
            response = requests.post(url, headers=headers, data=data, json=json_payload, timeout=self.config.request_timeout)
            response.raise_for_status()
            self.logger.debug(f"    - Received response from worker '{worker['name']}'.")
            return response.json()
        except requests.exceptions.RequestException as e:
            error_message = f"Request to container at {url} failed: {e}"
            print(f"\033[91m    - Error: {error_message}\033[0m")
            return {'status': 'error', 'message': error_message}

    def _send_request_to_worker(self, worker: Dict, function_name: str, request_id: str, body: Any) -> Dict:
        """Send request with data payload to a discovered worker container (legacy method)."""
        return self._send_request_to_worker_with_storage(worker, function_name, request_id, body, None)


    def _summarize_result(self, resp: Dict[str, Any]) -> Dict[str, Any]:
        summary: Dict[str, Any] = {}
        if not isinstance(resp, dict):
            return summary
        for k in ('status', 'minio_output_uploaded', 'minio_output_key'):
            if k in resp:
                summary[k] = resp[k]
        # Avoid large payloads in timeline
        if 'result' in resp and isinstance(resp['result'], (str, bytes)):
            summary['result_preview'] = str(resp['result'])[:128]
        return summary

    # Used by DittoScheduler.get_metrics()
    def get_cluster_resource_status(self) -> Dict[str, Any]:
        workers = self._get_workers()
        return {
            'cluster_status': {
                'nodes': [{
                    'id': w.get('id'), 
                    'name': w.get('name'), 
                    'host_port': w.get('host_port')} 
                    for w in workers],
                'containers': {
                    w.get('name') or f"worker-{i}": 
                    [w.get('id')] for i, w in enumerate(workers)},
            },
            'function_summaries': {},
        }
    
    # --- Debug Methods ---
    def get_debug_metrics(self) -> Dict[str, Any]:
        """Get collected debug metrics."""
        return self.debug_metrics.get_metrics()
    
    def save_debug_metrics(self, filename: Optional[str] = None) -> None:
        """Save debug metrics to file."""
        if filename is None:
            filename = self.config.debug_metrics_file
        self.debug_metrics.save_metrics(filename)
    
    def get_saved_plans(self) -> List[Dict[str, Any]]:
        """Get all saved execution plans."""
        return self.debug_plan_saver.get_saved_plans()
    
    def enable_debug_mode(self, log_level: str = "DEBUG", save_plans: bool = True) -> None:
        """Enable debug mode with specified settings."""
        self.config.debug_mode = True
        self.config.debug_log_level = log_level
        self.config.debug_save_plans = save_plans
        
        # Recreate debug context with new settings
        self.debug_logger, self.debug_metrics, self.debug_plan_saver = create_debug_context(self.config)
        
        self.debug_logger.info("Debug mode enabled via API", 
                             log_level=log_level, 
                             save_plans=save_plans)
    
    def disable_debug_mode(self) -> None:
        """Disable debug mode."""
        self.config.debug_mode = False
        self.debug_logger.info("Debug mode disabled via API")
    
    def print_debug_summary(self) -> None:
        """Print a summary of debug information."""
        if not self.config.debug_mode:
            self.logger.info("🔍 Debug mode is not enabled. Use enable_debug_mode() to activate.")
            return
        
        self.logger.info("🔍 Ditto Debug Summary")
        self.logger.info("=" * 50)
        
        # Configuration
        self.logger.info(f"Debug Mode: {self.config.debug_mode}")
        self.logger.info(f"Log Level: {self.config.debug_log_level}")
        self.logger.info(f"Save Plans: {self.config.debug_save_plans}")
        self.logger.info(f"Metrics File: {self.config.debug_metrics_file}")
        self.logger.info("")
        
        # Metrics summary
        metrics = self.debug_metrics.get_metrics()
        self.logger.info("📊 Metrics Summary:")
        self.logger.info(f"  - Workflow Registrations: {len(metrics.get('workflow_registrations', []))}")
        self.logger.info(f"  - Plan Evaluations: {len(metrics.get('plan_evaluations', []))}")
        self.logger.info(f"  - Placement Operations: {len(metrics.get('placement_operations', []))}")
        self.logger.info(f"  - DoP Computations: {len(metrics.get('dop_computations', []))}")
        self.logger.info(f"  - Grouping Operations: {len(metrics.get('grouping_operations', []))}")
        self.logger.info("")
        
        # Saved plans summary
        saved_plans = self.debug_plan_saver.get_saved_plans()
        self.logger.info(f"💾 Saved Plans: {len(saved_plans)}")
        for plan in saved_plans:
            self.logger.info(f"  - {plan['workflow_name']} ({plan['plan_type']}) - {plan['timestamp']}")
        self.logger.info("")
        
        # Recent workflow registrations
        recent_registrations = metrics.get('workflow_registrations', [])
        if recent_registrations:
            self.logger.info("📋 Recent Workflow Registrations:")
            for reg in recent_registrations[-3:]:  # Show last 3
                self.logger.info(f"  - {reg['workflow_name']}: {reg['duration_ms']:.2f}ms")
                if 'metadata' in reg and 'jct_ms' in reg['metadata']:
                    self.logger.info(f"    JCT: {reg['metadata']['jct_ms']:.2f}ms, Cost: {reg['metadata'].get('cost', 0):.4f}")
        self.logger.info("=" * 50)


