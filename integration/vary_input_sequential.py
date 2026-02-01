#!/usr/bin/env python3
"""
Sequential Vary-Input Experiment Runner

This script runs the Vary-Input scenario sequentially instead of firing all requests
simultaneously. This approach helps reduce container crashes and provides cleaner
statistics by allowing the scheduler to handle requests one at a time.

Usage:
- python vary_input_sequential.py                    # Run all schedulers
- python vary_input_sequential.py --scheduler ours   # Run specific scheduler only
- python vary_input_sequential.py --override         # Override existing results
"""

import os
import sys
import time
import json
import logging
import statistics
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

# Add the integration directory to the path so we can import from utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the necessary components from the main experiment runner
from utils.config import ExperimentConfig, FunctionExecutionResult, ExperimentResult
from utils.scheduler_strategy import (
    SchedulerInterface,
    OurScheduler,
    FaasPRSScheduler,
    DittoScheduler,
    PaletteScheduler,
)

# Import provider components
from provider.container_manager import ContainerManager
from provider.function_container_manager import FunctionContainerManager
from scheduler.ours.config import load_config
from scheduler.shared.safe_printing import safe_print_value, is_binary_field

# Color codes for terminal output
class Colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

class ColoredFormatter(logging.Formatter):
    """Custom formatter to add colors to log levels"""
    
    COLORS = {
        'DEBUG': Colors.CYAN,
        'INFO': Colors.GREEN,
        'WARNING': Colors.YELLOW,
        'ERROR': Colors.RED,
        'CRITICAL': Colors.RED + Colors.BOLD,
    }
    
    def format(self, record):
        message = super().format(record)
        if record.levelname in ['ERROR', 'CRITICAL']:
            message = f"{self.COLORS.get(record.levelname, '')}{message}{Colors.END}"
        return message

# Custom JSON encoder to handle non-serializable objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, '__class__') and obj.__class__.__name__ == 'TimeModel':
            return {
                'type': 'TimeModel',
                'observed_times_ms': getattr(obj, 'observed_times_ms', {})
            }
        return super().default(obj)

# Configure logging
def _get_cli_log_level(argv: list[str]) -> str | None:
    try:
        for i, arg in enumerate(argv[1:]):
            if isinstance(arg, str):
                if arg.startswith('LOG_LEVEL='):
                    return arg.split('=', 1)[1].upper()
                if arg in ('--log-level', '-l') and i + 2 <= len(argv) - 1:
                    return argv[i + 2].upper()
                if arg.startswith('--log-level='):
                    return arg.split('=', 1)[1].upper()
    except Exception as e:
        print(f"DEBUG: Error getting CLI log level: {e}")
    return None

_cli_level = _get_cli_log_level(sys.argv)
log_level = (_cli_level or os.getenv('LOG_LEVEL', 'WARNING')).upper()

# Create colored formatter and configure logging
colored_formatter = ColoredFormatter('%(levelname)s: %(message)s')
logging.basicConfig(level=getattr(logging, log_level, logging.WARNING))
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, log_level, logging.INFO))

# Add colored formatter to the root logger's handlers
for handler in logging.root.handlers:
    handler.setFormatter(colored_formatter)

# Adjust component verbosity based on LOG_LEVEL
suppress = (log_level != 'DEBUG')
logging.getLogger('scheduler.ours.function_orchestrator').setLevel(logging.DEBUG if not suppress else logging.WARNING)
logging.getLogger('provider.function_container_manager').setLevel(logging.DEBUG if not suppress else logging.WARNING)
logging.getLogger('scheduler.ours.placement').setLevel(logging.DEBUG if not suppress else logging.WARNING)
logging.getLogger('scheduler.ours.routing').setLevel(logging.DEBUG if not suppress else logging.WARNING)
logging.getLogger('scheduler.faaSPRS').setLevel(logging.DEBUG if not suppress else logging.WARNING)
logging.getLogger('scheduler.faaSPRS.function_orchestrator').setLevel(logging.DEBUG if not suppress else logging.WARNING)
logging.getLogger('scheduler.ditto').setLevel(logging.DEBUG if not suppress else logging.WARNING)
logging.getLogger('scheduler.ditto.function_orchestrator').setLevel(logging.DEBUG if not suppress else logging.WARNING)
logging.getLogger('scheduler.palette').setLevel(logging.DEBUG if not suppress else logging.WARNING)
logging.getLogger('scheduler.palette.function_orchestrator').setLevel(logging.DEBUG if not suppress else logging.WARNING)

# Ensure common output directories exist
os.makedirs("results", exist_ok=True)
os.makedirs("data", exist_ok=True)


class SequentialVaryInputRunner:
    """Sequential Vary-Input experiment runner class"""
    
    def __init__(self):
        self.function_manager = None
        self.worker_manager = None
        self.workflow_dag = None
        self.schedulers = {}
        
    def setup_environment(self) -> bool:
        """Setup the experiment environment"""
        print("🔧 Setting up sequential vary-input experiment environment...")
        
        try:
            # Load configuration
            load_config('production')
            print("✅ Configuration loaded")
            
            # Initialize function container manager
            self.function_manager = FunctionContainerManager()
            nodes = self.function_manager.get_nodes()
            self.function_manager.discover_existing_containers()

            # Discover existing worker containers
            self.worker_manager = ContainerManager()
            workers = self.worker_manager.discover_worker_containers()
                        
            total_fn_containers = sum(len(containers) for containers in self.function_manager.function_containers.values())
            total_workers = len(workers)

            print(f"✅ Environment ready: {len(nodes)} nodes, {total_fn_containers} containers")
            print(f"✅ Environment ready: {total_workers} workers")

            if not nodes:
                print("❌ No nodes discovered! Check if containers are running")
                return False
                
            return True
            
        except Exception as e:
            print(f"❌ Failed to setup environment: {e}")
            return False
    
    def load_workflow(self, workflow_name: str = "recognizer") -> bool:
        """Load workflow DAG"""
        
        dag_path = Path(f'functions/{workflow_name}/{workflow_name}_dag.yaml')
        try:
            with open(dag_path, 'r') as f:
                self.workflow_dag = yaml.safe_load(f)
            
            print(f"✅ Workflow loaded: {self.workflow_dag.get('name', 'Unknown')}")
            return True
            
        except FileNotFoundError:
            print(f"❌ Workflow DAG not found: {dag_path}")
            return False
        except Exception as e:
            print(f"❌ Failed to load workflow: {e}")
            return False
    
    def load_input_data(self, config: ExperimentConfig) -> bytes:
        """Load input data for the experiment"""
        if config.input_file:
            try:
                with open(config.input_file, 'rb') as f:
                    data = f.read()
                data_mb = len(data)/(1024**2)
                data_kb = len(data)/1024
                print(f"✅ Loaded input file: {data_mb:.2f} MB ({data_kb:.1f} KB)")
                return data
            except Exception as e:
                print(f"⚠️  Could not load input file {config.input_file}: {e}")
        
        # Try to load a real test image first
        test_image_paths = ['data/test.png', 'data/small_test.png', 'test.png']
        for path in test_image_paths:
            try:
                with open(path, 'rb') as f:
                    data = f.read()
                data_kb = len(data)/1024
                print(f"✅ Loaded test image: {data_kb:.1f} KB from {path}")
                return data
            except FileNotFoundError:
                continue
            except Exception as e:
                print(f"⚠️  Could not load test image {path}: {e}")
                continue
        
        # Generate synthetic data based on size as fallback
        size_bytes = int(config.input_size_mb * 1024 * 1024)
        synthetic_data = b"X" * size_bytes
        synthetic_kb = len(synthetic_data)/1024
        print(f"⚠️  Using synthetic data: {synthetic_kb:.1f} KB")
        return synthetic_data
    
    def get_scheduler(self, scheduler_type: str) -> SchedulerInterface:
        """Get scheduler instance by type"""
        if scheduler_type not in self.schedulers:
            if scheduler_type == "ours":
                scheduler = OurScheduler()
            elif scheduler_type == "FaasPRS":
                scheduler = FaasPRSScheduler(
                    container_manager=self.function_manager,
                )
            elif scheduler_type == "ditto":
                scheduler = DittoScheduler()
            elif scheduler_type == "palette":
                scheduler = PaletteScheduler(
                    container_manager=self.function_manager,
                )
            else:
                raise ValueError(f"Unknown scheduler type: {scheduler_type}")
            
            # Pass the appropriate manager to each scheduler
            if scheduler_type == "ditto":
                # Ditto needs the worker manager that has discovered workers
                if not scheduler.initialize(self.worker_manager):
                    raise Exception(f"Failed to initialize scheduler: {scheduler_type}")
            else:
                # Other schedulers (ours, FaasPRS, palette) use the function manager
                if not scheduler.initialize(self.function_manager):
                    raise Exception(f"Failed to initialize scheduler: {scheduler_type}")
            
            # Register workflow if it's loaded
            if self.workflow_dag and hasattr(scheduler, 'orchestrator') and hasattr(scheduler.orchestrator, 'register_workflow'):
                try:
                    init_start = time.time()
                    scheduler.orchestrator.register_workflow("recognizer", self.workflow_dag)
                    init_end = time.time()
                    init_ms = (init_end - init_start) * 1000.0
                    print(f"✅ Registered workflow 'recognizer' with {scheduler_type} scheduler")
                    print(f"⏱️  Initialization overhead: {init_ms:.3f} ms")
                    setattr(scheduler, 'init_overhead', {
                        'start': init_start,
                        'end': init_end,
                        'duration_ms': init_ms,
                    })
                except Exception as e:
                    print(f"⚠️  Failed to register workflow with {scheduler_type} scheduler: {e}")
            
            self.schedulers[scheduler_type] = scheduler
        
        return self.schedulers[scheduler_type]
    
    def _run_workflow_sequential(self, scheduler, input_data: bytes, request_id: str):
        """Run a complete workflow using the orchestrator SEQUENTIALLY.
        This is the key difference from the original - we wait for each request to complete
        before submitting the next one.
        Returns (function_results: List[FunctionExecutionResult], request_metrics: Dict)
        """
        start_time = time.time()
        
        try:
            # Use the orchestrator's run_workflow method
            result = scheduler.orchestrator.submit_workflow("recognizer", request_id, input_data)
            e2e_time = (time.time() - start_time) * 1000
            print(f"    ✅ Sequential workflow executed in {e2e_time:.3f} ms")
            
            # Check for ours-style results (stored in timeline['functions'])
            timeline = result.get('timeline') if isinstance(result, dict) else None
            if isinstance(timeline, dict) and 'functions' in timeline:
                # Extract complete function results including actual data
                for func_timeline in timeline['functions']:
                    if isinstance(func_timeline, dict) and 'function' in func_timeline:
                        func_name = func_timeline['function']
                        # Extract all available result data, not just timing and status
                        func_result = {}
                        
                        # Copy all fields from the function timeline, excluding orchestrator metadata
                        for key, value in func_timeline.items():
                            if key not in ['timeline', 'node_id', 'container_id']:
                                func_result[key] = value
                        
                        # Only print if we have meaningful result data
                        if func_result:
                            # Safely print function results with truncation for binary fields
                            safe_result = safe_print_value(func_result)
                            logger.debug(f"\n{func_name} result: {safe_result}")
            
            # Convert workflow result to individual function results
            function_results = []
            # Derive timing metrics from orchestrator timeline if available
            request_metrics = {
                "request_id": request_id,
                "end_to_end_latency_ms": e2e_time,
                "scheduling_overhead_ms": 0.0,
                "function_exec_ms": [],
                "function_exec_avg_ms": 0.0,
                "network_delay_ms": [],
            }
            
            timeline = result.get('timeline') if isinstance(result, dict) else None
            if isinstance(timeline, dict):
                try:
                    # Process timeline data
                    wf = timeline.get('workflow') or {}
                    if 'duration_ms' in wf:
                        request_metrics['end_to_end_latency_ms'] = wf['duration_ms']
                    
                    # Clean timing aggregation using the new timing structure
                    workflow_timestamps = timeline.get('timestamps', {})
                    
                    # 1. Workflow-level scheduling overhead (support both *_ms and *_duration_ms)
                    def _get_ms(d: Dict[str, Any], keys: List[str]) -> float:
                        for k in keys:
                            v = d.get(k)
                            if isinstance(v, (int, float)):
                                return float(v)
                        return 0.0

                    cost_model_ms = _get_ms(workflow_timestamps, ['cost_model_duration_ms', 'cost_model_ms'])
                    bottleneck_ms = _get_ms(workflow_timestamps, ['bottleneck_analysis_duration_ms', 'bottleneck_analysis_ms'])
                    workflow_scheduling_ms = cost_model_ms + bottleneck_ms
                    
                    # 2. Per-function scheduling overhead: prefer aggregated key, fallback to components
                    fn_calls = timeline.get('functions') or []
                    per_function_scheduling_ms = 0.0
                    for call in fn_calls:
                        ts = (call or {}).get('timestamps', {})
                        sched_ms = _get_ms(ts, ['scheduling_overhead_ms'])
                        if sched_ms == 0.0:
                            lns_ms = _get_ms(ts, ['lns_duration_ms', 'lns_ms'])
                            route_ms = _get_ms(ts, ['route_duration_ms', 'route_ms'])
                            sched_ms = lns_ms + route_ms
                        per_function_scheduling_ms += sched_ms
                    
                    # Extract derived metrics from timeline
                    for call in fn_calls:
                        derived = call.get('derived') or {}
                        if 'exec_ms' in derived:
                            request_metrics['function_exec_ms'].append(float(derived['exec_ms']))
                        if 'network_delay_ms' in derived:
                            request_metrics['network_delay_ms'].append(float(derived['network_delay_ms']))
                    
                    # 3. Total scheduling overhead: prefer aggregated workflow key when present
                    wf_sched_ms_agg = _get_ms(workflow_timestamps, ['scheduling_overhead_ms'])
                    total_scheduling_ms = (wf_sched_ms_agg if wf_sched_ms_agg > 0.0 else workflow_scheduling_ms) + per_function_scheduling_ms
                    request_metrics['scheduling_overhead_ms'] = total_scheduling_ms
                    
                    # 4. Extract orchestrator overhead components
                    overhead_components = {
                        'timeline_setup_ms': 0.0,
                        'container_discovery_ms': 0.0,
                        'container_selection_ms': 0.0,
                        'request_preparation_ms': 0.0,
                        'result_processing_ms': 0.0,
                        'timeline_merging_ms': 0.0,
                        'error_handling_ms': 0.0,
                        'input_preparation_ms': 0.0
                    }
                    
                    # Aggregate overhead from all function calls
                    for call in fn_calls:
                        ts = (call or {}).get('timestamps', {})
                        for component in overhead_components.keys():
                            # Treat route_duration_ms as container_selection_ms if explicit key missing
                            if component == 'container_selection_ms':
                                val = _get_ms(ts, ['container_selection_ms'])
                                if val == 0.0:
                                    val = _get_ms(ts, ['route_duration_ms', 'route_ms'])
                                overhead_components[component] += val
                            else:
                                overhead_components[component] += _get_ms(ts, [component])
                    
                    # Calculate total processing delay (system overhead)
                    total_processing_delay = sum(overhead_components.values())
                    request_metrics['processing_delay_ms'] = total_processing_delay
                    request_metrics['overhead_components'] = overhead_components
                    
                    # 5. Calculate component totals
                    total_exec_ms = sum(request_metrics['function_exec_ms']) if request_metrics['function_exec_ms'] else 0.0
                    total_network_ms = sum(request_metrics['network_delay_ms']) if request_metrics['network_delay_ms'] else 0.0
                    orchestration_ms = _get_ms(workflow_timestamps, ['workflow_orchestration_duration_ms', 'workflow_orchestration_ms'])
                    
                    # Store detailed timeline data for later extraction
                    request_metrics['detailed_timeline'] = timeline
                    
                    # Extract bottleneck information for ours scheduler
                    if hasattr(scheduler, 'orchestrator') and hasattr(scheduler.orchestrator, 'bottleneck_analysis'):
                        bottleneck_info = self._extract_bottleneck_info(scheduler.orchestrator.bottleneck_analysis)
                        request_metrics['bottleneck_info'] = bottleneck_info
                    
                    # Calculate function execution average
                    if request_metrics['function_exec_ms']:
                        try:
                            request_metrics['function_exec_avg_ms'] = statistics.mean(request_metrics['function_exec_ms'])
                        except Exception as e:
                            print(f"DEBUG: Error calculating mean: {e}")
                            request_metrics['function_exec_avg_ms'] = 0.0
                    else:
                        request_metrics['function_exec_avg_ms'] = 0.0
                except Exception as e:
                    print(f"DEBUG: Error processing timeline: {e}")
            
            # Create function results from timeline data
            timeline = result.get('timeline') if isinstance(result, dict) else None
            if isinstance(timeline, dict) and 'functions' in timeline:
                exec_time_by_function = {}
                tl_calls_by_func = {}
                
                try:
                    for func_timeline in timeline['functions']:
                        if isinstance(func_timeline, dict) and 'function' in func_timeline:
                            func_name = func_timeline['function']
                            tl_calls_by_func[func_name] = func_timeline
                            derived = func_timeline.get('derived') or {}
                            exec_ms = derived.get('exec_ms')
                            if isinstance(exec_ms, (int, float)):
                                exec_time_by_function[func_name] = float(exec_ms)
                    
                    executed_functions = list(tl_calls_by_func.keys())
                    if executed_functions:
                        for func_name in executed_functions:
                            tl_call = tl_calls_by_func.get(func_name)
                            success = (isinstance(tl_call, dict) and tl_call.get('status', 'ok') != 'error') if tl_call else True
                            err_msg = (tl_call.get('message') or tl_call.get('error') or tl_call.get('error_message')) if (isinstance(tl_call, dict) and tl_call.get('status') == 'error') else None
                            exec_ms_val = exec_time_by_function.get(func_name, 0.0)
                            
                            function_results.append(FunctionExecutionResult(
                                function_name=func_name,
                                request_id=f"{request_id}_{func_name}",
                                success=success,
                                execution_time_ms=exec_ms_val,
                                error_message=err_msg
                            ))
                except Exception as e:
                    print(f"DEBUG: Error creating function results: {e}")
            
            return function_results, request_metrics
            
        except Exception as e:
            e2e_time = (time.time() - start_time) * 1000
            # Create error results for all functions
            function_names = self.extract_function_names()
            function_results = []
            for func_name in function_names:
                function_results.append(FunctionExecutionResult(
                    function_name=func_name,
                    request_id=f"{request_id}_{func_name}",
                    success=False,
                    execution_time_ms=0.0,
                    error_message=str(e)
                ))
            return function_results, {
                "request_id": request_id,
                "end_to_end_latency_ms": e2e_time,
                "scheduling_overhead_ms": 0.0,
                "function_exec_ms": [],
                "function_exec_avg_ms": 0.0,
                "network_delay_ms": [],
            }
    
    def extract_function_names(self) -> List[str]:
        """Extract function names from workflow DAG"""
        if not self.workflow_dag:
            return []
        
        if 'workflow' in self.workflow_dag:
            workflow = self.workflow_dag['workflow']
            nodes = workflow.get('nodes', [])
            # Filter out END nodes (workflow termination markers, not real functions)
            return [node['id'] for node in nodes if node.get('type') == 'task' and node['id'] != 'END']
        else:
            # Fallback to old format
            pipeline = self.workflow_dag.get('pipeline', [])
            return [step.get('function', step) for step in pipeline if isinstance(step, dict)]
    
    def _extract_bottleneck_info(self, bottleneck_analysis):
        """Extract bottleneck analysis information"""
        try:
            if hasattr(bottleneck_analysis, '__dict__'):
                return {k: v for k, v in bottleneck_analysis.__dict__.items() if not k.startswith('_')}
            else:
                return str(bottleneck_analysis)
        except Exception:
            return "Unable to extract bottleneck info"
    
    def run_sequential_experiment(self, config: ExperimentConfig) -> ExperimentResult:
        """Run a single experiment SEQUENTIALLY - this is the key difference from the original"""
        print(f"\n🧪 Running SEQUENTIAL experiment: {config.experiment_id}")
        print(f"   📊 Input size: {config.input_size_mb}MB")
        print(f"   🔄 Concurrency: {config.concurrency_level} (but running SEQUENTIALLY)")
        print(f"   📈 Total requests: {config.total_requests}")
        print(f"   ⚙️  Scheduler: {config.scheduler_type}")
        
        # Print scenario-specific parameters if available
        if config.request_rate_per_sec is not None:
            print(f"   🚀 Request rate: {config.request_rate_per_sec} req/s")
        if config.bandwidth_cap_mbps is not None:
            print(f"   🌐 Bandwidth cap: {config.bandwidth_cap_mbps} Mbps")
        if config.injected_latency_ms is not None:
            print(f"   ⏱️  Injected latency: {config.injected_latency_ms} ms")
        if config.duration_sec is not None:
            print(f"   ⏰ Duration: {config.duration_sec} s")
        if config.expected_bottleneck is not None:
            print(f"   🎯 Expected bottleneck: {config.expected_bottleneck}")
        
        # Load input data
        input_data = self.load_input_data(config)
        
        # Get scheduler
        scheduler = self.get_scheduler(config.scheduler_type)
        
        # Extract function names
        function_names = self.extract_function_names()
        if not function_names:
            raise Exception("No functions found in workflow")
        
        # Execute workflow SEQUENTIALLY - one request at a time
        start_time = time.time()
        function_results = []
        per_request_metrics: Dict[str, Any] = {}
        
        print(f"   🔄 Running {config.total_requests} requests SEQUENTIALLY (one at a time)")
        
        # Sequential execution - wait for each request to complete before starting the next
        for i in range(config.total_requests):
            request_id = f"{config.experiment_id}_seq_{i}"
            print(f"📤 Submitting request {i+1}/{config.total_requests}: {request_id}")
            
            try:
                # Run the workflow sequentially
                result = self._run_workflow_sequential(scheduler, input_data, request_id)
                
                if isinstance(result, tuple):
                    fn_results, req_metrics = result
                    function_results.extend(fn_results)
                    per_request_metrics[req_metrics['request_id']] = req_metrics
                    print(f"    ✅ Request {i+1} completed: {req_metrics['end_to_end_latency_ms']:.3f}ms")
                else:
                    function_results.append(result)
                    print(f"    ✅ Request {i+1} completed")
                
                # Small delay between requests to ensure clean separation
                if i < config.total_requests - 1:  # Don't sleep after the last request
                    time.sleep(0.1)  # 100ms delay between requests
                    
            except Exception as e:
                print(f"   ❌ Request {i+1} failed: {e}")
                # Create error result for each function in workflow
                for function_name in function_names:
                    function_results.append(FunctionExecutionResult(
                        function_name=function_name,
                        request_id=f"{request_id}_{function_name}",
                        success=False,
                        execution_time_ms=0,
                        error_message=str(e)
                    ))
        
        total_execution_time = (time.time() - start_time) * 1000
        
        # Calculate metrics
        successful_results = [r for r in function_results if r.success]
        success_rate = len(successful_results) / len(function_results) if function_results else 0
        
        execution_times = [r.execution_time_ms for r in successful_results]
        avg_execution_time = statistics.mean(execution_times) if execution_times else 0
        
        # Throughput: total function calls over experiment wall time (seconds)
        if total_execution_time > 0 and success_rate == 1.0:
            throughput = len(function_results) / (total_execution_time / 1000) 
        else:
            throughput = 0
        
        # Get scheduler metrics and attach timing aggregates
        scheduler_metrics = scheduler.get_metrics()
        if scheduler_metrics is None:
            scheduler_metrics = {}
        
        # Add initialization overhead if available
        if hasattr(scheduler, 'init_overhead'):
            scheduler_metrics['init_overhead'] = scheduler.init_overhead
        
        # Aggregate per-request metrics to experiment level
        req_vals = list(per_request_metrics.values())
        if req_vals:
            scheduler_metrics['timing'] = {
                'requests': req_vals,
                'end_to_end_latency_avg_ms': statistics.mean(v['end_to_end_latency_ms'] for v in req_vals),
                'scheduling_overhead_avg_ms': statistics.mean(v['scheduling_overhead_ms'] for v in req_vals),
                'function_exec_avg_ms': statistics.mean(v['function_exec_avg_ms'] for v in req_vals),
                'network_delay_avg_ms': statistics.mean(
                    [x for v in req_vals for x in v.get('network_delay_ms', [])] or [0]
                ),
                'processing_delay_avg_ms': statistics.mean(v.get('processing_delay_ms', 0) for v in req_vals),
            }
            # Prefer timeline-derived function execution average if available
            try:
                if scheduler_metrics['timing']['function_exec_avg_ms'] > 0:
                    avg_execution_time = scheduler_metrics['timing']['function_exec_avg_ms']
            except Exception as e:
                print(f"DEBUG: Error getting function exec avg ms: {e}")
        
        result = ExperimentResult(
            config=config,
            total_execution_time_ms=total_execution_time,
            success_rate=success_rate,
            avg_execution_time_ms=avg_execution_time,
            throughput_req_per_sec=throughput,
            function_results=function_results,
            scheduler_metrics=scheduler_metrics,
            timestamp=time.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        print(f"   ✅ Sequential execution complete: {success_rate*100:.1f}% success, Avg time: {avg_execution_time:.3f}ms, Throughput: {throughput:.1f} req/s")
        
        return result
    
    def run_experiment_suite(self, configs: List[ExperimentConfig], override_existing: bool = False) -> List[ExperimentResult]:
        """Run a suite of sequential experiments with optional skipping and per-experiment saving"""
        print("🚀 Starting Sequential Vary-Input Experiment Suite")
        print("=" * 60)
        print("💾 Each experiment saves 3 files: results.json, timelines.json, timeseries.csv")
        
        # Filter out experiments that already have results (unless override is specified)
        if not override_existing:
            skipped_configs = []
            remaining_configs = []
            for config in configs:
                if self.check_existing_results(config):
                    skipped_configs.append(config)
                    print(f"⏭️  Skipping {config.experiment_id} - results already exist")
                else:
                    remaining_configs.append(config)
            
            if skipped_configs:
                print(f"⏭️  Skipped {len(skipped_configs)} experiments with existing results")
                print(f"🚀 Running {len(remaining_configs)} remaining experiments")
                configs = remaining_configs
        
        if not configs:
            print("✅ All experiments already completed!")
            return []
        
        results = []
        
        for i, config in enumerate(configs, 1):
            print(f"\n[{i}/{len(configs)}] Running sequential experiment: {config.experiment_id}")
            try:
                # Start per-experiment resource sampler with experiment-specific timeseries path
                try:
                    if self.function_manager:
                        # Ensure timeseries directory exists
                        os.makedirs("results/timeseries", exist_ok=True)
                        timeseries_path = f"results/timeseries/{config.experiment_id}_timeseries.csv"
                        self.function_manager.start_timeseries_sampler(
                            interval_s=1.0,  # 1s interval
                            file_path=timeseries_path,
                            scheduler_type=config.scheduler_type,
                            experiment_id=config.experiment_id,
                        )
                except Exception as e:
                    print(f"⚠️  Could not start per-experiment sampler: {e}")
                
                # Update sampler context for this experiment
                try:
                    if self.function_manager and hasattr(self.function_manager, '_sampler_scheduler_type'):
                        self.function_manager._sampler_scheduler_type = config.scheduler_type
                        self.function_manager._sampler_experiment_id = config.experiment_id
                except Exception:
                    pass
                
                result = self.run_sequential_experiment(config)
                results.append(result)
                
                # Save per experiment (always save individual files)
                result_files = self.save_results([result], per_scenario=True)
                timeline_files = self.save_timeline_files([result], per_scenario=True)
                
                # Print saved files
                print(f"\n📁 Files saved for experiment {config.experiment_id}:")
                for file_path in result_files:
                    print(f"   📄 {file_path}")
                for file_path in timeline_files:
                    print(f"   📊 {file_path}")
                
                # Check if timeseries file exists
                timeseries_file = f"results/timeseries/{config.experiment_id}_timeseries.csv"
                if os.path.exists(timeseries_file):
                    print(f"   📈 {timeseries_file}")
                
                # Sleep between experiments to allow system to stabilize
                if i < len(configs):  # Don't sleep after the last experiment
                    print(f"⏸️  Sleeping 5 seconds before next experiment...")
                    time.sleep(5)
                
            except Exception as e:
                print(f"❌ Experiment {config.experiment_id} failed: {e}")
                import traceback
                traceback.print_exc()
            finally:
                # Stop per-experiment sampler
                try:
                    if self.function_manager:
                        self.function_manager.stop_timeseries_sampler(timeout_s=3.0)
                except Exception:
                    pass
            print(f"⏸️  Sleeping 5 seconds before next experiment...")
            time.sleep(5)
        return results

    def check_existing_results(self, config: ExperimentConfig) -> bool:
        """Check if results already exist for a given experiment configuration"""
        results_file = f"results/{config.experiment_id}_results.json"
        timeline_file = f"results/timelines/{config.experiment_id}_timelines.json"
        timeseries_file = f"results/timeseries/{config.experiment_id}_timeseries.csv"
        
        return (
            os.path.exists(results_file)
            and os.path.exists(timeline_file)
            and os.path.exists(timeseries_file)
        )

    def save_results(self, results: List[ExperimentResult], filename: str = None, per_scenario: bool = False) -> List[str]:
        """Save experiment results to file(s)"""
        if per_scenario:
            # Save each experiment to separate files
            saved_files = []
            for result in results:
                experiment_filename = f"results/{result.config.experiment_id}_results.json"

                # Convert single result to serializable format
                result_dict = asdict(result)
                self._flatten_timing_metrics(result_dict)

                with open(experiment_filename, 'w') as f:
                    json.dump([result_dict], f, indent=2, cls=CustomJSONEncoder)

                saved_files.append(experiment_filename)
                print(f"📄 Experiment results saved to: {experiment_filename}")

            return saved_files
        else:
            # Original behavior - save all results to one file
            if not filename:
                filename = "results/sequential_vary_input_results.json"
            
            # Convert results to serializable format and flatten key timing metrics
            serializable_results = []
            for result in results:
                result_dict = asdict(result)
                self._flatten_timing_metrics(result_dict)
                serializable_results.append(result_dict)
            
            with open(filename, 'w') as f:
                json.dump(serializable_results, f, indent=2, cls=CustomJSONEncoder)
            
            return [filename]

    def _flatten_timing_metrics(self, result_dict: dict):
        """Flatten timing metrics to top-level for easier analysis/CSV export"""
        timing = None
        try:
            if isinstance(result_dict.get('scheduler_metrics'), dict):
                timing = result_dict['scheduler_metrics'].get('timing')
                # Also flatten init_overhead if present
                init_overhead = result_dict['scheduler_metrics'].get('init_overhead')
                if isinstance(init_overhead, dict):
                    result_dict['init_overhead_duration_ms'] = init_overhead.get('duration_ms')
        except Exception:
            timing = None
        if isinstance(timing, dict):
            result_dict['e2e_latency_avg_ms'] = timing.get('end_to_end_latency_avg_ms')
            result_dict['sched_overhead_avg_ms'] = timing.get('scheduling_overhead_avg_ms')
            result_dict['func_exec_avg_ms'] = timing.get('function_exec_avg_ms')
            result_dict['network_delay_avg_ms'] = timing.get('network_delay_avg_ms')

    def save_timeline_files(self, results: List[ExperimentResult], per_scenario: bool = False) -> List[str]:
        """Save detailed timeline files for each experiment"""
        timeline_files = []
        
        # Create timeline directory
        timeline_dir = "results/timelines"
        os.makedirs(timeline_dir, exist_ok=True)
        
        if per_scenario:
            # Save each experiment to separate timeline files
            for result in results:
                experiment_timeline_file = f"{timeline_dir}/{result.config.experiment_id}_timelines.json"

                # Extract timeline data for this scheduler directly from the orchestrator if available
                timeline_data = None
                try:
                    scheduler = self.get_scheduler(result.config.scheduler_type)
                    timeline_data = self.extract_scheduler_timelines(scheduler, result.config.scheduler_type)
                except Exception:
                    pass
                
                # Fallback: build minimal timeline from result if orchestrator data not available
                if not timeline_data:
                    timeline_data = {
                        "experiment_id": result.config.experiment_id,
                        "scheduler_type": result.config.scheduler_type,
                        "results": []
                    }
                
                with open(experiment_timeline_file, 'w') as f:
                    json.dump(timeline_data, f, indent=2, cls=CustomJSONEncoder)
                timeline_files.append(experiment_timeline_file)
                print(f"📊 Experiment timelines saved: {experiment_timeline_file}")
            return timeline_files
        else:
            # Original behavior - group by scheduler type
            by_scheduler = {}
            for result in results:
                scheduler_type = result.config.scheduler_type
                if scheduler_type not in by_scheduler:
                    by_scheduler[scheduler_type] = []
                by_scheduler[scheduler_type].append(result)
            
            # Save timeline files for each scheduler
            for scheduler_type, scheduler_results in by_scheduler.items():
                scheduler_timeline_file = f"{timeline_dir}/{scheduler_type}_sequential_timelines.json"
                
                timeline_data = {
                    "scheduler_type": scheduler_type,
                    "experiments": [],
                    "summary": {
                        "total_experiments": len(scheduler_results),
                        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
                    }
                }
                
                for result in scheduler_results:
                    experiment_timeline = self._create_experiment_timeline(result)
                    timeline_data["experiments"].append(experiment_timeline)
                
                # Save scheduler timeline file
                with open(scheduler_timeline_file, 'w') as f:
                    json.dump(timeline_data, f, indent=2, cls=CustomJSONEncoder)
                
                timeline_files.append(scheduler_timeline_file)
                print(f"📊 Timeline file saved: {scheduler_timeline_file}")
        
        return timeline_files

    def _create_experiment_timeline(self, result: ExperimentResult) -> dict:
        """Create timeline data for a single experiment result"""
        experiment_timeline = {
            "experiment_id": result.config.experiment_id,
            "config": {
                "input_size_mb": result.config.input_size_mb,
                "concurrency_level": result.config.concurrency_level,
                "total_requests": result.config.total_requests,
                "scheduler_type": result.config.scheduler_type
            },
            "metrics": {
                "total_execution_time_ms": result.total_execution_time_ms,
                "success_rate": result.success_rate,
                "avg_execution_time_ms": result.avg_execution_time_ms,
                "throughput_req_per_sec": result.throughput_req_per_sec
            },
            "function_results": [],
            "timeline_data": None
        }
        
        # Extract timeline data from scheduler metrics
        scheduler_metrics = result.scheduler_metrics
        if isinstance(scheduler_metrics, dict):
            # Extract timing information
            timing = scheduler_metrics.get('timing', {})
            if timing:
                experiment_timeline["timing_summary"] = {
                    "end_to_end_latency_avg_ms": timing.get('end_to_end_latency_avg_ms'),
                    "scheduling_overhead_avg_ms": timing.get('scheduling_overhead_avg_ms'),
                    "function_exec_avg_ms": timing.get('function_exec_avg_ms'),
                    "network_delay_avg_ms": timing.get('network_delay_avg_ms')
                }
            
            # Extract per-request timeline data
            requests_data = timing.get('requests', [])
            if requests_data:
                experiment_timeline["per_request_timelines"] = requests_data
            
            # Extract detailed timeline data from each request
            detailed_timelines = []
            for req_data in requests_data:
                if isinstance(req_data, dict) and 'detailed_timeline' in req_data:
                    detailed_timelines.append(req_data['detailed_timeline'])
            if detailed_timelines:
                experiment_timeline["detailed_timelines"] = detailed_timelines
            
            # Extract initialization overhead
            init_overhead = scheduler_metrics.get('init_overhead', {})
            if init_overhead:
                experiment_timeline["initialization_overhead"] = init_overhead
        
        # Extract function execution results
        for func_result in result.function_results:
            experiment_timeline["function_results"].append({
                "function_name": func_result.function_name,
                "request_id": func_result.request_id,
                "success": func_result.success,
                "execution_time_ms": func_result.execution_time_ms,
                "node_id": func_result.node_id,
                "container_id": func_result.container_id,
                "error_message": func_result.error_message
            })
        
        return experiment_timeline

    def extract_scheduler_timelines(self, scheduler, scheduler_type: str) -> Dict[str, Any]:
        """Extract timeline data directly from scheduler orchestrator"""
        timeline_data = {
            "scheduler_type": scheduler_type,
            "extraction_timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "orchestrator_timelines": {},
            "workflow_timelines": {},
            "function_timelines": {}
        }
        
        try:
            orchestrator = getattr(scheduler, 'orchestrator', None)
            if orchestrator:
                # Extract request timelines from orchestrator
                if hasattr(orchestrator, 'request_timelines'):
                    timeline_data["orchestrator_timelines"] = orchestrator.request_timelines
                
                # Extract workflow timelines
                if hasattr(orchestrator, 'workflows'):
                    timeline_data["workflow_timelines"] = orchestrator.workflows
                
                # Extract active workflows
                if hasattr(orchestrator, 'active_workflows'):
                    timeline_data["active_workflows"] = orchestrator.active_workflows
                
                # Extract workflow results
                if hasattr(orchestrator, 'workflow_results'):
                    timeline_data["workflow_results"] = orchestrator.workflow_results
                
        except Exception as e:
            timeline_data["extraction_error"] = str(e)
            print(f"Warning: Could not extract timeline data from {scheduler_type} scheduler: {e}")
        
        return timeline_data

    def print_summary(self, results: List[ExperimentResult]):
        """Print experiment summary"""
        print("\n📊 Sequential Vary-Input Experiment Summary")
        print("=" * 50)
        
        # Group by scheduler type
        by_scheduler = {}
        for result in results:
            scheduler_type = result.config.scheduler_type
            if scheduler_type not in by_scheduler:
                by_scheduler[scheduler_type] = []
            by_scheduler[scheduler_type].append(result)
        
        for scheduler_type, scheduler_results in by_scheduler.items():
            print(f"\n🔧 {scheduler_type.upper()} Scheduler (Sequential):")
            for result in scheduler_results:
                config = result.config
                total_calls = len(result.function_results)
                print(f"   {config.experiment_id}: {result.success_rate*100:.1f}% success, "
                      f"{result.avg_execution_time_ms:.3f}ms avg, {result.throughput_req_per_sec:.1f} req/s, "
                      f"calls: {total_calls}, total runtime: {result.total_execution_time_ms:.3f}ms")
                
                # Initialization overhead if available
                if isinstance(result.scheduler_metrics, dict):
                    _init = result.scheduler_metrics.get('init_overhead')
                    if isinstance(_init, dict) and 'duration_ms' in _init:
                        print(f"      init_overhead: {_init.get('duration_ms', 0):.3f}ms")

                # Extra per-experiment timing summary if present
                t = result.scheduler_metrics.get('timing') if isinstance(result.scheduler_metrics, dict) else None
                if t:
                    print(f"      timing → e2e_avg: {t.get('end_to_end_latency_avg_ms', 0):.3f}ms, "
                          f"sched_avg: {t.get('scheduling_overhead_avg_ms', 0):.3f}ms, "
                          f"exec_avg: {t.get('function_exec_avg_ms', 0):.3f}ms, "
                          f"network_avg: {t.get('network_delay_avg_ms', 0):.3f}ms, "
                          f"processing_avg: {t.get('processing_delay_avg_ms', 0):.3f}ms")


def create_vary_input_sequential_configs(
    schedulers: List[str] = ["ours", "FaasPRS", "palette"],
) -> List[ExperimentConfig]:
    """Create experiment configurations for sequential vary-input scenario"""
    configs = []
    
    # Vary-Input scenario parameters - varying input sizes
    input_sizes_mb = [5, 10, 15, 20, 25, 30]  # VARIES: 5MB to 30MB
    concurrency = 1      # Fixed baseline (but will run sequentially)
    request_rate_per_sec = 1  # Fixed baseline
    bandwidth_cap_mbps = 1000  # No cap
    injected_latency_ms = 1
    duration_sec = 10  # Fixed baseline
    replicates = 1
    refinement_time_cap_sec = 0.02
    expected_bottleneck = "Network (bandwidth/transfer)"
    
    for scheduler in schedulers:
        for input_size in input_sizes_mb:
            # Calculate total requests based on duration and request rate
            total_requests = int(duration_sec * request_rate_per_sec)
            
            # Determine input file based on size
            input_file = _get_input_file_for_size(input_size)
            
            # Create unique experiment ID for sequential vary-input
            experiment_id = f"vary-input-seq_{scheduler}_input{input_size}mb"
            
            config = ExperimentConfig(
                experiment_id=experiment_id,
                input_size_mb=input_size,
                concurrency_level=concurrency,
                total_requests=total_requests,
                scheduler_type=scheduler,
                workflow_name="recognizer",
                input_file=input_file,
                # New scenario parameters
                request_rate_per_sec=request_rate_per_sec,
                bandwidth_cap_mbps=bandwidth_cap_mbps,
                injected_latency_ms=injected_latency_ms,
                duration_sec=duration_sec,
                replicates=replicates,
                refinement_time_cap_sec=refinement_time_cap_sec,
                expected_bottleneck=expected_bottleneck
            )
            configs.append(config)
    
    return configs


def _get_input_file_for_size(size_mb: float) -> str:
    """Determine input file based on size"""
    if size_mb >= 30:
        return "data/test_30mb.png"
    elif size_mb >= 25:
        return "data/test_25mb.png"
    elif size_mb >= 20:
        return "data/test_20mb.png"
    elif size_mb >= 15:
        return "data/test_15mb.png"
    elif size_mb >= 10:
        return "data/test_10mb.png"
    elif size_mb >= 5:
        return "data/test_5mb.png"
    elif size_mb >= 2:
        return "data/test_2mb.png"
    elif size_mb >= 1:
        return "data/test_1mb.png"
    elif size_mb >= 0.1:
        return "data/test_100kb.png"
    elif size_mb <= 0.01:
        return "data/test_10kb.png"
    else:
        return "data/test.png"  # Default


def create_test_images_for_sequential():
    """Create test images for sequential vary-input scenario"""
    print("🖼️  Creating test images for sequential vary-input scenario...")
    
    # Import the image creation functions from the original experiment runner
    try:
        from integration.experiment_runner import create_test_image
    except ImportError:
        print("⚠️  Could not import image creation functions, using existing images")
        return
    
    # Create test images for all scenario sizes
    test_images = [
        (5 * 1024 * 1024, "data/test_5mb.png"),
        (10 * 1024 * 1024, "data/test_10mb.png"),
        (15 * 1024 * 1024, "data/test_15mb.png"),
        (20 * 1024 * 1024, "data/test_20mb.png"),
        (25 * 1024 * 1024, "data/test_25mb.png"),
        (30 * 1024 * 1024, "data/test_30mb.png"),
    ]
    
    for size_bytes, filename in test_images:
        if not os.path.exists(filename):
            try:
                create_test_image(size_bytes, filename)
            except Exception as e:
                print(f"⚠️  Could not create {filename}: {e}")
        else:
            size_mb = size_bytes / (1024 * 1024)
            print(f"   ✅ {filename} already exists ({size_mb:.1f} MB)")
    
    print("✅ Test images ready for sequential vary-input scenario")


def main():
    """Main entry point"""
    print("🎭 Sequential Vary-Input Experiment Runner")
    print("=" * 50)
    
    # Parse command line arguments
    override_existing = False
    scheduler_filter: List[str] | None = None
    
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--override" or arg == "-f":
            override_existing = True
        elif arg == "--scheduler" and i + 1 < len(sys.argv):
            scheduler_filter = [sys.argv[i + 1]]
        elif arg.startswith("--scheduler="):
            scheduler_filter = [arg.split("=", 1)[1]]
        elif arg.startswith("--log-level="):
            # Handle log level (already handled by logging setup)
            pass
        elif arg in ["--log-level", "-l"] and i + 1 < len(sys.argv):
            # Handle log level with space (already handled by logging setup)
            pass
    
    print("🧪 Running SEQUENTIAL VARY-INPUT experiments")
    if override_existing:
        print("   🔄 Override mode: Will rerun experiments even if results exist")
    else:
        print("   ⏭️  Skip mode: Will skip experiments that already have results")
    print("   📁 Per-experiment saving: Each experiment saves to separate files")
    
    # Create test images for scenarios
    create_test_images_for_sequential()
    
    # Create experiment configurations
    configs = create_vary_input_sequential_configs(
        schedulers=scheduler_filter if scheduler_filter else ["ours", "FaasPRS", "palette"],
    )
    
    if scheduler_filter:
        print(f"   - Schedulers (filtered): {', '.join(scheduler_filter)}")
    else:
        print(f"   - Schedulers: ours, FaasPRS, palette")
    print(f"   - Input sizes: 5MB, 10MB, 15MB, 20MB, 25MB, 30MB")
    print(f"   - Execution mode: SEQUENTIAL (one request at a time)")
    print(f"   - Total experiments: {len(configs)}")
    
    print(f"📋 Created {len(configs)} sequential experiment configurations")
    
    # Initialize experiment runner
    runner = SequentialVaryInputRunner()
    
    try:
        # Setup environment
        if not runner.setup_environment():
            print("❌ Environment setup failed")
            return 1
        
        # Load workflow
        if not runner.load_workflow("recognizer"):
            print("❌ Workflow loading failed")
            return 1
        
        # Run experiments with override option
        results = runner.run_experiment_suite(configs, override_existing=override_existing)
        
        if not results:
            print("❌ No experiments completed successfully")
            return 1

        # Print summary
        runner.print_summary(results)
        
        print(f"\n✅ Sequential vary-input experiment suite completed!")
        return 0
        
    except KeyboardInterrupt:
        print("\n🛑 Experiment interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Experiment failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
