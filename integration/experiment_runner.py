#!/usr/bin/env python3
"""
Modular Experiment Runner for Scheduler Comparison

This script provides a comprehensive framework for comparing different schedulers
with various input sizes and concurrency levels. It removes all test components
and uses real data and components.

Features:
- Multiple scheduler implementations (ours, FaasPRS, ditto)
- Configurable input sizes and concurrency levels
- Real workflow execution with actual data
- Comprehensive metrics collection
- Modular and extensible design
- Scenario-based testing with specific bottleneck conditions

Usage:
- python experiment_runner.py                    # Standard experiments (scenario-based)
- python experiment_runner.py smoke             # Smoke test (single small experiment for quick verification)
- python experiment_runner.py test              # Test experiments (minimal)
- python experiment_runner.py full              # Full test experiments
- python experiment_runner.py scale             # Scale testing
- python experiment_runner.py standard --override  # Override existing results
- python experiment_runner.py --scenario help   # List available scenario names
- python experiment_runner.py --scenario Baseline-CleanNet
- python experiment_runner.py -s baseline-cleannet,burst-spike
- python experiment_runner.py --split-legacy    # Split old bundled result files into individual files
- python experiment_runner.py --reorganize      # Comprehensive file reorganization (recommended)

Scenarios include (names for --scenario flag):
- Baseline-CleanNet
- Vary-Input
- Vary-Concurrency
- Vary-FiringRate
- Burst-Spike

Each scenario can be replicated multiple times independently for statistical confidence.
By default, replicates=1 (single run per scenario).
"""

import concurrent.futures
import json
import logging
import os
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
import trace
from typing import Any, Dict, List, Optional, Protocol
import yaml
from PIL import Image

from scheduler.shared.minio_config import get_minio_config


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
        # Get the original formatted message
        message = super().format(record)
        
        # Add color if it's an error or critical message
        if record.levelname in ['ERROR', 'CRITICAL']:
            message = f"{self.COLORS.get(record.levelname, '')}{message}{Colors.END}"
        
        return message

# Import orchestrator components
from provider.container_manager import ContainerManager
from provider.function_container_manager import FunctionContainerManager
from scheduler.ours.config import load_config
from scheduler.shared.safe_printing import safe_print_value, is_binary_field
from integration.utils.scheduler_strategy import (
    SchedulerInterface,
    OurScheduler,
    FaasPRSScheduler,
    DittoScheduler,
    PaletteScheduler,
)

from .utils import *

# Custom JSON encoder to handle non-serializable objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        # Handle TimeModel objects from Ditto scheduler
        if hasattr(obj, '__class__') and obj.__class__.__name__ == 'TimeModel':
            return {
                'type': 'TimeModel',
                'observed_times_ms': getattr(obj, 'observed_times_ms', {})
            }
        # Handle other non-serializable objects
        return super().default(obj)

# Configure logging (env or CLI driven)
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

# Create colored formatter
colored_formatter = ColoredFormatter('%(levelname)s: %(message)s')

# Configure logging with colored formatter
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

# Ensure common output directories exist once
os.makedirs("results", exist_ok=True)
os.makedirs("data", exist_ok=True)


class ExperimentRunner:
    """Main experiment runner class"""
    
    def __init__(self):
        self.function_manager = None
        self.worker_manager = None
        self.workflow_dag = None
        self.schedulers = {}
        
    def setup_environment(self) -> bool:
        """Setup the experiment environment"""
        print("🔧 Setting up experiment environment...")
        
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
        logging.getLogger('get_scheduler')
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
    
    def _run_workflow(self, scheduler, input_data: bytes, request_id: str):
        """Run a complete workflow using the orchestrator.
        Returns (function_results: List[FunctionExecutionResult], request_metrics: Dict)
        """
        start_time = time.time()
        
        try:
            # Use the orchestrator's run_workflow method
            result = scheduler.orchestrator.submit_workflow("recognizer", request_id, input_data)
            e2e_time = (time.time() - start_time) * 1000
            print(f"✅ Workflow executed in {e2e_time:.3f} ms")
            
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
                    
                    # Note: Input preparation timing is captured in the DAG executor
                    # and will be included in the timeline data when available
                    
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
                        try:
                            bottleneck_analysis = scheduler.orchestrator.bottleneck_analysis
                            if hasattr(bottleneck_analysis, 'get_bottleneck_summary'):
                                request_metrics['bottleneck_info'] = bottleneck_analysis.get_bottleneck_summary()
                        except Exception:
                            pass
                    
                    # Calculate function execution average
                    if request_metrics['function_exec_ms']:
                        try:
                            request_metrics['function_exec_avg_ms'] = statistics.mean(request_metrics['function_exec_ms'])
                        except Exception as e:
                            print(f"DEBUG: Error calculating mean: {e}")
                            import traceback
                            traceback.print_exc()
                            request_metrics['function_exec_avg_ms'] = 0.0
                    else:
                        # No execution times available from timeline
                        request_metrics['function_exec_avg_ms'] = 0.0
                except Exception as e:
                    print(f"DEBUG: Error processing timeline: {e}")
                    import traceback
                    traceback.print_exc()
            if isinstance(result, dict) and 'functions' in result:
                for func_name, func_result in result['functions'].items():
                    success = func_result.get('status') != 'error' if isinstance(func_result, dict) else True
                    function_results.append(FunctionExecutionResult(
                        function_name=func_name,
                        request_id=f"{request_id}_{func_name}",
                        success=success,
                        execution_time_ms=0.0,  # No individual execution time available
                        node_id=func_result.get('node_id') if isinstance(func_result, dict) else None,
                        container_id=func_result.get('container_id') if isinstance(func_result, dict) else None,
                        error_message=func_result.get('message') if isinstance(func_result, dict) and not success else None
                    ))
            else:
                # Create results only for functions that were actually executed
                timeline = result.get('timeline') if isinstance(result, dict) else None
                
                # Build a map of per-function execution times from timeline derived metrics when available
                exec_time_by_function = {}
                executed_functions = []
                tl_calls_by_func = {}
                
                try:
                    if isinstance(timeline, dict) and 'functions' in timeline:
                        for func_timeline in timeline['functions']:
                            if isinstance(func_timeline, dict) and 'function' in func_timeline:
                                func_name = func_timeline['function']
                                tl_calls_by_func[func_name] = func_timeline
                                derived = func_timeline.get('derived') or {}
                                # Prefer precise exec_ms from server-side timing if present
                                exec_ms = derived.get('exec_ms')
                                if isinstance(exec_ms, (int, float)):
                                    exec_time_by_function[func_name] = float(exec_ms)
                    executed_functions = list(tl_calls_by_func.keys())
                    
                    if executed_functions:
                        # Include all workflow functions; prefer precise timings when available
                        all_funcs = executed_functions or self.extract_function_names()
                        for func_name in all_funcs:
                            # Reflect true function status when available from timeline
                            tl_call = tl_calls_by_func.get(func_name) if isinstance(timeline, dict) else None
                            success = (isinstance(tl_call, dict) and tl_call.get('status', 'ok') != 'error') if tl_call else True
                            err_msg = (tl_call.get('message') or tl_call.get('error') or tl_call.get('error_message')) if (isinstance(tl_call, dict) and tl_call.get('status') == 'error') else None
                            exec_ms_val = exec_time_by_function.get(func_name, 0.0)
                            # Only print debug info if function failed
                            if not success:
                                print(f"DEBUG: Function {func_name} - success: {success}, status: {tl_call.get('status') if tl_call else 'no_timeline'}")
                            function_results.append(FunctionExecutionResult(
                                function_name=func_name,
                                request_id=f"{request_id}_{func_name}",
                                success=success,
                                execution_time_ms=exec_ms_val,
                                error_message=err_msg
                            ))
                    else:
                        # Fallback: create results for all functions in workflow
                        function_names = self.extract_function_names()
                        for func_name in function_names:
                            success = result.get('status') != 'error' if isinstance(result, dict) else True
                            function_results.append(FunctionExecutionResult(
                                function_name=func_name,
                                request_id=f"{request_id}_{func_name}",
                                success=success,
                                execution_time_ms=0.0,  # No individual execution time available
                                error_message=result.get('message') if isinstance(result, dict) and not success else None
                            ))
                except Exception as e:
                    print(f"DEBUG: Error creating function results: {e}")
                    import traceback
                    traceback.print_exc()
            
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
                    execution_time_ms=0.0,  # No individual execution time available
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
    
    def run_experiment(self, config: ExperimentConfig) -> ExperimentResult:
        """Run a single experiment"""
        print(f"\n🧪 Running experiment: {config.experiment_id}")
        print(f"   📊 Input size: {config.input_size_mb}MB")
        print(f"   🔄 Concurrency: {config.concurrency_level}")
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
        
        # Clear MinIO bucket before starting experiment
        self._clear_minio_bucket()
        
        # Load input data
        input_data = self.load_input_data(config)
        
        # Get scheduler
        scheduler = self.get_scheduler(config.scheduler_type)
        
        # Extract function names
        function_names = self.extract_function_names()
        if not function_names:
            raise Exception("No functions found in workflow")
                
        # Execute workflow multiple times with concurrency
        start_time = time.time()
        function_results = []
        per_request_metrics: Dict[str, Any] = {}
        
        # Use a semaphore to control concurrent requests
        import threading
        concurrency_semaphore = threading.Semaphore(config.concurrency_level)
        
        def run_with_concurrency_control(request_id, i):
            """Run a single request with concurrency control and queuing timing"""
            submission_time = time.time()  # Record when request is submitted
            
            with concurrency_semaphore:  # This ensures only concurrency_level requests run simultaneously
                if config.scheduler_type in ["ours", "FaasPRS", "ditto", "palette"]:
                    # Use workflow execution for workflow-aware schedulers
                    result = self._run_workflow(scheduler, input_data, request_id)
                    
                    # Calculate queuing delay (time between submission and execution start)
                    if isinstance(result, tuple):
                        fn_results, req_metrics = result
                        # Add queuing timing to request metrics
                        if 'queuing_delay_ms' not in req_metrics:
                            req_metrics['queuing_delay_ms'] = 0.0
                        # Extract execution start time from timeline if available
                        timeline = req_metrics.get('detailed_timeline', {})
                        if isinstance(timeline, dict) and 'workflow' in timeline:
                            workflow_start = timeline['workflow'].get('start')
                            if workflow_start:
                                queuing_delay = (workflow_start - submission_time) * 1000.0
                                req_metrics['queuing_delay_ms'] = queuing_delay
                        return result
                    else:
                        return result
                else:
                    # Use individual function execution for other schedulers
                    function_name = function_names[i % len(function_names)]
                    result = scheduler.execute_function(function_name, input_data, request_id)
                    return result
        
        # Check if this is a burst-spike scenario and implement burst pattern
        if config.experiment_id.startswith('burst-spike') and config.request_rate_per_sec is not None:
            # Implement burst pattern: baseline rate for most time, then spike rate
            burst_results = self._run_burst_pattern_experiment(
                config, scheduler, input_data, function_names, 
                run_with_concurrency_control, concurrency_semaphore
            )
            function_results, per_request_metrics = burst_results
        else:
            # Original behavior for non-burst scenarios
            with concurrent.futures.ThreadPoolExecutor(max_workers=config.total_requests) as executor:
                # Submit all workflow executions
                future_to_request = {}
                for i in range(config.total_requests):
                    request_id = f"{config.experiment_id}_workflow_{i}"
                    future = executor.submit(run_with_concurrency_control, request_id, i)
                    future_to_request[future] = (request_id, i)
                
                # Collect results
                for future in concurrent.futures.as_completed(future_to_request):
                    request_id, i = future_to_request[future]
                    try:
                        result = future.result()
                        if isinstance(result, tuple):
                            fn_results, req_metrics = result
                            function_results.extend(fn_results)
                            per_request_metrics[req_metrics['request_id']] = req_metrics
                        elif isinstance(result, list):
                            function_results.extend(result)
                        else:
                            function_results.append(result)
                    except Exception as e:
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
        if total_execution_time > 0 and success_rate==100:
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
                'queuing_delay_avg_ms': statistics.mean(v.get('queuing_delay_ms', 0) for v in req_vals),
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
        
        print(f"   ✅ Success: {success_rate*100:.1f}%, Avg time: {avg_execution_time:.3f}ms, Throughput: {throughput:.1f} req/s")
        # Print detailed timing metrics if available
        timing = scheduler_metrics.get('timing') if isinstance(scheduler_metrics, dict) else None
        init_overhead = scheduler_metrics.get('init_overhead') if isinstance(scheduler_metrics, dict) else None
        if timing or init_overhead:
            print("   🕒 Timing Metrics:")
            if init_overhead and isinstance(init_overhead, dict):
                print(f"      - Initialization overhead: {init_overhead.get('duration_ms', 0):.3f} ms")
                # Optionally show per-component breakdown when available (e.g., Ditto)
                try:
                    orchestrator = getattr(scheduler, 'orchestrator', None)
                    if orchestrator and hasattr(orchestrator, '_init_overhead_by_workflow'):
                        comp = (orchestrator._init_overhead_by_workflow or {}).get('recognizer', {}).get('components')
                        if isinstance(comp, dict) and comp:
                            # Sort components by duration desc and print compactly
                            comp_items = sorted(
                                ((k, float(v)) for k, v in comp.items() if isinstance(v, (int, float))),
                                key=lambda x: x[1], reverse=True
                            )
                            line_parts = [f"{k}: {v:.3f} ms" for k, v in comp_items]
                            print(f"      - Init components: {', '.join(line_parts)}")
                except Exception:
                    pass
            if timing:
                print(f"      - End-to-end avg: {timing.get('end_to_end_latency_avg_ms', 0):.3f} ms")
                print(f"      - Scheduling overhead avg: {timing.get('scheduling_overhead_avg_ms', 0):.3f} ms")
                print(f"      - Function exec avg: {timing.get('function_exec_avg_ms', 0):.3f} ms")
                print(f"      - Network delay avg: {timing.get('network_delay_avg_ms', 0):.3f} ms")
                print(f"      - Processing delay avg: {timing.get('processing_delay_avg_ms', 0):.3f} ms")
                print(f"      - Queuing delay avg: {timing.get('queuing_delay_avg_ms', 0):.3f} ms")
                # DEBUG: Detailed component breakdown for first request
                try:
                    first_req = (timing.get('requests') or [None])[0]
                    if isinstance(first_req, dict):
                        tl = first_req.get('detailed_timeline') or {}
                        wf_ts = (tl or {}).get('timestamps', {})
                        # Workflow-level components
                        def _gm(d, keys):
                            for k in keys:
                                v = d.get(k)
                                if isinstance(v, (int, float)):
                                    return float(v)
                            return 0.0
                        # DEBUG workflow components - only show for smoke tests
                        if config.experiment_id.startswith('smoke_test'):
                            print("      - DEBUG workflow components:")
                            print(f"        * cost_model_ms: {_gm(wf_ts, ['cost_model_duration_ms','cost_model_ms']):.3f}")
                            print(f"        * bottleneck_analysis_ms: {_gm(wf_ts, ['bottleneck_analysis_duration_ms','bottleneck_analysis_ms']):.3f}")
                            print(f"        * workflow_orchestration_ms: {_gm(wf_ts, ['workflow_orchestration_duration_ms','workflow_orchestration_ms']):.3f}")
                            print(f"        * workflow_execution_ms: {_gm(wf_ts, ['workflow_execution_ms']):.3f}")
                            # Per-function components
                            funcs = (tl or {}).get('functions', [])
                            if isinstance(funcs, list) and funcs:
                                print("      - DEBUG per-function components:")
                                for call in funcs:
                                    if not isinstance(call, dict):
                                        continue
                                    name = call.get('function') or call.get('id') or 'unknown'
                                    ts = call.get('timestamps', {}) or {}
                                    derived = call.get('derived', {}) or {}
                                    # Read values with fallbacks
                                    lns_ms = _gm(ts, ['lns_duration_ms','lns_ms'])
                                    route_ms = _gm(ts, ['route_duration_ms','route_ms'])
                                    sched_ms = _gm(ts, ['scheduling_overhead_ms']) or (lns_ms + route_ms)
                                    prep_ms = _gm(ts, ['request_preparation_ms'])
                                    disc_ms = _gm(ts, ['container_discovery_ms'])
                                    sel_ms = _gm(ts, ['container_selection_ms']) or route_ms
                                    res_ms = _gm(ts, ['result_processing_ms'])
                                    merge_ms = _gm(ts, ['timeline_merging_ms'])
                                    err_ms = _gm(ts, ['error_handling_ms'])
                                    svr_proc_ms = _gm(ts, ['server_processing_ms'])
                                    exec_ms = _gm(derived, ['exec_ms'])
                                    net_ms = _gm(derived, ['network_delay_ms'])
                                    net_ret_ms = _gm(derived, ['network_return_ms'])
                                    # Print single-line summary
                                    print(
                                        f"        * {name}: exec={exec_ms:.3f}ms, net={net_ms:.3f}ms, net_ret={net_ret_ms:.3f}ms, "
                                        f"sched={sched_ms:.3f}ms (lns={lns_ms:.3f}, route={route_ms:.3f}), prep={prep_ms:.3f}ms, "
                                        f"disc={disc_ms:.3f}ms, select={sel_ms:.3f}ms, res={res_ms:.3f}ms, merge={merge_ms:.3f}ms, "
                                        f"err={err_ms:.3f}ms, server_proc={svr_proc_ms:.3f}ms"
                                    )
                except Exception as _e_dbg:
                    print(f"DEBUG: component breakdown error: {_e_dbg}")
                
                # Report subcomponents if available - only show for smoke tests
                if config.experiment_id.startswith('smoke_test') and req_vals and 'overhead_components' in req_vals[0]:
                    print("      - Processing delay subcomponents:")
                    components = req_vals[0]['overhead_components']
                    for component, avg_ms in components.items():
                        if avg_ms > 0:
                            print(f"        * {component.replace('_ms', '').replace('_', ' ').title()}: {avg_ms:.3f} ms")
        
        
        return result
    
    def _run_burst_pattern_experiment(self, config: ExperimentConfig, scheduler, input_data, function_names, 
                                    run_with_concurrency_control, concurrency_semaphore):
        """
        Run experiment with burst pattern: baseline rate for most time, then spike rate for final 5 seconds.
        
        Pattern: 10 seconds at 2 req/s (20 requests) + 5 seconds at spike_rate req/s (500 requests)
        """
        import threading
        import time
        import concurrent.futures
        
        # Handle request_rate_per_sec as either a list [baseline, spike] or single value
        if isinstance(config.request_rate_per_sec, list):
            baseline_rate, spike_rate = config.request_rate_per_sec
        else:
            # Fallback for backward compatibility
            baseline_rate = 2.0  # Default baseline
            spike_rate = config.request_rate_per_sec
        
        print(f"🌊 Implementing burst pattern:")
        print(f"   📊 Baseline: {baseline_rate} req/s for {config.duration_sec - 5} seconds ({int((config.duration_sec - 5) * baseline_rate)} requests)")
        print(f"   ⚡ Spike: {spike_rate} req/s for 5 seconds ({int(5 * spike_rate)} requests)")
        
        function_results = []
        per_request_metrics = {}
        
        # Calculate burst pattern parameters
        baseline_duration = config.duration_sec - 5  # Most duration for baseline
        spike_duration = 5  # 5 seconds for spike
        
        baseline_requests = int(baseline_duration * baseline_rate)
        spike_requests = int(spike_duration * spike_rate)
        
        print(f"   📈 Total requests: {baseline_requests + spike_requests} ({baseline_requests} baseline + {spike_requests} spike)")
        
        # Use real-time submission with rate limiting instead of pre-scheduling all requests
        function_results = []
        per_request_metrics = {}
        
        # Phase 1: Baseline requests (baseline_rate req/s for baseline_duration seconds)
        print(f"   🟢 Starting baseline phase: {baseline_requests} requests at {baseline_rate} req/s")
        baseline_start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=config.concurrency_level) as executor:
            active_futures = {}
            
            # Submit baseline requests at the correct rate
            for i in range(baseline_requests):
                # Calculate when this request should be submitted
                target_submission_time = baseline_start_time + (i / baseline_rate)
                
                # Wait until it's time to submit this request
                current_time = time.time()
                if current_time < target_submission_time:
                    time.sleep(target_submission_time - current_time)
                
                # Submit the request immediately (no additional delay in the future)
                request_id = f"{config.experiment_id}_baseline_{i}"
                future = executor.submit(run_with_concurrency_control, request_id, i)
                active_futures[future] = (request_id, i, "baseline")
            
            # Phase 2: Spike requests (spike_rate req/s for spike_duration seconds)  
            print(f"   🔴 Starting spike phase: {spike_requests} requests at {spike_rate} req/s")
            spike_start_time = baseline_start_time + baseline_duration
            
            # Wait until spike phase should start
            current_time = time.time()
            if current_time < spike_start_time:
                time.sleep(spike_start_time - current_time)
            
            # Submit spike requests at the correct rate
            for i in range(spike_requests):
                # Calculate when this spike request should be submitted
                target_submission_time = spike_start_time + (i / spike_rate)
                
                # Wait until it's time to submit this request
                current_time = time.time()
                if current_time < target_submission_time:
                    time.sleep(target_submission_time - current_time)
                
                # Submit the spike request immediately
                request_id = f"{config.experiment_id}_spike_{i}"
                future = executor.submit(run_with_concurrency_control, request_id, i + baseline_requests)
                active_futures[future] = (request_id, i + baseline_requests, "spike")
            
            # Collect results as they complete
            print(f"   ⏳ Collecting results from {len(active_futures)} requests...")
            for future in concurrent.futures.as_completed(active_futures):
                request_id, i, phase = active_futures[future]
                try:
                    result = future.result()
                    if isinstance(result, tuple):
                        fn_results, req_metrics = result
                        function_results.extend(fn_results)
                        # Add phase information to metrics
                        req_metrics['burst_phase'] = phase
                        per_request_metrics[req_metrics['request_id']] = req_metrics
                    elif isinstance(result, list):
                        function_results.extend(result)
                    else:
                        function_results.append(result)
                except Exception as e:
                    # Create error result for each function in workflow
                    for function_name in function_names:
                        function_results.append(FunctionExecutionResult(
                            function_name=function_name,
                            request_id=f"{request_id}_{function_name}",
                            success=False,
                            execution_time_ms=0,
                            error_message=str(e)
                        ))
        
        return function_results, per_request_metrics
    
    
    def run_experiment_suite(self, configs: List[ExperimentConfig], override_existing: bool = False) -> List[ExperimentResult]:
        """Run a suite of experiments with optional skipping and per-experiment saving"""
        print("🚀 Starting Experiment Suite")
        print("=" * 50)
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
            print(f"\n[{i}/{len(configs)}] Running experiment: {config.experiment_id}")
            try:
                # Start per-experiment resource sampler with experiment-specific timeseries path
                try:
                    if self.function_manager:
                        # Ensure timeseries directory exists
                        os.makedirs("results/timeseries", exist_ok=True)
                        timeseries_path = f"results/timeseries/{config.experiment_id}_timeseries.csv"
                        self.function_manager.start_timeseries_sampler(
                            interval_s=1.0,  # 1s interval matches parallel stats collection time (~1-2s for 30 containers)
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
                
                result = self.run_experiment(config)
                results.append(result)
                
                # Save per experiment (always save individual files)
                result_files = self.save_results([result], per_scenario=True)  # per_scenario=True means individual files
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
                
                # Per-experiment sampler writes directly to experiment-specific timeseries file
                
            except Exception as e:
                print(f"❌ Experiment {config.experiment_id} failed: {e}")
                import traceback
                traceback.print_exc()
            finally:
                # Stop per-experiment sampler
                try:
                    if self.function_manager:
                        # Give sampler 3 seconds to finish current iteration and write data
                        self.function_manager.stop_timeseries_sampler(timeout_s=3.0)
                except Exception:
                    pass
        
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
                filename = "results/experiment_results.json"
            
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
            result_dict['queuing_delay_avg_ms'] = timing.get('queuing_delay_avg_ms')
    
    def save_timeline_files(self, results: List[ExperimentResult], per_scenario: bool = False) -> List[str]:
        """Save detailed timeline files for each scheduler or per scenario"""
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
                scheduler_timeline_file = f"{timeline_dir}/{scheduler_type}_timelines.json"
                
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
                
                # For FaasPRS, extract additional data
                if scheduler_type == "FaasPRS":
                    if hasattr(orchestrator, '_lp_plan_by_workflow'):
                        timeline_data["lp_plans"] = orchestrator._lp_plan_by_workflow
                    if hasattr(orchestrator, '_placement_by_workflow'):
                        timeline_data["placement_plans"] = orchestrator._placement_by_workflow
                    if hasattr(orchestrator, '_init_overhead_by_workflow'):
                        timeline_data["init_overheads"] = orchestrator._init_overhead_by_workflow
                
                # For ours scheduler, extract additional data
                if scheduler_type == "ours":
                    if hasattr(orchestrator, 'dag_executor'):
                        timeline_data["dag_executor_info"] = {
                            "type": type(orchestrator.dag_executor).__name__,
                            "available_methods": [method for method in dir(orchestrator.dag_executor) if not method.startswith('_')]
                        }
                    
                    # Extract cost model data
                    if hasattr(orchestrator, 'cost_models'):
                        timeline_data["cost_models_info"] = {
                            "type": type(orchestrator.cost_models).__name__,
                            "available_methods": [method for method in dir(orchestrator.cost_models) if not method.startswith('_')]
                        }
                    
                    # Extract bottleneck identifier data
                    if hasattr(orchestrator, 'bottleneck_identifier'):
                        timeline_data["bottleneck_identifier_info"] = {
                            "type": type(orchestrator.bottleneck_identifier).__name__,
                            "available_methods": [method for method in dir(orchestrator.bottleneck_identifier) if not method.startswith('_')]
                        }
                
                # For Palette scheduler, extract additional data
                if scheduler_type == "palette":
                    if hasattr(orchestrator, 'bucket_managers'):
                        timeline_data["bucket_managers_info"] = {
                            "num_buckets": orchestrator.num_buckets,
                            "functions": list(orchestrator.bucket_managers.keys()),
                        }
                    
                    # Extract bucket statistics
                    if hasattr(orchestrator, 'get_bucket_statistics'):
                        try:
                            timeline_data["bucket_statistics"] = orchestrator.get_bucket_statistics()
                        except Exception as e:
                            timeline_data["bucket_statistics_error"] = str(e)
                
                # For Ditto scheduler, extract additional data
                if scheduler_type == "ditto":
                    # Extract predictor data (contains TimeModel objects)
                    if hasattr(orchestrator, 'predictor'):
                        predictor = orchestrator.predictor
                        timeline_data["predictor_info"] = {
                            "type": type(predictor).__name__,
                            "stage_models": {}
                        }
                        # Convert TimeModel objects to serializable format
                        if hasattr(predictor, '_stage_models'):
                            for stage_id, time_model in predictor._stage_models.items():
                                timeline_data["predictor_info"]["stage_models"][stage_id] = {
                                    "type": "TimeModel",
                                    "observed_times_ms": getattr(time_model, 'observed_times_ms', {})
                                }
                    
                    # Extract other Ditto components
                    if hasattr(orchestrator, 'grouper'):
                        timeline_data["grouper_info"] = {
                            "type": type(orchestrator.grouper).__name__,
                            "shuffle_threshold_bytes": getattr(orchestrator.grouper, 'shuffle_threshold_bytes', None)
                        }
                    
                    if hasattr(orchestrator, 'dop_computer'):
                        timeline_data["dop_computer_info"] = {
                            "type": type(orchestrator.dop_computer).__name__
                        }
                    
                    if hasattr(orchestrator, 'placement'):
                        timeline_data["placement_info"] = {
                            "type": type(orchestrator.placement).__name__
                        }
                    
                    if hasattr(orchestrator, 'plan_evaluator'):
                        timeline_data["plan_evaluator_info"] = {
                            "type": type(orchestrator.plan_evaluator).__name__
                        }
                
        except Exception as e:
            timeline_data["extraction_error"] = str(e)
            print(f"Warning: Could not extract timeline data from {scheduler_type} scheduler: {e}")
        
        return timeline_data
    
    def _clear_minio_bucket(self) -> None:
        """Clear MinIO bucket before starting new experiment"""
        try:
            import boto3
            from botocore.config import Config
            
            # Pick up whichever MinIO endpoint matches the current environment
            minio_cfg = get_minio_config()
            minio_host = minio_cfg["host"]
            minio_port = minio_cfg["port"]
            access_key = minio_cfg["access_key"]
            secret_key = minio_cfg["secret_key"]
            bucket_name = minio_cfg["bucket_name"]
            
            # Create S3 client for MinIO
            s3_client = boto3.client(
                's3',
                endpoint_url=f'http://{minio_host}:{minio_port}',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name='us-east-1',
                config=Config(signature_version='s3v4'),
                use_ssl=False
            )
            
            # List and delete all objects in bucket
            paginator = s3_client.get_paginator('list_objects_v2')
            page_count = 0
            object_count = 0
            
            for page in paginator.paginate(Bucket=bucket_name):
                if 'Contents' in page:
                    objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                    if objects_to_delete:
                        s3_client.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': objects_to_delete}
                        )
                        object_count += len(objects_to_delete)
                    page_count += 1
            
            if object_count > 0:
                print(f"🧹 Cleared MinIO bucket '{bucket_name}': removed {object_count} objects")
            else:
                print(f"🧹 MinIO bucket '{bucket_name}' was already empty")
                
        except Exception as e:
            print(f"⚠️  Failed to clear MinIO bucket: {e}")
    
    def print_summary(self, results: List[ExperimentResult]):
        """Print experiment summary"""
        print("\n📊 Experiment Summary")
        print("=" * 30)
        
        # Group by scheduler type
        by_scheduler = {}
        for result in results:
            scheduler_type = result.config.scheduler_type
            if scheduler_type not in by_scheduler:
                by_scheduler[scheduler_type] = []
            by_scheduler[scheduler_type].append(result)
        
        for scheduler_type, scheduler_results in by_scheduler.items():
            print(f"\n🔧 {scheduler_type.upper()} Scheduler:")
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
                          f"processing_avg: {t.get('processing_delay_avg_ms', 0):.3f}ms, "
                          f"queuing_avg: {t.get('queuing_delay_avg_ms', 0):.3f}ms")
                    # Optional compact subcomponents breakdown (averaged across requests)
                    try:
                        reqs = t.get('requests') if isinstance(t, dict) else None
                        if isinstance(reqs, list) and reqs:
                            # Discover component keys from first request
                            first_components = (reqs[0] or {}).get('overhead_components', {})
                            if isinstance(first_components, dict) and first_components:
                                comp_totals = {k: 0.0 for k in first_components.keys()}
                                count = float(len(reqs))
                                for rv in reqs:
                                    comps = (rv or {}).get('overhead_components', {})
                                    if isinstance(comps, dict):
                                        for k in comp_totals.keys():
                                            v = comps.get(k, 0.0)
                                            if isinstance(v, (int, float)):
                                                comp_totals[k] += float(v)
                                # Compute averages
                                comp_avgs = {k: (comp_totals[k] / count) for k in comp_totals.keys()}
                                # Split into processing vs scheduling related components
                                processing_keys = [
                                    'timeline_setup_ms',
                                    'request_preparation_ms',
                                    'result_processing_ms',
                                    'timeline_merging_ms',
                                ]
                                scheduling_keys = [
                                    'container_discovery_ms',
                                    'container_selection_ms',
                                ]
                                # Build compact strings
                                proc_items = [(k, comp_avgs.get(k, 0.0)) for k in processing_keys if comp_avgs.get(k, 0.0) > 0.0]
                                sched_items = [(k, comp_avgs.get(k, 0.0)) for k in scheduling_keys if comp_avgs.get(k, 0.0) > 0.0]
                                if proc_items:
                                    proc_items.sort(key=lambda x: x[1], reverse=True)
                                    proc_parts = [f"{k.replace('_ms','').replace('_',' ')}: {v:.3f}ms" for k, v in proc_items]
                                    print(f"      processing components → {', '.join(proc_parts)}")
                                if sched_items:
                                    sched_items.sort(key=lambda x: x[1], reverse=True)
                                    sched_parts = [f"{k.replace('_ms','').replace('_',' ')}: {v:.3f}ms" for k, v in sched_items]
                                    print(f"      scheduling components → {', '.join(sched_parts)}")
                    except Exception:
                        pass
                

def create_experiment_configs(
    input_sizes: List[float] = [0.1, 1.0],
    concurrency_levels: List[int] = [1, 5],
    schedulers: List[str] = ["ours"],
    requests_per_experiment: int = 7
) -> List[ExperimentConfig]:
    """Create experiment configurations"""
    configs = []
    
    for scheduler in schedulers:
        for input_size in input_sizes:
            for concurrency in concurrency_levels:
                config = ExperimentConfig(
                    experiment_id=f"exp_input{input_size}mb_conc{concurrency}_{scheduler}",
                    input_size_mb=input_size,
                    concurrency_level=concurrency,
                    total_requests=requests_per_experiment,
                    scheduler_type=scheduler,
                    workflow_name="recognizer",
                    input_file="data/test.png"  # Use actual test image
                )
                configs.append(config)
    
    return configs


def create_scale_testing_configs(
    schedulers: List[str] = ["ours"],
    requests_per_experiment: int = 7
) -> List[ExperimentConfig]:
    """Create scale testing configurations with large inputs and high concurrency"""
    configs = []
    
    # Large input sizes for scale testing
    large_input_sizes = [5.0, 10.0]
    
    # High concurrency levels for scale testing
    high_concurrency_levels = [10, 20, 50]
    
    # Standard configurations for comparison
    standard_input_sizes = [0.1, 1.0]
    standard_concurrency_levels = [1, 5]
    
    for scheduler in schedulers:
        # Add standard configurations
        for input_size in standard_input_sizes:
            for concurrency in standard_concurrency_levels:
                config = ExperimentConfig(
                    experiment_id=f"standard_input{input_size}mb_conc{concurrency}_{scheduler}",
                    input_size_mb=input_size,
                    concurrency_level=concurrency,
                    total_requests=requests_per_experiment,
                    scheduler_type=scheduler,
                    workflow_name="recognizer",
                    input_file="data/test.png"  # Use actual test image
                )
                configs.append(config)
        
        # Add large input size tests
        for input_size in large_input_sizes:
            for concurrency in standard_concurrency_levels:
                config = ExperimentConfig(
                    experiment_id=f"large_input{input_size}mb_conc{concurrency}_{scheduler}",
                    input_size_mb=input_size,
                    concurrency_level=concurrency,
                    total_requests=requests_per_experiment,
                    scheduler_type=scheduler,
                    workflow_name="recognizer",
                    input_file=f"data/test_{int(input_size)}mb.png"  # Use generated large images
                )
                configs.append(config)
        
        # Add high concurrency tests
        for input_size in standard_input_sizes:
            for concurrency in high_concurrency_levels:
                config = ExperimentConfig(
                    experiment_id=f"high_conc_input{input_size}mb_conc{concurrency}_{scheduler}",
                    input_size_mb=input_size,
                    concurrency_level=concurrency,
                    total_requests=requests_per_experiment,
                    scheduler_type=scheduler,
                    workflow_name="recognizer",
                    input_file="data/test.png"  # Use actual test image
                )
                configs.append(config)
        
        # Add extreme scale tests (large inputs + high concurrency)
        for input_size in large_input_sizes:
            for concurrency in high_concurrency_levels:
                config = ExperimentConfig(
                    experiment_id=f"extreme_input{input_size}mb_conc{concurrency}_{scheduler}",
                    input_size_mb=input_size,
                    concurrency_level=concurrency,
                    total_requests=requests_per_experiment,
                    scheduler_type=scheduler,
                    workflow_name="recognizer",
                    input_file=f"data/test_{int(input_size)}mb.png"  # Use generated large images
                )
                configs.append(config)
    
    return configs


def run_scale_testing():
    """Run scale testing with large inputs and high concurrency"""
    print("🚀 Running SCALE TESTING experiments")
    
    # Create large test images if they don't exist
    create_large_test_images()
    
    # Create scale testing configurations
    configs = create_scale_testing_configs(
        schedulers=["ours"],
        requests_per_experiment=7
    )
    
    print(f"   - Large input sizes: 5MB, 10MB, 20MB, 50MB")
    print(f"   - High concurrency: 10, 20, 50")
    print(f"   - Extreme combinations")
    print(f"   - Total experiments: {len(configs)}")
    
    return configs

def create_test_image(size_bytes: int, filename: str):
    """Create a proper PNG test image of specified size that can be loaded and resized"""
    if not os.path.exists(filename):
        try:
            base_image_path = 'data/test.png'
            
            # Decide strategy based on target size
            if os.path.exists(base_image_path):
                base_size = os.path.getsize(base_image_path)
                
                if size_bytes < base_size * 0.5:
                    # For images smaller than half the base size, resize/downsample
                    _create_downsampled_test_image(base_image_path, filename, size_bytes)
                else:
                    # For images larger than base, tile it
                    _create_tiled_test_image(base_image_path, filename, size_bytes)
            else:
                # Create from scratch if base image doesn't exist
                _create_gradient_test_image(filename, size_bytes)
    
        except Exception as e:
            print(f"⚠️  Error creating test image {filename}: {e}")
            # Last resort: create a simple gradient image
            _create_gradient_test_image(filename, size_bytes)

def _create_downsampled_test_image(base_image_path: str, output_path: str, target_size_bytes: int):
    """Create a smaller test image by downsampling the base image"""
    import math
    
    # Load base image
    base_img = Image.open(base_image_path)
    base_width, base_height = base_img.size
    base_size_bytes = os.path.getsize(base_image_path)
    
    # Estimate the scale factor needed
    # PNG compression is variable, so we estimate
    size_ratio = target_size_bytes / base_size_bytes
    scale_factor = math.sqrt(size_ratio)  # Scale both dimensions
    
    # Calculate new dimensions (minimum 224x224 for model compatibility)
    new_width = max(224, int(base_width * scale_factor))
    new_height = max(224, int(base_height * scale_factor))
    
    # Resize the image
    resized_img = base_img.resize((new_width, new_height), Image.Resampling.LANCZOS)
    
    # Save with different compression levels to try to hit target size
    # Start with high quality and reduce if needed
    for quality in [95, 85, 75, 65]:
        resized_img.save(output_path, 'PNG', optimize=True)
        actual_size = os.path.getsize(output_path)
        
        # If we're close enough to target (within 50%), accept it
        if actual_size <= target_size_bytes * 1.5:
            break
        
        # If still too large, try reducing dimensions further
        if actual_size > target_size_bytes * 2:
            new_width = int(new_width * 0.8)
            new_height = int(new_height * 0.8)
            new_width = max(224, new_width)
            new_height = max(224, new_height)
            resized_img = base_img.resize((new_width, new_height), Image.Resampling.LANCZOS)
    
    actual_size = os.path.getsize(output_path)
    size_kb = actual_size / 1024
    size_mb = actual_size / (1024 * 1024)
    
    if size_mb >= 1:
        print(f"✅ Created downsampled test image: {output_path} ({size_mb:.2f} MB, {new_width}x{new_height})")
    else:
        print(f"✅ Created downsampled test image: {output_path} ({size_kb:.1f} KB, {new_width}x{new_height})")

def _create_gradient_test_image(filename: str, target_size_bytes: int):
    """Create a gradient test image from scratch"""
    # Estimate dimensions based on target size
    # PNG compression varies, so we estimate conservatively
    import math
    estimated_pixels = target_size_bytes * 2  # Conservative estimate
    side_length = int(math.sqrt(estimated_pixels / 3))  # 3 bytes per RGB pixel
    side_length = max(224, side_length)  # At least 224x224 for model compatibility
    
    # Create gradient image
    image = Image.new('RGB', (side_length, side_length), color='white')
    pixels = image.load()
    
    for i in range(side_length):
        for j in range(side_length):
            r = (i * 255) // side_length
            g = (j * 255) // side_length
            b = ((i + j) * 255) // (side_length + side_length)
            pixels[i, j] = (r, g, b)
    
    image.save(filename, 'PNG', optimize=True)
    actual_size = os.path.getsize(filename)
    size_mb = actual_size / (1024 * 1024)
    print(f"✅ Created gradient test image: {filename} ({size_mb:.2f} MB, {side_length}x{side_length})")

def _create_tiled_test_image(base_image_path: str, output_path: str, target_size_bytes: int):
    """Create a larger test image by tiling the base image"""
    import math
    
    # Load base image
    base_img = Image.open(base_image_path)
    base_width, base_height = base_img.size
    base_size_bytes = os.path.getsize(base_image_path)
    
    # Use binary search to find the right number of tiles
    # Start with a rough estimate based on linear scaling
    min_copies = max(1, int((target_size_bytes * 0.5) / base_size_bytes))
    max_copies = max(1, int((target_size_bytes * 3) / base_size_bytes))
    
    best_output_path = None
    best_size_diff = float('inf')
    
    # Try a few different tile counts to get close to target
    for estimated_copies in [min_copies, (min_copies + max_copies) // 2, max_copies]:
        # Calculate grid dimensions for roughly square layout
        grid_size = int(math.ceil(math.sqrt(estimated_copies)))
        
        # Create output image
        padding = 20  # Small padding between tiles
        output_width = grid_size * base_width + (grid_size - 1) * padding
        output_height = grid_size * base_height + (grid_size - 1) * padding
        
        output_img = Image.new('RGB', (output_width, output_height), 'white')
        
        # Tile the base image
        for i in range(estimated_copies):
            row = i // grid_size
            col = i % grid_size
            x = col * (base_width + padding)
            y = row * (base_height + padding)
            if x < output_width and y < output_height:
                output_img.paste(base_img, (x, y))
        
        # Save with optimization
        temp_path = output_path + f".temp{estimated_copies}"
        output_img.save(temp_path, 'PNG', optimize=True)
        
        actual_size = os.path.getsize(temp_path)
        size_diff = abs(actual_size - target_size_bytes)
        
        # Keep the version closest to target
        if size_diff < best_size_diff:
            best_size_diff = size_diff
            if best_output_path and os.path.exists(best_output_path):
                os.remove(best_output_path)
            best_output_path = temp_path
        else:
            os.remove(temp_path)
        
        # If we're within 20% of target, that's good enough
        if actual_size >= target_size_bytes * 0.8 and actual_size <= target_size_bytes * 1.2:
            break
    
    # Rename the best version to final output
    if best_output_path:
        if os.path.exists(output_path):
            os.remove(output_path)
        os.rename(best_output_path, output_path)
    
    actual_size = os.path.getsize(output_path)
    size_mb = actual_size / (1024 * 1024)
    print(f"✅ Created tiled test image: {output_path} ({size_mb:.2f} MB, {output_width}x{output_height})")

def create_large_test_images():
    """Create large test images for scale testing"""
    
    print("🖼️  Creating proper PNG test images for scale testing...")
    
    # Create 5MB test image
    create_test_image(5 * 1024 * 1024, "data/test_5mb.png")
    
    # Create 10MB test image
    create_test_image(10 * 1024 * 1024, "data/test_10mb.png")
    
    print("✅ Scale test images created successfully")

def create_scenario_test_images():
    """Create test images for scenario testing with specific sizes"""
    
    print("🖼️  Creating proper PNG test images...")
    
    # Create test images for all scenario sizes
    create_test_image(10 * 1024, "data/test_10kb.png")          # 10KB for Small-Msgs
    create_test_image(100 * 1024, "data/test_100kb.png")        # 100KB for Small-Msgs  
    create_test_image(200 * 1024, "data/test_200kb.png")          # 200KB for Small-Msgs
    create_test_image(500 * 1024, "data/test_500kb.png")         # 500KB for Small-Msgs
    create_test_image(1 * 1024 * 1024, "data/test_1mb.png")    # 1MB for Small-Msgs, Vary-Input
    create_test_image(5 * 1024 * 1024, "data/test_5mb.png")    # 5MB for baseline scenarios, Vary-Input
    create_test_image(10 * 1024 * 1024, "data/test_10mb.png")  # 10MB for Vary-Input
    create_test_image(20 * 1024 * 1024, "data/test_20mb.png")  # 20MB for Burst-Spike, Vary-Input
    create_test_image(50 * 1024 * 1024, "data/test_50mb.png")  # 50MB for Vary-Input
    
    print("✅ All test images created successfully")


def create_smoke_test_configs(
    schedulers: List[str] = ["ours"]
) -> List[ExperimentConfig]:
    """Create a single, small configuration for smoke testing to verify everything works"""
    configs = []
    
    # Single small scenario for smoke testing
    smoke_scenario = {
        "name": "Smoke-Test",
        "input_size_mb": 0.01,  # 10 KB - very small
        "concurrency": 5,
        "request_rate_per_sec": 1.0,
        "bandwidth_cap_mbps": 1000.0,  # No cap
        "injected_latency_ms": 1.0,
        "duration_sec": 10.0,  # Very short duration
        "replicates": 1,
        "refinement_time_cap_sec": 0.02,
        "expected_bottleneck": "None (smoke test)"
    }
    
    for scheduler in schedulers:
        # Calculate total requests based on duration and request rate
        total_requests = int(smoke_scenario["duration_sec"] * smoke_scenario["request_rate_per_sec"])
        
        # Use existing test image for speed
        input_file = "data/test.png"
        
        config = ExperimentConfig(
            experiment_id=f"smoke_test_{scheduler}",
            input_size_mb=smoke_scenario["input_size_mb"],
            concurrency_level=smoke_scenario["concurrency"],
            total_requests=total_requests,
            scheduler_type=scheduler,
            workflow_name="recognizer",
            input_file=input_file,
            # New scenario parameters
            request_rate_per_sec=smoke_scenario["request_rate_per_sec"],
            bandwidth_cap_mbps=smoke_scenario["bandwidth_cap_mbps"],
            injected_latency_ms=smoke_scenario["injected_latency_ms"],
            duration_sec=smoke_scenario["duration_sec"],
            replicates=smoke_scenario["replicates"],
            refinement_time_cap_sec=smoke_scenario["refinement_time_cap_sec"],
            expected_bottleneck=smoke_scenario["expected_bottleneck"]
        )
        configs.append(config)
    
    return configs


def create_scenario_configs(
    schedulers: List[str] = ["ours", "FaasPRS", "ditto", "palette"],
    scenario_names: List[str] | None = None,
) -> List[ExperimentConfig]:
    """Create experiment configurations for the 5 specific scenarios"""
    configs = []
    
    # Scenario definitions - each varies only ONE variable while others are fixed
    # Fixed baseline values: input_size=5MB, concurrency=1, request_rate=1 req/s, duration=120s
    scenarios = [
        {
            "name": "Baseline-CleanNet",
            "input_size_mb": 5,  # Fixed baseline
            "concurrency": 1,      # Fixed baseline
            "request_rate_per_sec": 1,  # Fixed baseline
            "bandwidth_cap_mbps": 1000,  # No cap (very high)
            "injected_latency_ms": 1,
            "duration_sec": 5,  # Fixed baseline
            "replicates": 1,
            "refinement_time_cap_sec": 0.02,
            "expected_bottleneck": "None (balanced)"
        },
        # Temporarily disabled due to BytesIO issues
        {
            "name": "Vary-Input",
            "input_size_mb": [5, 10, 15, 20],  # VARIES: 5MB to 50MB
            "concurrency": 1,      # Fixed baseline
            "request_rate_per_sec": 1,  # Fixed baseline
            "bandwidth_cap_mbps": 1000,  # No cap
            "injected_latency_ms": 1,
            "duration_sec": 10,  # Fixed baseline
            "replicates": 1,
            "refinement_time_cap_sec": 0.02,
            "expected_bottleneck": "Network (bandwidth/transfer)"
        },
        # {
        #     "name": "Vary-Concurrency",
        #     "input_size_mb": 5,  # Fixed baseline
        #     "concurrency": [1, 5, 10, 20], #, 50],  # VARIES: 1, 20, 100
        #     "request_rate_per_sec": 1,  # Fixed baseline
        #     "bandwidth_cap_mbps": 1000,  # No cap
        #     "injected_latency_ms": 1,
        #     "duration_sec": 10,  # Extended for higher concurrency
        #     "replicates": 1,
        #     "refinement_time_cap_sec": 0.02,
        #     "expected_bottleneck": "Compute / contention (queues)"
        # },
        {
            "name": "Vary-FiringRate",
            "input_size_mb": 5,  # Fixed baseline
            "concurrency": 5,      # Fixed moderate level
            "request_rate_per_sec": [5, 10, ], #20], # 30], # 100, 200],  # VARIES: 1, 50, 200 req/s
            "bandwidth_cap_mbps": 1000,  # No cap
            "injected_latency_ms": 1,
            "duration_sec": 5,  # Extended for higher rates
            "replicates": 1,
            "refinement_time_cap_sec": 0.02,
            "expected_bottleneck": "Queueing / compute contention"
        },
        # {
        #     "name": "Small-Msgs",
        #     "input_size_mb": [0.01, 0.05, 0.1, 0.5, 1],  # VARIES: 10KB, 100KB, 1MB
        #     "concurrency": 50,     # Fixed high level
        #     "request_rate_per_sec": 100,  # Fixed high rate
        #     "bandwidth_cap_mbps": 1000,  # No cap
        #     "injected_latency_ms": 1,
        #     "duration_sec": 10,  # Fixed duration
        #     "replicates": 1,
        #     "refinement_time_cap_sec": 0.02,
        #     "expected_bottleneck": "Network (latency)"
        # },
        # {
        #     "name": "Burst-Spike",
        #     "input_size_mb": 2,  # Moderate size for better testing
        #     "concurrency": [5, 10, 20],  # VARIES: baseline 5 → spike 20
        #     "request_rate_per_sec": [2, 20],  # VARIES: baseline 2 → spike 50 req/s
        #     "bandwidth_cap_mbps": 1000,  # No cap
        #     "injected_latency_ms": 1,
        #     "duration_sec": 15,  # Fixed duration
        #     "replicates": 1,
        #     "refinement_time_cap_sec": 0.02,
        #     "expected_bottleneck": "Queueing / transient network"
        # }
    ]
    # Optional filtering by scenario name(s)
    if scenario_names:
        def _norm(name: str) -> str:
            return str(name).strip().lower().replace(" ", "-")
        wanted = {_norm(s) for s in scenario_names}
        scenarios = [s for s in scenarios if _norm(s["name"]) in wanted or s["name"] in scenario_names]

    for scheduler in schedulers:
        for scenario in scenarios:
            # Handle scenarios with varying parameters (lists) vs fixed parameters (single values)
            varying_params = {}
            fixed_params = {}
            
            # Identify which parameters vary and which are fixed
            for param_name, param_value in scenario.items():
                if isinstance(param_value, list):
                    # Special case for burst-spike: request_rate_per_sec list represents [baseline, spike] rates
                    if scenario["name"] == "Burst-Spike" and param_name == "request_rate_per_sec":
                        fixed_params[param_name] = param_value  # Keep as list for burst pattern
                    else:
                        varying_params[param_name] = param_value
                else:
                    fixed_params[param_name] = param_value
            
            # If no varying parameters, create single experiment
            if not varying_params:
                # Single experiment with fixed parameters
                _create_single_experiment_config(scenario, scheduler, configs)
            else:
                # Create multiple experiments for each combination of varying parameters
                _create_varying_experiment_configs(scenario, scheduler, configs, varying_params, fixed_params)
    
    return configs


def _create_single_experiment_config(scenario: dict, scheduler: str, configs: list):
    """Create a single experiment configuration for scenarios with fixed parameters"""
    # For burst scenario, handle rate as [baseline, spike] pair
    if scenario["name"] == "Burst-Spike" and isinstance(scenario["request_rate_per_sec"], list):
        baseline_rate, spike_rate = scenario["request_rate_per_sec"]
        baseline_requests = int((scenario["duration_sec"] - 5) * baseline_rate)  # baseline for most time
        spike_requests = int(5 * spike_rate)  # spike for final 5s
        total_requests = baseline_requests + spike_requests
    else:
        # Calculate total requests based on duration and request rate
        total_requests = int(scenario["duration_sec"] * scenario["request_rate_per_sec"])
    
    # Determine input file based on size
    input_file = _get_input_file_for_size(scenario["input_size_mb"])
    
    # Generate multiple independent experiment configurations based on replicates
    replicates = scenario.get("replicates", 1)
    for replicate_num in range(1, replicates + 1):
        # Create unique experiment ID for each replicate
        if replicates > 1:
            experiment_id = f"{scenario['name'].replace(' ', '_').replace('(', '').replace(')', '').lower()}_{scheduler}_rep{replicate_num}"
        else:
            experiment_id = f"{scenario['name'].replace(' ', '_').replace('(', '').replace(')', '').lower()}_{scheduler}"
        
        # Handle request_rate_per_sec for Burst-Spike scenario (convert list to representative value)
        if scenario["name"] == "Burst-Spike" and isinstance(scenario["request_rate_per_sec"], list):
            # Use the spike rate as the representative rate for the experiment config
            representative_rate = max(scenario["request_rate_per_sec"])
        else:
            representative_rate = scenario["request_rate_per_sec"]
        
        config = ExperimentConfig(
            experiment_id=experiment_id,
            input_size_mb=scenario["input_size_mb"],
            concurrency_level=scenario["concurrency"],
            total_requests=total_requests,
            scheduler_type=scheduler,
            workflow_name="recognizer",
            input_file=input_file,
            # New scenario parameters
            request_rate_per_sec=representative_rate,
            bandwidth_cap_mbps=scenario["bandwidth_cap_mbps"],
            injected_latency_ms=scenario["injected_latency_ms"],
            duration_sec=scenario["duration_sec"],
            replicates=replicates,
            refinement_time_cap_sec=scenario["refinement_time_cap_sec"],
            expected_bottleneck=scenario["expected_bottleneck"]
        )
        configs.append(config)


def _create_varying_experiment_configs(scenario: dict, scheduler: str, configs: list, varying_params: dict, fixed_params: dict):
    """Create multiple experiment configurations for scenarios with varying parameters"""
    import itertools
    
    # Get all combinations of varying parameters
    param_names = list(varying_params.keys())
    param_values = list(varying_params.values())
    
    # Generate all combinations
    for combination in itertools.product(*param_values):
        # Create a scenario dict with this combination
        current_scenario = fixed_params.copy()
        for i, param_name in enumerate(param_names):
            current_scenario[param_name] = combination[i]
        
        # Calculate total requests based on duration and request rate
        # Handle special case for Burst-Spike where request_rate_per_sec is a list [baseline, spike]
        if scenario["name"] == "Burst-Spike" and isinstance(current_scenario["request_rate_per_sec"], list):
            # For burst scenario, use a more realistic total requests calculation
            baseline_rate, spike_rate = current_scenario["request_rate_per_sec"]
            baseline_requests = int((current_scenario["duration_sec"] - 5) * baseline_rate)
            spike_requests = int(5 * spike_rate)
            total_requests = baseline_requests + spike_requests
        else:
            # Normal calculation for other scenarios
            total_requests = int(current_scenario["duration_sec"] * current_scenario["request_rate_per_sec"])
        
        # Determine input file based on size
        input_file = _get_input_file_for_size(current_scenario["input_size_mb"])
        
        # Create experiment ID with parameter values
        param_suffix = "_".join([f"{param_names[i]}{combination[i]}" for i in range(len(combination))])
        experiment_id = f"{scenario['name'].replace(' ', '_').replace('(', '').replace(')', '').lower()}_{scheduler}_{param_suffix}"
        
        # Handle request_rate_per_sec for Burst-Spike scenario (convert list to representative value)
        if scenario["name"] == "Burst-Spike" and isinstance(current_scenario["request_rate_per_sec"], list):
            # Use the spike rate as the representative rate for the experiment config
            representative_rate = max(current_scenario["request_rate_per_sec"])
        else:
            representative_rate = current_scenario["request_rate_per_sec"]
        
        config = ExperimentConfig(
            experiment_id=experiment_id,
            input_size_mb=current_scenario["input_size_mb"],
            concurrency_level=current_scenario["concurrency"],
            total_requests=total_requests,
            scheduler_type=scheduler,
            workflow_name="recognizer",
            input_file=input_file,
            # New scenario parameters
            request_rate_per_sec=representative_rate,
            bandwidth_cap_mbps=current_scenario["bandwidth_cap_mbps"],
            injected_latency_ms=current_scenario["injected_latency_ms"],
            duration_sec=current_scenario["duration_sec"],
            replicates=current_scenario.get("replicates", 1),
            refinement_time_cap_sec=current_scenario["refinement_time_cap_sec"],
            expected_bottleneck=current_scenario["expected_bottleneck"]
        )
        configs.append(config)


def _get_input_file_for_size(size_mb: float) -> str:
    """Determine input file based on size"""
    if size_mb >= 50:
        return "data/test_50mb.png"
    elif size_mb >= 20:
        return "data/test_20mb.png"
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


def main():
    """Main entry point"""
    import sys
    
    print("🎭 Modular Experiment Runner")
    print("=" * 40)
    
    # Parse command line arguments
    test_mode = "standard"
    override_existing = False
    scenario_filter: List[str] | None = None
    
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg.lower() in ["smoke", "test", "scale", "standard"]:
            test_mode = arg.lower()
        elif arg == "--override" or arg == "-f":
            override_existing = True
        elif arg.startswith("--scenario="):
            value = arg.split("=", 1)[1]
            if value.strip().lower() == "help":
                print("Available scenarios (use exact or hyphenated lowercase):")
                print("  - Baseline-CleanNet")
                print("  - Vary-Input   (temporarily disabled)")
                print("  - Vary-Concurrency")
                print("  - Vary-FiringRate")
                print("  - Burst-Spike")
                return 0
            scenario_filter = [s.strip() for s in value.split(",") if s.strip()]
        elif arg in ["--scenario", "-s"] and i + 1 < len(sys.argv):
            value = sys.argv[i + 1]
            if value.strip().lower() == "help":
                print("Available scenarios (use exact or hyphenated lowercase):")
                print("  - Baseline-CleanNet")
                print("  - Vary-Input   (temporarily disabled)")
                print("  - Vary-Concurrency")
                print("  - Vary-FiringRate")
                print("  - Burst-Spike")
                return 0
            scenario_filter = [s.strip() for s in value.split(",") if s.strip()]
        elif arg.startswith("--log-level="):
            # Handle log level (already handled by logging setup)
            pass
        elif arg in ["--log-level", "-l"] and i + 1 < len(sys.argv):
            # Handle log level with space (already handled by logging setup)
            pass
    
    # Handle legacy file splitting (removed - no longer needed)
    
    if test_mode == "scale":
        configs = run_scale_testing()
    elif test_mode == "smoke":
        print("🧪 Running SMOKE TEST (quick verification)")
        # Use existing test.png for smoke test to avoid slow image creation
        configs = create_smoke_test_configs(
            schedulers=["ours"]  # Only test one scheduler for speed
        )
        print(f"   - Single small experiment to verify everything works")
        print(f"   - Input size: 0.01 MB (10 KB)")
        print(f"   - Duration: 10 seconds")
        print(f"   - Requests: 10")
        print(f"   - Total experiments: {len(configs)}")
    elif test_mode == "test":
        print("🧪 Running TEST experiments")
        input_sizes = [1] # [1, 2, 5, 10, 20]
        concurrency_levels = [1]
        configs = create_experiment_configs(
            input_sizes=input_sizes,
            concurrency_levels=concurrency_levels,
            schedulers=["ours", "FaasPRS", "ditto", "palette"], # "ours", "FaasPRS", "ditto"
            requests_per_experiment=10
        )
        print(f"   - Input sizes (MB): {input_sizes}")
        print(f"   - Concurrency: {concurrency_levels}")
        print(f"   - Requests per experiment: 10")
        print(f"   - Total experiments: {len(configs)}")
    else:  # standard mode - now scenarios
        print("🧪 Running STANDARD experiments (scenario-based)")
        if override_existing:
            print("   🔄 Override mode: Will rerun experiments even if results exist")
        else:
            print("   ⏭️  Skip mode: Will skip experiments that already have results")
        print("   📁 Per-scenario saving: Each scenario saves to separate files")
        # Create test images for scenarios
        create_scenario_test_images()
        configs = create_scenario_configs(
            schedulers=["ours", "FaasPRS"], #"palette", ], #"ditto"],
            scenario_names=scenario_filter,
        )
        if scenario_filter:
            print(f"   - Scenarios (filtered): {', '.join(scenario_filter)}")
        else:
            # print(f"   - Scenarios: Baseline-CleanNet, Vary-Input, Vary-Concurrency, Vary-FiringRate, Small-Msgs, Burst-Spike")
            print(f"   - Scenarios: Baseline-CleanNet, Vary-Concurrency, Vary-FiringRate, Small-Msgs, Burst-Spike (Vary-Input temporarily disabled)")
        print(f"   - Schedulers: ours, FaasPRS, palette")
        print(f"   - Total experiments: {len(configs)}")
    
    print(f"📋 Created {len(configs)} experiment configurations")
    
    # Initialize experiment runner
    runner = ExperimentRunner()
    
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
        
        print(f"\n✅ Experiment suite completed!")
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
