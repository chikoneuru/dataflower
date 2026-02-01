from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class ExperimentConfig:
    """Configuration for a single experiment"""
    experiment_id: str
    input_size_mb: float
    concurrency_level: int
    total_requests: int
    scheduler_type: str
    workflow_name: str = "recognizer"
    input_file: Optional[str] = None
    # New scenario parameters
    request_rate_per_sec: Optional[float] = None  # Request firing rate (req/s)
    bandwidth_cap_mbps: Optional[float] = None    # Bandwidth cap (Mbps)
    injected_latency_ms: Optional[float] = None   # Injected latency (ms)
    duration_sec: Optional[float] = None          # Duration (s)
    replicates: Optional[int] = None              # Number of replicates
    refinement_time_cap_sec: Optional[float] = None  # Refinement time cap (s)
    expected_bottleneck: Optional[str] = None     # Expected bottleneck type


@dataclass
class FunctionExecutionResult:
    """Result from a single function execution"""
    function_name: str
    request_id: str
    success: bool
    execution_time_ms: float
    error_message: Optional[str] = None
    node_id: Optional[str] = None
    container_id: Optional[str] = None


@dataclass
class ExperimentResult:
    """Results from a complete experiment"""
    config: ExperimentConfig
    total_execution_time_ms: float
    success_rate: float
    avg_execution_time_ms: float
    throughput_req_per_sec: float
    function_results: List[FunctionExecutionResult]
    scheduler_metrics: Dict[str, Any]
    timestamp: str