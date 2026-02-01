from abc import ABC
from typing import Any, Dict, Protocol

from integration.utils.config import FunctionExecutionResult
from provider.function_container_manager import FunctionContainerManager


class SchedulerInterface(Protocol):
    """Interface that all schedulers must implement"""
    
    def initialize(self, container_manager: FunctionContainerManager) -> bool:
        """Initialize the scheduler with container manager"""
        ...

    def execute_function(self, function_name: str, payload: bytes, request_id: str) -> FunctionExecutionResult:
        """Execute a single function with the given payload"""
        ...
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get scheduler-specific metrics"""
        ...