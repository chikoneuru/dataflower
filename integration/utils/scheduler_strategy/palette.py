"""
Palette Scheduler Strategy
Wrapper for Palette scheduler with bucket-based hash routing
"""
from typing import Any, Dict, Optional
from .interface import SchedulerInterface


class PaletteScheduler(SchedulerInterface):
    """Palette scheduler implementation"""
    
    def __init__(self, container_manager=None):
        self.container_manager = container_manager
        self.orchestrator = None
    
    def initialize(self, container_manager) -> bool:
        """Initialize Palette scheduler with container manager"""
        try:
            from scheduler.palette.function_orchestrator import FunctionOrchestrator
            
            # Use provided container manager or fall back to stored one
            cm = container_manager if container_manager is not None else self.container_manager
            
            # Initialize orchestrator with container manager
            self.orchestrator = FunctionOrchestrator(container_manager=cm)
            self.container_manager = cm
            
            return True
        except Exception as e:
            print(f"Failed to initialize Palette scheduler: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def execute_function(self, function_name: str, input_data: bytes, request_id: str) -> Any:
        """Execute a single function (not typically used for Palette)"""
        # Palette is workflow-oriented, but we can support single function execution
        if not self.orchestrator:
            raise RuntimeError("Orchestrator not initialized")
        
        return self.orchestrator.invoke_function(function_name, request_id, input_data)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get scheduler metrics"""
        if not self.orchestrator:
            return {}
        
        metrics = {}
        
        # Get bucket statistics (Palette-specific)
        try:
            bucket_stats = self.orchestrator.get_bucket_statistics()
            metrics['bucket_statistics'] = bucket_stats
        except Exception as e:
            print(f"Warning: Could not get bucket statistics: {e}")
        
        # Get resource metrics (shared interface)
        try:
            resource_metrics = self.orchestrator.get_resource_metrics()
            metrics['resource_metrics'] = resource_metrics
        except Exception as e:
            print(f"Warning: Could not get resource metrics: {e}")
        
        return metrics

