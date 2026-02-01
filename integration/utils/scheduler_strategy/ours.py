import logging
import time
from typing import Any, Dict

from integration.utils.config import FunctionExecutionResult
from provider.function_container_manager import FunctionContainerManager
from scheduler.ours.function_orchestrator import FunctionOrchestrator

from .interface import SchedulerInterface

logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class OurScheduler(SchedulerInterface):
    """Our advanced scheduler implementation"""
    
    def __init__(self):
        self.orchestrator = None
        self.container_manager = None
        
    def initialize(self, container_manager: FunctionContainerManager) -> bool:
        """Initialize our scheduler"""
        try:
            self.container_manager = container_manager
            # Share the FunctionContainerManager so resource reporting matches FaasPRS
            self.orchestrator = FunctionOrchestrator(container_manager=self.container_manager)
            return True
        except Exception as e:
            logger.error(f"Failed to initialize our scheduler: {e}")
            return False
    
    def execute_function(self, function_name: str, payload: bytes, request_id: str) -> FunctionExecutionResult:
        """Execute function using our orchestrator"""
        start_time = time.time()
        
        try:
            result = self.orchestrator.invoke_function(function_name, request_id, payload)
            execution_time = (time.time() - start_time) * 1000
            
            # Check if the function execution was successful
            # Enhanced success detection with logging
            if isinstance(result, dict):
                status = result.get('status')
                success = status != 'error'
                if not success:
                    # Create a readable version of the result for debugging
                    readable_result = result.copy() if isinstance(result, dict) else result
                    if isinstance(readable_result, dict):
                        # Truncate any base64 image data fields
                        for key in readable_result:
                            if key.endswith('_b64') or key.endswith('_img_b64') or 'img' in key.lower():
                                if isinstance(readable_result[key], str) and len(readable_result[key]) > 100:
                                    img_data = readable_result[key]
                                    readable_result[key] = f"{img_data[:50]}...({len(img_data)} chars total)"
                    print(f"   ❌ {function_name} failed: status='{status}', result={readable_result}")
            else:
                success = True
            
            return FunctionExecutionResult(
                function_name=function_name,
                request_id=request_id,
                success=success,
                execution_time_ms=execution_time,
                node_id=result.get('node_id') if isinstance(result, dict) else None,
                container_id=result.get('container_id') if isinstance(result, dict) else None,
                error_message=result.get('message') if isinstance(result, dict) and not success else None
            )
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            print(f"\033[31m ERROR: {e}\033[0m")
            return FunctionExecutionResult(
                function_name=function_name,
                request_id=request_id,
                success=False,
                execution_time_ms=execution_time,
                error_message=str(e)
            )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get our scheduler metrics (snapshot-based only)"""
        metrics = {
            "scheduler_type": "ours",
            "placement_strategy": "2-phase",
            "routing_strategy": "least_loaded"
        }
        
        # Add snapshot/resource monitor metrics if orchestrator is available
        if self.orchestrator:
            try:
                # Get cluster-wide resource status
                cluster_status = self.orchestrator.get_cluster_resource_status()
                metrics["cluster_resource_status"] = cluster_status
                
                # Get resource monitor metrics
                resource_metrics = self.orchestrator.get_resource_metrics()
                metrics["resource_monitor"] = resource_metrics
                
            except Exception as e:
                logger.warning(f"Failed to get resource metrics: {e}")
                metrics["resource_error"] = str(e)
        
        return metrics


