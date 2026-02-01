import time
from typing import Dict, Any

from integration.utils.config import FunctionExecutionResult
from provider.function_manager import FunctionManager
from scheduler.faaSPRS import config as faasprs_config
from scheduler.faaSPRS.function_orchestrator import FunctionOrchestrator

from .interface import SchedulerInterface


class FaasPRSScheduler(SchedulerInterface):
    def __init__(self, container_manager):
        # Ensure FaasPRS config is initialized
        if faasprs_config.get_config() is None:
            faasprs_config.initialize_config()

        # Store the shared container manager
        self.container_manager = container_manager
        self.workflow_dag = None
        
    def initialize(self, container_manager) -> bool:
        # Use the shared container manager
        self.container_manager = container_manager
        self.orchestrator = FunctionOrchestrator(container_manager=self.container_manager)
        return True
    def register_workflow(self, workflow_name, workflow_dag):
        self.workflow_dag = workflow_dag
        # Call orchestrator's register_workflow method
        if hasattr(self, 'orchestrator') and self.orchestrator:
            self.orchestrator.register_workflow(workflow_name, workflow_dag)

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
            print(f"\033[31mERROR: {e}\033[0m")
            return FunctionExecutionResult(
                function_name=function_name,
                request_id=request_id,
                success=False,
                execution_time_ms=execution_time,
                error_message=str(e)
            )
    
    def execute_workflow(self, workflow_name, request_id, input_data):
        if self.workflow_dag is None:
            raise RuntimeError("Workflow not registered for FaasPRS")

        # Delegate to orchestrator; return a single overall result
        result = self.orchestrator.submit_workflow(workflow_name, request_id, input_data)
        total_ms = 0.0
        try:
            total_ms = float(((result or {}).get("timeline") or {}).get("workflow", {}).get("duration_ms", 0.0))
        except Exception:
            total_ms = 0.0
        return [
            FunctionExecutionResult(
                function_name=workflow_name,
                request_id=request_id,
                success=True,
                execution_time_ms=total_ms,
                error_message=None,
            )
        ]

    def get_metrics(self) -> Dict[str, Any]:
        """Get scheduler-specific metrics"""
        metrics = {"scheduler_type": "FaasPRS"}
        
        # Add orchestrator metrics if available
        if hasattr(self, 'orchestrator') and self.orchestrator:
            try:
                # Get resource metrics from orchestrator
                resource_metrics = self.orchestrator.get_resource_metrics()
                if resource_metrics:
                    metrics.update(resource_metrics)
                
                # Get cluster resource status
                cluster_status = self.orchestrator.get_cluster_resource_status()
                if cluster_status:
                    metrics['cluster_resource_status'] = cluster_status
            except Exception as e:
                print(f"\033[31mError collecting FaasPRS metrics: {e}\033[0m")
                import traceback
                traceback.print_exc()
        
        return metrics

    def collect_metrics(self):
        """Collect metrics for the scheduler"""
        return self.get_metrics()