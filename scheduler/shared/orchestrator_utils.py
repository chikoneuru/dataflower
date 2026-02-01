"""
Shared Orchestrator Utilities Module

This module contains common orchestrator functionality that can be reused
across different schedulers. It provides:
- Resource monitoring and metrics
- DAG analysis and parent task resolution
- Container management utilities
"""

import time
from typing import Dict, List, Optional, Any
import requests

from functions.dag_loader import get_parent_tasks, get_dag_analysis
from provider.function_container_manager import ContainerStatus, FunctionContainer
from provider.resource_monitor import resource_monitor


class OrchestratorUtils:
    """Shared orchestrator utilities for all schedulers"""
    
    @staticmethod
    def get_parent_tasks_from_workflows(function_name: str, active_workflows: Dict[str, Dict]) -> Optional[List[str]]:
        """Get parent tasks from active workflow DAGs"""
        parent_tasks = []
        
        # Search through all active workflows for dependencies
        for workflow_name, workflow_info in active_workflows.items():
            dag = workflow_info.get('dag', {})
            parents = get_parent_tasks(function_name, dag)
            if parents:
                parent_tasks.extend(parents)
        
        return list(set(parent_tasks)) if parent_tasks else None
    
    @staticmethod
    def get_dag_structure_from_workflows(function_name: str, active_workflows: Dict[str, Dict]) -> Optional[Dict]:
        """Get DAG structure from active workflows"""
        # Find the workflow containing this function
        for workflow_name, workflow_info in active_workflows.items():
            dag = workflow_info.get('dag', {})
            
            # Use the DAG analysis function from dag_loader
            analysis = get_dag_analysis(function_name, dag, workflow_name)
            if analysis:
                return analysis
        
        return None
    
    @staticmethod
    def build_existing_containers_map(function_container_manager) -> Dict[str, List[FunctionContainer]]:
        """Build a mapping function_name -> List[FunctionContainer] from manager registry"""
        mapping: Dict[str, List[FunctionContainer]] = {}
        try:
            # Use known API to list containers per function
            # function_containers is an internal registry on the manager
            registry = getattr(function_container_manager, 'function_containers', {})
            if isinstance(registry, dict):
                for fname, containers in registry.items():
                    mapping[fname] = list(containers)
        except Exception as e:
            print(f"\033[31m ERROR: {e}\033[0m")
        return mapping
    
    @staticmethod
    def get_resource_metrics() -> Dict:
        """Get resource utilization metrics for all function invocations"""
        try:
            return resource_monitor.export_metrics('json')
        except Exception as e:
            return {'error': str(e)}
    
    @staticmethod
    def get_function_resource_summary(function_name: str, function_container_manager) -> Dict:
        """Get resource utilization summary for a specific function"""
        try:
            # Get metrics from resource monitor
            monitor_summary = resource_monitor.get_metrics_summary(function_name)
            
            # Get current container metrics
            containers = function_container_manager.get_function_containers(function_name)
            running_containers = [c for c in containers if c.status == ContainerStatus.RUNNING]
            
            current_metrics = []
            for container in running_containers:
                container_metrics = function_container_manager.get_container_resource_metrics(container.container_id)
                if container_metrics:
                    current_metrics.append(container_metrics)
            
            # Calculate current resource usage
            current_cpu = sum(m['cpu_percent'] for m in current_metrics) if current_metrics else 0
            current_memory = sum(m['memory_rss_mb'] for m in current_metrics) if current_metrics else 0
            current_gpu = sum(m['gpu_utilization'] or 0 for m in current_metrics) if current_metrics else 0
            
            return {
                'function_name': function_name,
                'running_containers': len(running_containers),
                'current_cpu_percent': current_cpu,
                'current_memory_mb': current_memory,
                'current_gpu_percent': current_gpu,
                'historical_summary': monitor_summary,
                'container_details': [
                    {
                        'container_id': c.container_id[:12],
                        'node_id': c.node_id,
                        'host_port': c.host_port,
                        'status': c.status.value,
                        'request_count': c.request_count,
                        'last_accessed': c.last_accessed
                    }
                    for c in running_containers
                ]
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    @staticmethod
    def get_cluster_resource_status(function_container_manager) -> Dict:
        """Get comprehensive cluster resource status"""
        try:
            cluster_status = function_container_manager.get_cluster_status()
            
            # Add resource monitoring data
            all_metrics = resource_monitor.get_all_metrics()
            function_summaries = {}
            
            for function_name in function_container_manager.function_containers.keys():
                function_summaries[function_name] = OrchestratorUtils.get_function_resource_summary(
                    function_name, function_container_manager
                )
            
            return {
                'cluster_status': cluster_status,
                'function_summaries': function_summaries,
                'total_invocations': len(all_metrics),
                'timestamp': time.time()
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    @staticmethod
    def extract_resource_utilization(function_container_manager) -> Dict[str, Any]:
        """
        Extract comprehensive resource utilization metrics for containers and nodes.
        Returns aggregated metrics suitable for experiment reporting.
        """
        try:
            # Get all container resource metrics
            container_metrics = function_container_manager.get_all_containers_resource_metrics()
            
            # Get worker node resource utilization
            worker_node_utilization = function_container_manager.get_worker_node_resource_utilization()
            
            # Get node capacity summary and bottleneck detection
            node_capacity_summary = function_container_manager.get_node_capacity_summary()
            node_bottlenecks = function_container_manager.detect_node_bottlenecks()
            
            # Get node resource summaries
            nodes = function_container_manager.get_nodes()
            node_summaries = {}
            for node in nodes:
                if hasattr(node, 'node_id'):
                    node_id = node.node_id
                elif isinstance(node, dict):
                    node_id = node.get('node_id')
                else:
                    continue
                
                try:
                    node_summaries[node_id] = function_container_manager.get_node_resource_summary(node_id)
                except Exception as e:
                    node_summaries[node_id] = {'error': str(e)}
            
            # Get resource monitor summaries
            resource_monitor_summary = {}
            for function_name in function_container_manager.function_containers.keys():
                try:
                    resource_monitor_summary[function_name] = resource_monitor.get_metrics_summary(function_name)
                except Exception as e:
                    resource_monitor_summary[function_name] = {'error': str(e)}
            
            # Calculate aggregate metrics
            total_containers = len(container_metrics)
            total_cpu_percent = sum(m.get('cpu_percent', 0) for m in container_metrics.values())
            total_memory_mb = sum(m.get('memory_rss_mb', 0) for m in container_metrics.values())
            total_gpu_utilization = sum(m.get('gpu_utilization', 0) or 0 for m in container_metrics.values())
            
            # Calculate worker node aggregates
            total_worker_nodes = len(worker_node_utilization)
            total_worker_cpu_percent = sum(w.get('cpu_usage_percent', 0) for w in worker_node_utilization.values())
            total_worker_memory_mb = sum(w.get('memory_usage_mb', 0) for w in worker_node_utilization.values())
            total_worker_cpu_limit = sum(w.get('cpu_limit_cores', 0) for w in worker_node_utilization.values())
            total_worker_memory_limit_gb = sum(w.get('memory_limit_gb', 0) for w in worker_node_utilization.values())
            
            # Use actual node count from node_summaries for normalization since worker_node_utilization is empty
            actual_node_count = len(node_summaries)
            
            # Calculate normalized per-node metrics for experiment reporting
            # This provides average resource usage per node for easier comparison across schedulers
            normalized_cpu_per_node = total_cpu_percent / actual_node_count if actual_node_count > 0 else 0.0
            normalized_memory_per_node = total_memory_mb / actual_node_count if actual_node_count > 0 else 0.0
            
            # Calculate per-function aggregates
            function_aggregates = {}
            for function_name, containers in function_container_manager.function_containers.items():
                running_containers = [c for c in containers if c.status == ContainerStatus.RUNNING]
                if running_containers:
                    function_cpu = sum(
                        container_metrics.get(c.container_id, {}).get('cpu_percent', 0) 
                        for c in running_containers
                    )
                    function_memory = sum(
                        container_metrics.get(c.container_id, {}).get('memory_rss_mb', 0) 
                        for c in running_containers
                    )
                    function_gpu = sum(
                        container_metrics.get(c.container_id, {}).get('gpu_utilization', 0) or 0 
                        for c in running_containers
                    )
                    
                    function_aggregates[function_name] = {
                        'running_containers': len(running_containers),
                        'total_cpu_percent': function_cpu,
                        'total_memory_mb': function_memory,
                        'total_gpu_utilization': function_gpu,
                        'avg_cpu_per_container': function_cpu / len(running_containers) if running_containers else 0,
                        'avg_memory_per_container': function_memory / len(running_containers) if running_containers else 0
                    }
            
            return {
                'timestamp': time.time(),
                'total_containers': total_containers,
                'total_cpu_percent': total_cpu_percent,
                'total_memory_mb': total_memory_mb,
                'total_gpu_utilization': total_gpu_utilization,
                'container_metrics': container_metrics,
                'node_summaries': node_summaries,
                'function_aggregates': function_aggregates,
                'resource_monitor_summaries': resource_monitor_summary,
                'system_baseline': resource_monitor.system_baseline,
                # Normalized per-node metrics for experiment reporting
                'normalized': {
                    'cpu_percent_per_node': normalized_cpu_per_node,
                    'memory_mb_per_node': normalized_memory_per_node,
                    # Include divisor for verification
                    'divisor_nodes': actual_node_count,
                },
                # Worker node metrics
                'worker_nodes': {
                    'total_nodes': actual_node_count,  # Use actual node count instead of empty worker_node_utilization
                    'total_cpu_usage_percent': total_worker_cpu_percent,
                    'total_memory_usage_mb': total_worker_memory_mb,
                    'total_cpu_limit_cores': total_worker_cpu_limit,
                    'total_memory_limit_gb': total_worker_memory_limit_gb,
                    'avg_cpu_utilization_percent': total_worker_cpu_percent / actual_node_count if actual_node_count > 0 else 0,
                    'avg_memory_utilization_percent': (total_worker_memory_mb / (total_worker_memory_limit_gb * 1024)) * 100 if total_worker_memory_limit_gb > 0 else 0,
                    'node_details': worker_node_utilization
                },
                # Node capacity and bottleneck information
                'node_capacity': node_capacity_summary,
                'bottlenecks': node_bottlenecks,
                'bottleneck_summary': {
                    'total_bottlenecked_nodes': sum(1 for b in node_bottlenecks.values() if b.get('is_bottlenecked', False)),
                    'cpu_bottlenecked_nodes': sum(1 for b in node_bottlenecks.values() if b.get('cpu_bottleneck', False)),
                    'memory_bottlenecked_nodes': sum(1 for b in node_bottlenecks.values() if b.get('memory_bottleneck', False)),
                    'network_bottlenecked_nodes': sum(1 for b in node_bottlenecks.values() if b.get('network_bottleneck', False))
                }
            }
            
        except Exception as e:
            return {'error': str(e), 'timestamp': time.time()}

    @staticmethod
    def send_request_to_container(container: Any,
                                  function_name: str,
                                  body: Any,
                                  timeline: Optional[Dict] = None,
                                  timeout: int = 300) -> Dict:
        """Send HTTP request to a function container and record timing/derived metrics.

        Works with both scheduler container types:
        - Ours: FunctionContainer object with host_port attribute
        - FaasPRS: dict with 'host_port' key
        """
        # Build URL
        host_port = container['host_port'] if isinstance(container, dict) else getattr(container, 'host_port', None)
        url = f"http://127.0.0.1:{host_port}/invoke"

        headers: Dict[str, str] = {}
        json_payload: Optional[Dict[str, Any]] = None

        # Prepare payload
        prep_start = time.time()
        if isinstance(body, bytes):
            import base64
            img_b64 = base64.b64encode(body).decode('utf-8')
            json_payload = {"img_b64": img_b64}
            headers['Content-Type'] = 'application/json'
        elif isinstance(body, dict):
            headers['Content-Type'] = 'application/json'
            json_payload = body
        elif isinstance(body, str):
            headers['Content-Type'] = 'application/json'
            json_payload = {"data": body}
        else:
            return {'status': 'error', 'message': f'Unsupported payload type: {type(body)}'}
        prep_end = time.time()
        if timeline is not None:
            timeline.setdefault('timestamps', {})
            timeline['timestamps']['request_preparation_ms'] = (prep_end - prep_start) * 1000.0

        try:
            # Record client send time
            if timeline is not None:
                timeline.setdefault('timestamps', {})
                timeline['timestamps']['client_send'] = time.time()

            # Measure total client-side container request time
            req_start = time.time()
            response = requests.post(url, headers=headers, json=json_payload, timeout=timeout)

            # Record client receive time
            if timeline is not None:
                client_recv = time.time()
                timeline['timestamps']['client_receive'] = client_recv

            response.raise_for_status()
            result = response.json()
            
            # Debug: log response for troubleshooting
            if result.get('status') == 'error':
                print(f"\033[91mDEBUG: Function returned error status: {result.get('message', 'No message')}\033[0m")

            # Attach server-side timestamps if present
            server_ts = result.get('server_timestamps') or {}
            if timeline is not None and isinstance(server_ts, dict):
                timeline['server_timestamps'] = server_ts

                # Calculate derived metrics
                c_send = timeline['timestamps'].get('client_send')
                c_recv = timeline['timestamps'].get('client_receive')
                s_exec_start = server_ts.get('exec_start')
                s_exec_end = server_ts.get('exec_end')
                s_sent = server_ts.get('sent')

                derived: Dict[str, float] = {}
                try:
                    if c_send is not None and s_exec_start is not None:
                        derived['network_delay_ms'] = (s_exec_start - c_send) * 100.0 * 10
                        # keep precision but avoid float drift across schedulers
                        derived['network_delay_ms'] = (s_exec_start - c_send) * 1000.0
                    if s_exec_start is not None and s_exec_end is not None:
                        derived['exec_ms'] = (s_exec_end - s_exec_start) * 1000.0
                    if s_sent is not None and c_recv is not None:
                        derived['network_return_ms'] = (c_recv - s_sent) * 1000.0
                    # Compute server-side processing that isn't part of exec_ms
                    total_client_ms = (client_recv - c_send) * 1000.0 if (c_send is not None and client_recv is not None) else None
                    if total_client_ms is not None:
                        net_ms = (derived.get('network_delay_ms', 0.0) or 0.0) + (derived.get('network_return_ms', 0.0) or 0.0)
                        exec_ms = (derived.get('exec_ms', 0.0) or 0.0)
                        server_processing_ms = total_client_ms - (net_ms + exec_ms)
                        if 'timestamps' in timeline:
                            timeline['timestamps']['server_processing_ms'] = max(server_processing_ms, 0.0)
                except Exception as e:
                    print(f"\033[31mERROR in derived metrics calculation: {e}\033[0m")
                if derived:
                    timeline['derived'] = derived
            result.setdefault('status', 'ok')
            return result

        except requests.exceptions.RequestException as e:
            raise Exception(f"Request to container at {url} failed: {e}")

    @staticmethod
    def send_request_with_storage(container: Any,
                                 function_name: str,
                                 body: Any,
                                 storage_context: Optional[Any] = None,
                                 timeline: Optional[Dict] = None,
                                 timeout: int = 300) -> Dict:
        """Enhanced container communication with storage support.
        
        This method extends the basic send_request_to_container with storage context
        support for remote data transfer across all schedulers.
        
        Args:
            container: Container object or dict with host_port
            function_name: Name of the function to invoke
            body: Request body (input data)
            storage_context: Storage context for remote data retrieval
            timeline: Timeline for metrics collection
            timeout: Request timeout in seconds
            
        Returns:
            Dict: Container response with status and data
        """
        # Build URL
        host_port = container['host_port'] if isinstance(container, dict) else getattr(container, 'host_port', None)
        url = f"http://127.0.0.1:{host_port}/invoke"

        headers: Dict[str, str] = {'Content-Type': 'application/json'}
        json_payload: Dict[str, Any] = {}

        # Prepare payload based on storage context
        prep_start = time.time()
        
        if storage_context:
            # Function has dependencies - send storage context
            json_payload = {
                'input_ref': {
                    'src': storage_context.src,
                    'input_key': storage_context.input_key,
                    # pass through local relative path if provided
                    'path': storage_context.path,
                    'storage_config': storage_context.storage_config,
                    'scheduler_type': storage_context.scheduler_type
                }
            }
        elif isinstance(body, bytes):
            # Direct binary input
            import base64
            img_b64 = base64.b64encode(body).decode('utf-8')
            json_payload = {"img_b64": img_b64}
        elif isinstance(body, dict):
            # Direct dictionary input
            json_payload = body
        elif isinstance(body, str):
            # Direct string input
            json_payload = {"data": body}
        else:
            return {'status': 'error', 'message': f'Unsupported payload type: {type(body)}'}
        
        prep_end = time.time()
        if timeline is not None:
            timeline.setdefault('timestamps', {})
            timeline['timestamps']['request_preparation_ms'] = (prep_end - prep_start) * 1000.0

        try:
            # Record client send time
            if timeline is not None:
                timeline.setdefault('timestamps', {})
                timeline['timestamps']['client_send'] = time.time()

            # Measure total client-side container request time
            req_start = time.time()
            response = requests.post(url, headers=headers, json=json_payload, timeout=timeout)

            # Record client receive time
            if timeline is not None:
                client_recv = time.time()
                timeline['timestamps']['client_receive'] = client_recv

            response.raise_for_status()
            result = response.json()
            
            # Debug: log response for troubleshooting
            if result.get('status') == 'error':
                print(f"\033[91mDEBUG: Function returned error status: {result.get('message', 'No message')}\033[0m")

            # Attach server-side timestamps if present
            server_ts = result.get('server_timestamps') or {}
            if timeline is not None and isinstance(server_ts, dict):
                timeline['server_timestamps'] = server_ts

                # Calculate derived metrics
                c_send = timeline['timestamps'].get('client_send')
                c_recv = timeline['timestamps'].get('client_receive')
                s_exec_start = server_ts.get('exec_start')
                s_exec_end = server_ts.get('exec_end')
                s_sent = server_ts.get('sent')

                derived: Dict[str, float] = {}
                try:
                    if c_send is not None and s_exec_start is not None:
                        derived['network_delay_ms'] = (s_exec_start - c_send) * 1000.0
                    if s_exec_start is not None and s_exec_end is not None:
                        derived['exec_ms'] = (s_exec_end - s_exec_start) * 1000.0
                    if s_sent is not None and c_recv is not None:
                        derived['network_return_ms'] = (c_recv - s_sent) * 1000.0
                    # Compute server-side processing that isn't part of exec_ms
                    total_client_ms = (client_recv - c_send) * 1000.0 if (c_send is not None and client_recv is not None) else None
                    if total_client_ms is not None:
                        net_ms = (derived.get('network_delay_ms', 0.0) or 0.0) + (derived.get('network_return_ms', 0.0) or 0.0)
                        exec_ms = (derived.get('exec_ms', 0.0) or 0.0)
                        server_processing_ms = total_client_ms - (net_ms + exec_ms)
                        if 'timestamps' in timeline:
                            timeline['timestamps']['server_processing_ms'] = max(server_processing_ms, 0.0)
                except Exception as e:
                    print(f"\033[31mERROR in derived metrics calculation: {e}\033[0m")
                if derived:
                    timeline['derived'] = derived
            result.setdefault('status', 'ok')
            return result

        except requests.exceptions.RequestException as e:
            raise Exception(f"Request to container at {url} failed: {e}")
