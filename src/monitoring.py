"""
Monitoring and Metrics Collection for DataFlower
Tracks performance, resource usage, and system health
"""

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Deque
import json
import logging
from enum import Enum
import numpy as np
from prometheus_client import Counter, Gauge, Histogram, Summary, start_http_server
import psutil
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics collected"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class MetricPoint:
    """Single metric data point"""
    timestamp: datetime
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp.isoformat(),
            'value': self.value,
            'labels': self.labels
        }


@dataclass
class WorkflowMetrics:
    """Metrics for a workflow execution"""
    workflow_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    total_execution_time: float = 0.0
    task_execution_times: List[float] = field(default_factory=list)
    resource_usage: Dict[str, List[float]] = field(default_factory=dict)
    cost_estimate: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'workflow_id': self.workflow_id,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'total_tasks': self.total_tasks,
            'completed_tasks': self.completed_tasks,
            'failed_tasks': self.failed_tasks,
            'total_execution_time': self.total_execution_time,
            'avg_task_execution_time': np.mean(self.task_execution_times) if self.task_execution_times else 0,
            'resource_usage': self.resource_usage,
            'cost_estimate': self.cost_estimate
        }


class PrometheusMetrics:
    """Prometheus metrics for DataFlower"""
    
    def __init__(self):
        # Workflow metrics
        self.workflow_total = Counter(
            'dataflower_workflows_total',
            'Total number of workflows processed',
            ['status']
        )
        
        self.workflow_duration = Histogram(
            'dataflower_workflow_duration_seconds',
            'Workflow execution duration in seconds',
            buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600]
        )
        
        # Task metrics
        self.task_total = Counter(
            'dataflower_tasks_total',
            'Total number of tasks processed',
            ['status', 'function']
        )
        
        self.task_duration = Histogram(
            'dataflower_task_duration_seconds',
            'Task execution duration in seconds',
            ['function'],
            buckets=[0.1, 0.5, 1, 5, 10, 30, 60, 120, 300]
        )
        
        self.task_retries = Counter(
            'dataflower_task_retries_total',
            'Total number of task retries',
            ['function']
        )
        
        # Resource metrics
        self.cpu_usage = Gauge(
            'dataflower_cpu_usage_percent',
            'CPU usage percentage'
        )
        
        self.memory_usage = Gauge(
            'dataflower_memory_usage_bytes',
            'Memory usage in bytes'
        )
        
        self.active_workflows = Gauge(
            'dataflower_active_workflows',
            'Number of currently active workflows'
        )
        
        self.queue_size = Gauge(
            'dataflower_queue_size',
            'Number of tasks in queue',
            ['priority']
        )
        
        # Scheduler metrics
        self.scheduler_decisions = Counter(
            'dataflower_scheduler_decisions_total',
            'Total scheduling decisions made',
            ['strategy']
        )
        
        self.scheduler_efficiency = Gauge(
            'dataflower_scheduler_efficiency',
            'Scheduler efficiency ratio'
        )
    
    def record_workflow_start(self, workflow_id: str):
        """Record workflow start"""
        self.active_workflows.inc()
    
    def record_workflow_complete(self, workflow_id: str, duration: float, status: str):
        """Record workflow completion"""
        self.workflow_total.labels(status=status).inc()
        self.workflow_duration.observe(duration)
        self.active_workflows.dec()
    
    def record_task_complete(self, function: str, duration: float, status: str, retries: int = 0):
        """Record task completion"""
        self.task_total.labels(status=status, function=function).inc()
        self.task_duration.labels(function=function).observe(duration)
        if retries > 0:
            self.task_retries.labels(function=function).inc(retries)
    
    def update_resource_metrics(self):
        """Update resource usage metrics"""
        self.cpu_usage.set(psutil.cpu_percent())
        self.memory_usage.set(psutil.virtual_memory().used)
    
    def update_queue_metrics(self, queue_sizes: Dict[str, int]):
        """Update queue size metrics"""
        for priority, size in queue_sizes.items():
            self.queue_size.labels(priority=priority).set(size)


class MetricsCollector:
    """Collects and aggregates metrics"""
    
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.metrics: Dict[str, Deque[MetricPoint]] = defaultdict(
            lambda: deque(maxlen=window_size)
        )
        self.workflow_metrics: Dict[str, WorkflowMetrics] = {}
        self.prometheus_metrics = PrometheusMetrics()
        
    def record_metric(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Record a metric value"""
        point = MetricPoint(
            timestamp=datetime.now(),
            value=value,
            labels=labels or {}
        )
        self.metrics[name].append(point)
    
    def start_workflow(self, workflow_id: str, total_tasks: int):
        """Start tracking a workflow"""
        self.workflow_metrics[workflow_id] = WorkflowMetrics(
            workflow_id=workflow_id,
            start_time=datetime.now(),
            total_tasks=total_tasks
        )
        self.prometheus_metrics.record_workflow_start(workflow_id)
    
    def complete_workflow(self, workflow_id: str, status: str):
        """Complete workflow tracking"""
        if workflow_id in self.workflow_metrics:
            metrics = self.workflow_metrics[workflow_id]
            metrics.end_time = datetime.now()
            duration = (metrics.end_time - metrics.start_time).total_seconds()
            metrics.total_execution_time = duration
            
            self.prometheus_metrics.record_workflow_complete(
                workflow_id, duration, status
            )
    
    def record_task_execution(self, workflow_id: str, task_id: str, 
                            function: str, duration: float, 
                            status: str, retries: int = 0):
        """Record task execution metrics"""
        if workflow_id in self.workflow_metrics:
            metrics = self.workflow_metrics[workflow_id]
            metrics.task_execution_times.append(duration)
            
            if status == "completed":
                metrics.completed_tasks += 1
            elif status == "failed":
                metrics.failed_tasks += 1
        
        self.prometheus_metrics.record_task_complete(
            function, duration, status, retries
        )
        
        self.record_metric(f"task_duration_{function}", duration)
    
    def record_resource_usage(self, workflow_id: str, resource_type: str, value: float):
        """Record resource usage for a workflow"""
        if workflow_id in self.workflow_metrics:
            metrics = self.workflow_metrics[workflow_id]
            if resource_type not in metrics.resource_usage:
                metrics.resource_usage[resource_type] = []
            metrics.resource_usage[resource_type].append(value)
    
    def get_metric_stats(self, name: str) -> Dict[str, float]:
        """Get statistics for a metric"""
        if name not in self.metrics or not self.metrics[name]:
            return {}
        
        values = [point.value for point in self.metrics[name]]
        return {
            'count': len(values),
            'mean': np.mean(values),
            'std': np.std(values),
            'min': np.min(values),
            'max': np.max(values),
            'p50': np.percentile(values, 50),
            'p95': np.percentile(values, 95),
            'p99': np.percentile(values, 99)
        }
    
    def get_workflow_metrics(self, workflow_id: str) -> Optional[WorkflowMetrics]:
        """Get metrics for a specific workflow"""
        return self.workflow_metrics.get(workflow_id)
    
    def export_metrics(self, filepath: str):
        """Export metrics to JSON file"""
        export_data = {
            'timestamp': datetime.now().isoformat(),
            'metrics': {},
            'workflows': {}
        }
        
        # Export metric statistics
        for name in self.metrics:
            export_data['metrics'][name] = self.get_metric_stats(name)
        
        # Export workflow metrics
        for wf_id, wf_metrics in self.workflow_metrics.items():
            export_data['workflows'][wf_id] = wf_metrics.to_dict()
        
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)


class PerformanceMonitor:
    """Monitors system performance and health"""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
        self.monitoring = False
        self.monitor_task = None
        
    async def start_monitoring(self, interval: int = 5):
        """Start continuous monitoring"""
        self.monitoring = True
        self.monitor_task = asyncio.create_task(self._monitor_loop(interval))
        
        # Start Prometheus metrics server
        start_http_server(8000)
        logger.info("Prometheus metrics server started on port 8000")
    
    async def stop_monitoring(self):
        """Stop monitoring"""
        self.monitoring = False
        if self.monitor_task:
            await self.monitor_task
    
    async def _monitor_loop(self, interval: int):
        """Main monitoring loop"""
        while self.monitoring:
            try:
                # Collect system metrics
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                network = psutil.net_io_counters()
                
                # Record metrics
                self.metrics_collector.record_metric('system_cpu_percent', cpu_percent)
                self.metrics_collector.record_metric('system_memory_percent', memory.percent)
                self.metrics_collector.record_metric('system_memory_used_gb', 
                                                    memory.used / (1024**3))
                self.metrics_collector.record_metric('system_disk_percent', disk.percent)
                self.metrics_collector.record_metric('network_bytes_sent', 
                                                    network.bytes_sent)
                self.metrics_collector.record_metric('network_bytes_recv', 
                                                    network.bytes_recv)
                
                # Update Prometheus metrics
                self.metrics_collector.prometheus_metrics.update_resource_metrics()
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(interval)


class AlertManager:
    """Manages alerts based on metrics thresholds"""
    
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url
        self.alert_rules = []
        self.active_alerts = {}
        
    def add_rule(self, name: str, metric: str, threshold: float, 
                 condition: str = "greater", duration: int = 60):
        """Add an alert rule"""
        self.alert_rules.append({
            'name': name,
            'metric': metric,
            'threshold': threshold,
            'condition': condition,
            'duration': duration,
            'triggered_at': None
        })
    
    def check_alerts(self, metrics: Dict[str, float]):
        """Check if any alerts should be triggered"""
        current_time = datetime.now()
        
        for rule in self.alert_rules:
            metric_value = metrics.get(rule['metric'])
            if metric_value is None:
                continue
            
            # Check threshold
            triggered = False
            if rule['condition'] == 'greater' and metric_value > rule['threshold']:
                triggered = True
            elif rule['condition'] == 'less' and metric_value < rule['threshold']:
                triggered = True
            
            if triggered:
                if rule['name'] not in self.active_alerts:
                    # New alert
                    self.active_alerts[rule['name']] = {
                        'triggered_at': current_time,
                        'metric': rule['metric'],
                        'value': metric_value,
                        'threshold': rule['threshold']
                    }
                    asyncio.create_task(self._send_alert(rule['name'], metric_value))
            else:
                # Clear alert if it exists
                if rule['name'] in self.active_alerts:
                    del self.active_alerts[rule['name']]
                    logger.info(f"Alert {rule['name']} cleared")
    
    async def _send_alert(self, alert_name: str, value: float):
        """Send alert notification"""
        alert_data = {
            'alert': alert_name,
            'value': value,
            'timestamp': datetime.now().isoformat(),
            'active_alerts': list(self.active_alerts.keys())
        }
        
        logger.warning(f"ALERT: {alert_name} triggered with value {value}")
        
        if self.webhook_url:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(self.webhook_url, json=alert_data) as resp:
                        if resp.status != 200:
                            logger.error(f"Failed to send alert: {resp.status}")
            except Exception as e:
                logger.error(f"Error sending alert: {e}")


class TracingCollector:
    """Distributed tracing for workflow execution"""
    
    def __init__(self):
        self.traces = {}
        self.spans = defaultdict(list)
        
    def start_trace(self, trace_id: str, operation: str):
        """Start a new trace"""
        self.traces[trace_id] = {
            'id': trace_id,
            'operation': operation,
            'start_time': time.time(),
            'spans': []
        }
    
    def start_span(self, trace_id: str, span_id: str, operation: str, 
                   parent_span_id: Optional[str] = None):
        """Start a new span within a trace"""
        if trace_id not in self.traces:
            return
        
        span = {
            'id': span_id,
            'trace_id': trace_id,
            'operation': operation,
            'parent_id': parent_span_id,
            'start_time': time.time(),
            'end_time': None,
            'tags': {},
            'logs': []
        }
        
        self.spans[trace_id].append(span)
    
    def end_span(self, trace_id: str, span_id: str):
        """End a span"""
        for span in self.spans.get(trace_id, []):
            if span['id'] == span_id:
                span['end_time'] = time.time()
                span['duration'] = span['end_time'] - span['start_time']
                break
    
    def add_span_tag(self, trace_id: str, span_id: str, key: str, value: Any):
        """Add a tag to a span"""
        for span in self.spans.get(trace_id, []):
            if span['id'] == span_id:
                span['tags'][key] = value
                break
    
    def add_span_log(self, trace_id: str, span_id: str, message: str):
        """Add a log entry to a span"""
        for span in self.spans.get(trace_id, []):
            if span['id'] == span_id:
                span['logs'].append({
                    'timestamp': time.time(),
                    'message': message
                })
                break
    
    def end_trace(self, trace_id: str):
        """End a trace"""
        if trace_id in self.traces:
            self.traces[trace_id]['end_time'] = time.time()
            self.traces[trace_id]['duration'] = (
                self.traces[trace_id]['end_time'] - 
                self.traces[trace_id]['start_time']
            )
            self.traces[trace_id]['spans'] = self.spans.get(trace_id, [])
    
    def export_trace(self, trace_id: str) -> Optional[Dict[str, Any]]:
        """Export a complete trace"""
        if trace_id in self.traces:
            return self.traces[trace_id]
        return None
