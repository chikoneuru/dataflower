"""
Debug utilities for Ditto scheduler.

Provides debug logging, metrics collection, and plan saving functionality.
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional
from datetime import datetime

from scheduler.shared.safe_printing import safe_print_value


class DittoDebugLogger:
    """Debug logger for Ditto components."""
    
    def __init__(self, debug_mode: bool = False, log_level: str = "INFO"):
        self.debug_mode = debug_mode
        self.log_level = log_level
        self.log_levels = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}
        self.current_level = self.log_levels.get(log_level, 1)
        
        # Set up standard logging integration
        self.logger = logging.getLogger('scheduler.ditto.debug')
        if debug_mode:
            self.logger.setLevel(getattr(logging, log_level, logging.INFO))
        
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message if debug mode is enabled."""
        if self.debug_mode and self.current_level <= 0:
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            self.logger.debug(f"🐛 [DEBUG {timestamp}] {message}")
            self.logger.debug(f"{message} {kwargs}" if kwargs else message)
            if kwargs:
                for key, value in kwargs.items():
                    safe_value = safe_print_value(value)
                    self.logger.debug(f"    {key}: {safe_value}")
    
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        if self.current_level <= 1:
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            self.logger.info(f"ℹ️  [INFO {timestamp}] {message}")
            self.logger.info(f"{message} {kwargs}" if kwargs else message)
            if kwargs:
                for key, value in kwargs.items():
                    safe_value = safe_print_value(value)
                    self.logger.info(f"    {key}: {safe_value}")
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        if self.current_level <= 2:
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            self.logger.warning(f"⚠️  [WARNING {timestamp}] {message}")
            self.logger.warning(f"{message} {kwargs}" if kwargs else message)
            if kwargs:
                for key, value in kwargs.items():
                    safe_value = safe_print_value(value)
                    self.logger.warning(f"    {key}: {safe_value}")
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        if self.current_level <= 3:
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            print(f"\033[91m❌ [ERROR {timestamp}] {message}\033[0m")
            self.logger.error(f"{message} {kwargs}" if kwargs else message)
            if kwargs:
                for key, value in kwargs.items():
                    safe_value = safe_print_value(value)
                    self.logger.error(f"    {key}: {safe_value}")


class DittoMetricsCollector:
    """Collects debug metrics during Ditto execution."""
    
    def __init__(self, debug_mode: bool = False, debug_logger: Optional[DittoDebugLogger] = None):
        self.debug_mode = debug_mode
        self.debug_logger = debug_logger
        self.metrics: Dict[str, Any] = {
            "workflow_registrations": [],
            "plan_evaluations": [],
            "placement_operations": [],
            "dop_computations": [],
            "grouping_operations": [],
            "execution_timings": {}
        }
        self.start_times: Dict[str, float] = {}
        
    def start_timer(self, operation: str) -> None:
        """Start timing an operation."""
        if self.debug_mode:
            self.start_times[operation] = time.time()
    
    def end_timer(self, operation: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """End timing an operation and record metrics."""
        if self.debug_mode and operation in self.start_times:
            duration = time.time() - self.start_times[operation]
            
            if operation.startswith("workflow_registration_"):
                self.metrics["workflow_registrations"].append({
                    "workflow_name": operation.replace("workflow_registration_", ""),
                    "duration_ms": duration * 1000,
                    "timestamp": datetime.now().isoformat(),
                    "metadata": metadata or {}
                })
            elif operation.startswith("plan_evaluation_"):
                self.metrics["plan_evaluations"].append({
                    "plan_id": operation.replace("plan_evaluation_", ""),
                    "duration_ms": duration * 1000,
                    "timestamp": datetime.now().isoformat(),
                    "metadata": metadata or {}
                })
            elif operation.startswith("placement_"):
                self.metrics["placement_operations"].append({
                    "operation": operation,
                    "duration_ms": duration * 1000,
                    "timestamp": datetime.now().isoformat(),
                    "metadata": metadata or {}
                })
            elif operation.startswith("dop_computation_"):
                self.metrics["dop_computations"].append({
                    "computation_id": operation.replace("dop_computation_", ""),
                    "duration_ms": duration * 1000,
                    "timestamp": datetime.now().isoformat(),
                    "metadata": metadata or {}
                })
            elif operation.startswith("grouping_"):
                self.metrics["grouping_operations"].append({
                    "operation": operation,
                    "duration_ms": duration * 1000,
                    "timestamp": datetime.now().isoformat(),
                    "metadata": metadata or {}
                })
            else:
                self.metrics["execution_timings"][operation] = {
                    "duration_ms": duration * 1000,
                    "timestamp": datetime.now().isoformat(),
                    "metadata": metadata or {}
                }
            
            del self.start_times[operation]
    
    def record_metric(self, category: str, name: str, value: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Record a metric value."""
        if self.debug_mode:
            if category not in self.metrics:
                self.metrics[category] = []
            
            self.metrics[category].append({
                "name": name,
                "value": value,
                "timestamp": datetime.now().isoformat(),
                "metadata": metadata or {}
            })
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get all collected metrics."""
        return self.metrics.copy()
    
    def save_metrics(self, filename: str) -> None:
        """Save metrics to a JSON file."""
        if self.debug_mode:
            try:
                with open(filename, 'w') as f:
                    json.dump(self.metrics, f, indent=2, default=str)
                self.debug_logger.info(f"💾 Debug metrics saved to: {filename}")
            except Exception as e:
                self.debug_logger.error(f"❌ Failed to save debug metrics: {e}")


class DittoPlanSaver:
    """Saves execution plans for debugging."""
    
    def __init__(self, debug_mode: bool = False, debug_logger: Optional[DittoDebugLogger] = None):
        self.debug_mode = debug_mode
        self.debug_logger = debug_logger
        self.saved_plans: List[Dict[str, Any]] = []
    
    def save_plan(self, workflow_name: str, plan: Dict[str, Any], plan_type: str = "execution") -> None:
        """Save an execution plan."""
        if self.debug_mode:
            plan_data = {
                "workflow_name": workflow_name,
                "plan_type": plan_type,
                "timestamp": datetime.now().isoformat(),
                "plan": plan
            }
            self.saved_plans.append(plan_data)
            
            # Save to file
            filename = f"results/ditto_plan_{workflow_name}_{plan_type}.json"
            try:
                with open(filename, 'w') as f:
                    json.dump(plan_data, f, indent=2, default=str)
                self.debug_logger.info(f"💾 Execution plan saved to: {filename}")
            except Exception as e:
                self.debug_logger.error(f"❌ Failed to save execution plan: {e}")
    
    def get_saved_plans(self) -> List[Dict[str, Any]]:
        """Get all saved plans."""
        return self.saved_plans.copy()


def create_debug_context(config) -> tuple[DittoDebugLogger, DittoMetricsCollector, DittoPlanSaver]:
    """Create debug context objects based on configuration."""
    logger = DittoDebugLogger(config.debug_mode, config.debug_log_level)
    metrics = DittoMetricsCollector(config.debug_mode, debug_logger=logger)
    plan_saver = DittoPlanSaver(config.debug_mode, debug_logger=logger)
    
    return logger, metrics, plan_saver
