"""
Advanced Task Scheduling and Resource Optimization for DataFlower
Implements various scheduling algorithms and resource allocation strategies
"""

import asyncio
import heapq
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any
from enum import Enum
import numpy as np
from collections import defaultdict
import networkx as nx
import logging

from .workflow_engine import Task, Workflow, TaskStatus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchedulingStrategy(Enum):
    """Available scheduling strategies"""
    FIFO = "fifo"  # First In First Out
    PRIORITY = "priority"  # Priority-based scheduling
    SJF = "sjf"  # Shortest Job First
    COST_OPTIMIZED = "cost_optimized"  # Minimize cost
    DEADLINE_AWARE = "deadline_aware"  # Meet deadlines
    LOAD_BALANCED = "load_balanced"  # Balance load across resources
    ML_BASED = "ml_based"  # Machine learning based prediction


@dataclass
class ResourcePool:
    """Represents available compute resources"""
    cpu_cores: int
    memory_gb: float
    gpu_count: int = 0
    network_bandwidth_gbps: float = 10.0
    storage_gb: float = 1000.0
    
    def can_allocate(self, requirements: Dict[str, float]) -> bool:
        """Check if resources can be allocated"""
        return (
            self.cpu_cores >= requirements.get('cpu', 0) and
            self.memory_gb >= requirements.get('memory', 0) and
            self.gpu_count >= requirements.get('gpu', 0)
        )
    
    def allocate(self, requirements: Dict[str, float]):
        """Allocate resources"""
        self.cpu_cores -= requirements.get('cpu', 0)
        self.memory_gb -= requirements.get('memory', 0)
        self.gpu_count -= requirements.get('gpu', 0)
    
    def release(self, requirements: Dict[str, float]):
        """Release allocated resources"""
        self.cpu_cores += requirements.get('cpu', 0)
        self.memory_gb += requirements.get('memory', 0)
        self.gpu_count += requirements.get('gpu', 0)


@dataclass
class TaskSchedulingInfo:
    """Extended task information for scheduling"""
    task: Task
    priority: float = 0.0
    estimated_duration: float = 0.0  # seconds
    estimated_cost: float = 0.0
    deadline: Optional[datetime] = None
    arrival_time: datetime = field(default_factory=datetime.now)
    dependencies_completed: Set[str] = field(default_factory=set)
    resource_requirements: Dict[str, float] = field(default_factory=dict)
    
    def __lt__(self, other):
        """For priority queue comparison"""
        return self.priority > other.priority  # Higher priority first


class SchedulerBase(ABC):
    """Base class for task schedulers"""
    
    @abstractmethod
    async def schedule(self, 
                       tasks: List[TaskSchedulingInfo], 
                       resources: ResourcePool) -> List[TaskSchedulingInfo]:
        """Schedule tasks for execution"""
        pass


class FIFOScheduler(SchedulerBase):
    """First In First Out scheduler"""
    
    async def schedule(self, 
                       tasks: List[TaskSchedulingInfo], 
                       resources: ResourcePool) -> List[TaskSchedulingInfo]:
        """Schedule tasks in FIFO order"""
        # Sort by arrival time
        sorted_tasks = sorted(tasks, key=lambda t: t.arrival_time)
        scheduled = []
        
        for task_info in sorted_tasks:
            if resources.can_allocate(task_info.resource_requirements):
                resources.allocate(task_info.resource_requirements)
                scheduled.append(task_info)
        
        return scheduled


class PriorityScheduler(SchedulerBase):
    """Priority-based scheduler using a heap"""
    
    async def schedule(self, 
                       tasks: List[TaskSchedulingInfo], 
                       resources: ResourcePool) -> List[TaskSchedulingInfo]:
        """Schedule tasks based on priority"""
        # Use a max heap (negated priorities)
        heap = []
        for task in tasks:
            heapq.heappush(heap, task)
        
        scheduled = []
        while heap:
            task_info = heapq.heappop(heap)
            if resources.can_allocate(task_info.resource_requirements):
                resources.allocate(task_info.resource_requirements)
                scheduled.append(task_info)
            else:
                break  # Can't schedule more tasks
        
        return scheduled


class ShortestJobFirstScheduler(SchedulerBase):
    """Shortest Job First scheduler"""
    
    async def schedule(self, 
                       tasks: List[TaskSchedulingInfo], 
                       resources: ResourcePool) -> List[TaskSchedulingInfo]:
        """Schedule shortest jobs first"""
        # Sort by estimated duration
        sorted_tasks = sorted(tasks, key=lambda t: t.estimated_duration)
        scheduled = []
        
        for task_info in sorted_tasks:
            if resources.can_allocate(task_info.resource_requirements):
                resources.allocate(task_info.resource_requirements)
                scheduled.append(task_info)
        
        return scheduled


class CostOptimizedScheduler(SchedulerBase):
    """Cost-optimized scheduler for serverless platforms"""
    
    def __init__(self, cost_model: Optional[Dict[str, float]] = None):
        self.cost_model = cost_model or {
            'cpu_hour': 0.024,
            'memory_gb_hour': 0.003,
            'gpu_hour': 0.90,
            'data_transfer_gb': 0.09
        }
    
    def calculate_cost(self, task_info: TaskSchedulingInfo) -> float:
        """Calculate estimated cost for a task"""
        duration_hours = task_info.estimated_duration / 3600
        
        cpu_cost = (task_info.resource_requirements.get('cpu', 1) * 
                   self.cost_model['cpu_hour'] * duration_hours)
        memory_cost = (task_info.resource_requirements.get('memory', 0.5) * 
                      self.cost_model['memory_gb_hour'] * duration_hours)
        gpu_cost = (task_info.resource_requirements.get('gpu', 0) * 
                   self.cost_model['gpu_hour'] * duration_hours)
        
        return cpu_cost + memory_cost + gpu_cost
    
    async def schedule(self, 
                       tasks: List[TaskSchedulingInfo], 
                       resources: ResourcePool) -> List[TaskSchedulingInfo]:
        """Schedule tasks to minimize cost"""
        # Calculate cost for each task
        for task in tasks:
            task.estimated_cost = self.calculate_cost(task)
        
        # Sort by cost per unit of work (cost efficiency)
        sorted_tasks = sorted(tasks, 
                            key=lambda t: t.estimated_cost / max(t.priority, 0.1))
        
        scheduled = []
        for task_info in sorted_tasks:
            if resources.can_allocate(task_info.resource_requirements):
                resources.allocate(task_info.resource_requirements)
                scheduled.append(task_info)
        
        return scheduled


class DeadlineAwareScheduler(SchedulerBase):
    """Deadline-aware scheduler using EDF (Earliest Deadline First)"""
    
    async def schedule(self, 
                       tasks: List[TaskSchedulingInfo], 
                       resources: ResourcePool) -> List[TaskSchedulingInfo]:
        """Schedule tasks to meet deadlines"""
        current_time = datetime.now()
        
        # Separate tasks with and without deadlines
        deadline_tasks = [t for t in tasks if t.deadline]
        no_deadline_tasks = [t for t in tasks if not t.deadline]
        
        # Sort deadline tasks by deadline
        deadline_tasks.sort(key=lambda t: t.deadline)
        
        scheduled = []
        
        # Schedule deadline tasks first
        for task_info in deadline_tasks:
            if resources.can_allocate(task_info.resource_requirements):
                # Check if we can meet the deadline
                expected_completion = (current_time + 
                                     timedelta(seconds=task_info.estimated_duration))
                if expected_completion <= task_info.deadline:
                    resources.allocate(task_info.resource_requirements)
                    scheduled.append(task_info)
        
        # Then schedule non-deadline tasks
        for task_info in no_deadline_tasks:
            if resources.can_allocate(task_info.resource_requirements):
                resources.allocate(task_info.resource_requirements)
                scheduled.append(task_info)
        
        return scheduled


class LoadBalancedScheduler(SchedulerBase):
    """Load-balanced scheduler across multiple resource pools"""
    
    def __init__(self, resource_pools: List[ResourcePool]):
        self.resource_pools = resource_pools
        self.pool_loads = [0.0] * len(resource_pools)
    
    def calculate_load(self, pool: ResourcePool) -> float:
        """Calculate current load of a resource pool"""
        cpu_util = 1.0 - (pool.cpu_cores / 100.0)  # Assume max 100 cores
        mem_util = 1.0 - (pool.memory_gb / 1000.0)  # Assume max 1000 GB
        gpu_util = 1.0 - (pool.gpu_count / 10.0) if pool.gpu_count > 0 else 0
        
        return (cpu_util + mem_util + gpu_util) / 3
    
    async def schedule(self, 
                       tasks: List[TaskSchedulingInfo], 
                       resources: ResourcePool) -> List[TaskSchedulingInfo]:
        """Schedule tasks with load balancing"""
        scheduled = []
        
        for task_info in tasks:
            # Find the least loaded pool that can handle the task
            best_pool_idx = -1
            min_load = float('inf')
            
            for i, pool in enumerate(self.resource_pools):
                if pool.can_allocate(task_info.resource_requirements):
                    load = self.calculate_load(pool)
                    if load < min_load:
                        min_load = load
                        best_pool_idx = i
            
            if best_pool_idx >= 0:
                self.resource_pools[best_pool_idx].allocate(
                    task_info.resource_requirements
                )
                task_info.metadata = {'assigned_pool': best_pool_idx}
                scheduled.append(task_info)
        
        return scheduled


class MLBasedScheduler(SchedulerBase):
    """Machine Learning based scheduler using historical data"""
    
    def __init__(self):
        self.history = []
        self.model = None  # Placeholder for ML model
        
    def train_model(self, historical_data: List[Dict[str, Any]]):
        """Train ML model on historical execution data"""
        # In a real implementation, this would train a model
        # using scikit-learn, TensorFlow, or PyTorch
        
        # Extract features and labels from historical data
        features = []
        labels = []
        
        for record in historical_data:
            # Features: task characteristics
            feature = [
                record.get('cpu_requirement', 1),
                record.get('memory_requirement', 0.5),
                record.get('input_size', 0),
                record.get('dependency_count', 0),
                record.get('time_of_day', 12),
                record.get('day_of_week', 3)
            ]
            features.append(feature)
            
            # Label: actual execution time
            labels.append(record.get('actual_duration', 60))
        
        # Simplified: use mean as prediction
        if labels:
            self.mean_duration = np.mean(labels)
            self.std_duration = np.std(labels)
        else:
            self.mean_duration = 60
            self.std_duration = 10
    
    def predict_duration(self, task_info: TaskSchedulingInfo) -> float:
        """Predict task execution duration using ML model"""
        # Simplified prediction
        base_duration = self.mean_duration if hasattr(self, 'mean_duration') else 60
        
        # Adjust based on resource requirements
        cpu_factor = task_info.resource_requirements.get('cpu', 1)
        memory_factor = task_info.resource_requirements.get('memory', 0.5)
        
        predicted = base_duration * (0.7 * cpu_factor + 0.3 * memory_factor)
        
        # Add some randomness to simulate prediction uncertainty
        noise = random.gauss(0, self.std_duration if hasattr(self, 'std_duration') else 10)
        
        return max(1, predicted + noise)
    
    async def schedule(self, 
                       tasks: List[TaskSchedulingInfo], 
                       resources: ResourcePool) -> List[TaskSchedulingInfo]:
        """Schedule tasks using ML predictions"""
        # Predict duration for each task
        for task in tasks:
            task.estimated_duration = self.predict_duration(task)
        
        # Sort by predicted duration (SJF with ML predictions)
        sorted_tasks = sorted(tasks, key=lambda t: t.estimated_duration)
        
        scheduled = []
        for task_info in sorted_tasks:
            if resources.can_allocate(task_info.resource_requirements):
                resources.allocate(task_info.resource_requirements)
                scheduled.append(task_info)
        
        return scheduled


class HybridScheduler(SchedulerBase):
    """Hybrid scheduler that combines multiple strategies"""
    
    def __init__(self, strategies: List[Tuple[SchedulerBase, float]]):
        """
        Initialize with weighted strategies
        strategies: List of (scheduler, weight) tuples
        """
        self.strategies = strategies
        self.normalize_weights()
    
    def normalize_weights(self):
        """Normalize strategy weights to sum to 1"""
        total_weight = sum(weight for _, weight in self.strategies)
        self.strategies = [(scheduler, weight/total_weight) 
                          for scheduler, weight in self.strategies]
    
    async def schedule(self, 
                       tasks: List[TaskSchedulingInfo], 
                       resources: ResourcePool) -> List[TaskSchedulingInfo]:
        """Schedule using weighted combination of strategies"""
        # Get recommendations from each strategy
        all_recommendations = []
        
        for scheduler, weight in self.strategies:
            # Create a copy of resources for each scheduler
            resources_copy = ResourcePool(
                cpu_cores=resources.cpu_cores,
                memory_gb=resources.memory_gb,
                gpu_count=resources.gpu_count
            )
            
            recommendations = await scheduler.schedule(tasks.copy(), resources_copy)
            all_recommendations.append((recommendations, weight))
        
        # Score each task based on weighted recommendations
        task_scores = defaultdict(float)
        
        for recommendations, weight in all_recommendations:
            for i, task_info in enumerate(recommendations):
                # Higher position = higher score
                score = (len(recommendations) - i) * weight
                task_scores[task_info.task.id] += score
        
        # Sort tasks by combined score
        scored_tasks = [(task_info, task_scores[task_info.task.id]) 
                       for task_info in tasks]
        scored_tasks.sort(key=lambda x: x[1], reverse=True)
        
        # Schedule based on combined scores
        scheduled = []
        for task_info, _ in scored_tasks:
            if resources.can_allocate(task_info.resource_requirements):
                resources.allocate(task_info.resource_requirements)
                scheduled.append(task_info)
        
        return scheduled


class AdaptiveScheduler:
    """Adaptive scheduler that switches strategies based on workload"""
    
    def __init__(self):
        self.schedulers = {
            SchedulingStrategy.FIFO: FIFOScheduler(),
            SchedulingStrategy.PRIORITY: PriorityScheduler(),
            SchedulingStrategy.SJF: ShortestJobFirstScheduler(),
            SchedulingStrategy.COST_OPTIMIZED: CostOptimizedScheduler(),
            SchedulingStrategy.DEADLINE_AWARE: DeadlineAwareScheduler(),
            SchedulingStrategy.ML_BASED: MLBasedScheduler()
        }
        self.current_strategy = SchedulingStrategy.PRIORITY
        self.performance_history = defaultdict(list)
    
    def analyze_workload(self, tasks: List[TaskSchedulingInfo]) -> SchedulingStrategy:
        """Analyze workload characteristics and select best strategy"""
        # Check for deadline constraints
        deadline_tasks = sum(1 for t in tasks if t.deadline)
        if deadline_tasks > len(tasks) * 0.3:
            return SchedulingStrategy.DEADLINE_AWARE
        
        # Check for cost sensitivity (high resource requirements)
        high_resource_tasks = sum(
            1 for t in tasks 
            if t.resource_requirements.get('gpu', 0) > 0 or
               t.resource_requirements.get('memory', 0) > 10
        )
        if high_resource_tasks > len(tasks) * 0.2:
            return SchedulingStrategy.COST_OPTIMIZED
        
        # Check task duration variance
        if tasks:
            durations = [t.estimated_duration for t in tasks]
            variance = np.var(durations) if durations else 0
            if variance > 1000:  # High variance
                return SchedulingStrategy.SJF
        
        # Default to priority-based
        return SchedulingStrategy.PRIORITY
    
    async def schedule(self, 
                       tasks: List[TaskSchedulingInfo], 
                       resources: ResourcePool) -> List[TaskSchedulingInfo]:
        """Adaptively schedule tasks"""
        # Select strategy based on workload
        strategy = self.analyze_workload(tasks)
        
        if strategy != self.current_strategy:
            logger.info(f"Switching scheduling strategy from "
                       f"{self.current_strategy.value} to {strategy.value}")
            self.current_strategy = strategy
        
        # Use selected scheduler
        scheduler = self.schedulers[strategy]
        scheduled = await scheduler.schedule(tasks, resources)
        
        # Record performance for learning
        self.performance_history[strategy].append({
            'tasks_scheduled': len(scheduled),
            'tasks_total': len(tasks),
            'timestamp': datetime.now()
        })
        
        return scheduled
