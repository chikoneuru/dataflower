"""
Comparison Experiments for DataFlower
Benchmarks against other workflow orchestration systems
"""

import asyncio
import random
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json
import logging
import sys
from abc import ABC, abstractmethod

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.workflow_engine import Workflow, Task, WorkflowEngine, ServerlessExecutor
from src.scheduler import (
    SchedulingStrategy, AdaptiveScheduler, FIFOScheduler, 
    PriorityScheduler, CostOptimizedScheduler, MLBasedScheduler
)
from src.monitoring import MetricsCollector
from experiments.performance_experiments import WorkflowGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WorkflowSystem(ABC):
    """Abstract base class for workflow systems"""
    
    @abstractmethod
    async def execute_workflow(self, workflow: Workflow) -> Dict[str, Any]:
        """Execute a workflow and return metrics"""
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        """Get system name"""
        pass


class DataFlowerSystem(WorkflowSystem):
    """DataFlower implementation"""
    
    def __init__(self, scheduling_strategy: SchedulingStrategy = SchedulingStrategy.PRIORITY):
        self.name = f"DataFlower-{scheduling_strategy.value}"
        self.scheduling_strategy = scheduling_strategy
        self.executor = ServerlessExecutor(platform="local")
        self.engine = WorkflowEngine(executor=self.executor)
        
    async def execute_workflow(self, workflow: Workflow) -> Dict[str, Any]:
        """Execute workflow using DataFlower"""
        start_time = time.time()
        
        workflow_id = await self.engine.submit_workflow(workflow)
        
        # Wait for completion
        while True:
            status = self.engine.get_workflow_status(workflow_id)
            if status and status['status'] in ['completed', 'failed']:
                break
            await asyncio.sleep(0.5)
        
        end_time = time.time()
        
        return {
            'system': self.name,
            'workflow_id': workflow_id,
            'execution_time': end_time - start_time,
            'status': status['status'],
            'tasks_completed': status.get('tasks_status', {})
        }
    
    def get_name(self) -> str:
        return self.name


class SimpleDAGSystem(WorkflowSystem):
    """Simple DAG-based workflow system (baseline)"""
    
    def __init__(self):
        self.name = "SimpleDAG"
        
    async def execute_workflow(self, workflow: Workflow) -> Dict[str, Any]:
        """Execute workflow using simple DAG traversal"""
        start_time = time.time()
        
        # Convert to graph
        graph = workflow.to_graph()
        
        # Simple topological execution
        import networkx as nx
        try:
            topo_order = list(nx.topological_sort(graph))
        except nx.NetworkXError:
            return {
                'system': self.name,
                'execution_time': 0,
                'status': 'failed',
                'error': 'Not a DAG'
            }
        
        # Execute tasks sequentially (no parallelism)
        completed_tasks = 0
        for task_id in topo_order:
            # Simulate task execution
            await asyncio.sleep(random.uniform(0.05, 0.15))
            completed_tasks += 1
        
        end_time = time.time()
        
        return {
            'system': self.name,
            'execution_time': end_time - start_time,
            'status': 'completed',
            'tasks_completed': completed_tasks
        }
    
    def get_name(self) -> str:
        return self.name


class ParallelExecutorSystem(WorkflowSystem):
    """Parallel execution system with fixed concurrency"""
    
    def __init__(self, max_concurrency: int = 10):
        self.name = f"ParallelExecutor-{max_concurrency}"
        self.max_concurrency = max_concurrency
        
    async def execute_workflow(self, workflow: Workflow) -> Dict[str, Any]:
        """Execute workflow with parallel task execution"""
        start_time = time.time()
        
        graph = workflow.to_graph()
        import networkx as nx
        
        completed = set()
        in_progress = set()
        
        while len(completed) < len(workflow.tasks):
            # Find ready tasks
            ready = []
            for task in workflow.tasks:
                if task.id not in completed and task.id not in in_progress:
                    # Check dependencies
                    if all(dep in completed for dep in task.dependencies):
                        ready.append(task)
            
            # Execute ready tasks up to concurrency limit
            batch = ready[:self.max_concurrency - len(in_progress)]
            
            if batch:
                # Start batch execution
                tasks = []
                for task in batch:
                    in_progress.add(task.id)
                    tasks.append(self._execute_task(task))
                
                # Wait for batch to complete
                await asyncio.gather(*tasks)
                
                # Mark as completed
                for task in batch:
                    in_progress.remove(task.id)
                    completed.add(task.id)
            else:
                await asyncio.sleep(0.1)
        
        end_time = time.time()
        
        return {
            'system': self.name,
            'execution_time': end_time - start_time,
            'status': 'completed',
            'tasks_completed': len(completed)
        }
    
    async def _execute_task(self, task: Task):
        """Simulate task execution"""
        await asyncio.sleep(random.uniform(0.05, 0.15))
    
    def get_name(self) -> str:
        return self.name


class QueueBasedSystem(WorkflowSystem):
    """Queue-based workflow system (like Celery)"""
    
    def __init__(self, num_workers: int = 5):
        self.name = f"QueueBased-{num_workers}"
        self.num_workers = num_workers
        
    async def execute_workflow(self, workflow: Workflow) -> Dict[str, Any]:
        """Execute workflow using queue-based approach"""
        start_time = time.time()
        
        # Create task queue
        task_queue = asyncio.Queue()
        completed = set()
        
        # Add all tasks to queue (respecting dependencies)
        graph = workflow.to_graph()
        
        # Worker coroutine
        async def worker(worker_id: int):
            while True:
                try:
                    task = await asyncio.wait_for(task_queue.get(), timeout=1.0)
                    # Check if dependencies are met
                    if all(dep in completed for dep in task.dependencies):
                        # Execute task
                        await asyncio.sleep(random.uniform(0.05, 0.15))
                        completed.add(task.id)
                    else:
                        # Re-queue if dependencies not met
                        await task_queue.put(task)
                        await asyncio.sleep(0.05)
                except asyncio.TimeoutError:
                    if len(completed) == len(workflow.tasks):
                        break
        
        # Add initial tasks
        for task in workflow.tasks:
            await task_queue.put(task)
        
        # Start workers
        workers = [asyncio.create_task(worker(i)) for i in range(self.num_workers)]
        
        # Wait for all tasks to complete
        await asyncio.gather(*workers)
        
        end_time = time.time()
        
        return {
            'system': self.name,
            'execution_time': end_time - start_time,
            'status': 'completed',
            'tasks_completed': len(completed)
        }
    
    def get_name(self) -> str:
        return self.name


class ComparisonExperiment:
    """Compare different workflow systems"""
    
    def __init__(self, name: str, output_dir: str = "results"):
        self.name = name
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.results = []
        
    async def run(self,
                  systems: List[WorkflowSystem],
                  workflow_types: List[str] = ['linear', 'parallel', 'dag', 'mapreduce'],
                  workflow_sizes: List[int] = [10, 20, 50, 100],
                  num_iterations: int = 10):
        """Run comparison experiment"""
        logger.info(f"Starting Comparison Experiment with {len(systems)} systems")
        
        for workflow_type in workflow_types:
            for workflow_size in workflow_sizes:
                logger.info(f"Testing {workflow_type} workflow with {workflow_size} tasks")
                
                for iteration in range(num_iterations):
                    # Generate workflow
                    if workflow_type == 'linear':
                        workflow = WorkflowGenerator.generate_linear_workflow(workflow_size)
                    elif workflow_type == 'parallel':
                        workflow = WorkflowGenerator.generate_parallel_workflow(
                            num_parallel=min(5, workflow_size // 4),
                            depth=workflow_size // 5
                        )
                    elif workflow_type == 'dag':
                        workflow = WorkflowGenerator.generate_dag_workflow(workflow_size)
                    else:  # mapreduce
                        workflow = WorkflowGenerator.generate_mapreduce_workflow(
                            num_mappers=workflow_size // 3,
                            num_reducers=workflow_size // 6
                        )
                    
                    # Test each system
                    for system in systems:
                        result = await system.execute_workflow(workflow)
                        
                        self.results.append({
                            'system': system.get_name(),
                            'workflow_type': workflow_type,
                            'workflow_size': workflow_size,
                            'iteration': iteration,
                            'execution_time': result['execution_time'],
                            'status': result['status']
                        })
        
        self.save_results()
        self.plot_comparison_results()
    
    def save_results(self):
        """Save experiment results"""
        output_file = self.output_dir / f"{self.name}_results.json"
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        logger.info(f"Results saved to {output_file}")
    
    def plot_comparison_results(self):
        """Plot comparison results"""
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        # Set style
        sns.set_style("whitegrid")
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 12))
        fig.suptitle('Workflow System Comparison', fontsize=16)
        
        # Performance by workflow type
        ax = axes[0, 0]
        pivot_data = df.pivot_table(
            values='execution_time', 
            index='workflow_type', 
            columns='system', 
            aggfunc='mean'
        )
        pivot_data.plot(kind='bar', ax=ax)
        ax.set_title('Average Execution Time by Workflow Type')
        ax.set_xlabel('Workflow Type')
        ax.set_ylabel('Execution Time (s)')
        ax.legend(title='System', bbox_to_anchor=(1.05, 1), loc='upper left')
        
        # Scalability comparison
        ax = axes[0, 1]
        for system in df['system'].unique():
            system_data = df[df['system'] == system]
            avg_times = system_data.groupby('workflow_size')['execution_time'].mean()
            ax.plot(avg_times.index, avg_times.values, marker='o', label=system)
        ax.set_title('Scalability Comparison')
        ax.set_xlabel('Workflow Size (tasks)')
        ax.set_ylabel('Execution Time (s)')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Performance distribution
        ax = axes[1, 0]
        sns.violinplot(data=df, x='system', y='execution_time', ax=ax)
        ax.set_title('Execution Time Distribution')
        ax.set_xlabel('System')
        ax.set_ylabel('Execution Time (s)')
        ax.tick_params(axis='x', rotation=45)
        
        # Relative performance (normalized to SimpleDAG)
        ax = axes[1, 1]
        baseline_system = 'SimpleDAG'
        if baseline_system in df['system'].values:
            baseline_data = df[df['system'] == baseline_system].groupby('workflow_size')['execution_time'].mean()
            
            for system in df['system'].unique():
                if system != baseline_system:
                    system_data = df[df['system'] == system].groupby('workflow_size')['execution_time'].mean()
                    relative_perf = baseline_data / system_data
                    ax.plot(relative_perf.index, relative_perf.values, marker='s', label=system)
            
            ax.axhline(y=1, color='gray', linestyle='--', alpha=0.7)
            ax.set_title(f'Relative Performance (vs {baseline_system})')
            ax.set_xlabel('Workflow Size (tasks)')
            ax.set_ylabel('Speedup Factor')
            ax.legend()
            ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        output_file = self.output_dir / f"{self.name}_plots.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Plots saved to {output_file}")
        
        # Additional detailed analysis
        self.plot_detailed_analysis(df)
    
    def plot_detailed_analysis(self, df: pd.DataFrame):
        """Create detailed analysis plots"""
        fig, axes = plt.subplots(2, 3, figsize=(16, 10))
        fig.suptitle('Detailed System Analysis', fontsize=16)
        
        # Heatmap of performance
        ax = axes[0, 0]
        pivot = df.pivot_table(
            values='execution_time',
            index='workflow_type',
            columns='system',
            aggfunc='mean'
        )
        sns.heatmap(pivot, annot=True, fmt='.2f', cmap='YlOrRd', ax=ax)
        ax.set_title('Performance Heatmap')
        
        # Success rate by system
        ax = axes[0, 1]
        success_rates = df.groupby('system').apply(
            lambda x: (x['status'] == 'completed').mean() * 100
        )
        ax.bar(success_rates.index, success_rates.values)
        ax.set_title('Success Rate by System')
        ax.set_xlabel('System')
        ax.set_ylabel('Success Rate (%)')
        ax.tick_params(axis='x', rotation=45)
        
        # Coefficient of variation (consistency)
        ax = axes[0, 2]
        cv_data = df.groupby('system')['execution_time'].agg(['mean', 'std'])
        cv_data['cv'] = cv_data['std'] / cv_data['mean']
        ax.bar(cv_data.index, cv_data['cv'])
        ax.set_title('Execution Time Consistency')
        ax.set_xlabel('System')
        ax.set_ylabel('Coefficient of Variation')
        ax.tick_params(axis='x', rotation=45)
        
        # Performance by workflow size (box plot)
        ax = axes[1, 0]
        df_subset = df[df['workflow_size'].isin([20, 50])]
        sns.boxplot(data=df_subset, x='workflow_size', y='execution_time', hue='system', ax=ax)
        ax.set_title('Performance Distribution by Size')
        ax.set_xlabel('Workflow Size')
        ax.set_ylabel('Execution Time (s)')
        
        # Scaling efficiency
        ax = axes[1, 1]
        for system in df['system'].unique():
            system_data = df[df['system'] == system]
            sizes = sorted(system_data['workflow_size'].unique())
            if len(sizes) > 1:
                times = [system_data[system_data['workflow_size'] == s]['execution_time'].mean() 
                        for s in sizes]
                # Calculate scaling factor
                scaling_factors = [times[i] / times[0] / (sizes[i] / sizes[0]) 
                                 for i in range(len(sizes))]
                ax.plot(sizes, scaling_factors, marker='o', label=system)
        
        ax.axhline(y=1, color='gray', linestyle='--', alpha=0.7)
        ax.set_title('Scaling Efficiency')
        ax.set_xlabel('Workflow Size')
        ax.set_ylabel('Scaling Factor (1.0 = Linear)')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Ranking table
        ax = axes[1, 2]
        ax.axis('tight')
        ax.axis('off')
        
        # Calculate rankings
        rankings = []
        for workflow_type in df['workflow_type'].unique():
            type_data = df[df['workflow_type'] == workflow_type]
            avg_times = type_data.groupby('system')['execution_time'].mean().sort_values()
            for rank, (system, time) in enumerate(avg_times.items(), 1):
                rankings.append({
                    'Workflow Type': workflow_type,
                    'Rank': rank,
                    'System': system,
                    'Avg Time': f"{time:.2f}s"
                })
        
        rankings_df = pd.DataFrame(rankings[:12])  # Show top results
        table = ax.table(cellText=rankings_df.values,
                        colLabels=rankings_df.columns,
                        cellLoc='center',
                        loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        ax.set_title('System Rankings', pad=20)
        
        plt.tight_layout()
        output_file = self.output_dir / f"{self.name}_detailed_analysis.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Detailed analysis saved to {output_file}")


class SchedulerComparisonExperiment:
    """Compare different scheduling strategies"""
    
    def __init__(self, output_dir: str = "results"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.results = []
        
    async def run(self,
                  strategies: List[SchedulingStrategy],
                  workload_patterns: List[str] = ['uniform', 'bursty', 'mixed'],
                  num_workflows: int = 50):
        """Run scheduler comparison"""
        logger.info("Starting Scheduler Comparison Experiment")
        
        for pattern in workload_patterns:
            logger.info(f"Testing {pattern} workload pattern")
            
            # Generate workload based on pattern
            workflows = self._generate_workload(pattern, num_workflows)
            
            for strategy in strategies:
                logger.info(f"Testing {strategy.value} scheduling strategy")
                
                # Create DataFlower system with specific scheduler
                system = DataFlowerSystem(scheduling_strategy=strategy)
                
                # Run workflows
                start_time = time.time()
                results = []
                
                for workflow in workflows:
                    result = await system.execute_workflow(workflow)
                    results.append(result)
                
                end_time = time.time()
                
                # Calculate metrics
                total_time = end_time - start_time
                successful = sum(1 for r in results if r['status'] == 'completed')
                avg_time = np.mean([r['execution_time'] for r in results])
                
                self.results.append({
                    'strategy': strategy.value,
                    'workload_pattern': pattern,
                    'num_workflows': num_workflows,
                    'total_time': total_time,
                    'throughput': num_workflows / total_time,
                    'success_rate': successful / num_workflows,
                    'avg_execution_time': avg_time
                })
        
        self.save_results()
        self.plot_scheduler_comparison()
    
    def _generate_workload(self, pattern: str, num_workflows: int) -> List[Workflow]:
        """Generate workload based on pattern"""
        workflows = []
        
        if pattern == 'uniform':
            # Uniform workload
            for i in range(num_workflows):
                size = random.randint(15, 25)
                workflow = WorkflowGenerator.generate_dag_workflow(size)
                workflows.append(workflow)
                
        elif pattern == 'bursty':
            # Bursty workload (alternating small and large)
            for i in range(num_workflows):
                if i % 10 < 2:  # 20% large workflows
                    size = random.randint(50, 100)
                else:
                    size = random.randint(5, 15)
                workflow = WorkflowGenerator.generate_dag_workflow(size)
                workflows.append(workflow)
                
        else:  # mixed
            # Mixed workload types
            for i in range(num_workflows):
                workflow_type = random.choice(['linear', 'parallel', 'dag'])
                size = random.randint(10, 40)
                
                if workflow_type == 'linear':
                    workflow = WorkflowGenerator.generate_linear_workflow(size)
                elif workflow_type == 'parallel':
                    workflow = WorkflowGenerator.generate_parallel_workflow(4, size // 4)
                else:
                    workflow = WorkflowGenerator.generate_dag_workflow(size)
                
                workflows.append(workflow)
        
        return workflows
    
    def save_results(self):
        """Save results"""
        output_file = self.output_dir / "scheduler_comparison_results.json"
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        logger.info(f"Results saved to {output_file}")
    
    def plot_scheduler_comparison(self):
        """Plot scheduler comparison results"""
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('Scheduler Strategy Comparison', fontsize=16)
        
        # Throughput by strategy
        ax = axes[0, 0]
        pivot = df.pivot_table(
            values='throughput',
            index='strategy',
            columns='workload_pattern',
            aggfunc='mean'
        )
        pivot.plot(kind='bar', ax=ax)
        ax.set_title('Throughput by Strategy')
        ax.set_xlabel('Scheduling Strategy')
        ax.set_ylabel('Throughput (workflows/s)')
        ax.legend(title='Workload Pattern')
        
        # Average execution time
        ax = axes[0, 1]
        pivot = df.pivot_table(
            values='avg_execution_time',
            index='strategy',
            columns='workload_pattern',
            aggfunc='mean'
        )
        pivot.plot(kind='bar', ax=ax)
        ax.set_title('Average Execution Time')
        ax.set_xlabel('Scheduling Strategy')
        ax.set_ylabel('Execution Time (s)')
        ax.legend(title='Workload Pattern')
        
        # Success rate
        ax = axes[1, 0]
        pivot = df.pivot_table(
            values='success_rate',
            index='strategy',
            columns='workload_pattern',
            aggfunc='mean'
        )
        pivot.plot(kind='bar', ax=ax)
        ax.set_title('Success Rate')
        ax.set_xlabel('Scheduling Strategy')
        ax.set_ylabel('Success Rate')
        ax.legend(title='Workload Pattern')
        
        # Overall efficiency
        ax = axes[1, 1]
        df['efficiency'] = df['throughput'] * df['success_rate']
        pivot = df.pivot_table(
            values='efficiency',
            index='strategy',
            columns='workload_pattern',
            aggfunc='mean'
        )
        sns.heatmap(pivot, annot=True, fmt='.3f', cmap='Greens', ax=ax)
        ax.set_title('Overall Efficiency (Throughput × Success Rate)')
        
        plt.tight_layout()
        output_file = self.output_dir / "scheduler_comparison_plots.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Plots saved to {output_file}")


async def main():
    """Run all comparison experiments"""
    logger.info("Starting DataFlower Comparison Experiments")
    
    # System comparison
    systems = [
        SimpleDAGSystem(),
        QueueBasedSystem(num_workers=5),
        ParallelExecutorSystem(max_concurrency=10),
        DataFlowerSystem(scheduling_strategy=SchedulingStrategy.PRIORITY),
        DataFlowerSystem(scheduling_strategy=SchedulingStrategy.COST_OPTIMIZED),
        DataFlowerSystem(scheduling_strategy=SchedulingStrategy.ML_BASED)
    ]
    
    comparison_exp = ComparisonExperiment("system_comparison")
    await comparison_exp.run(
        systems=systems,
        workflow_types=['linear', 'parallel', 'dag'],
        workflow_sizes=[20, 50],
        num_iterations=5
    )
    
    # Scheduler comparison
    scheduler_exp = SchedulerComparisonExperiment()
    await scheduler_exp.run(
        strategies=[
            SchedulingStrategy.FIFO,
            SchedulingStrategy.PRIORITY,
            SchedulingStrategy.SJF,
            SchedulingStrategy.COST_OPTIMIZED
        ],
        workload_patterns=['uniform', 'bursty', 'mixed'],
        num_workflows=30
    )
    
    logger.info("All comparison experiments completed")


if __name__ == "__main__":
    asyncio.run(main())
