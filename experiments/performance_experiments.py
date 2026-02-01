"""
Performance Benchmarking Experiments for DataFlower
Tests throughput, latency, and resource efficiency
"""

import asyncio
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json
import logging
import sys

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.workflow_engine import (
    Workflow, Task, WorkflowEngine, ServerlessExecutor, StateManager
)
from src.scheduler import (
    SchedulingStrategy, AdaptiveScheduler, TaskSchedulingInfo,
    ResourcePool, FIFOScheduler, PriorityScheduler, ShortestJobFirstScheduler,
    CostOptimizedScheduler, DeadlineAwareScheduler, MLBasedScheduler
)
from src.monitoring import MetricsCollector, PerformanceMonitor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WorkflowGenerator:
    """Generates synthetic workflows for testing"""
    
    @staticmethod
    def generate_linear_workflow(num_tasks: int) -> Workflow:
        """Generate a linear chain workflow"""
        tasks = []
        for i in range(num_tasks):
            task = Task(
                id=f"task_{i}",
                name=f"Linear Task {i}",
                function="data_transformation",
                inputs={"index": i},
                dependencies=[f"task_{i-1}"] if i > 0 else [],
                resource_requirements={
                    "cpu": random.uniform(0.5, 2.0),
                    "memory": random.uniform(0.5, 4.0)
                }
            )
            tasks.append(task)
        
        return Workflow(
            name="Linear Workflow",
            description=f"Linear chain of {num_tasks} tasks",
            tasks=tasks
        )
    
    @staticmethod
    def generate_parallel_workflow(num_parallel: int, depth: int) -> Workflow:
        """Generate a workflow with parallel branches"""
        tasks = []
        task_id = 0
        
        for level in range(depth):
            level_tasks = []
            for branch in range(num_parallel):
                task = Task(
                    id=f"task_{task_id}",
                    name=f"Parallel Task L{level}B{branch}",
                    function="data_processing",
                    inputs={"level": level, "branch": branch},
                    dependencies=[f"task_{i}" for i in range(max(0, task_id - num_parallel), task_id)] 
                                if level > 0 else [],
                    resource_requirements={
                        "cpu": random.uniform(0.5, 2.0),
                        "memory": random.uniform(0.5, 4.0)
                    }
                )
                tasks.append(task)
                level_tasks.append(f"task_{task_id}")
                task_id += 1
        
        return Workflow(
            name="Parallel Workflow",
            description=f"Parallel workflow with {num_parallel} branches and depth {depth}",
            tasks=tasks
        )
    
    @staticmethod
    def generate_dag_workflow(num_tasks: int, edge_probability: float = 0.3) -> Workflow:
        """Generate a random DAG workflow"""
        tasks = []
        
        for i in range(num_tasks):
            # Random dependencies from earlier tasks
            dependencies = []
            for j in range(i):
                if random.random() < edge_probability:
                    dependencies.append(f"task_{j}")
            
            task = Task(
                id=f"task_{i}",
                name=f"DAG Task {i}",
                function=random.choice(["data_ingestion", "data_transformation", 
                                       "data_validation", "data_aggregation"]),
                inputs={"index": i},
                dependencies=dependencies[:3],  # Limit to 3 dependencies
                resource_requirements={
                    "cpu": random.uniform(0.5, 4.0),
                    "memory": random.uniform(0.5, 8.0),
                    "gpu": 1 if random.random() < 0.1 else 0
                }
            )
            tasks.append(task)
        
        return Workflow(
            name="DAG Workflow",
            description=f"Random DAG with {num_tasks} tasks",
            tasks=tasks
        )
    
    @staticmethod
    def generate_mapreduce_workflow(num_mappers: int, num_reducers: int) -> Workflow:
        """Generate a MapReduce-style workflow"""
        tasks = []
        
        # Mapper tasks
        for i in range(num_mappers):
            task = Task(
                id=f"mapper_{i}",
                name=f"Mapper {i}",
                function="data_transformation",
                inputs={"mapper_id": i, "data_shard": i},
                dependencies=[],
                resource_requirements={
                    "cpu": 1.0,
                    "memory": 2.0
                }
            )
            tasks.append(task)
        
        # Shuffler task
        shuffler = Task(
            id="shuffler",
            name="Shuffler",
            function="data_aggregation",
            inputs={"operation": "shuffle"},
            dependencies=[f"mapper_{i}" for i in range(num_mappers)],
            resource_requirements={
                "cpu": 2.0,
                "memory": 4.0
            }
        )
        tasks.append(shuffler)
        
        # Reducer tasks
        for i in range(num_reducers):
            task = Task(
                id=f"reducer_{i}",
                name=f"Reducer {i}",
                function="data_aggregation",
                inputs={"reducer_id": i},
                dependencies=["shuffler"],
                resource_requirements={
                    "cpu": 1.5,
                    "memory": 3.0
                }
            )
            tasks.append(task)
        
        # Final aggregator
        final = Task(
            id="final_aggregator",
            name="Final Aggregator",
            function="data_aggregation",
            inputs={"operation": "final"},
            dependencies=[f"reducer_{i}" for i in range(num_reducers)],
            resource_requirements={
                "cpu": 2.0,
                "memory": 4.0
            }
        )
        tasks.append(final)
        
        return Workflow(
            name="MapReduce Workflow",
            description=f"MapReduce with {num_mappers} mappers and {num_reducers} reducers",
            tasks=tasks
        )


class PerformanceExperiment:
    """Base class for performance experiments"""
    
    def __init__(self, name: str, output_dir: str = "results"):
        self.name = name
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.results = []
        self.metrics_collector = MetricsCollector()
        
    async def run_workflow(self, 
                          workflow: Workflow, 
                          engine: WorkflowEngine) -> Dict[str, Any]:
        """Run a single workflow and collect metrics"""
        start_time = time.time()
        
        # Submit workflow
        workflow_id = await engine.submit_workflow(workflow)
        
        # Wait for completion
        while True:
            status = engine.get_workflow_status(workflow_id)
            if status and status['status'] in ['completed', 'failed', 'cancelled']:
                break
            await asyncio.sleep(0.5)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Collect metrics
        workflow_metrics = self.metrics_collector.get_workflow_metrics(workflow_id)
        
        result = {
            'workflow_id': workflow_id,
            'workflow_type': workflow.name,
            'num_tasks': len(workflow.tasks),
            'execution_time': execution_time,
            'status': status['status'],
            'timestamp': datetime.now().isoformat()
        }
        
        if workflow_metrics:
            result.update({
                'completed_tasks': workflow_metrics.completed_tasks,
                'failed_tasks': workflow_metrics.failed_tasks,
                'avg_task_time': np.mean(workflow_metrics.task_execution_times) 
                                if workflow_metrics.task_execution_times else 0
            })
        
        return result
    
    def save_results(self):
        """Save experiment results to file"""
        output_file = self.output_dir / f"{self.name}_results.json"
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        logger.info(f"Results saved to {output_file}")
    
    def plot_results(self):
        """Generate plots for experiment results"""
        if not self.results:
            logger.warning("No results to plot")
            return
        
        df = pd.DataFrame(self.results)
        
        # Create subplots
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle(f"{self.name} Results", fontsize=16)
        
        # Plot 1: Execution time by workflow type
        if 'workflow_type' in df.columns:
            ax = axes[0, 0]
            df.boxplot(column='execution_time', by='workflow_type', ax=ax)
            ax.set_title('Execution Time by Workflow Type')
            ax.set_xlabel('Workflow Type')
            ax.set_ylabel('Execution Time (s)')
        
        # Plot 2: Task completion rate
        if 'completed_tasks' in df.columns and 'num_tasks' in df.columns:
            ax = axes[0, 1]
            df['completion_rate'] = df['completed_tasks'] / df['num_tasks']
            ax.hist(df['completion_rate'], bins=20, edgecolor='black')
            ax.set_title('Task Completion Rate Distribution')
            ax.set_xlabel('Completion Rate')
            ax.set_ylabel('Frequency')
        
        # Plot 3: Execution time over time
        if 'timestamp' in df.columns:
            ax = axes[1, 0]
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            ax.plot(df.index, df['execution_time'], marker='o')
            ax.set_title('Execution Time Over Experiment')
            ax.set_xlabel('Workflow Number')
            ax.set_ylabel('Execution Time (s)')
        
        # Plot 4: Task time distribution
        if 'avg_task_time' in df.columns:
            ax = axes[1, 1]
            ax.hist(df['avg_task_time'], bins=20, edgecolor='black')
            ax.set_title('Average Task Time Distribution')
            ax.set_xlabel('Average Task Time (s)')
            ax.set_ylabel('Frequency')
        
        plt.tight_layout()
        output_file = self.output_dir / f"{self.name}_plots.png"
        plt.savefig(output_file)
        plt.close()
        logger.info(f"Plots saved to {output_file}")


class ThroughputExperiment(PerformanceExperiment):
    """Experiment to measure system throughput"""
    
    async def run(self, 
                  num_workflows: int = 100,
                  workflow_sizes: List[int] = [10, 20, 50, 100],
                  platforms: List[str] = ["local"]):
        """Run throughput experiment"""
        logger.info(f"Starting Throughput Experiment with {num_workflows} workflows")
        
        for platform in platforms:
            executor = ServerlessExecutor(platform=platform)
            engine = WorkflowEngine(executor=executor)
            
            for size in workflow_sizes:
                logger.info(f"Testing workflow size: {size} on platform: {platform}")
                
                # Generate workflows
                workflows = []
                for i in range(num_workflows // len(workflow_sizes)):
                    workflow_type = random.choice(['linear', 'parallel', 'dag'])
                    
                    if workflow_type == 'linear':
                        workflow = WorkflowGenerator.generate_linear_workflow(size)
                    elif workflow_type == 'parallel':
                        workflow = WorkflowGenerator.generate_parallel_workflow(
                            num_parallel=min(5, size // 2),
                            depth=size // 5
                        )
                    else:
                        workflow = WorkflowGenerator.generate_dag_workflow(size)
                    
                    workflows.append(workflow)
                
                # Run workflows and measure throughput
                start_time = time.time()
                tasks = [self.run_workflow(wf, engine) for wf in workflows]
                results = await asyncio.gather(*tasks)
                end_time = time.time()
                
                total_time = end_time - start_time
                throughput = len(workflows) / total_time
                
                # Record results
                for result in results:
                    result.update({
                        'platform': platform,
                        'workflow_size': size,
                        'throughput': throughput
                    })
                    self.results.append(result)
                
                logger.info(f"Throughput: {throughput:.2f} workflows/second")
        
        self.save_results()
        self.plot_results()


class LatencyExperiment(PerformanceExperiment):
    """Experiment to measure task and workflow latency"""
    
    async def run(self,
                  num_iterations: int = 50,
                  workflow_types: List[str] = ['linear', 'parallel', 'dag', 'mapreduce']):
        """Run latency experiment"""
        logger.info(f"Starting Latency Experiment with {num_iterations} iterations")
        
        executor = ServerlessExecutor(platform="local")
        engine = WorkflowEngine(executor=executor)
        
        for workflow_type in workflow_types:
            logger.info(f"Testing workflow type: {workflow_type}")
            
            latencies = []
            
            for i in range(num_iterations):
                # Generate workflow
                if workflow_type == 'linear':
                    workflow = WorkflowGenerator.generate_linear_workflow(20)
                elif workflow_type == 'parallel':
                    workflow = WorkflowGenerator.generate_parallel_workflow(5, 4)
                elif workflow_type == 'dag':
                    workflow = WorkflowGenerator.generate_dag_workflow(20)
                else:  # mapreduce
                    workflow = WorkflowGenerator.generate_mapreduce_workflow(10, 5)
                
                # Measure end-to-end latency
                result = await self.run_workflow(workflow, engine)
                
                result['workflow_type'] = workflow_type
                result['iteration'] = i
                
                latencies.append(result['execution_time'])
                self.results.append(result)
            
            # Calculate latency statistics
            p50 = np.percentile(latencies, 50)
            p95 = np.percentile(latencies, 95)
            p99 = np.percentile(latencies, 99)
            
            logger.info(f"{workflow_type} - P50: {p50:.3f}s, P95: {p95:.3f}s, P99: {p99:.3f}s")
        
        self.save_results()
        self.plot_latency_distribution()
    
    def plot_latency_distribution(self):
        """Plot latency distribution"""
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        plt.figure(figsize=(12, 6))
        
        # Violin plot for latency distribution
        plt.subplot(1, 2, 1)
        if 'workflow_type' in df.columns:
            sns.violinplot(data=df, x='workflow_type', y='execution_time')
            plt.title('Latency Distribution by Workflow Type')
            plt.xlabel('Workflow Type')
            plt.ylabel('Latency (s)')
            plt.xticks(rotation=45)
        
        # CDF plot
        plt.subplot(1, 2, 2)
        for workflow_type in df['workflow_type'].unique():
            wf_data = df[df['workflow_type'] == workflow_type]['execution_time'].sort_values()
            y = np.arange(1, len(wf_data) + 1) / len(wf_data)
            plt.plot(wf_data, y, label=workflow_type, marker='o', markersize=2)
        
        plt.title('Latency CDF')
        plt.xlabel('Latency (s)')
        plt.ylabel('Cumulative Probability')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        output_file = self.output_dir / f"{self.name}_latency_distribution.png"
        plt.savefig(output_file)
        plt.close()
        logger.info(f"Latency distribution plot saved to {output_file}")


class ResourceEfficiencyExperiment(PerformanceExperiment):
    """Experiment to measure resource utilization efficiency"""
    
    async def run(self,
                  num_workflows: int = 30,
                  resource_configs: List[Dict[str, Any]] = None):
        """Run resource efficiency experiment"""
        logger.info("Starting Resource Efficiency Experiment")
        
        if resource_configs is None:
            resource_configs = [
                {'cpu_cores': 4, 'memory_gb': 8},
                {'cpu_cores': 8, 'memory_gb': 16},
                {'cpu_cores': 16, 'memory_gb': 32},
                {'cpu_cores': 32, 'memory_gb': 64}
            ]
        
        for config in resource_configs:
            logger.info(f"Testing resource config: {config}")
            
            # Create resource pool
            resource_pool = ResourcePool(**config)
            
            # Create engine with resource constraints
            executor = ServerlessExecutor(platform="local")
            engine = WorkflowEngine(executor=executor)
            
            # Generate mixed workload
            workflows = []
            for i in range(num_workflows):
                workflow_type = random.choice(['linear', 'parallel', 'dag'])
                size = random.randint(10, 50)
                
                if workflow_type == 'linear':
                    workflow = WorkflowGenerator.generate_linear_workflow(size)
                elif workflow_type == 'parallel':
                    workflow = WorkflowGenerator.generate_parallel_workflow(
                        num_parallel=min(5, size // 3),
                        depth=size // 5
                    )
                else:
                    workflow = WorkflowGenerator.generate_dag_workflow(size)
                
                workflows.append(workflow)
            
            # Run workflows and monitor resource usage
            start_time = time.time()
            
            # Track resource utilization
            utilization_samples = []
            monitor_task = asyncio.create_task(
                self._monitor_resources(resource_pool, utilization_samples)
            )
            
            # Run workflows
            tasks = [self.run_workflow(wf, engine) for wf in workflows]
            results = await asyncio.gather(*tasks)
            
            # Stop monitoring
            monitor_task.cancel()
            
            end_time = time.time()
            
            # Calculate efficiency metrics
            avg_utilization = np.mean(utilization_samples) if utilization_samples else 0
            total_time = end_time - start_time
            throughput = len(workflows) / total_time
            
            # Record results
            for result in results:
                result.update({
                    'resource_config': str(config),
                    'cpu_cores': config['cpu_cores'],
                    'memory_gb': config['memory_gb'],
                    'avg_utilization': avg_utilization,
                    'throughput': throughput
                })
                self.results.append(result)
            
            logger.info(f"Average utilization: {avg_utilization:.2%}, "
                       f"Throughput: {throughput:.2f} wf/s")
        
        self.save_results()
        self.plot_efficiency_results()
    
    async def _monitor_resources(self, resource_pool: ResourcePool, samples: List[float]):
        """Monitor resource utilization"""
        while True:
            try:
                # Calculate current utilization
                cpu_util = 1.0 - (resource_pool.cpu_cores / 32.0)  # Assume max 32 cores
                mem_util = 1.0 - (resource_pool.memory_gb / 64.0)  # Assume max 64 GB
                
                utilization = (cpu_util + mem_util) / 2
                samples.append(utilization)
                
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
    
    def plot_efficiency_results(self):
        """Plot resource efficiency results"""
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        plt.figure(figsize=(12, 6))
        
        # Efficiency vs resources
        plt.subplot(1, 2, 1)
        if 'cpu_cores' in df.columns and 'avg_utilization' in df.columns:
            grouped = df.groupby('cpu_cores')['avg_utilization'].mean()
            plt.bar(grouped.index.astype(str), grouped.values)
            plt.title('Resource Utilization by Configuration')
            plt.xlabel('CPU Cores')
            plt.ylabel('Average Utilization')
        
        # Throughput vs resources
        plt.subplot(1, 2, 2)
        if 'cpu_cores' in df.columns and 'throughput' in df.columns:
            grouped = df.groupby('cpu_cores')['throughput'].mean()
            plt.plot(grouped.index, grouped.values, marker='o', markersize=10)
            plt.title('Throughput vs Resources')
            plt.xlabel('CPU Cores')
            plt.ylabel('Throughput (workflows/s)')
            plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        output_file = self.output_dir / f"{self.name}_efficiency.png"
        plt.savefig(output_file)
        plt.close()
        logger.info(f"Efficiency plots saved to {output_file}")


async def main():
    """Run all performance experiments"""
    logger.info("Starting DataFlower Performance Experiments")
    
    # Throughput experiment
    throughput_exp = ThroughputExperiment("throughput")
    await throughput_exp.run(
        num_workflows=40,
        workflow_sizes=[10, 20, 30],
        platforms=["local"]
    )
    
    # Latency experiment
    latency_exp = LatencyExperiment("latency")
    await latency_exp.run(
        num_iterations=20,
        workflow_types=['linear', 'parallel', 'dag']
    )
    
    # Resource efficiency experiment
    efficiency_exp = ResourceEfficiencyExperiment("resource_efficiency")
    await efficiency_exp.run(
        num_workflows=20,
        resource_configs=[
            {'cpu_cores': 4, 'memory_gb': 8},
            {'cpu_cores': 8, 'memory_gb': 16},
            {'cpu_cores': 16, 'memory_gb': 32}
        ]
    )
    
    logger.info("All performance experiments completed")


if __name__ == "__main__":
    asyncio.run(main())
