"""
Scalability Experiments for DataFlower
Tests system behavior under increasing load and scale
"""

import asyncio
import random
import time
from datetime import datetime
from typing import List, Dict, Any
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.workflow_engine import (
    Workflow, Task, WorkflowEngine, ServerlessExecutor, StateManager
)
from src.scheduler import (
    ResourcePool, AdaptiveScheduler, TaskSchedulingInfo,
    FIFOScheduler, PriorityScheduler, CostOptimizedScheduler
)
from src.monitoring import MetricsCollector, PerformanceMonitor
from experiments.performance_experiments import WorkflowGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ScalabilityExperiment:
    """Base class for scalability experiments"""
    
    def __init__(self, name: str, output_dir: str = "results"):
        self.name = name
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.results = []
        
    def save_results(self):
        """Save experiment results"""
        output_file = self.output_dir / f"{self.name}_results.json"
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        logger.info(f"Results saved to {output_file}")


class WorkflowScalabilityExperiment(ScalabilityExperiment):
    """Test scalability with increasing number of workflows"""
    
    async def run(self,
                  workflow_counts: List[int] = [10, 50, 100, 200, 500, 1000],
                  task_size: int = 20):
        """Run workflow scalability experiment"""
        logger.info(f"Starting Workflow Scalability Experiment")
        
        for count in workflow_counts:
            logger.info(f"Testing with {count} workflows")
            
            # Create engine
            executor = ServerlessExecutor(platform="local")
            engine = WorkflowEngine(executor=executor)
            
            # Generate workflows
            workflows = []
            for i in range(count):
                workflow_type = random.choice(['linear', 'parallel', 'dag'])
                
                if workflow_type == 'linear':
                    workflow = WorkflowGenerator.generate_linear_workflow(task_size)
                elif workflow_type == 'parallel':
                    workflow = WorkflowGenerator.generate_parallel_workflow(4, 5)
                else:
                    workflow = WorkflowGenerator.generate_dag_workflow(task_size)
                
                workflows.append(workflow)
            
            # Run workflows concurrently
            start_time = time.time()
            
            # Submit all workflows
            workflow_ids = []
            for workflow in workflows:
                wf_id = await engine.submit_workflow(workflow)
                workflow_ids.append(wf_id)
            
            # Wait for all to complete
            completed = 0
            while completed < count:
                completed = 0
                for wf_id in workflow_ids:
                    status = engine.get_workflow_status(wf_id)
                    if status and status['status'] in ['completed', 'failed']:
                        completed += 1
                
                if completed < count:
                    await asyncio.sleep(1)
            
            end_time = time.time()
            total_time = end_time - start_time
            
            # Calculate metrics
            throughput = count / total_time
            avg_latency = total_time / count
            
            result = {
                'workflow_count': count,
                'total_time': total_time,
                'throughput': throughput,
                'avg_latency': avg_latency,
                'task_size': task_size,
                'total_tasks': count * task_size
            }
            
            self.results.append(result)
            logger.info(f"Completed {count} workflows in {total_time:.2f}s "
                       f"(throughput: {throughput:.2f} wf/s)")
        
        self.save_results()
        self.plot_scalability_results()
    
    def plot_scalability_results(self):
        """Plot workflow scalability results"""
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('Workflow Scalability Analysis', fontsize=16)
        
        # Throughput vs workflow count
        ax = axes[0, 0]
        ax.plot(df['workflow_count'], df['throughput'], marker='o', markersize=8)
        ax.set_xlabel('Number of Workflows')
        ax.set_ylabel('Throughput (workflows/s)')
        ax.set_title('Throughput Scalability')
        ax.grid(True, alpha=0.3)
        
        # Total time vs workflow count
        ax = axes[0, 1]
        ax.plot(df['workflow_count'], df['total_time'], marker='s', markersize=8, color='orange')
        ax.set_xlabel('Number of Workflows')
        ax.set_ylabel('Total Time (s)')
        ax.set_title('Execution Time Scaling')
        ax.grid(True, alpha=0.3)
        
        # Average latency vs workflow count
        ax = axes[1, 0]
        ax.plot(df['workflow_count'], df['avg_latency'], marker='^', markersize=8, color='green')
        ax.set_xlabel('Number of Workflows')
        ax.set_ylabel('Average Latency (s)')
        ax.set_title('Latency Scaling')
        ax.grid(True, alpha=0.3)
        
        # Efficiency (ideal vs actual)
        ax = axes[1, 1]
        baseline_throughput = df.iloc[0]['throughput']
        ideal_scaling = [baseline_throughput] * len(df)
        ax.plot(df['workflow_count'], df['throughput'], marker='o', label='Actual', markersize=8)
        ax.plot(df['workflow_count'], ideal_scaling, '--', label='Ideal', alpha=0.7)
        ax.set_xlabel('Number of Workflows')
        ax.set_ylabel('Throughput (workflows/s)')
        ax.set_title('Scaling Efficiency')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        output_file = self.output_dir / f"{self.name}_plots.png"
        plt.savefig(output_file)
        plt.close()
        logger.info(f"Plots saved to {output_file}")


class TaskScalabilityExperiment(ScalabilityExperiment):
    """Test scalability with increasing number of tasks per workflow"""
    
    async def run(self,
                  task_counts: List[int] = [10, 50, 100, 200, 500, 1000],
                  num_workflows: int = 10):
        """Run task scalability experiment"""
        logger.info(f"Starting Task Scalability Experiment")
        
        for task_count in task_counts:
            logger.info(f"Testing with {task_count} tasks per workflow")
            
            # Create engine
            executor = ServerlessExecutor(platform="local")
            engine = WorkflowEngine(executor=executor)
            
            # Generate workflows with varying task counts
            workflows = []
            for i in range(num_workflows):
                workflow = WorkflowGenerator.generate_dag_workflow(task_count)
                workflows.append(workflow)
            
            # Run workflows
            start_time = time.time()
            
            workflow_results = []
            for workflow in workflows:
                wf_start = time.time()
                wf_id = await engine.submit_workflow(workflow)
                
                # Wait for completion
                while True:
                    status = engine.get_workflow_status(wf_id)
                    if status and status['status'] in ['completed', 'failed']:
                        break
                    await asyncio.sleep(0.5)
                
                wf_end = time.time()
                workflow_results.append({
                    'workflow_id': wf_id,
                    'execution_time': wf_end - wf_start,
                    'status': status['status']
                })
            
            end_time = time.time()
            total_time = end_time - start_time
            
            # Calculate metrics
            avg_execution_time = np.mean([r['execution_time'] for r in workflow_results])
            success_rate = sum(1 for r in workflow_results if r['status'] == 'completed') / len(workflow_results)
            
            result = {
                'task_count': task_count,
                'num_workflows': num_workflows,
                'total_time': total_time,
                'avg_execution_time': avg_execution_time,
                'success_rate': success_rate,
                'total_tasks': task_count * num_workflows
            }
            
            self.results.append(result)
            logger.info(f"Completed workflows with {task_count} tasks in avg {avg_execution_time:.2f}s")
        
        self.save_results()
        self.plot_task_scalability()
    
    def plot_task_scalability(self):
        """Plot task scalability results"""
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('Task Scalability Analysis', fontsize=16)
        
        # Execution time vs task count
        ax = axes[0, 0]
        ax.plot(df['task_count'], df['avg_execution_time'], marker='o', markersize=8)
        ax.set_xlabel('Tasks per Workflow')
        ax.set_ylabel('Average Execution Time (s)')
        ax.set_title('Execution Time vs Task Count')
        ax.set_xscale('log')
        ax.grid(True, alpha=0.3)
        
        # Success rate vs task count
        ax = axes[0, 1]
        ax.plot(df['task_count'], df['success_rate'] * 100, marker='s', markersize=8, color='green')
        ax.set_xlabel('Tasks per Workflow')
        ax.set_ylabel('Success Rate (%)')
        ax.set_title('Success Rate vs Task Count')
        ax.set_ylim([0, 105])
        ax.grid(True, alpha=0.3)
        
        # Time per task
        ax = axes[1, 0]
        df['time_per_task'] = df['avg_execution_time'] / df['task_count']
        ax.plot(df['task_count'], df['time_per_task'], marker='^', markersize=8, color='purple')
        ax.set_xlabel('Tasks per Workflow')
        ax.set_ylabel('Time per Task (s)')
        ax.set_title('Per-Task Overhead')
        ax.set_xscale('log')
        ax.grid(True, alpha=0.3)
        
        # Scaling factor
        ax = axes[1, 1]
        baseline_time = df.iloc[0]['avg_execution_time'] / df.iloc[0]['task_count']
        df['scaling_factor'] = (df['avg_execution_time'] / df['task_count']) / baseline_time
        ax.plot(df['task_count'], df['scaling_factor'], marker='d', markersize=8, color='red')
        ax.axhline(y=1, color='gray', linestyle='--', alpha=0.7)
        ax.set_xlabel('Tasks per Workflow')
        ax.set_ylabel('Scaling Factor')
        ax.set_title('Scaling Efficiency (1.0 = Perfect)')
        ax.set_xscale('log')
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        output_file = self.output_dir / f"{self.name}_plots.png"
        plt.savefig(output_file)
        plt.close()
        logger.info(f"Plots saved to {output_file}")


class ResourceScalabilityExperiment(ScalabilityExperiment):
    """Test scalability with varying resource configurations"""
    
    async def run(self,
                  resource_multipliers: List[int] = [1, 2, 4, 8, 16],
                  base_config: Dict[str, Any] = None,
                  workload_size: int = 100):
        """Run resource scalability experiment"""
        logger.info(f"Starting Resource Scalability Experiment")
        
        if base_config is None:
            base_config = {'cpu_cores': 4, 'memory_gb': 8, 'gpu_count': 0}
        
        for multiplier in resource_multipliers:
            # Scale resources
            scaled_config = {
                'cpu_cores': base_config['cpu_cores'] * multiplier,
                'memory_gb': base_config['memory_gb'] * multiplier,
                'gpu_count': base_config['gpu_count'] * multiplier
            }
            
            logger.info(f"Testing with {multiplier}x resources: {scaled_config}")
            
            # Create resource pool
            resource_pool = ResourcePool(**scaled_config)
            
            # Create engine
            executor = ServerlessExecutor(platform="local")
            engine = WorkflowEngine(executor=executor)
            
            # Generate workload
            workflows = []
            for i in range(workload_size):
                workflow_type = random.choice(['linear', 'parallel', 'dag'])
                task_count = random.randint(10, 30)
                
                if workflow_type == 'linear':
                    workflow = WorkflowGenerator.generate_linear_workflow(task_count)
                elif workflow_type == 'parallel':
                    workflow = WorkflowGenerator.generate_parallel_workflow(
                        num_parallel=min(multiplier * 2, task_count // 3),
                        depth=task_count // (multiplier * 2)
                    )
                else:
                    workflow = WorkflowGenerator.generate_dag_workflow(task_count)
                
                workflows.append(workflow)
            
            # Run workload
            start_time = time.time()
            
            # Track max concurrent workflows
            max_concurrent = min(multiplier * 10, workload_size)
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def run_with_limit(workflow):
                async with semaphore:
                    wf_id = await engine.submit_workflow(workflow)
                    while True:
                        status = engine.get_workflow_status(wf_id)
                        if status and status['status'] in ['completed', 'failed']:
                            return status
                        await asyncio.sleep(0.5)
            
            tasks = [run_with_limit(wf) for wf in workflows]
            results = await asyncio.gather(*tasks)
            
            end_time = time.time()
            total_time = end_time - start_time
            
            # Calculate metrics
            throughput = workload_size / total_time
            success_count = sum(1 for r in results if r['status'] == 'completed')
            success_rate = success_count / workload_size
            
            # Calculate resource efficiency
            resource_units = scaled_config['cpu_cores'] + scaled_config['memory_gb'] / 8
            efficiency = throughput / resource_units
            
            result = {
                'resource_multiplier': multiplier,
                'cpu_cores': scaled_config['cpu_cores'],
                'memory_gb': scaled_config['memory_gb'],
                'workload_size': workload_size,
                'total_time': total_time,
                'throughput': throughput,
                'success_rate': success_rate,
                'efficiency': efficiency,
                'max_concurrent': max_concurrent
            }
            
            self.results.append(result)
            logger.info(f"Completed with {multiplier}x resources: "
                       f"throughput={throughput:.2f} wf/s, efficiency={efficiency:.3f}")
        
        self.save_results()
        self.plot_resource_scalability()
    
    def plot_resource_scalability(self):
        """Plot resource scalability results"""
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('Resource Scalability Analysis', fontsize=16)
        
        # Throughput vs resources
        ax = axes[0, 0]
        ax.plot(df['resource_multiplier'], df['throughput'], marker='o', markersize=10, linewidth=2)
        ax.set_xlabel('Resource Multiplier')
        ax.set_ylabel('Throughput (workflows/s)')
        ax.set_title('Throughput Scaling with Resources')
        ax.set_xscale('log', base=2)
        ax.grid(True, alpha=0.3)
        
        # Efficiency vs resources
        ax = axes[0, 1]
        ax.plot(df['resource_multiplier'], df['efficiency'], marker='s', markersize=10, 
                linewidth=2, color='orange')
        ax.set_xlabel('Resource Multiplier')
        ax.set_ylabel('Efficiency (throughput/resource)')
        ax.set_title('Resource Utilization Efficiency')
        ax.set_xscale('log', base=2)
        ax.grid(True, alpha=0.3)
        
        # Speedup vs resources
        ax = axes[1, 0]
        baseline_time = df.iloc[0]['total_time']
        df['speedup'] = baseline_time / df['total_time']
        ax.plot(df['resource_multiplier'], df['speedup'], marker='^', markersize=10, 
                linewidth=2, color='green', label='Actual')
        ax.plot(df['resource_multiplier'], df['resource_multiplier'], '--', 
                linewidth=2, alpha=0.7, color='gray', label='Linear')
        ax.set_xlabel('Resource Multiplier')
        ax.set_ylabel('Speedup')
        ax.set_title('Speedup vs Resources')
        ax.set_xscale('log', base=2)
        ax.set_yscale('log', base=2)
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Success rate vs resources
        ax = axes[1, 1]
        ax.bar(df['resource_multiplier'].astype(str), df['success_rate'] * 100)
        ax.set_xlabel('Resource Multiplier')
        ax.set_ylabel('Success Rate (%)')
        ax.set_title('Reliability with Scaling')
        ax.set_ylim([0, 105])
        
        plt.tight_layout()
        output_file = self.output_dir / f"{self.name}_plots.png"
        plt.savefig(output_file)
        plt.close()
        logger.info(f"Plots saved to {output_file}")


class ConcurrencyScalabilityExperiment(ScalabilityExperiment):
    """Test scalability with varying levels of concurrency"""
    
    async def run(self,
                  concurrency_levels: List[int] = [1, 5, 10, 20, 50, 100],
                  total_workflows: int = 200):
        """Run concurrency scalability experiment"""
        logger.info(f"Starting Concurrency Scalability Experiment")
        
        for concurrency in concurrency_levels:
            logger.info(f"Testing with concurrency level: {concurrency}")
            
            # Create engine
            executor = ServerlessExecutor(platform="local")
            engine = WorkflowEngine(executor=executor)
            
            # Generate workflows
            workflows = []
            for i in range(total_workflows):
                task_count = random.randint(10, 30)
                workflow = WorkflowGenerator.generate_dag_workflow(task_count)
                workflows.append(workflow)
            
            # Run with limited concurrency
            start_time = time.time()
            
            semaphore = asyncio.Semaphore(concurrency)
            completed_times = []
            
            async def run_with_concurrency(workflow, index):
                async with semaphore:
                    wf_start = time.time()
                    wf_id = await engine.submit_workflow(workflow)
                    
                    while True:
                        status = engine.get_workflow_status(wf_id)
                        if status and status['status'] in ['completed', 'failed']:
                            wf_end = time.time()
                            completed_times.append(wf_end - start_time)
                            return {
                                'workflow_id': wf_id,
                                'status': status['status'],
                                'duration': wf_end - wf_start,
                                'completion_time': wf_end - start_time
                            }
                        await asyncio.sleep(0.5)
            
            tasks = [run_with_concurrency(wf, i) for i, wf in enumerate(workflows)]
            results = await asyncio.gather(*tasks)
            
            end_time = time.time()
            total_time = end_time - start_time
            
            # Calculate metrics
            throughput = total_workflows / total_time
            avg_latency = np.mean([r['duration'] for r in results])
            p50_latency = np.percentile([r['duration'] for r in results], 50)
            p95_latency = np.percentile([r['duration'] for r in results], 95)
            
            # Calculate fairness (Jain's index)
            completion_times = [r['completion_time'] for r in results]
            if completion_times:
                mean_ct = np.mean(completion_times)
                squared_sum = sum(ct ** 2 for ct in completion_times)
                fairness = (sum(completion_times) ** 2) / (len(completion_times) * squared_sum)
            else:
                fairness = 0
            
            result = {
                'concurrency_level': concurrency,
                'total_workflows': total_workflows,
                'total_time': total_time,
                'throughput': throughput,
                'avg_latency': avg_latency,
                'p50_latency': p50_latency,
                'p95_latency': p95_latency,
                'fairness_index': fairness
            }
            
            self.results.append(result)
            logger.info(f"Concurrency {concurrency}: throughput={throughput:.2f} wf/s, "
                       f"avg_latency={avg_latency:.2f}s")
        
        self.save_results()
        self.plot_concurrency_results()
    
    def plot_concurrency_results(self):
        """Plot concurrency scalability results"""
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('Concurrency Scalability Analysis', fontsize=16)
        
        # Throughput vs concurrency
        ax = axes[0, 0]
        ax.plot(df['concurrency_level'], df['throughput'], marker='o', markersize=10, linewidth=2)
        ax.set_xlabel('Concurrency Level')
        ax.set_ylabel('Throughput (workflows/s)')
        ax.set_title('Throughput vs Concurrency')
        ax.set_xscale('log')
        ax.grid(True, alpha=0.3)
        
        # Latency percentiles
        ax = axes[0, 1]
        ax.plot(df['concurrency_level'], df['avg_latency'], marker='o', label='Average', markersize=8)
        ax.plot(df['concurrency_level'], df['p50_latency'], marker='s', label='P50', markersize=8)
        ax.plot(df['concurrency_level'], df['p95_latency'], marker='^', label='P95', markersize=8)
        ax.set_xlabel('Concurrency Level')
        ax.set_ylabel('Latency (s)')
        ax.set_title('Latency vs Concurrency')
        ax.set_xscale('log')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Fairness index
        ax = axes[1, 0]
        ax.plot(df['concurrency_level'], df['fairness_index'], marker='d', 
                markersize=10, linewidth=2, color='purple')
        ax.set_xlabel('Concurrency Level')
        ax.set_ylabel("Jain's Fairness Index")
        ax.set_title('Fairness vs Concurrency')
        ax.set_xscale('log')
        ax.set_ylim([0, 1.1])
        ax.grid(True, alpha=0.3)
        
        # Efficiency (throughput per concurrency)
        ax = axes[1, 1]
        df['efficiency'] = df['throughput'] / df['concurrency_level']
        ax.plot(df['concurrency_level'], df['efficiency'], marker='h', 
                markersize=10, linewidth=2, color='brown')
        ax.set_xlabel('Concurrency Level')
        ax.set_ylabel('Efficiency (throughput/concurrency)')
        ax.set_title('Concurrency Efficiency')
        ax.set_xscale('log')
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        output_file = self.output_dir / f"{self.name}_plots.png"
        plt.savefig(output_file)
        plt.close()
        logger.info(f"Plots saved to {output_file}")


async def main():
    """Run all scalability experiments"""
    logger.info("Starting DataFlower Scalability Experiments")
    
    # Workflow scalability
    workflow_exp = WorkflowScalabilityExperiment("workflow_scalability")
    await workflow_exp.run(
        workflow_counts=[10, 25, 50, 100, 200],
        task_size=20
    )
    
    # Task scalability
    task_exp = TaskScalabilityExperiment("task_scalability")
    await task_exp.run(
        task_counts=[10, 25, 50, 100, 200],
        num_workflows=5
    )
    
    # Resource scalability
    resource_exp = ResourceScalabilityExperiment("resource_scalability")
    await resource_exp.run(
        resource_multipliers=[1, 2, 4, 8],
        workload_size=50
    )
    
    # Concurrency scalability
    concurrency_exp = ConcurrencyScalabilityExperiment("concurrency_scalability")
    await concurrency_exp.run(
        concurrency_levels=[1, 5, 10, 25, 50],
        total_workflows=100
    )
    
    logger.info("All scalability experiments completed")


if __name__ == "__main__":
    asyncio.run(main())
