"""
Fault Tolerance Experiments for DataFlower
Tests system resilience and recovery mechanisms
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
from enum import Enum

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.workflow_engine import (
    Workflow, Task, WorkflowEngine, ServerlessExecutor, 
    StateManager, TaskStatus, WorkflowStatus
)
from src.scheduler import ResourcePool, AdaptiveScheduler
from src.monitoring import MetricsCollector
from experiments.performance_experiments import WorkflowGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FaultType(Enum):
    """Types of faults to inject"""
    TASK_FAILURE = "task_failure"
    TIMEOUT = "timeout"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    NETWORK_PARTITION = "network_partition"
    CRASH_FAILURE = "crash_failure"
    BYZANTINE = "byzantine"


class FaultInjector:
    """Injects various types of faults into the system"""
    
    def __init__(self, fault_probability: float = 0.1):
        self.fault_probability = fault_probability
        self.fault_history = []
        self.enabled = True
        
    def should_inject_fault(self) -> bool:
        """Determine if a fault should be injected"""
        return self.enabled and random.random() < self.fault_probability
    
    def inject_task_failure(self, task_id: str) -> bool:
        """Inject a task failure"""
        if self.should_inject_fault():
            self.fault_history.append({
                'type': FaultType.TASK_FAILURE,
                'task_id': task_id,
                'timestamp': datetime.now()
            })
            logger.warning(f"Injecting task failure for {task_id}")
            return True
        return False
    
    def inject_timeout(self, task_id: str, delay: float = 10.0) -> float:
        """Inject a timeout delay"""
        if self.should_inject_fault():
            self.fault_history.append({
                'type': FaultType.TIMEOUT,
                'task_id': task_id,
                'delay': delay,
                'timestamp': datetime.now()
            })
            logger.warning(f"Injecting timeout for {task_id}: {delay}s")
            return delay
        return 0
    
    def inject_resource_exhaustion(self) -> bool:
        """Inject resource exhaustion"""
        if self.should_inject_fault():
            self.fault_history.append({
                'type': FaultType.RESOURCE_EXHAUSTION,
                'timestamp': datetime.now()
            })
            logger.warning("Injecting resource exhaustion")
            return True
        return False
    
    def inject_crash(self, component: str) -> bool:
        """Inject a crash failure"""
        if self.should_inject_fault():
            self.fault_history.append({
                'type': FaultType.CRASH_FAILURE,
                'component': component,
                'timestamp': datetime.now()
            })
            logger.warning(f"Injecting crash failure for {component}")
            return True
        return False


class FaultTolerantExecutor(ServerlessExecutor):
    """Executor with fault injection capabilities"""
    
    def __init__(self, platform: str = "local", fault_injector: Optional[FaultInjector] = None):
        super().__init__(platform)
        self.fault_injector = fault_injector or FaultInjector()
        
    async def execute_task(self, task: Task, inputs: Dict[str, Any]) -> Any:
        """Execute task with potential fault injection"""
        # Check for timeout injection
        if self.fault_injector:
            timeout_delay = self.fault_injector.inject_timeout(task.id)
            if timeout_delay > 0:
                await asyncio.sleep(timeout_delay)
                # This will cause the task to timeout
        
        # Check for task failure injection
        if self.fault_injector and self.fault_injector.inject_task_failure(task.id):
            raise Exception(f"Injected failure for task {task.id}")
        
        # Normal execution
        return await super().execute_task(task, inputs)


class CheckpointManager:
    """Manages workflow checkpoints for recovery"""
    
    def __init__(self, checkpoint_dir: str = "checkpoints"):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
    def save_checkpoint(self, workflow_id: str, state: Dict[str, Any]):
        """Save workflow checkpoint"""
        checkpoint_file = self.checkpoint_dir / f"{workflow_id}.json"
        with open(checkpoint_file, 'w') as f:
            json.dump(state, f)
        logger.info(f"Checkpoint saved for workflow {workflow_id}")
    
    def load_checkpoint(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Load workflow checkpoint"""
        checkpoint_file = self.checkpoint_dir / f"{workflow_id}.json"
        if checkpoint_file.exists():
            with open(checkpoint_file, 'r') as f:
                state = json.load(f)
            logger.info(f"Checkpoint loaded for workflow {workflow_id}")
            return state
        return None
    
    def delete_checkpoint(self, workflow_id: str):
        """Delete workflow checkpoint"""
        checkpoint_file = self.checkpoint_dir / f"{workflow_id}.json"
        if checkpoint_file.exists():
            checkpoint_file.unlink()
            logger.info(f"Checkpoint deleted for workflow {workflow_id}")


class FaultToleranceExperiment:
    """Base class for fault tolerance experiments"""
    
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


class TaskFailureExperiment(FaultToleranceExperiment):
    """Test resilience to task failures"""
    
    async def run(self,
                  failure_rates: List[float] = [0.0, 0.05, 0.1, 0.2, 0.3],
                  num_workflows: int = 20,
                  max_retries: int = 3):
        """Run task failure experiment"""
        logger.info(f"Starting Task Failure Experiment")
        
        for failure_rate in failure_rates:
            logger.info(f"Testing with failure rate: {failure_rate}")
            
            # Create fault injector
            fault_injector = FaultInjector(fault_probability=failure_rate)
            
            # Create engine with fault-tolerant executor
            executor = FaultTolerantExecutor(platform="local", fault_injector=fault_injector)
            engine = WorkflowEngine(executor=executor)
            
            # Generate workflows
            workflows = []
            for i in range(num_workflows):
                workflow = WorkflowGenerator.generate_dag_workflow(20)
                # Set retry policy
                for task in workflow.tasks:
                    task.retries = max_retries
                workflows.append(workflow)
            
            # Run workflows
            start_time = time.time()
            
            results_list = []
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
                
                # Count retries from fault history
                task_retries = sum(1 for f in fault_injector.fault_history 
                                 if f['type'] == FaultType.TASK_FAILURE)
                
                results_list.append({
                    'workflow_id': wf_id,
                    'status': status['status'],
                    'execution_time': wf_end - wf_start,
                    'task_retries': task_retries
                })
            
            end_time = time.time()
            
            # Calculate metrics
            success_count = sum(1 for r in results_list if r['status'] == 'completed')
            success_rate = success_count / num_workflows
            avg_execution_time = np.mean([r['execution_time'] for r in results_list])
            total_retries = sum(r['task_retries'] for r in results_list)
            
            result = {
                'failure_rate': failure_rate,
                'num_workflows': num_workflows,
                'success_rate': success_rate,
                'avg_execution_time': avg_execution_time,
                'total_retries': total_retries,
                'avg_retries_per_workflow': total_retries / num_workflows,
                'max_retries': max_retries
            }
            
            self.results.append(result)
            logger.info(f"Failure rate {failure_rate}: success_rate={success_rate:.2%}, "
                       f"avg_time={avg_execution_time:.2f}s")
        
        self.save_results()
        self.plot_results()
    
    def plot_results(self):
        """Plot task failure experiment results"""
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('Task Failure Resilience Analysis', fontsize=16)
        
        # Success rate vs failure rate
        ax = axes[0, 0]
        ax.plot(df['failure_rate'] * 100, df['success_rate'] * 100, 
                marker='o', markersize=10, linewidth=2)
        ax.set_xlabel('Task Failure Rate (%)')
        ax.set_ylabel('Workflow Success Rate (%)')
        ax.set_title('Success Rate vs Failure Rate')
        ax.grid(True, alpha=0.3)
        
        # Execution time vs failure rate
        ax = axes[0, 1]
        ax.plot(df['failure_rate'] * 100, df['avg_execution_time'], 
                marker='s', markersize=10, linewidth=2, color='orange')
        ax.set_xlabel('Task Failure Rate (%)')
        ax.set_ylabel('Average Execution Time (s)')
        ax.set_title('Execution Time Impact')
        ax.grid(True, alpha=0.3)
        
        # Retries vs failure rate
        ax = axes[1, 0]
        ax.plot(df['failure_rate'] * 100, df['avg_retries_per_workflow'], 
                marker='^', markersize=10, linewidth=2, color='green')
        ax.set_xlabel('Task Failure Rate (%)')
        ax.set_ylabel('Average Retries per Workflow')
        ax.set_title('Retry Overhead')
        ax.grid(True, alpha=0.3)
        
        # Recovery efficiency
        ax = axes[1, 1]
        df['recovery_efficiency'] = df['success_rate'] / (1 - df['failure_rate'] + 0.01)
        ax.plot(df['failure_rate'] * 100, df['recovery_efficiency'], 
                marker='d', markersize=10, linewidth=2, color='purple')
        ax.set_xlabel('Task Failure Rate (%)')
        ax.set_ylabel('Recovery Efficiency')
        ax.set_title('Recovery Effectiveness')
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        output_file = self.output_dir / f"{self.name}_plots.png"
        plt.savefig(output_file)
        plt.close()
        logger.info(f"Plots saved to {output_file}")


class TimeoutExperiment(FaultToleranceExperiment):
    """Test handling of timeouts"""
    
    async def run(self,
                  timeout_rates: List[float] = [0.0, 0.05, 0.1, 0.15, 0.2],
                  num_workflows: int = 20,
                  timeout_duration: float = 5.0):
        """Run timeout experiment"""
        logger.info(f"Starting Timeout Experiment")
        
        for timeout_rate in timeout_rates:
            logger.info(f"Testing with timeout rate: {timeout_rate}")
            
            # Create fault injector for timeouts
            fault_injector = FaultInjector(fault_probability=timeout_rate)
            
            # Create engine
            executor = FaultTolerantExecutor(platform="local", fault_injector=fault_injector)
            engine = WorkflowEngine(executor=executor)
            
            # Generate workflows with timeout settings
            workflows = []
            for i in range(num_workflows):
                workflow = WorkflowGenerator.generate_linear_workflow(15)
                # Set aggressive timeouts
                for task in workflow.tasks:
                    task.timeout = 10  # 10 second timeout
                workflows.append(workflow)
            
            # Run workflows
            timeout_counts = []
            completed_counts = []
            
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
                
                # Count timeouts from fault history
                timeouts = sum(1 for f in fault_injector.fault_history 
                             if f['type'] == FaultType.TIMEOUT)
                timeout_counts.append(timeouts)
                completed_counts.append(1 if status['status'] == 'completed' else 0)
            
            # Calculate metrics
            success_rate = sum(completed_counts) / num_workflows
            avg_timeouts = np.mean(timeout_counts)
            max_timeouts = max(timeout_counts)
            
            result = {
                'timeout_rate': timeout_rate,
                'num_workflows': num_workflows,
                'success_rate': success_rate,
                'avg_timeouts_per_workflow': avg_timeouts,
                'max_timeouts': max_timeouts,
                'timeout_duration': timeout_duration
            }
            
            self.results.append(result)
            logger.info(f"Timeout rate {timeout_rate}: success_rate={success_rate:.2%}, "
                       f"avg_timeouts={avg_timeouts:.2f}")
        
        self.save_results()
        self.plot_timeout_results()
    
    def plot_timeout_results(self):
        """Plot timeout experiment results"""
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('Timeout Handling Analysis', fontsize=16)
        
        # Success rate vs timeout rate
        ax = axes[0, 0]
        ax.plot(df['timeout_rate'] * 100, df['success_rate'] * 100, 
                marker='o', markersize=10, linewidth=2)
        ax.set_xlabel('Timeout Rate (%)')
        ax.set_ylabel('Success Rate (%)')
        ax.set_title('Impact of Timeouts on Success')
        ax.grid(True, alpha=0.3)
        
        # Average timeouts per workflow
        ax = axes[0, 1]
        ax.bar(df['timeout_rate'].astype(str), df['avg_timeouts_per_workflow'])
        ax.set_xlabel('Timeout Rate')
        ax.set_ylabel('Average Timeouts per Workflow')
        ax.set_title('Timeout Frequency')
        
        # Max timeouts
        ax = axes[1, 0]
        ax.plot(df['timeout_rate'] * 100, df['max_timeouts'], 
                marker='s', markersize=10, linewidth=2, color='red')
        ax.set_xlabel('Timeout Rate (%)')
        ax.set_ylabel('Maximum Timeouts')
        ax.set_title('Worst Case Timeouts')
        ax.grid(True, alpha=0.3)
        
        # Timeout impact factor
        ax = axes[1, 1]
        df['impact_factor'] = (1 - df['success_rate']) / (df['timeout_rate'] + 0.01)
        ax.plot(df['timeout_rate'] * 100, df['impact_factor'], 
                marker='^', markersize=10, linewidth=2, color='purple')
        ax.set_xlabel('Timeout Rate (%)')
        ax.set_ylabel('Impact Factor')
        ax.set_title('Timeout Impact on System')
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        output_file = self.output_dir / f"{self.name}_timeout_plots.png"
        plt.savefig(output_file)
        plt.close()
        logger.info(f"Plots saved to {output_file}")


class CheckpointRecoveryExperiment(FaultToleranceExperiment):
    """Test checkpoint and recovery mechanisms"""
    
    async def run(self,
                  checkpoint_intervals: List[int] = [5, 10, 20, 50],
                  failure_probability: float = 0.1,
                  num_workflows: int = 20):
        """Run checkpoint recovery experiment"""
        logger.info(f"Starting Checkpoint Recovery Experiment")
        
        for checkpoint_interval in checkpoint_intervals:
            logger.info(f"Testing with checkpoint interval: {checkpoint_interval} tasks")
            
            # Create checkpoint manager
            checkpoint_manager = CheckpointManager()
            
            # Create fault injector
            fault_injector = FaultInjector(fault_probability=failure_probability)
            
            # Create engine
            executor = FaultTolerantExecutor(platform="local", fault_injector=fault_injector)
            engine = WorkflowEngine(executor=executor)
            
            recovery_times = []
            checkpoint_counts = []
            
            for i in range(num_workflows):
                workflow = WorkflowGenerator.generate_linear_workflow(50)
                
                # Simulate workflow execution with checkpointing
                wf_start = time.time()
                wf_id = await engine.submit_workflow(workflow)
                
                tasks_completed = 0
                checkpoints_created = 0
                recovery_count = 0
                
                while True:
                    status = engine.get_workflow_status(wf_id)
                    
                    if status:
                        # Check if we should create a checkpoint
                        current_completed = sum(
                            1 for task_status in status.get('tasks_status', {}).values()
                            if task_status == 'completed'
                        )
                        
                        if current_completed > tasks_completed:
                            tasks_completed = current_completed
                            
                            # Create checkpoint at intervals
                            if tasks_completed % checkpoint_interval == 0:
                                checkpoint_manager.save_checkpoint(wf_id, status)
                                checkpoints_created += 1
                        
                        # Simulate recovery from checkpoint on failure
                        if status['status'] == 'failed':
                            checkpoint = checkpoint_manager.load_checkpoint(wf_id)
                            if checkpoint and recovery_count < 3:
                                recovery_count += 1
                                logger.info(f"Recovering workflow {wf_id} from checkpoint")
                                # In a real system, we would restart from checkpoint
                                # For simulation, we continue
                        
                        if status['status'] in ['completed', 'failed']:
                            break
                    
                    await asyncio.sleep(0.5)
                
                wf_end = time.time()
                
                recovery_times.append(wf_end - wf_start)
                checkpoint_counts.append(checkpoints_created)
                
                # Clean up checkpoint
                checkpoint_manager.delete_checkpoint(wf_id)
            
            # Calculate metrics
            avg_recovery_time = np.mean(recovery_times)
            avg_checkpoints = np.mean(checkpoint_counts)
            
            result = {
                'checkpoint_interval': checkpoint_interval,
                'failure_probability': failure_probability,
                'avg_recovery_time': avg_recovery_time,
                'avg_checkpoints_per_workflow': avg_checkpoints,
                'checkpoint_overhead': avg_checkpoints * 0.1  # Assume 0.1s per checkpoint
            }
            
            self.results.append(result)
            logger.info(f"Checkpoint interval {checkpoint_interval}: "
                       f"avg_recovery_time={avg_recovery_time:.2f}s, "
                       f"avg_checkpoints={avg_checkpoints:.1f}")
        
        self.save_results()
        self.plot_checkpoint_results()
    
    def plot_checkpoint_results(self):
        """Plot checkpoint recovery results"""
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('Checkpoint Recovery Analysis', fontsize=16)
        
        # Recovery time vs checkpoint interval
        ax = axes[0, 0]
        ax.plot(df['checkpoint_interval'], df['avg_recovery_time'], 
                marker='o', markersize=10, linewidth=2)
        ax.set_xlabel('Checkpoint Interval (tasks)')
        ax.set_ylabel('Average Recovery Time (s)')
        ax.set_title('Recovery Time vs Checkpoint Frequency')
        ax.grid(True, alpha=0.3)
        
        # Checkpoints per workflow
        ax = axes[0, 1]
        ax.bar(df['checkpoint_interval'].astype(str), df['avg_checkpoints_per_workflow'])
        ax.set_xlabel('Checkpoint Interval')
        ax.set_ylabel('Average Checkpoints per Workflow')
        ax.set_title('Checkpoint Frequency')
        
        # Checkpoint overhead
        ax = axes[1, 0]
        ax.plot(df['checkpoint_interval'], df['checkpoint_overhead'], 
                marker='s', markersize=10, linewidth=2, color='orange')
        ax.set_xlabel('Checkpoint Interval (tasks)')
        ax.set_ylabel('Checkpoint Overhead (s)')
        ax.set_title('Overhead from Checkpointing')
        ax.grid(True, alpha=0.3)
        
        # Efficiency (recovery time / checkpoint overhead)
        ax = axes[1, 1]
        df['efficiency'] = df['avg_recovery_time'] / (df['checkpoint_overhead'] + 1)
        ax.plot(df['checkpoint_interval'], df['efficiency'], 
                marker='^', markersize=10, linewidth=2, color='green')
        ax.set_xlabel('Checkpoint Interval (tasks)')
        ax.set_ylabel('Recovery Efficiency')
        ax.set_title('Checkpoint Efficiency')
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        output_file = self.output_dir / f"{self.name}_checkpoint_plots.png"
        plt.savefig(output_file)
        plt.close()
        logger.info(f"Plots saved to {output_file}")


class CascadingFailureExperiment(FaultToleranceExperiment):
    """Test resilience to cascading failures"""
    
    async def run(self,
                  initial_failure_points: List[int] = [1, 2, 3, 5],
                  propagation_probability: float = 0.3,
                  num_workflows: int = 20):
        """Run cascading failure experiment"""
        logger.info(f"Starting Cascading Failure Experiment")
        
        for failure_points in initial_failure_points:
            logger.info(f"Testing with {failure_points} initial failure points")
            
            results_list = []
            
            for i in range(num_workflows):
                # Create a complex workflow with dependencies
                workflow = WorkflowGenerator.generate_dag_workflow(30, edge_probability=0.4)
                
                # Simulate cascading failure
                failed_tasks = set()
                cascade_depth = 0
                
                # Initial failures
                for _ in range(failure_points):
                    task_idx = random.randint(0, len(workflow.tasks) - 1)
                    failed_tasks.add(workflow.tasks[task_idx].id)
                
                # Propagate failures
                graph = workflow.to_graph()
                new_failures = failed_tasks.copy()
                
                while new_failures:
                    cascade_depth += 1
                    next_failures = set()
                    
                    for failed_task in new_failures:
                        # Find dependent tasks
                        if failed_task in graph:
                            for successor in graph.successors(failed_task):
                                if random.random() < propagation_probability:
                                    if successor not in failed_tasks:
                                        next_failures.add(successor)
                                        failed_tasks.add(successor)
                    
                    new_failures = next_failures
                    
                    # Limit cascade depth to prevent infinite loops
                    if cascade_depth > 10:
                        break
                
                # Calculate impact
                total_tasks = len(workflow.tasks)
                failed_count = len(failed_tasks)
                failure_ratio = failed_count / total_tasks
                
                results_list.append({
                    'failed_tasks': failed_count,
                    'total_tasks': total_tasks,
                    'failure_ratio': failure_ratio,
                    'cascade_depth': cascade_depth
                })
            
            # Aggregate results
            avg_failure_ratio = np.mean([r['failure_ratio'] for r in results_list])
            max_failure_ratio = max(r['failure_ratio'] for r in results_list)
            avg_cascade_depth = np.mean([r['cascade_depth'] for r in results_list])
            
            result = {
                'initial_failure_points': failure_points,
                'propagation_probability': propagation_probability,
                'avg_failure_ratio': avg_failure_ratio,
                'max_failure_ratio': max_failure_ratio,
                'avg_cascade_depth': avg_cascade_depth,
                'containment_rate': 1 - avg_failure_ratio
            }
            
            self.results.append(result)
            logger.info(f"Initial failures {failure_points}: "
                       f"avg_failure_ratio={avg_failure_ratio:.2%}, "
                       f"cascade_depth={avg_cascade_depth:.1f}")
        
        self.save_results()
        self.plot_cascading_results()
    
    def plot_cascading_results(self):
        """Plot cascading failure results"""
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('Cascading Failure Analysis', fontsize=16)
        
        # Failure ratio vs initial failures
        ax = axes[0, 0]
        ax.plot(df['initial_failure_points'], df['avg_failure_ratio'] * 100, 
                marker='o', markersize=10, linewidth=2, label='Average')
        ax.plot(df['initial_failure_points'], df['max_failure_ratio'] * 100, 
                marker='s', markersize=10, linewidth=2, label='Maximum', color='red')
        ax.set_xlabel('Initial Failure Points')
        ax.set_ylabel('Failure Ratio (%)')
        ax.set_title('Cascading Failure Impact')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Cascade depth
        ax = axes[0, 1]
        ax.bar(df['initial_failure_points'].astype(str), df['avg_cascade_depth'])
        ax.set_xlabel('Initial Failure Points')
        ax.set_ylabel('Average Cascade Depth')
        ax.set_title('Failure Propagation Depth')
        
        # Containment rate
        ax = axes[1, 0]
        ax.plot(df['initial_failure_points'], df['containment_rate'] * 100, 
                marker='^', markersize=10, linewidth=2, color='green')
        ax.set_xlabel('Initial Failure Points')
        ax.set_ylabel('Containment Rate (%)')
        ax.set_title('Failure Containment Effectiveness')
        ax.grid(True, alpha=0.3)
        
        # Amplification factor
        ax = axes[1, 1]
        df['amplification'] = df['avg_failure_ratio'] * 30 / df['initial_failure_points']
        ax.plot(df['initial_failure_points'], df['amplification'], 
                marker='d', markersize=10, linewidth=2, color='purple')
        ax.set_xlabel('Initial Failure Points')
        ax.set_ylabel('Failure Amplification Factor')
        ax.set_title('Cascading Amplification')
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        output_file = self.output_dir / f"{self.name}_cascading_plots.png"
        plt.savefig(output_file)
        plt.close()
        logger.info(f"Plots saved to {output_file}")


async def main():
    """Run all fault tolerance experiments"""
    logger.info("Starting DataFlower Fault Tolerance Experiments")
    
    # Task failure experiment
    task_failure_exp = TaskFailureExperiment("task_failure")
    await task_failure_exp.run(
        failure_rates=[0.0, 0.1, 0.2, 0.3],
        num_workflows=10,
        max_retries=3
    )
    
    # Timeout experiment
    timeout_exp = TimeoutExperiment("timeout")
    await timeout_exp.run(
        timeout_rates=[0.0, 0.05, 0.1, 0.15],
        num_workflows=10,
        timeout_duration=5.0
    )
    
    # Checkpoint recovery experiment
    checkpoint_exp = CheckpointRecoveryExperiment("checkpoint_recovery")
    await checkpoint_exp.run(
        checkpoint_intervals=[5, 10, 20],
        failure_probability=0.1,
        num_workflows=10
    )
    
    # Cascading failure experiment
    cascading_exp = CascadingFailureExperiment("cascading_failure")
    await cascading_exp.run(
        initial_failure_points=[1, 2, 3],
        propagation_probability=0.3,
        num_workflows=10
    )
    
    logger.info("All fault tolerance experiments completed")


if __name__ == "__main__":
    asyncio.run(main())
