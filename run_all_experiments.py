#!/usr/bin/env python3
"""
Main experiment runner for DataFlower
Orchestrates and runs all experiments with configurable parameters
"""

import asyncio
import argparse
import logging
import sys
import time
from pathlib import Path
from datetime import datetime
import json
import yaml

# Add current directory to path
sys.path.append(str(Path(__file__).parent))

from experiments.performance_experiments import (
    ThroughputExperiment, LatencyExperiment, ResourceEfficiencyExperiment
)
from experiments.scalability_experiments import (
    WorkflowScalabilityExperiment, TaskScalabilityExperiment,
    ResourceScalabilityExperiment, ConcurrencyScalabilityExperiment
)
from experiments.fault_tolerance_experiments import (
    TaskFailureExperiment, TimeoutExperiment,
    CheckpointRecoveryExperiment, CascadingFailureExperiment
)
from experiments.comparison_experiments import (
    ComparisonExperiment, SchedulerComparisonExperiment,
    DataFlowerSystem, SimpleDAGSystem, QueueBasedSystem, ParallelExecutorSystem
)
from src.scheduler import SchedulingStrategy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('experiments.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ExperimentConfig:
    """Configuration for experiments"""
    
    @staticmethod
    def load_config(config_file: str = "configs/experiment_config.yaml") -> dict:
        """Load experiment configuration from file"""
        config_path = Path(config_file)
        if config_path.exists():
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            # Return default configuration
            return ExperimentConfig.get_default_config()
    
    @staticmethod
    def get_default_config() -> dict:
        """Get default experiment configuration"""
        return {
            'performance': {
                'throughput': {
                    'enabled': True,
                    'num_workflows': 40,
                    'workflow_sizes': [10, 20, 30],
                    'platforms': ['local']
                },
                'latency': {
                    'enabled': True,
                    'num_iterations': 20,
                    'workflow_types': ['linear', 'parallel', 'dag']
                },
                'resource_efficiency': {
                    'enabled': True,
                    'num_workflows': 20,
                    'resource_configs': [
                        {'cpu_cores': 4, 'memory_gb': 8},
                        {'cpu_cores': 8, 'memory_gb': 16},
                        {'cpu_cores': 16, 'memory_gb': 32}
                    ]
                }
            },
            'scalability': {
                'workflow_scalability': {
                    'enabled': True,
                    'workflow_counts': [10, 25, 50, 100],
                    'task_size': 20
                },
                'task_scalability': {
                    'enabled': True,
                    'task_counts': [10, 25, 50, 100],
                    'num_workflows': 5
                },
                'resource_scalability': {
                    'enabled': True,
                    'resource_multipliers': [1, 2, 4, 8],
                    'workload_size': 50
                },
                'concurrency_scalability': {
                    'enabled': True,
                    'concurrency_levels': [1, 5, 10, 25],
                    'total_workflows': 100
                }
            },
            'fault_tolerance': {
                'task_failure': {
                    'enabled': True,
                    'failure_rates': [0.0, 0.1, 0.2, 0.3],
                    'num_workflows': 10,
                    'max_retries': 3
                },
                'timeout': {
                    'enabled': True,
                    'timeout_rates': [0.0, 0.05, 0.1, 0.15],
                    'num_workflows': 10,
                    'timeout_duration': 5.0
                },
                'checkpoint_recovery': {
                    'enabled': True,
                    'checkpoint_intervals': [5, 10, 20],
                    'failure_probability': 0.1,
                    'num_workflows': 10
                },
                'cascading_failure': {
                    'enabled': True,
                    'initial_failure_points': [1, 2, 3],
                    'propagation_probability': 0.3,
                    'num_workflows': 10
                }
            },
            'comparison': {
                'system_comparison': {
                    'enabled': True,
                    'workflow_types': ['linear', 'parallel', 'dag'],
                    'workflow_sizes': [20, 50],
                    'num_iterations': 5
                },
                'scheduler_comparison': {
                    'enabled': True,
                    'strategies': ['fifo', 'priority', 'sjf', 'cost_optimized'],
                    'workload_patterns': ['uniform', 'bursty', 'mixed'],
                    'num_workflows': 30
                }
            }
        }


class ExperimentRunner:
    """Main experiment runner"""
    
    def __init__(self, config: dict, output_dir: str = "results"):
        self.config = config
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.results_summary = {}
        
    async def run_performance_experiments(self):
        """Run performance experiments"""
        logger.info("=" * 60)
        logger.info("RUNNING PERFORMANCE EXPERIMENTS")
        logger.info("=" * 60)
        
        perf_config = self.config.get('performance', {})
        
        # Throughput experiment
        if perf_config.get('throughput', {}).get('enabled', True):
            logger.info("Running Throughput Experiment...")
            exp_config = perf_config['throughput']
            experiment = ThroughputExperiment("throughput", str(self.output_dir))
            await experiment.run(
                num_workflows=exp_config['num_workflows'],
                workflow_sizes=exp_config['workflow_sizes'],
                platforms=exp_config['platforms']
            )
            self.results_summary['throughput'] = "Completed"
        
        # Latency experiment
        if perf_config.get('latency', {}).get('enabled', True):
            logger.info("Running Latency Experiment...")
            exp_config = perf_config['latency']
            experiment = LatencyExperiment("latency", str(self.output_dir))
            await experiment.run(
                num_iterations=exp_config['num_iterations'],
                workflow_types=exp_config['workflow_types']
            )
            self.results_summary['latency'] = "Completed"
        
        # Resource efficiency experiment
        if perf_config.get('resource_efficiency', {}).get('enabled', True):
            logger.info("Running Resource Efficiency Experiment...")
            exp_config = perf_config['resource_efficiency']
            experiment = ResourceEfficiencyExperiment("resource_efficiency", str(self.output_dir))
            await experiment.run(
                num_workflows=exp_config['num_workflows'],
                resource_configs=exp_config['resource_configs']
            )
            self.results_summary['resource_efficiency'] = "Completed"
    
    async def run_scalability_experiments(self):
        """Run scalability experiments"""
        logger.info("=" * 60)
        logger.info("RUNNING SCALABILITY EXPERIMENTS")
        logger.info("=" * 60)
        
        scale_config = self.config.get('scalability', {})
        
        # Workflow scalability
        if scale_config.get('workflow_scalability', {}).get('enabled', True):
            logger.info("Running Workflow Scalability Experiment...")
            exp_config = scale_config['workflow_scalability']
            experiment = WorkflowScalabilityExperiment("workflow_scalability", str(self.output_dir))
            await experiment.run(
                workflow_counts=exp_config['workflow_counts'],
                task_size=exp_config['task_size']
            )
            self.results_summary['workflow_scalability'] = "Completed"
        
        # Task scalability
        if scale_config.get('task_scalability', {}).get('enabled', True):
            logger.info("Running Task Scalability Experiment...")
            exp_config = scale_config['task_scalability']
            experiment = TaskScalabilityExperiment("task_scalability", str(self.output_dir))
            await experiment.run(
                task_counts=exp_config['task_counts'],
                num_workflows=exp_config['num_workflows']
            )
            self.results_summary['task_scalability'] = "Completed"
        
        # Resource scalability
        if scale_config.get('resource_scalability', {}).get('enabled', True):
            logger.info("Running Resource Scalability Experiment...")
            exp_config = scale_config['resource_scalability']
            experiment = ResourceScalabilityExperiment("resource_scalability", str(self.output_dir))
            await experiment.run(
                resource_multipliers=exp_config['resource_multipliers'],
                workload_size=exp_config['workload_size']
            )
            self.results_summary['resource_scalability'] = "Completed"
        
        # Concurrency scalability
        if scale_config.get('concurrency_scalability', {}).get('enabled', True):
            logger.info("Running Concurrency Scalability Experiment...")
            exp_config = scale_config['concurrency_scalability']
            experiment = ConcurrencyScalabilityExperiment("concurrency_scalability", str(self.output_dir))
            await experiment.run(
                concurrency_levels=exp_config['concurrency_levels'],
                total_workflows=exp_config['total_workflows']
            )
            self.results_summary['concurrency_scalability'] = "Completed"
    
    async def run_fault_tolerance_experiments(self):
        """Run fault tolerance experiments"""
        logger.info("=" * 60)
        logger.info("RUNNING FAULT TOLERANCE EXPERIMENTS")
        logger.info("=" * 60)
        
        fault_config = self.config.get('fault_tolerance', {})
        
        # Task failure experiment
        if fault_config.get('task_failure', {}).get('enabled', True):
            logger.info("Running Task Failure Experiment...")
            exp_config = fault_config['task_failure']
            experiment = TaskFailureExperiment("task_failure", str(self.output_dir))
            await experiment.run(
                failure_rates=exp_config['failure_rates'],
                num_workflows=exp_config['num_workflows'],
                max_retries=exp_config['max_retries']
            )
            self.results_summary['task_failure'] = "Completed"
        
        # Timeout experiment
        if fault_config.get('timeout', {}).get('enabled', True):
            logger.info("Running Timeout Experiment...")
            exp_config = fault_config['timeout']
            experiment = TimeoutExperiment("timeout", str(self.output_dir))
            await experiment.run(
                timeout_rates=exp_config['timeout_rates'],
                num_workflows=exp_config['num_workflows'],
                timeout_duration=exp_config['timeout_duration']
            )
            self.results_summary['timeout'] = "Completed"
        
        # Checkpoint recovery experiment
        if fault_config.get('checkpoint_recovery', {}).get('enabled', True):
            logger.info("Running Checkpoint Recovery Experiment...")
            exp_config = fault_config['checkpoint_recovery']
            experiment = CheckpointRecoveryExperiment("checkpoint_recovery", str(self.output_dir))
            await experiment.run(
                checkpoint_intervals=exp_config['checkpoint_intervals'],
                failure_probability=exp_config['failure_probability'],
                num_workflows=exp_config['num_workflows']
            )
            self.results_summary['checkpoint_recovery'] = "Completed"
        
        # Cascading failure experiment
        if fault_config.get('cascading_failure', {}).get('enabled', True):
            logger.info("Running Cascading Failure Experiment...")
            exp_config = fault_config['cascading_failure']
            experiment = CascadingFailureExperiment("cascading_failure", str(self.output_dir))
            await experiment.run(
                initial_failure_points=exp_config['initial_failure_points'],
                propagation_probability=exp_config['propagation_probability'],
                num_workflows=exp_config['num_workflows']
            )
            self.results_summary['cascading_failure'] = "Completed"
    
    async def run_comparison_experiments(self):
        """Run comparison experiments"""
        logger.info("=" * 60)
        logger.info("RUNNING COMPARISON EXPERIMENTS")
        logger.info("=" * 60)
        
        comp_config = self.config.get('comparison', {})
        
        # System comparison
        if comp_config.get('system_comparison', {}).get('enabled', True):
            logger.info("Running System Comparison Experiment...")
            exp_config = comp_config['system_comparison']
            
            # Create systems to compare
            systems = [
                SimpleDAGSystem(),
                QueueBasedSystem(num_workers=5),
                ParallelExecutorSystem(max_concurrency=10),
                DataFlowerSystem(scheduling_strategy=SchedulingStrategy.PRIORITY),
                DataFlowerSystem(scheduling_strategy=SchedulingStrategy.COST_OPTIMIZED)
            ]
            
            experiment = ComparisonExperiment("system_comparison", str(self.output_dir))
            await experiment.run(
                systems=systems,
                workflow_types=exp_config['workflow_types'],
                workflow_sizes=exp_config['workflow_sizes'],
                num_iterations=exp_config['num_iterations']
            )
            self.results_summary['system_comparison'] = "Completed"
        
        # Scheduler comparison
        if comp_config.get('scheduler_comparison', {}).get('enabled', True):
            logger.info("Running Scheduler Comparison Experiment...")
            exp_config = comp_config['scheduler_comparison']
            
            # Convert strategy names to enums
            strategies = [
                SchedulingStrategy[s.upper()] 
                for s in exp_config['strategies']
            ]
            
            experiment = SchedulerComparisonExperiment(str(self.output_dir))
            await experiment.run(
                strategies=strategies,
                workload_patterns=exp_config['workload_patterns'],
                num_workflows=exp_config['num_workflows']
            )
            self.results_summary['scheduler_comparison'] = "Completed"
    
    async def run_all(self):
        """Run all experiments"""
        start_time = time.time()
        
        logger.info("Starting DataFlower Experiments")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Start time: {datetime.now().isoformat()}")
        
        try:
            # Run experiment categories
            await self.run_performance_experiments()
            await self.run_scalability_experiments()
            await self.run_fault_tolerance_experiments()
            await self.run_comparison_experiments()
            
            # Save summary
            self.save_summary(time.time() - start_time)
            
            logger.info("=" * 60)
            logger.info("ALL EXPERIMENTS COMPLETED SUCCESSFULLY")
            logger.info(f"Total time: {time.time() - start_time:.2f} seconds")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Error during experiments: {e}", exc_info=True)
            raise
    
    def save_summary(self, total_time: float):
        """Save experiment summary"""
        summary = {
            'timestamp': datetime.now().isoformat(),
            'total_time_seconds': total_time,
            'output_directory': str(self.output_dir),
            'experiments': self.results_summary,
            'configuration': self.config
        }
        
        summary_file = self.output_dir / "experiment_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Experiment summary saved to {summary_file}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Run DataFlower Experiments")
    parser.add_argument(
        '--config', '-c',
        type=str,
        default="configs/experiment_config.yaml",
        help="Path to experiment configuration file"
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        default="results",
        help="Output directory for results"
    )
    parser.add_argument(
        '--experiments', '-e',
        nargs='+',
        choices=['performance', 'scalability', 'fault_tolerance', 'comparison', 'all'],
        default=['all'],
        help="Which experiment categories to run"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = ExperimentConfig.load_config(args.config)
    
    # Filter experiments if specific ones requested
    if 'all' not in args.experiments:
        filtered_config = {}
        for category in args.experiments:
            if category in config:
                filtered_config[category] = config[category]
        config = filtered_config
    
    # Create runner
    runner = ExperimentRunner(config, args.output)
    
    # Run experiments
    asyncio.run(runner.run_all())


if __name__ == "__main__":
    main()
