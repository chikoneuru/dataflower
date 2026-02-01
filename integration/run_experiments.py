#!/usr/bin/env python3
"""
Simple Experiment Runner Script

This script runs scheduler comparison experiments using the configuration file.
"""

import sys
from pathlib import Path

import yaml

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from experiment_runner import (ExperimentConfig, ExperimentRunner,
                               create_experiment_configs)


def load_config(config_file: str = "experiment_config.yaml") -> dict:
    """Load experiment configuration from YAML file"""
    try:
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"❌ Configuration file not found: {config_file}")
        print("   Using default configuration...")
        return {}
    except Exception as e:
        print(f"❌ Failed to load configuration: {e}")
        return {}


def main():
    """Main entry point"""
    print("🎭 Scheduler Comparison Experiment Runner")
    print("=" * 50)
    
    # Load configuration
    config = load_config()
    
    # Extract configuration parameters
    input_sizes = config.get('input_sizes', [0.1, 1.0])
    concurrency_levels = config.get('concurrency_levels', [1, 5])
    schedulers = config.get('schedulers', ["ours"])
    requests_per_experiment = config.get('requests_per_experiment', 7)
    workflow_name = config.get('workflow_name', "recognizer")
    
    print(f"📋 Configuration loaded:")
    print(f"   Input sizes: {input_sizes} MB")
    print(f"   Concurrency levels: {concurrency_levels}")
    print(f"   Schedulers: {schedulers}")
    print(f"   Requests per experiment: {requests_per_experiment}")
    print(f"   Workflow: {workflow_name}")
    
    # Create experiment configurations
    configs = create_experiment_configs(
        input_sizes=input_sizes,
        concurrency_levels=concurrency_levels,
        schedulers=schedulers,
        requests_per_experiment=requests_per_experiment
    )
    
    print(f"\n📊 Created {len(configs)} experiment configurations")
    
    # Initialize experiment runner
    runner = ExperimentRunner()
    
    try:
        # Setup environment
        if not runner.setup_environment():
            print("❌ Environment setup failed")
            return 1
        
        # Load workflow
        if not runner.load_workflow(workflow_name):
            print("❌ Workflow loading failed")
            return 1
        
        # Run experiments
        results = runner.run_experiment_suite(configs)
        
        if not results:
            print("❌ No experiments completed successfully")
            return 1
        
        # Save results
        filename = runner.save_results(results)
        
        # Print summary
        runner.print_summary(results)
        
        print(f"\n✅ Experiment suite completed! Results saved to {filename}")
        return 0
        
    except KeyboardInterrupt:
        print("\n🛑 Experiment interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Experiment failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
