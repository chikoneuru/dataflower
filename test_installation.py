#!/usr/bin/env python3
"""
Test script to verify DataFlower installation and basic functionality
"""

import sys
import asyncio
from pathlib import Path

# Add current directory to path
sys.path.append(str(Path(__file__).parent))

def test_imports():
    """Test that all modules can be imported"""
    print("Testing imports...")
    try:
        from src.workflow_engine import Workflow, Task, WorkflowEngine
        from src.scheduler import AdaptiveScheduler, SchedulingStrategy
        from src.monitoring import MetricsCollector
        from experiments.performance_experiments import WorkflowGenerator
        print("✓ All core modules imported successfully")
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False


async def test_basic_workflow():
    """Test basic workflow execution"""
    print("\nTesting basic workflow execution...")
    try:
        from src.workflow_engine import Workflow, Task, WorkflowEngine, ServerlessExecutor
        
        # Create a simple workflow
        tasks = [
            Task(id="task_0", name="Task 0", function="data_ingestion", dependencies=[]),
            Task(id="task_1", name="Task 1", function="data_transformation", dependencies=["task_0"]),
            Task(id="task_2", name="Task 2", function="data_validation", dependencies=["task_1"])
        ]
        
        workflow = Workflow(
            name="Test Workflow",
            description="Simple test workflow",
            tasks=tasks
        )
        
        # Create engine and execute
        executor = ServerlessExecutor(platform="local")
        engine = WorkflowEngine(executor=executor)
        
        # Submit workflow
        workflow_id = await engine.submit_workflow(workflow)
        print(f"  Workflow submitted: {workflow_id}")
        
        # Wait for completion
        max_wait = 10  # seconds
        waited = 0
        while waited < max_wait:
            status = engine.get_workflow_status(workflow_id)
            if status and status['status'] in ['completed', 'failed']:
                break
            await asyncio.sleep(0.5)
            waited += 0.5
        
        # Check result
        if status and status['status'] == 'completed':
            print("✓ Workflow executed successfully")
            return True
        else:
            print(f"✗ Workflow failed or timed out: {status}")
            return False
            
    except Exception as e:
        print(f"✗ Error executing workflow: {e}")
        return False


async def test_scheduler():
    """Test scheduler functionality"""
    print("\nTesting scheduler...")
    try:
        from src.scheduler import FIFOScheduler, TaskSchedulingInfo, ResourcePool
        from src.workflow_engine import Task
        
        # Create scheduler
        scheduler = FIFOScheduler()
        
        # Create resource pool
        resources = ResourcePool(cpu_cores=4, memory_gb=8)
        
        # Create test tasks
        tasks = []
        for i in range(3):
            task = Task(id=f"task_{i}", name=f"Task {i}", function="test")
            task_info = TaskSchedulingInfo(
                task=task,
                priority=i,
                resource_requirements={'cpu': 1, 'memory': 1}
            )
            tasks.append(task_info)
        
        # Schedule tasks
        scheduled = await scheduler.schedule(tasks, resources)
        
        if len(scheduled) > 0:
            print(f"✓ Scheduler working: scheduled {len(scheduled)} tasks")
            return True
        else:
            print("✗ Scheduler failed to schedule tasks")
            return False
            
    except Exception as e:
        print(f"✗ Error testing scheduler: {e}")
        return False


def test_monitoring():
    """Test monitoring functionality"""
    print("\nTesting monitoring...")
    try:
        from src.monitoring import MetricsCollector
        
        # Create metrics collector
        collector = MetricsCollector()
        
        # Record some metrics
        collector.record_metric("test_metric", 42.0)
        collector.start_workflow("test_workflow", 10)
        collector.complete_workflow("test_workflow", "completed")
        
        # Get stats
        stats = collector.get_metric_stats("test_metric")
        
        if stats and 'mean' in stats:
            print("✓ Monitoring working: metrics recorded")
            return True
        else:
            print("✗ Monitoring failed to record metrics")
            return False
            
    except Exception as e:
        print(f"✗ Error testing monitoring: {e}")
        return False


def test_workflow_generator():
    """Test workflow generation"""
    print("\nTesting workflow generation...")
    try:
        from experiments.performance_experiments import WorkflowGenerator
        
        # Generate different workflow types
        linear = WorkflowGenerator.generate_linear_workflow(5)
        parallel = WorkflowGenerator.generate_parallel_workflow(3, 2)
        dag = WorkflowGenerator.generate_dag_workflow(10)
        mapreduce = WorkflowGenerator.generate_mapreduce_workflow(4, 2)
        
        # Validate workflows
        workflows = [linear, parallel, dag, mapreduce]
        for wf in workflows:
            if not wf.validate_dag():
                print(f"✗ Invalid workflow generated: {wf.name}")
                return False
        
        print("✓ All workflow types generated successfully")
        return True
        
    except Exception as e:
        print(f"✗ Error testing workflow generator: {e}")
        return False


async def main():
    """Run all tests"""
    print("=" * 60)
    print("DataFlower Installation Test")
    print("=" * 60)
    
    tests = [
        ("Import Test", test_imports()),
        ("Workflow Execution", await test_basic_workflow()),
        ("Scheduler", await test_scheduler()),
        ("Monitoring", test_monitoring()),
        ("Workflow Generator", test_workflow_generator())
    ]
    
    print("\n" + "=" * 60)
    print("Test Results:")
    print("-" * 60)
    
    passed = 0
    failed = 0
    
    for name, result in tests:
        status = "PASSED" if result else "FAILED"
        symbol = "✓" if result else "✗"
        print(f"{symbol} {name:<30} {status}")
        if result:
            passed += 1
        else:
            failed += 1
    
    print("-" * 60)
    print(f"Total: {passed} passed, {failed} failed")
    print("=" * 60)
    
    if failed == 0:
        print("\n🎉 All tests passed! DataFlower is ready to use.")
        print("\nNext steps:")
        print("1. Run experiments: python run_all_experiments.py")
        print("2. View results: check the results/ directory")
        print("3. Start Jupyter: docker-compose up jupyter")
        return 0
    else:
        print("\n⚠️ Some tests failed. Please check the installation.")
        print("\nTroubleshooting:")
        print("1. Ensure all dependencies are installed: pip install -r requirements.txt")
        print("2. Check Redis is running: redis-cli ping")
        print("3. Review error messages above")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
