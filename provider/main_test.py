"""
Main testing file for the Enhanced Scheduling System
Allows easy testing of different configurations and algorithms
"""

import json
import unittest
from typing import Any, Dict

from scheduler.ours.config import (ConfigPresets, SystemConfig, get_config,
                                   load_config)
# Updated to use the unified orchestrator
from scheduler.ours.orchestrator import Orchestrator


def create_mock_nodes(num_nodes):
    """Create mock node data for testing"""
    nodes = []
    for i in range(num_nodes):
        nodes.append({
            'id': f'node-{i+1}',
            'name': f'Mock Node {i+1}',
            'status': 'ready',
            'cpu': 4,
            'memory': 8
        })
    return nodes


def create_mock_workflow(num_tasks):
    """Create mock workflow data for testing"""
    tasks = []
    for i in range(num_tasks):
        tasks.append({
            'id': f'task-{i+1}',
            'name': f'Mock Task {i+1}',
            'status': 'pending',
            'priority': 'medium',
            'estimated_time': 10
        })
    return tasks

class SchedulingSystemTester:
    """Main testing class for the scheduling system"""
    
    def __init__(self, config_preset: str = "testing"):
        load_config(preset=config_preset)
        self.config = get_config()
        self.orchestrator = Orchestrator(config=self.config)
        self.logger = self.orchestrator.logger
        self.test_results = []
     
    def run_full_test(self, num_nodes=3, num_tasks=5):
        self.logger.info("="*20 + " STARTING FULL SYSTEM TEST " + "="*20)
        nodes = create_mock_nodes(num_nodes)
        tasks = create_mock_workflow(num_tasks)
        
        self.logger.info(f"Running test with {self.config.name} config...")
        self.logger.info(f"Created {len(nodes)} mock nodes and {len(tasks)} mock tasks")
        
        # Test basic orchestrator functionality
        try:
            # Test cluster node retrieval (will work if kubectl is available)
            cluster_nodes = self.orchestrator.get_cluster_nodes()
            self.logger.info(f"Retrieved {len(cluster_nodes)} cluster nodes")
            self.log_test_result("cluster_nodes", {"count": len(cluster_nodes)})
        except Exception as e:
            self.logger.info(f"Cluster node retrieval failed (expected in test environment): {e}")
        
        # Test data locality status
        try:
            locality_status = self.orchestrator.get_cluster_data_locality_status()
            self.logger.info("Data locality status retrieved successfully")
            self.log_test_result("data_locality_status", {"status": "available"})
        except Exception as e:
            self.logger.info(f"Data locality status failed: {e}")
            self.log_test_result("data_locality_status", {"status": "failed", "error": str(e)})

        self.logger.info("\n" + "="*20 + " PERFORMANCE SUMMARY " + "="*20)
        summary = self.orchestrator.get_performance_summary()
        self.logger.info(json.dumps(summary, indent=2))
        self.logger.info("="*20 + " FULL SYSTEM TEST COMPLETE " + "="*20)

    def log_test_result(self, test_name: str, result_data: Dict[str, Any]):
        result = {"test_name": test_name, "data": result_data}
        self.test_results.append(result)
        self.logger.debug(f"Logged test result for '{test_name}'")

if __name__ == '__main__':
    tester = SchedulingSystemTester(config_preset="testing")
    tester.run_full_test()
