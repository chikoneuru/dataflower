"""
Test file for Cost Models component
"""

import unittest

from scheduler.ours.cost_models import CostModels, NodeProfile, TaskProfile


class TestCostModels(unittest.TestCase):
    """Test cases for Cost Models"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.cost_models = CostModels()
        self.task = TaskProfile(
            task_id="test_task",
            function_name="test_function",
            input_size=1024 * 1024,  # 1MB
            estimated_compute_time=1.0,
            memory_requirement=512,  # 512MB
            cpu_intensity=0.5
        )
        self.node = NodeProfile(
            node_id="test_node",
            cpu_cores=4,
            memory_total=8192,  # 8GB
            memory_available=4096,  # 4GB
            cpu_utilization=0.3,
            network_bandwidth=1000,  # 1Gbps
            container_cache={}
        )
    
    def test_mock_compute_cost(self):
        """Test mock compute cost estimation"""
        cost = self.cost_models.mock_estimate_compute_cost(self.task, self.node)
        self.assertIsInstance(cost, float)
        self.assertGreater(cost, 0)
        print(f"Mock compute cost: {cost}")
    
    def test_mock_transfer_cost(self):
        """Test mock transfer cost estimation"""
        cost = self.cost_models.mock_estimate_transfer_cost(1024 * 1024, self.node, self.node)
        self.assertIsInstance(cost, float)
        self.assertGreater(cost, 0)
        print(f"Mock transfer cost: {cost}")
    
    def test_mock_cold_start_cost(self):
        """Test mock cold start cost estimation"""
        cost = self.cost_models.mock_estimate_cold_start_cost("test_function", self.node)
        self.assertIsInstance(cost, float)
        self.assertGreaterEqual(cost, 0)
        print(f"Mock cold start cost: {cost}")
    
    def test_mock_get_all_costs(self):
        """Test mock get all costs"""
        costs = self.cost_models.mock_get_all_costs(self.task, self.node, [])
        self.assertIsInstance(costs, dict)
        self.assertIn('compute', costs)
        self.assertIn('transfer', costs)
        self.assertIn('cold_start', costs)
        self.assertIn('interference', costs)
        self.assertIn('total', costs)
        print(f"Mock all costs: {costs}")
    
    def test_create_mock_task(self):
        """Test creating mock task"""
        task = self.cost_models.create_mock_task("test_task", "test_function")
        self.assertIsInstance(task, TaskProfile)
        self.assertEqual(task.task_id, "test_task")
        self.assertEqual(task.function_name, "test_function")
        print(f"Mock task: {task}")
    
    def test_create_mock_node(self):
        """Test creating mock node"""
        node = self.cost_models.create_mock_node("test_node")
        self.assertIsInstance(node, NodeProfile)
        self.assertEqual(node.node_id, "test_node")
        print(f"Mock node: {node}")

if __name__ == '__main__':
    unittest.main()