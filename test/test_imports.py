"""
Test file to verify all imports work correctly
"""

import unittest

class TestImports(unittest.TestCase):
    """Test cases for module imports"""
    
    def test_import_core_components(self):
        """Test importing core components"""
        try:
            from scheduler.ours.cost_models import CostModels, TaskProfile, NodeProfile
            from scheduler.ours.mode_classifier import ModeClassifier, DAGCosts
            from scheduler.ours.scheduler import Scheduler, TaskAssignment
            from scheduler.ours.placement_optimizer import PlacementOptimizer, OptimizationResult
            from scheduler.ours.orchestrator import Orchestrator
            print("✅ Core components imported successfully")
        except ImportError as e:
            self.fail(f"Failed to import core components: {e}")
    
    def test_import_config(self):
        """Test importing configuration system"""
        try:
            from scheduler.ours.config import (
                SystemConfig, CostModelConfig, ModeClassifierConfig,
                SchedulingConfig, OptimizationConfig, ConfigPresets
            )
            print("✅ Configuration system imported successfully")
        except ImportError as e:
            self.fail(f"Failed to import configuration system: {e}")
    
    def test_import_legacy_components(self):
        """Test importing legacy components"""
        try:
            from scheduler.ours.scheduler import Scheduler
            from provider.container_manager import ContainerManager
            from provider.function_manager import FunctionManager
            from scheduler.ours.metrics import Metrics
            print("✅ Legacy components imported successfully")
        except ImportError as e:
            self.fail(f"Failed to import legacy components: {e}")
    
    def test_import_from_init(self):
        """Test importing from __init__.py"""
        try:
            from provider import ContainerManager, FunctionManager
            from scheduler.ours import Orchestrator, Scheduler, CostModels, ModeClassifier, PlacementOptimizer
            print("✅ __init__.py imports work successfully")
        except ImportError as e:
            self.fail(f"Failed to import from __init__.py: {e}")

    def test_create_orchestrator(self):
        """Test creating orchestrator"""
        try:
            from scheduler.ours import Orchestrator
            from scheduler.ours.config import initialize_config
            # Initialize config first
            initialize_config('testing')
            # Create a basic orchestrator for testing
            orchestrator = Orchestrator(None, None)
            self.assertIsNotNone(orchestrator)
            print("✅ Orchestrator created successfully")
        except Exception as e:
            self.fail(f"Failed to create orchestrator: {e}")

if __name__ == '__main__':
    unittest.main()
