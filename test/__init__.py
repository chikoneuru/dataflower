"""
Test Package

This package contains all testing modules for the DataFlower serverless platform.
It includes performance tests, unit tests, integration tests, and testing utilities.
"""

# Performance testing modules
from .performance_test import PerformanceTest
from .async_test import test_to_one, test_to_all

# Simple testing utilities
from .simple_test import *

# Test configuration and utilities
try:
    from .test_config import TestConfig
except ImportError:
    TestConfig = None

# Testing constants and utilities
__all__ = [
    # Main test classes
    'PerformanceTest',
    
    # Async test functions
    'test_to_one',
    'test_to_all',
    
    # Configuration
    'TestConfig',
    
    # Simple test utilities (imported via *)
]

# Test package metadata
__version__ = '1.0.0'
__description__ = 'Testing suite for DataFlower serverless platform'

# # Test configuration defaults
# DEFAULT_TEST_CONFIG = {
#     'pre_warm_time': 60,
#     'default_duration': 120,
#     'default_rpm_rates': [5, 10, 20, 30, 50],
#     'default_size_tests': [(0.1, 'text'), (1, 'text'), (5, 'text')],
#     'default_parallel_levels': [1, 5, 10, 20],
#     'timeout': 90,
#     'join_timeout': 120
# }

# def get_test_config():
#     """Get default test configuration"""
#     return DEFAULT_TEST_CONFIG.copy()

# def run_all_performance_tests(workflow='wordcount'):
#     """Convenience function to run all performance tests"""
#     test = PerformanceTest()
#     return test.run_all_tests(workflow)

# def run_simple_rpm_test(rates=None):
#     """Convenience function to run simple RPM tests"""
#     from .simple_test import main as run_simple_test
#     import sys
    
#     if rates:
#         original_argv = sys.argv.copy()
#         sys.argv = ['simple_test.py', ','.join(map(str, rates))]
#         try:
#             run_simple_test()
#         finally:
#             sys.argv = original_argv
#     else:
#         run_simple_test()