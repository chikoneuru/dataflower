# flake8: noqa
"""
Provider package for the serverless platform.
"""

__version__ = "0.1.0"

# Core components
from .container_manager import ContainerManager
from .function_manager import FunctionManager

# Local scheduler components
from .cache_manager import CacheManager
from .data_locality_config import DataLocalityPresets, DataLocalitySystemConfig
from .remote_storage_adapter import create_storage_adapter, RemoteStorageAdapter

# Public API of the provider package
__all__ = [
    
    # Core Provider Components
    "ContainerManager",
    "FunctionManager",
    "CacheManager",
    "DataLocalityPresets",
    "DataLocalitySystemConfig",
    "create_storage_adapter",
    "RemoteStorageAdapter",

    # Test Utilities
    "SchedulingSystemTester"
]
