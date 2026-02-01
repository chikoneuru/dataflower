# flake8: noqa
"""
Provider package for the serverless platform.
"""

__version__ = "0.1.0"

# Core components
from .container_manager import ContainerManager
# Local scheduler components
from .data_locality_config import DataLocalityPresets, DataLocalitySystemConfig
from .function_container_manager import FunctionContainerManager
from .function_manager import FunctionManager
from .remote_storage_adapter import (RemoteStorageAdapter,
                                     create_storage_adapter)

# Public API of the provider package
__all__ = [
    
    # Core Provider Components
    "ContainerManager",
    "FunctionManager", 
    "FunctionContainerManager",
    "CacheManager",
    "DataLocalityPresets",
    "DataLocalitySystemConfig",
    "create_storage_adapter",
    "RemoteStorageAdapter",

    # Test Utilities
    "SchedulingSystemTester"
]
