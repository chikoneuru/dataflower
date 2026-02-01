"""
Configuration file for the FaasPRS Scheduling System
Provides cluster and scheduling parameters for simulation
"""

from dataclasses import dataclass, field
from typing import Optional

# --------------------------------------
# Component Configurations
# --------------------------------------

@dataclass
class SchedulingConfig:
    """Configuration for Scheduling Policies"""
    algorithm: str = "network_bound"   # Options: "network_bound", "compute_bound"
    enable_priority_queue: bool = True
    max_retries: int = 3


@dataclass
class ClusterConfig:
    """Configuration for Cluster Resources"""
    servers: int = 8
    memory_per_server: int = 128       # GB
    bandwidth: int = 50                # MB/s
    scaling_interval: int = 60         # seconds
    cold_start_time: float = 1.5       # seconds
    warm_start_time: float = 0.05      # seconds
    cpu_cores: int = 16                # cores per server
    cache_enabled: bool = True
    cache_size: int = 1000             # number of cached containers


@dataclass
class GlobalConfig:
    """Overall FaasPRS Configuration"""
    scheduling: SchedulingConfig = field(default_factory=SchedulingConfig)
    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    enable_logging: bool = True
    log_level: str = "INFO"
    enable_metrics: bool = True


# --------------------------------------
# Global Config Accessors
# --------------------------------------

_config: Optional[GlobalConfig] = None


def initialize_config():
    """Initialize global configuration (default values)"""
    global _config
    _config = GlobalConfig()


def get_config() -> GlobalConfig:
    """Return the active global configuration"""
    return _config
