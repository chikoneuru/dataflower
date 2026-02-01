"""
Palette Scheduler Configuration
"""
import logging
from dataclasses import dataclass
from typing import Optional


@dataclass
class PaletteConfig:
    """Configuration for Palette scheduler"""
    
    # Bucket configuration
    num_buckets: int = 256  # Number of hash buckets for color mapping
    
    # Bucket assignment strategy
    bucket_assignment_strategy: str = "round_robin"  # Options: round_robin, capacity_weighted, random
    per_function_buckets: bool = True  # If True, each function has its own bucket space
    
    # Color hint configuration
    color_hint_source: str = "request_id"  # Options: request_id, auto_generated, explicit
    hash_algorithm: str = "builtin"  # Options: builtin (Python hash()), md5, sha256
    
    # Container selection
    enable_fallback: bool = True  # Enable fallback if target container unavailable
    fallback_strategy: str = "same_node"  # Options: same_node, rehash, random
    
    # Dynamic rebalancing
    enable_rebalancing: bool = False  # Rebalance buckets when containers change
    rebalancing_threshold: float = 0.3  # Trigger rebalancing if load imbalance > 30%
    
    # Logging
    enable_logging: bool = True
    log_level: str = "INFO"
    
    # Capacity awareness
    capacity_weighted_assignment: bool = False  # Weight bucket assignment by node capacity
    
    # Container queue management
    enable_container_queue: bool = False  # Enable container queue system to prevent routing conflicts
    queue_max_depth: int = 10  # Maximum queue depth per container
    queue_request_timeout: float = 300.0  # Request timeout in queue (seconds)
    queue_process_interval: float = 0.1  # Queue processing interval (seconds)


# Global config instance
_config: Optional[PaletteConfig] = None


def initialize_config(config: Optional[PaletteConfig] = None) -> None:
    """Initialize global configuration"""
    global _config
    if config is None:
        _config = PaletteConfig()
    else:
        _config = config


def get_config() -> Optional[PaletteConfig]:
    """Get global configuration"""
    return _config


def load_config(config_dict: Optional[dict] = None) -> PaletteConfig:
    """Load configuration from dictionary"""
    if config_dict is None:
        config = PaletteConfig()
    else:
        config = PaletteConfig(**{k: v for k, v in config_dict.items() if k in PaletteConfig.__annotations__})
    
    initialize_config(config)
    return config

