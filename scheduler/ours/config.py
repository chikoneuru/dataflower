"""
Configuration file for the Enhanced Scheduling System
Allows easy switching between different algorithms and modes
"""

from typing import Dict, Any, Literal, Optional
from dataclasses import dataclass, field
import os
import json

# Algorithm types for each component
CostModelAlgorithm = Literal["real", "mock"]
ModeClassifierAlgorithm = Literal["ccr", "mock", "ml"]
SchedulingAlgorithm = Literal["network_bound", "compute_bound", "mock", "hybrid", "random_worker"]
OptimizationAlgorithm = Literal["local_search", "simulated_annealing", "genetic", "mock", "none"]

@dataclass
class CostModelConfig:
    """Configuration for Cost Models component"""
    algorithm: CostModelAlgorithm = "mock"
    base_cold_start: float = 2.0
    congestion_factor: float = 1.2
    cpu_interference_factor: float = 0.5
    memory_penalty_factor: float = 1.0
    enable_caching: bool = True
    cache_size: int = 1000

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CostModelConfig":
        return cls(**{k: v for k, v in data.items() if hasattr(cls, k)})

@dataclass
class ModeClassifierConfig:
    """Configuration for Mode Classifier component"""
    algorithm: ModeClassifierAlgorithm = "ccr"
    threshold: float = 1.0
    network_bound_weights: Dict[str, float] = None
    compute_bound_weights: Dict[str, float] = None
    enable_adaptive_threshold: bool = False
    history_window: int = 10
    
    def __post_init__(self):
        if self.network_bound_weights is None:
            self.network_bound_weights = {
                'alpha': 0.1,   # Low weight on compute
                'beta': 0.7,    # High weight on transfer
                'gamma': 0.1,   # Low weight on cold start
                'delta': 0.1    # Low weight on interference
            }
        if self.compute_bound_weights is None:
            self.compute_bound_weights = {
                'alpha': 0.6,   # High weight on compute
                'beta': 0.1,    # Low weight on transfer
                'gamma': 0.1,   # Low weight on cold start
                'delta': 0.2    # Medium weight on interference
            }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ModeClassifierConfig":
        return cls(**{k: v for k, v in data.items() if hasattr(cls, k)})

@dataclass
class SchedulingConfig:
    """Configuration for Scheduling Policies component"""
    algorithm: SchedulingAlgorithm = "network_bound"
    enable_priority_queue: bool = True
    max_retries: int = 3
    locality_bonus: float = 100.0
    load_balance_penalty: float = 0.1
    enable_dynamic_switching: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchedulingConfig":
        return cls(**{k: v for k, v in data.items() if hasattr(cls, k)})

@dataclass
class OptimizationConfig:
    """Configuration for Placement Optimizer component"""
    algorithm: OptimizationAlgorithm = "local_search"
    max_iterations: int = 100
    time_limit: float = 5.0
    initial_temp: float = 100.0
    cooling_rate: float = 0.95
    final_temp: float = 0.1
    population_size: int = 50  # For genetic algorithm
    mutation_rate: float = 0.1  # For genetic algorithm
    crossover_rate: float = 0.8  # For genetic algorithm
    enable_parallel: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OptimizationConfig":
        return cls(**{k: v for k, v in data.items() if hasattr(cls, k)})

@dataclass
class SystemConfig:
    """Overall system configuration"""
    cost_models: CostModelConfig = None
    mode_classifier: ModeClassifierConfig = None
    scheduling: SchedulingConfig = None
    optimization: OptimizationConfig = None
    enable_logging: bool = True
    log_level: str = "INFO"
    enable_metrics: bool = True
    metrics_interval: float = 1.0
    
    def __post_init__(self):
        if self.cost_models is None:
            self.cost_models = CostModelConfig()
        if self.mode_classifier is None:
            self.mode_classifier = ModeClassifierConfig()
        if self.scheduling is None:
            self.scheduling = SchedulingConfig()
        if self.optimization is None:
            self.optimization = OptimizationConfig()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SystemConfig":
        return cls(
            cost_models=CostModelConfig.from_dict(data.get("cost_models", {})),
            mode_classifier=ModeClassifierConfig.from_dict(data.get("mode_classifier", {})),
            scheduling=SchedulingConfig.from_dict(data.get("scheduling", {})),
            optimization=OptimizationConfig.from_dict(data.get("optimization", {})),
            enable_logging=data.get("enable_logging", True),
            log_level=data.get("log_level", "INFO"),
            enable_metrics=data.get("enable_metrics", True),
            metrics_interval=data.get("metrics_interval", 1.0)
        )

# Mapping of presets to their filenames
PRESET_FILES = {
    "development": "development.json",
    "production": "production.json",
    "testing": "testing.json"
}

# Load from file utility - DEFINED AT MODULE LEVEL
def load_config_from_file(preset: str) -> Optional[Dict]:
    filename = PRESET_FILES.get(preset)
    if not filename:
        return None
    
    filepath = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "configs", filename)
    if not os.path.exists(filepath):
        return None
    
    with open(filepath, 'r') as f:
        return json.load(f)

# The main config object, populated by `get_config`
_config: Optional[SystemConfig] = None

# Accessor
def get_config() -> Optional[SystemConfig]:
    return _config

# Config presets
class ConfigPresets:
    @staticmethod
    def get_development_config() -> SystemConfig:
        data = load_config_from_file("development")
        return SystemConfig.from_dict(data) if data else None

    @staticmethod
    def get_production_config() -> SystemConfig:
        data = load_config_from_file("production")
        return SystemConfig.from_dict(data) if data else None
    
    @staticmethod
    def get_testing_config() -> SystemConfig:
        data = load_config_from_file("testing")
        return SystemConfig.from_dict(data) if data else None

# Initializer
def load_config(preset: str = "development"):
    global _config
    if preset == "development":
        _config = ConfigPresets.get_development_config()
    elif preset == "production":
        _config = ConfigPresets.get_production_config()
    elif preset == "testing":
        _config = ConfigPresets.get_testing_config()
    else:
        raise ValueError(f"Unknown config preset: {preset}")

    if _config is None:
        raise FileNotFoundError(f"Could not load config file for preset: {preset}")

# Alias for backward compatibility
def initialize_config(preset: str = "development"):
    """Alias for load_config for backward compatibility"""
    load_config(preset)
