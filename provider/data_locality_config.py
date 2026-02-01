"""
Configuration for Data Locality Management
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class RemoteStorageConfig:
    """Configuration for remote storage"""
    type: str = "minio"  # "minio", "local_file", "mock"
    
    # MinIO configuration (perfect for large files: images, videos, npy)
    minio_host: str = "minio"
    minio_port: int = 9000
    access_key: str = "dataflower"
    secret_key: str = "dataflower123"
    bucket_name: str = "dataflower-storage"
    
    # Local file configuration
    base_path: str = "/tmp/dataflower_remote"
    
    # Common configuration
    prefix: str = "dataflower/"
    
    @classmethod
    def from_dict(cls, config: Dict) -> 'RemoteStorageConfig':
        """Create from dictionary"""
        return cls(**config)
    
    @classmethod
    def get_minio_config(cls, minio_host: str = "minio", minio_port: int = 9000,
                        access_key: str = "dataflower", secret_key: str = "dataflower123",
                        bucket_name: str = "dataflower-storage") -> 'RemoteStorageConfig':
        """Create MinIO configuration (perfect for large files)"""
        return cls(
            type="minio",
            minio_host=minio_host,
            minio_port=minio_port,
            access_key=access_key,
            secret_key=secret_key,
            bucket_name=bucket_name
        )
    
    @classmethod
    def get_local_file_config(cls, base_path: str = "/tmp/dataflower_remote") -> 'RemoteStorageConfig':
        """Create local file configuration"""
        return cls(
            type="local_file",
            base_path=base_path
        )
    
    @classmethod
    def get_mock_config(cls) -> 'RemoteStorageConfig':
        """Create mock configuration for testing"""
        return cls(type="mock")


@dataclass
class CacheConfig:
    """Configuration for local caching"""
    cache_dir: str = "/tmp/dataflower_cache"
    max_disk_size_gb: int = 10
    max_memory_size_mb: int = 1024
    max_items: int = 10000
    max_age_hours: int = 24
    cleanup_interval_seconds: int = 300
    eviction_strategy: str = "lru"  # "lru", "lfu", "fifo"
    
    @classmethod
    def from_dict(cls, config: Dict) -> 'CacheConfig':
        """Create from dictionary"""
        return cls(**config)
    
    @classmethod
    def get_development_config(cls) -> 'CacheConfig':
        """Get configuration for development"""
        return cls(
            cache_dir="/tmp/dataflower_cache_dev",
            max_disk_size_gb=1,
            max_memory_size_mb=256,
            max_items=100,
            max_age_hours=1
        )
    
    @classmethod
    def get_production_config(cls) -> 'CacheConfig':
        """Get configuration for production"""
        return cls(
            cache_dir="/data/dataflower_cache",
            max_disk_size_gb=50,
            max_memory_size_mb=4096,
            max_items=50000,
            max_age_hours=48
        )


@dataclass
class LocalityConfig:
    """Configuration for data locality management"""
    node_id: str = "unknown"
    enable_locality: bool = True
    enable_metrics: bool = True
    locality_weights: Dict[str, float] = field(default_factory=lambda: {
        'local_data_bonus': 100.0,
        'transfer_cost_penalty': 1.0,
        'cache_hit_bonus': 50.0,
        'cross_node_penalty': 10.0
    })
    
    @classmethod
    def from_dict(cls, config: Dict) -> 'LocalityConfig':
        """Create from dictionary"""
        return cls(**config)


@dataclass
class StorageStrategy:
    """Defines which storage backend to use for different environments"""
    testing: str = "mock"           # For unit tests
    development: str = "minio"      # For local development  
    staging: str = "minio"          # For staging environment
    production: str = "minio"       # For production
    
    def get_storage_type(self, environment: str) -> str:
        """Get storage type for given environment"""
        return getattr(self, environment, "mock")
    
    @classmethod
    def from_dict(cls, config: Dict) -> 'StorageStrategy':
        """Create from dictionary"""
        return cls(**config)


@dataclass
class DataLocalitySystemConfig:
    """Complete configuration for data locality system"""
    remote_storage: RemoteStorageConfig = field(default_factory=RemoteStorageConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)  
    locality: LocalityConfig = field(default_factory=LocalityConfig)
    storage_strategy: StorageStrategy = field(default_factory=StorageStrategy)
    current_environment: str = "development"  # Default environment
    
    @classmethod
    def from_dict(cls, config: Dict) -> 'DataLocalitySystemConfig':
        """Create from dictionary"""
        return cls(
            remote_storage=RemoteStorageConfig.from_dict(config.get('remote_storage', {})),
            cache=CacheConfig.from_dict(config.get('cache', {})),
            locality=LocalityConfig.from_dict(config.get('locality', {})),
            storage_strategy=StorageStrategy.from_dict(config.get('storage_strategy', {})),
            current_environment=config.get('current_environment', 'development')
        )
    
    def get_effective_storage_config(self) -> RemoteStorageConfig:
        """Get storage config based on current environment and strategy"""
        storage_type = self.storage_strategy.get_storage_type(self.current_environment)
        
        if storage_type == "mock":
            return RemoteStorageConfig.get_mock_config()
        elif storage_type == "minio":
            return RemoteStorageConfig.get_minio_config()
        elif storage_type == "local_file":
            return RemoteStorageConfig.get_local_file_config(
                base_path=self.remote_storage.base_path
            )
        else:
            # Fallback to configured storage
            return self.remote_storage
    
    def set_environment(self, environment: str) -> 'DataLocalitySystemConfig':
        """Change environment and return new config with appropriate storage"""
        new_config = DataLocalitySystemConfig(
            remote_storage=self.remote_storage,
            cache=self.cache,
            locality=self.locality,
            storage_strategy=self.storage_strategy,
            current_environment=environment
        )
        # Update remote storage based on new environment
        new_config.remote_storage = new_config.get_effective_storage_config()
        return new_config
    
    @classmethod
    def get_config_for_environment(cls, environment: str, node_id: str = "node") -> 'DataLocalitySystemConfig':
        """Get configuration for specific environment - flexible storage selection"""
        config = cls(
            remote_storage=RemoteStorageConfig(),  # Will be set by get_effective_storage_config
            cache=CacheConfig.get_development_config() if environment in ['testing', 'development'] else CacheConfig.get_production_config(),
            locality=LocalityConfig(node_id=node_id),
            storage_strategy=StorageStrategy(),
            current_environment=environment
        )
        # Set appropriate storage based on environment
        config.remote_storage = config.get_effective_storage_config()
        return config
    
    @classmethod
    def get_development_config(cls, node_id: str = "dev-node") -> 'DataLocalitySystemConfig':
        """Get development configuration (uses minio by default)"""
        return cls.get_config_for_environment("development", node_id)
    
    @classmethod
    def get_testing_config(cls, node_id: str = "test-node") -> 'DataLocalitySystemConfig':
        """Get testing configuration (uses mock by default)"""
        return cls.get_config_for_environment("testing", node_id)
        
    @classmethod
    def get_production_config(cls, node_id: str = "prod-node") -> 'DataLocalitySystemConfig':
        """Get production configuration (uses minio by default)"""
        return cls.get_config_for_environment("production", node_id)
    
# Removed S3 configuration method - not needed for this project
    
    @classmethod
    def get_local_cluster_config(cls, node_id: str, shared_storage_path: str = "/shared/dataflower") -> 'DataLocalitySystemConfig':
        """Get configuration for local cluster with shared filesystem"""
        return cls(
            remote_storage=RemoteStorageConfig.get_local_file_config(shared_storage_path),
            cache=CacheConfig(cache_dir=f"/tmp/dataflower_cache_{node_id}"),
            locality=LocalityConfig(node_id=node_id)
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'remote_storage': self.remote_storage.__dict__,
            'cache': self.cache.__dict__,
            'locality': self.locality.__dict__,
            'storage_strategy': self.storage_strategy.__dict__,
            'current_environment': self.current_environment
        }


# Preset configurations
class DataLocalityPresets:
    """Predefined configuration presets"""
    
    @staticmethod
    def for_environment(environment: str, node_ids: List[str]) -> Dict[str, DataLocalitySystemConfig]:
        """Get configurations for multiple nodes in specific environment"""
        configs = {}
        for node_id in node_ids:
            configs[node_id] = DataLocalitySystemConfig.get_config_for_environment(environment, node_id)
        return configs
    
    @staticmethod
    def development(node_id: str = "dev-node") -> DataLocalitySystemConfig:
        """Development preset (uses minio by default, easily configurable)"""
        return DataLocalitySystemConfig.get_development_config(node_id)
    
    @staticmethod
    def testing(node_id: str = "test-node") -> DataLocalitySystemConfig:
        """Testing preset (uses mock by default, easily configurable)"""
        return DataLocalitySystemConfig.get_testing_config(node_id)
    
    @staticmethod
    def production(node_id: str = "prod-node") -> DataLocalitySystemConfig:
        """Production preset (uses s3 by default, easily configurable)"""
        return DataLocalitySystemConfig.get_production_config(node_id)
    
    @staticmethod
    def local_cluster(node_ids: List[str], shared_path: str = "/shared/dataflower") -> Dict[str, DataLocalitySystemConfig]:
        """Local cluster preset with shared filesystem"""
        configs = {}
        for node_id in node_ids:
            configs[node_id] = DataLocalitySystemConfig.get_local_cluster_config(node_id, shared_path)
        return configs

def load_config_from_file(config_path: str) -> DataLocalitySystemConfig:
    """Load configuration from JSON/YAML file"""
    import json
    from pathlib import Path
    
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, 'r') as f:
        if config_path.endswith('.json'):
            config_dict = json.load(f)
        elif config_path.endswith(('.yaml', '.yml')):
            import yaml
            config_dict = yaml.safe_load(f)
        else:
            raise ValueError("Configuration file must be JSON or YAML")
    
    return DataLocalitySystemConfig.from_dict(config_dict)


def save_config_to_file(config: DataLocalitySystemConfig, config_path: str):
    """Save configuration to JSON/YAML file"""
    import json
    from pathlib import Path
    
    config_file = Path(config_path)
    config_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(config_file, 'w') as f:
        if config_path.endswith('.json'):
            json.dump(config.to_dict(), f, indent=2)
        elif config_path.endswith(('.yaml', '.yml')):
            import yaml
            yaml.dump(config.to_dict(), f, default_flow_style=False)
        else:
            raise ValueError("Configuration file must be JSON or YAML")


# Example configuration files
def create_example_configs(output_dir: str = "configs"):
    """Create example configuration files showing flexible storage strategy"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Base configuration with custom storage strategy
    custom_strategy = StorageStrategy(
        testing="mock",
        development="minio", 
        staging="minio",
        production="minio"
    )
    
    base_config = DataLocalitySystemConfig(
        storage_strategy=custom_strategy,
        current_environment="development"  # Can be changed easily
    )
    
    # Create configs for different environments
    environments = ["testing", "development", "staging", "production"]
    
    for env in environments:
        env_config = base_config.set_environment(env)
        save_config_to_file(env_config, str(output_path / f"{env}_config.json"))
    
    # Multi-node cluster config
    cluster_configs = DataLocalityPresets.for_environment("development", ["node1", "node2", "node3"])
    for node_id, config in cluster_configs.items():
        save_config_to_file(config, str(output_path / f"cluster_{node_id}.json"))
    
    print(f"Flexible configuration files created in {output_dir}/")
    print("To switch environments, just change 'current_environment' in the config!")


if __name__ == "__main__":
    # Create example configurations
    create_example_configs()
