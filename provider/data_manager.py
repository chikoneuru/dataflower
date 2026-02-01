"""
Data Manager for coordinating local caching and remote storage
"""

import hashlib
import json
import logging
import os
import pickle
import threading
import time
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


class DataLocation(Enum):
    """Possible data locations"""
    LOCAL_ONLY = "local_only"
    REMOTE_ONLY = "remote_only"  
    BOTH = "both"
    NONE = "none"

@dataclass
class DataMetadata:
    """Metadata for cached data"""
    data_id: str
    node_id: str
    function_name: str
    step_name: str
    size_bytes: int
    created_time: float
    last_accessed: float
    access_count: int
    data_type: str  # 'input', 'output', 'intermediate'
    dependencies: List[str]  # List of dependent data IDs
    cache_priority: int = 1  # 1=low, 5=high priority

@dataclass
class DataAccessRequest:
    """Request to access data"""
    data_id: str
    requesting_function: str
    requesting_node: str
    access_type: str  # 'read', 'write'
    preferred_location: DataLocation = DataLocation.BOTH

class DataManager:
    """
    Coordinates data caching and remote storage access
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.node_id = self.config.get('node_id', 'unknown')
        self.cache_dir = Path(self.config.get('cache_dir', '/mnt/node_data/dataflower_cache'))
        self.remote_storage = None  # Will be initialized with storage adapter

        # Local cache management
        self.local_cache: Dict[str, DataMetadata] = {}
        self.cache_lock = threading.RLock()

        # Remote storage tracking
        self.remote_data_registry: Dict[str, DataMetadata] = {}
        self.registry_lock = threading.RLock()

        # Performance metrics
        self.access_stats = {
            'local_hits': 0,
            'local_misses': 0,
            'remote_fetches': 0,
            'cache_evictions': 0,
        }

        self._setup_cache_directory()
        self._setup_logging()
        
    def _setup_cache_directory(self):
        """Create local cache directory structure"""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        (self.cache_dir / 'data').mkdir(exist_ok=True)
        (self.cache_dir / 'metadata').mkdir(exist_ok=True)

    def _setup_logging(self):
        """Setup logging for data manager"""
        self.logger = logging.getLogger(f"data_manager.{self.node_id}")
        
    def set_remote_storage_adapter(self, storage_adapter):
        """Set the remote storage adapter"""
        self.remote_storage = storage_adapter
        
    def generate_data_id(self, function_name: str, step_name: str, 
                        input_hash: str = None) -> str:
        """Generate unique data ID"""
        if input_hash:
            raw_id = f"{function_name}:{step_name}:{input_hash}"
        else:
            raw_id = f"{function_name}:{step_name}:{time.time()}"
        
        return hashlib.sha256(raw_id.encode()).hexdigest()[:16]
    
    def store_data(self, data_id: str, data: Any, metadata: DataMetadata) -> bool:
        """
        Store data both locally and remotely

        Args:
            data_id: Unique identifier for the data
            data: The actual data to store
            metadata: Metadata about the data

        Returns:
            bool: Success status
        """
        try:
            # 1. Store locally first
            local_success = self._store_local(data_id, data, metadata)

            # 2. Store remotely (async to avoid blocking)
            remote_success = True
            if self.remote_storage:
                try:
                    remote_success = self.remote_storage.store_data(data_id, data, metadata)
                except Exception as e:
                    self.logger.warning(f"Remote storage failed for {data_id}: {e}")
                    remote_success = False

            # 3. Update registries
            with self.cache_lock:
                self.local_cache[data_id] = metadata

            if remote_success:
                with self.registry_lock:
                    self.remote_data_registry[data_id] = metadata

            self.logger.info(f"Stored data {data_id} (local: {local_success}, remote: {remote_success})")
            return local_success

        except Exception as e:
            self.logger.error(f"Failed to store data {data_id}: {e}")
            return False
    
    def get_data(self, request: DataAccessRequest) -> Tuple[Optional[Any], DataLocation]:
        """
        Retrieve data based on rules

        Returns:
            Tuple of (data, actual_location)
        """
        data_id = request.data_id

        # 1. Check if data exists locally (same node)
        if request.requesting_node == self.node_id:
            local_data = self._get_local(data_id)
            if local_data is not None:
                self.access_stats['local_hits'] += 1
                self._update_access_time(data_id)
                return local_data, DataLocation.LOCAL_ONLY

        # 2. For cross-node access or when data not found locally, check remote storage
        if request.requesting_node != self.node_id or request.preferred_location == DataLocation.REMOTE_ONLY:
            self.access_stats['local_misses'] += 1

            if self.remote_storage:
                remote_data = self.remote_storage.get_data(data_id)
                if remote_data is not None:
                    self.access_stats['remote_fetches'] += 1
                    return remote_data, DataLocation.REMOTE_ONLY

        self.logger.warning(f"Data {data_id} not found in any location")
        return None, DataLocation.NONE
    
    def check_data_availability(self, data_id: str, node_id: str) -> DataLocation:
        """
        Check where data is available without fetching it

        Logic:
        - If requesting from same node: LOCAL_ONLY (prioritize local access)
        - If requesting from different node: REMOTE_ONLY (use remote storage)
        - If data not found: NONE
        """
        # Check if data is available locally on this node
        local_available = data_id in self.local_cache and node_id == self.node_id

        # Check if data is available in remote storage
        remote_available = data_id in self.remote_data_registry

        # For same-node requests, prioritize local access
        if node_id == self.node_id:
            if local_available:
                return DataLocation.LOCAL_ONLY
            elif remote_available:
                return DataLocation.REMOTE_ONLY
            else:
                return DataLocation.NONE
        else:
            # For cross-node requests, use remote storage
            if remote_available:
                return DataLocation.REMOTE_ONLY
            else:
                return DataLocation.NONE
    
    def get_local_data_ids(self) -> Set[str]:
        """Get all data IDs available locally on this node"""
        with self.cache_lock:
            return set(self.local_cache.keys())
    
    def get_function_data_locality(self, function_name: str, node_id: str) -> Dict[str, DataLocation]:
        """Get data locality information for a specific function"""
        locality_map = {}
        
        # Check local cache
        with self.cache_lock:
            for data_id, metadata in self.local_cache.items():
                if metadata.function_name == function_name:
                    locality_map[data_id] = self.check_data_availability(data_id, node_id)
        
        # Check remote registry for missing data
        with self.registry_lock:
            for data_id, metadata in self.remote_data_registry.items():
                if metadata.function_name == function_name and data_id not in locality_map:
                    locality_map[data_id] = DataLocation.REMOTE_ONLY
        
        return locality_map
    
    def evict_cache(self, eviction_strategy: str = "lru") -> int:
        """
        Evict cached data based on strategy
        
        Returns:
            Number of items evicted
        """
        max_cache_size = self.config.get('max_cache_items', 1000)
        
        with self.cache_lock:
            if len(self.local_cache) <= max_cache_size:
                return 0
            
            items_to_evict = len(self.local_cache) - max_cache_size
            
            if eviction_strategy == "lru":
                # Sort by last accessed time
                sorted_items = sorted(
                    self.local_cache.items(),
                    key=lambda x: x[1].last_accessed
                )
            elif eviction_strategy == "lfu":
                # Sort by access count
                sorted_items = sorted(
                    self.local_cache.items(),
                    key=lambda x: x[1].access_count
                )
            else:
                # Default to FIFO
                sorted_items = sorted(
                    self.local_cache.items(),
                    key=lambda x: x[1].created_time
                )
            
            evicted_count = 0
            for data_id, metadata in sorted_items[:items_to_evict]:
                if self._evict_local(data_id):
                    evicted_count += 1
                    
            self.access_stats['cache_evictions'] += evicted_count
            return evicted_count
    
    def _store_local(self, data_id: str, data: Any, metadata: DataMetadata) -> bool:
        """Store data in local cache"""
        try:
            data_path = self.cache_dir / 'data' / f"{data_id}.pkl"
            metadata_path = self.cache_dir / 'metadata' / f"{data_id}.json"
            
            # Serialize and store data
            with open(data_path, 'wb') as f:
                pickle.dump(data, f)
            
            # Store metadata
            with open(metadata_path, 'w') as f:
                json.dump(asdict(metadata), f, indent=2)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store local data {data_id}: {e}")
            return False
    
    def _get_local(self, data_id: str) -> Optional[Any]:
        """Get data from local cache"""
        try:
            data_path = self.cache_dir / 'data' / f"{data_id}.pkl"
            
            if data_path.exists():
                with open(data_path, 'rb') as f:
                    return pickle.load(f)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to get local data {data_id}: {e}")
            return None
    
    def _update_access_time(self, data_id: str):
        """Update last access time and count"""
        with self.cache_lock:
            if data_id in self.local_cache:
                self.local_cache[data_id].last_accessed = time.time()
                self.local_cache[data_id].access_count += 1
    
    def _evict_local(self, data_id: str) -> bool:
        """Remove data from local cache"""
        try:
            data_path = self.cache_dir / 'data' / f"{data_id}.pkl"
            metadata_path = self.cache_dir / 'metadata' / f"{data_id}.json"
            
            if data_path.exists():
                data_path.unlink()
            if metadata_path.exists():
                metadata_path.unlink()
                
            with self.cache_lock:
                if data_id in self.local_cache:
                    del self.local_cache[data_id]
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to evict local data {data_id}: {e}")
            return False
    
    def get_cache_stats(self) -> Dict:
        """Get cache performance statistics"""
        total_accesses = self.access_stats['local_hits'] + self.access_stats['local_misses']
        hit_rate = self.access_stats['local_hits'] / total_accesses if total_accesses > 0 else 0
        
        return {
            'node_id': self.node_id,
            'cache_size': len(self.local_cache),
            'remote_registry_size': len(self.remote_data_registry),
            'hit_rate': hit_rate,
            'local_hits': self.access_stats['local_hits'], # Explicitly add for clarity
            'local_misses': self.access_stats['local_misses'], # Explicitly add for clarity
            'remote_fetches': self.access_stats['remote_fetches'], # Explicitly add for clarity
            'cache_evictions': self.access_stats['cache_evictions'] # Explicitly add for clarity
        }
    
    def cleanup_expired_cache(self, max_age_hours: int = 24) -> int:
        """Remove expired cache entries"""
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600

        expired_ids = []
        with self.cache_lock:
            for data_id, metadata in self.local_cache.items():
                if current_time - metadata.created_time > max_age_seconds:
                    expired_ids.append(data_id)

        cleaned_count = 0
        for data_id in expired_ids:
            if self._evict_local(data_id):
                cleaned_count += 1

        return cleaned_count
