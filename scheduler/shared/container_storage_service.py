#!/usr/bin/env python3
"""
Container-Level Storage Service for All Schedulers

This service provides a unified interface for remote data transfer across all
DataFlower schedulers (ours, ditto, palette, faaSPRS).
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

from .minio_config import get_minio_config, get_minio_storage_config


@dataclass
class StorageContext:
    """Storage context for function containers"""
    src: str  # 'local', 'remote', or 'none'
    input_key: Optional[str] = None
    # Optional relative path under /mnt/node_data for local retrieval
    path: Optional[str] = None
    storage_config: Optional[Dict[str, Any]] = None
    scheduler_type: str = 'ours'


class ContainerStorageService:
    """
    Unified storage service for all schedulers.
    
    Provides:
    - Function output storage
    - Input data retrieval
    - Storage context creation
    - Scheduler-specific optimizations
    """
    
    def __init__(self, scheduler_type: str = 'ours'):
        """
        Initialize storage service for specific scheduler type.
        
        Args:
            scheduler_type: Type of scheduler ('ours', 'ditto', 'palette', 'faaSPRS')
        """
        self.scheduler_type = scheduler_type
        self.minio_config = get_minio_config()
        self.storage_config = get_minio_storage_config()
        self.logger = logging.getLogger(f"{__name__}.{scheduler_type}")
        
        # Scheduler-specific settings
        self._setup_scheduler_specific_config()
    
    def _setup_scheduler_specific_config(self):
        """Setup scheduler-specific storage configurations"""
        if self.scheduler_type == 'palette':
            # Palette uses bucket-aware routing
            self.use_bucket_routing = True
        elif self.scheduler_type == 'ditto':
            # Ditto uses worker-based routing
            self.use_worker_routing = True
        elif self.scheduler_type == 'faaSPRS':
            # FaasPRS uses LP-optimized placement
            self.use_lp_placement = True
        else:
            # Ours uses direct routing
            self.use_direct_routing = True
    
    def store_function_output(self, function_name: str, request_id: str, 
                            output_data: Any, node_id: Optional[str] = None) -> str:
        """
        Store function output and return storage key.
        
        Args:
            function_name: Name of the function
            request_id: Unique request identifier
            output_data: Function output data
            node_id: Node where function executed (for locality)
            
        Returns:
            str: Storage key for retrieving the data later
        """
        try:
            # Generate storage key
            storage_key = f"{request_id}_{function_name}"
            
            # Store to MinIO
            self._store_to_minio(storage_key, output_data, node_id)
            
            self.logger.info(f"Stored function output: {storage_key}")
            return storage_key
            
        except Exception as e:
            self.logger.error(f"Failed to store function output: {e}")
            raise
    
    def retrieve_function_input(self, storage_key: str) -> Any:
        """
        Retrieve input data for function.
        
        Args:
            storage_key: Storage key returned by store_function_output
            
        Returns:
            Any: Retrieved input data
        """
        try:
            # Retrieve from MinIO
            data = self._retrieve_from_minio(storage_key)
            
            self.logger.info(f"Retrieved function input: {storage_key}")
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve function input: {e}")
            raise
    
    def create_storage_context(self, input_key: Optional[str] = None, 
                             scheduler_specific: Optional[Dict] = None) -> StorageContext:
        """
        Create storage context for function containers.
        
        Args:
            input_key: Key for input data (if function has dependencies)
            scheduler_specific: Scheduler-specific parameters
            
        Returns:
            StorageContext: Context for container to use
        """
        if input_key:
            # Function has dependencies - create remote storage context
            storage_config = self.storage_config.copy()
            
            # Add scheduler-specific optimizations
            if scheduler_specific:
                storage_config.update(scheduler_specific)
            
            return StorageContext(
                src='remote',
                input_key=input_key,
                storage_config=storage_config,
                scheduler_type=self.scheduler_type
            )
        else:
            # Function has no dependencies
            return StorageContext(
                src='none',
                scheduler_type=self.scheduler_type
            )
    
    def _store_to_minio(self, storage_key: str, data: Any, node_id: Optional[str] = None):
        """Store data to MinIO"""
        try:
            import boto3
            from botocore.config import Config
            
            # Create S3 client for MinIO
            s3_client = boto3.client(
                's3',
                endpoint_url=f"http://{self.minio_config['host']}:{self.minio_config['port']}",
                aws_access_key_id=self.minio_config['access_key'],
                aws_secret_access_key=self.minio_config['secret_key'],
                region_name='us-east-1',
                config=Config(signature_version='s3v4')
            )
            
            # Serialize data
            if isinstance(data, (dict, list)):
                data_bytes = json.dumps(data).encode('utf-8')
            elif isinstance(data, str):
                data_bytes = data.encode('utf-8')
            elif isinstance(data, bytes):
                data_bytes = data
            else:
                data_bytes = str(data).encode('utf-8')
            
            # Store in MinIO
            bucket_name = self.minio_config['bucket_name']
            object_key = f"dataflower/data/{storage_key}.pkl"
            
            s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=data_bytes
            )
            
        except Exception as e:
            self.logger.error(f"Failed to store to MinIO: {e}")
            raise
    
    def _retrieve_from_minio(self, storage_key: str) -> Any:
        """Retrieve data from MinIO"""
        try:
            import boto3
            from botocore.config import Config
            
            # Create S3 client for MinIO
            s3_client = boto3.client(
                's3',
                endpoint_url=f"http://{self.minio_config['host']}:{self.minio_config['port']}",
                aws_access_key_id=self.minio_config['access_key'],
                aws_secret_access_key=self.minio_config['secret_key'],
                region_name='us-east-1',
                config=Config(signature_version='s3v4')
            )
            
            # Retrieve from MinIO
            bucket_name = self.minio_config['bucket_name']
            object_key = f"dataflower/data/{storage_key}.pkl"
            
            self.logger.info(f"Retrieving from MinIO: Bucket='{bucket_name}', Key='{object_key}'")
            
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            data_bytes = response['Body'].read()
            
            self.logger.info(f"Successfully retrieved {len(data_bytes)} bytes from MinIO key: {object_key}")
                
            # Deserialize data
            try:
                return json.loads(data_bytes.decode('utf-8'))
            except json.JSONDecodeError:
                return data_bytes.decode('utf-8')
                
        except Exception as e:
            error_msg = f"Failed to retrieve from MinIO: {e}\n"
            error_msg += f"  Bucket: {bucket_name}\n"
            error_msg += f"  Key: {object_key}\n"
            error_msg += f"  Storage key: {storage_key}\n"
            error_msg += f"  MinIO endpoint: http://{self.minio_config['host']}:{self.minio_config['port']}"
            self.logger.error(error_msg)
            raise


class UnifiedInputHandler:
    """
    Handle input data preparation for any scheduler type.
    """
    
    def __init__(self, scheduler_type: str = 'ours'):
        self.scheduler_type = scheduler_type
        self.logger = logging.getLogger(f"{__name__}.{scheduler_type}")
    
    def prepare_function_input(self, function_name: str, dependencies: List[str], 
                             workflow_results: Dict, request_id: str) -> tuple[Optional[str], Optional[StorageContext]]:
        """
        Prepare input for function based on scheduler type and dependencies.
        
        Args:
            function_name: Name of the function
            dependencies: List of function dependencies
            workflow_results: Results from previous functions
            request_id: Request identifier
            
        Returns:
            tuple: (input_data, storage_context) - one will be None
        """
        if not dependencies:
            # First function - return direct input
            return None, None
        
        # Function has dependencies - determine input source
        input_key = self._determine_input_key(function_name, dependencies, workflow_results, request_id)
        
        if input_key:
            # Create storage context for remote retrieval
            storage_service = ContainerStorageService(self.scheduler_type)
            storage_context = storage_service.create_storage_context(input_key)
            return None, storage_context
        else:
            # Fallback to direct input
            return None, None
    
    def _determine_input_key(self, function_name: str, dependencies: List[str], 
                           workflow_results: Dict, request_id: str) -> Optional[str]:
        """Determine the storage key for function input"""
        # For now, use simple dependency resolution
        # This can be enhanced with scheduler-specific logic
        
        if dependencies:
            # Use the first dependency's output
            dependency = dependencies[0]
            if dependency in workflow_results:
                return f"{request_id}_{dependency}"
        
        return None


# Factory function for creating storage services
def create_storage_service(scheduler_type: str) -> ContainerStorageService:
    """Factory function to create storage service for specific scheduler"""
    return ContainerStorageService(scheduler_type)


def create_input_handler(scheduler_type: str) -> UnifiedInputHandler:
    """Factory function to create input handler for specific scheduler"""
    return UnifiedInputHandler(scheduler_type)
