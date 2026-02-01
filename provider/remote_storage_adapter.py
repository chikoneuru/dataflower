"""
Remote Storage Adapter for external storage systems (local filesystem, etc.)
"""

import json
import logging
import os
import pickle
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional

from .data_manager import DataAccessRequest, DataMetadata


class RemoteStorageAdapter(ABC):
    """Abstract base class for remote storage adapters"""
    
    @abstractmethod
    def store_data(self, data_id: str, data: Any, metadata: DataMetadata) -> bool:
        """Store data in remote storage"""
        pass
    
    @abstractmethod
    def get_data(self, data_id: str) -> Optional[Any]:
        """Retrieve data from remote storage"""
        pass
    
    @abstractmethod
    def check_exists(self, data_id: str) -> bool:
        """Check if data exists in remote storage"""
        pass
    
    @abstractmethod
    def delete_data(self, data_id: str) -> bool:
        """Delete data from remote storage"""
        pass
    
    @abstractmethod
    def list_data_ids(self, prefix: str = "") -> List[str]:
        """List all data IDs with optional prefix"""
        pass


class MinIOStorageAdapter(RemoteStorageAdapter):
    """
    MinIO storage adapter (S3-compatible, self-hosted)
    Perfect for large files: images, videos, text, npy arrays
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        
        # Initialize logging first
        self.logger = logging.getLogger("remote_storage.minio")
        self.prefix = self.config.get('prefix', 'dataflower/')
        
        # Use centralized MinIO configuration
        try:
            from scheduler.shared.minio_config import get_minio_config
            minio_config = get_minio_config()
            minio_host = minio_config['host']
            minio_port = minio_config['port']
            access_key = minio_config['access_key']
            secret_key = minio_config['secret_key']
            bucket_name = minio_config['bucket_name']
        except ImportError:
            # Fallback to config-based approach
            minio_host = self.config.get('minio_host', 'minio')
            minio_port = self.config.get('minio_port', 9000)
            access_key = self.config.get('access_key', 'dataflower')
            secret_key = self.config.get('secret_key', 'dataflower123')
            bucket_name = self.config.get('bucket_name', 'dataflower-storage')
        
        # Store config for lazy initialization
        self.minio_config = {
            **self.config,
            'aws_access_key_id': access_key,
            'aws_secret_access_key': secret_key,
            'region': 'us-east-1',  # MinIO doesn't care about region
            'endpoint_url': f'http://{minio_host}:{minio_port}',
            'use_ssl': False
        }
        
        # Get bucket name
        self.bucket_name = bucket_name
        
        # Initialize S3 client as None - will be created on first use
        self.s3_client = None
        self._connected = False
    
    def _setup_logging(self):
        """Setup logging"""
        self.logger = logging.getLogger("remote_storage.minio")
    
    def _connect(self):
        """Lazy initialization of MinIO connection"""
        if self._connected:
            return
            
        try:
            # Import boto3 only when MinIO is actually used
            import boto3
            from botocore.config import Config
            
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.minio_config['endpoint_url'],
                aws_access_key_id=self.minio_config['aws_access_key_id'],
                aws_secret_access_key=self.minio_config['aws_secret_access_key'],
                region_name=self.minio_config['region'],
                config=Config(signature_version='s3v4'),
                use_ssl=self.minio_config['use_ssl']
            )
            
            # Test connection and create bucket if needed
            self._ensure_bucket_exists()
            self._connected = True
            
        except ImportError:
            raise ImportError("boto3 package not installed. Install with: pip install boto3")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MinIO at {self.minio_config['endpoint_url']}: {e}")
    
    def _get_data_key(self, data_id: str) -> str:
        """Generate MinIO key for data"""
        return f"{self.prefix}data/{data_id}.pkl"
    
    def _get_metadata_key(self, data_id: str) -> str:
        """Generate MinIO key for metadata"""
        return f"{self.prefix}metadata/{data_id}.json"
        
    def _ensure_bucket_exists(self):
        """Ensure the bucket exists, create if it doesn't"""
        try:
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            self.logger.info(f"MinIO bucket {self.bucket_name} exists")
        except Exception as e:
            # Handle any bucket-not-found error (404, NoSuchBucket, etc.)
            error_str = str(e)
            if '404' in error_str or 'NoSuchBucket' in error_str or 'Not Found' in error_str:
                try:
                    # Create the bucket
                    try:
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    except Exception as create_error:
                        # If the bucket already exists or is already owned, treat as success
                        ce_str = str(create_error)
                        if (
                            'BucketAlreadyOwnedByYou' in ce_str
                            or 'BucketAlreadyExists' in ce_str
                        ):
                            self.logger.info(
                                f"MinIO bucket {self.bucket_name} already exists/owned; proceeding"
                            )
                        else:
                            raise create_error
                    self.logger.info(f"Created MinIO bucket: {self.bucket_name}")
                    
                    # Verify bucket was created
                    self.s3_client.head_bucket(Bucket=self.bucket_name)
                    self.logger.info(f"MinIO bucket {self.bucket_name} verified after creation")
                    
                except Exception as create_error:
                    self.logger.error(f"Failed to create MinIO bucket {self.bucket_name}: {create_error}")
                    raise ConnectionError(f"Cannot create MinIO bucket {self.bucket_name}: {create_error}")
            else:
                # Other error (permissions, network, etc.)
                self.logger.warning(f"Could not verify bucket {self.bucket_name}: {e}")
                # Don't raise exception for verification issues, bucket might still work
    
    def store_data(self, data_id: str, data: Any, metadata: DataMetadata) -> bool:
        """Store data in MinIO with optimized handling for different data types"""
        try:
            # Ensure connection is established
            self._connect()
            # Handle different data types optimally
            try:
                import numpy as np
                is_numpy_available = True
            except ImportError:
                is_numpy_available = False
            
            if is_numpy_available and isinstance(data, np.ndarray):
                # Store numpy arrays efficiently
                import io
                buffer = io.BytesIO()
                np.save(buffer, data)
                data_bytes = buffer.getvalue()
                content_type = 'application/octet-stream'
                self.logger.debug(f"Storing numpy array {data_id} ({data.shape})")
                
            elif isinstance(data, (str, dict, list)):
                # Store text/JSON as UTF-8
                if isinstance(data, str):
                    data_bytes = data.encode('utf-8')
                    content_type = 'text/plain'
                else:
                    data_bytes = json.dumps(data).encode('utf-8')
                    content_type = 'application/json'
                self.logger.debug(f"Storing text/JSON data {data_id}")
                
            elif isinstance(data, bytes):
                # Store binary data (images, videos) as-is
                data_bytes = data
                content_type = 'application/octet-stream'
                self.logger.debug(f"Storing binary data {data_id} ({len(data_bytes)} bytes)")
                
            else:
                # Fallback to pickle for other Python objects
                data_bytes = pickle.dumps(data)
                content_type = 'application/octet-stream'
                self.logger.debug(f"Storing pickled data {data_id}")
            
            # Store in MinIO with retry logic for bucket issues
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=self._get_data_key(data_id),
                    Body=data_bytes,
                    ContentType=content_type,
                    Metadata={
                        'data-id': data_id,
                        'function-name': metadata.function_name,
                        'node-id': metadata.node_id,
                        'created-time': str(metadata.created_time),
                        'data-type': metadata.data_type,
                        'size-bytes': str(len(data_bytes))
                    }
                )
            except Exception as e:
                # If bucket doesn't exist, try to create it and retry
                if 'NoSuchBucket' in str(e) or 'bucket does not exist' in str(e).lower():
                    self.logger.info(f"Bucket not found during store, attempting to create: {self.bucket_name}")
                    self._ensure_bucket_exists()
                    # Retry the operation
                    self.s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=self._get_data_key(data_id),
                        Body=data_bytes,
                        ContentType=content_type,
                        Metadata={
                            'data-id': data_id,
                            'function-name': metadata.function_name,
                            'node-id': metadata.node_id,
                            'created-time': str(metadata.created_time),
                            'data-type': metadata.data_type,
                            'size-bytes': str(len(data_bytes))
                        }
                    )
                else:
                    raise e
            
            # Store metadata separately with same retry logic
            metadata_json = json.dumps({
                'data_id': metadata.data_id,
                'node_id': metadata.node_id,
                'function_name': metadata.function_name,
                'step_name': metadata.step_name,
                'size_bytes': len(data_bytes),
                'created_time': metadata.created_time,
                'last_accessed': metadata.last_accessed,
                'access_count': metadata.access_count,
                'data_type': metadata.data_type,
                'dependencies': metadata.dependencies,
                'cache_priority': metadata.cache_priority,
                'content_type': content_type
            })
            
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=self._get_metadata_key(data_id),
                    Body=metadata_json,
                    ContentType='application/json'
                )
            except Exception as e:
                # If bucket doesn't exist, try to create it and retry
                if 'NoSuchBucket' in str(e) or 'bucket does not exist' in str(e).lower():
                    self.logger.info(f"Bucket not found during metadata store, attempting to create: {self.bucket_name}")
                    self._ensure_bucket_exists()
                    # Retry the operation
                    self.s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=self._get_metadata_key(data_id),
                        Body=metadata_json,
                        ContentType='application/json'
                    )
                else:
                    raise e
            
            self.logger.info(f"Stored data {data_id} in MinIO ({len(data_bytes)} bytes)")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store data {data_id} in MinIO: {e}")
            return False
    
    def get_data(self, data_id: str) -> Optional[Any]:
        """Retrieve data from MinIO with type-aware deserialization"""
        try:
            # Ensure connection is established
            self._connect()
            # Get object and metadata
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self._get_data_key(data_id)
            )
            
            data_bytes = response['Body'].read()
            content_type = response.get('ContentType', 'application/octet-stream')
            
            # Deserialize based on content type
            if content_type == 'text/plain':
                data = data_bytes.decode('utf-8')
            elif content_type == 'application/json':
                data = json.loads(data_bytes.decode('utf-8'))
            elif 'numpy' in response.get('Metadata', {}).get('data-type', '') or data_id.endswith('.npy'):
                # Handle numpy arrays
                try:
                    import io

                    import numpy as np
                    buffer = io.BytesIO(data_bytes)
                    data = np.load(buffer)
                except ImportError:
                    self.logger.warning("NumPy not available, storing as raw bytes")
                    data = data_bytes
            else:
                # Try pickle first, fallback to raw bytes
                try:
                    data = pickle.loads(data_bytes)
                except Exception:
                    data = data_bytes
            
            self.logger.debug(f"Retrieved data {data_id} from MinIO")
            return data
            
        except self.s3_client.exceptions.NoSuchKey:
            self.logger.debug(f"Data {data_id} not found in MinIO")
            return None
        except Exception as e:
            self.logger.error(f"Failed to retrieve data {data_id} from MinIO: {e}")
            return None
    
    def check_exists(self, data_id: str) -> bool:
        """Check if data exists in MinIO"""
        try:
            # Ensure connection is established
            self._connect()
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=self._get_data_key(data_id)
            )
            return True
        except Exception:
            return False
    
    def delete_data(self, data_id: str) -> bool:
        """Delete data and metadata from MinIO"""
        try:
            # Ensure connection is established
            self._connect()
            # Delete data object
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=self._get_data_key(data_id)
            )
            
            # Delete metadata object
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=self._get_metadata_key(data_id)
            )
            
            self.logger.info(f"Deleted data {data_id} from MinIO")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete data {data_id} from MinIO: {e}")
            return False
    
    def list_data_ids(self, prefix: str = "") -> List[str]:
        """List all data IDs in MinIO"""
        try:
            # Ensure connection is established
            self._connect()
            paginator = self.s3_client.get_paginator('list_objects_v2')
            search_prefix = f"{self.prefix}data/{prefix}"
            
            data_ids = []
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=search_prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        # Extract data_id from key (remove prefix and .pkl extension)
                        data_id = key.replace(f"{self.prefix}data/", "").replace(".pkl", "")
                        data_ids.append(data_id)
            
            return data_ids
            
        except Exception as e:
            self.logger.error(f"Failed to list data IDs from MinIO: {e}")
            return []


class LocalFileStorageAdapter(RemoteStorageAdapter):
    """
    Local filesystem storage adapter (useful for testing or shared filesystem)
    """
    
    def __init__(self, base_path: str, config: Dict = None):
        self.base_path = Path(base_path)
        self.config = config or {}
        
        # Create directory structure
        self.data_dir = self.base_path / 'data'
        self.metadata_dir = self.base_path / 'metadata'
        
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging"""
        self.logger = logging.getLogger("remote_storage.local_file")
    
    def store_data(self, data_id: str, data: Any, metadata: DataMetadata) -> bool:
        """Store data and metadata in local filesystem"""
        try:
            # Store data
            data_path = self.data_dir / f"{data_id}.pkl"
            with open(data_path, 'wb') as f:
                pickle.dump(data, f)
            
            # Store metadata
            metadata_path = self.metadata_dir / f"{data_id}.json"
            metadata_dict = {
                'data_id': metadata.data_id,
                'node_id': metadata.node_id,
                'function_name': metadata.function_name,
                'step_name': metadata.step_name,
                'size_bytes': metadata.size_bytes,
                'created_time': metadata.created_time,
                'last_accessed': metadata.last_accessed,
                'access_count': metadata.access_count,
                'data_type': metadata.data_type,
                'dependencies': metadata.dependencies,
                'cache_priority': metadata.cache_priority
            }
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata_dict, f, indent=2)
            
            self.logger.debug(f"Stored data {data_id} in local storage")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store data {data_id} in local storage: {e}")
            return False
    
    def get_data(self, data_id: str) -> Optional[Any]:
        """Retrieve data from local filesystem"""
        try:
            data_path = self.data_dir / f"{data_id}.pkl"
            
            if data_path.exists():
                with open(data_path, 'rb') as f:
                    data = pickle.load(f)
                self.logger.debug(f"Retrieved data {data_id} from local storage")
                return data
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve data {data_id} from local storage: {e}")
            return None
    
    def check_exists(self, data_id: str) -> bool:
        """Check if data exists in local filesystem"""
        data_path = self.data_dir / f"{data_id}.pkl"
        return data_path.exists()
    
    def delete_data(self, data_id: str) -> bool:
        """Delete data and metadata from local filesystem"""
        try:
            data_path = self.data_dir / f"{data_id}.pkl"
            metadata_path = self.metadata_dir / f"{data_id}.json"
            
            if data_path.exists():
                data_path.unlink()
            if metadata_path.exists():
                metadata_path.unlink()
            
            self.logger.debug(f"Deleted data {data_id} from local storage")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete data {data_id} from local storage: {e}")
            return False
    
    def list_data_ids(self, prefix: str = "") -> List[str]:
        """List all data IDs in local filesystem"""
        try:
            data_ids = []
            
            for file_path in self.data_dir.glob(f"{prefix}*.pkl"):
                data_id = file_path.stem
                data_ids.append(data_id)
            
            return data_ids
            
        except Exception as e:
            self.logger.error(f"Failed to list data IDs from local storage: {e}")
            return []



class MockRemoteStorageAdapter(RemoteStorageAdapter):
    """
    Mock storage adapter for testing
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.storage: Dict[str, Any] = {}
        self.metadata_store: Dict[str, DataMetadata] = {}
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging"""
        self.logger = logging.getLogger("remote_storage.mock")
    
    def store_data(self, data_id: str, data: Any, metadata: DataMetadata) -> bool:
        """Store data in memory"""
        self.storage[data_id] = data
        self.metadata_store[data_id] = metadata
        self.logger.debug(f"[Mock] Stored data {data_id}")
        return True
    
    def get_data(self, data_id: str) -> Optional[Any]:
        """Retrieve data from memory"""
        data = self.storage.get(data_id)
        if data is not None:
            self.logger.debug(f"[Mock] Retrieved data {data_id}")
        return data
    
    def check_exists(self, data_id: str) -> bool:
        """Check if data exists in memory"""
        return data_id in self.storage
    
    def delete_data(self, data_id: str) -> bool:
        """Delete data from memory"""
        if data_id in self.storage:
            del self.storage[data_id]
        if data_id in self.metadata_store:
            del self.metadata_store[data_id]
        self.logger.debug(f"[Mock] Deleted data {data_id}")
        return True
    
    def list_data_ids(self, prefix: str = "") -> List[str]:
        """List all data IDs in memory"""
        return [data_id for data_id in self.storage.keys() if data_id.startswith(prefix)]


def create_storage_adapter(storage_type: str, config: Dict) -> RemoteStorageAdapter:
    """Factory function to create storage adapters"""
    
    if storage_type == "minio":
        return MinIOStorageAdapter(config)
    elif storage_type == "local_file":
        return LocalFileStorageAdapter(
            base_path=config.get('base_path', '/tmp/dataflower_remote'),
            config=config
        )
    elif storage_type == "mock":
        return MockRemoteStorageAdapter(config)
    else:
        raise ValueError(f"Unknown storage type: {storage_type}. Available types: minio, local_file, mock")
