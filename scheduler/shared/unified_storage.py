"""
Unified Storage Service

This module provides a unified interface for managing both local and remote storage.
It coordinates between local disk storage and MinIO for cross-node data transfer.
"""

import logging
import os
import time
from typing import Any, Dict, Optional, Union
from dataclasses import dataclass


@dataclass
class StorageResult:
    """Result of a storage operation"""
    success: bool
    key: Optional[str] = None
    size_bytes: Optional[int] = None
    error_message: Optional[str] = None


class UnifiedStorageService:
    """
    Unified storage service that coordinates between local and remote storage.
    
    This service provides a single interface for:
    - Local disk storage for fast access
    - MinIO storage for cross-node data transfer
    - Automatic fallback and coordination between storage types
    """
    
    def __init__(self, minio_service=None, local_storage=None):
        """
        Initialize unified storage service
        
        Args:
            minio_service: MinIO service instance for remote storage
            local_storage: Local storage service instance
        """
        self.minio_service = minio_service
        self.local_storage = local_storage
        self.logger = logging.getLogger("unified_storage")
        
        # Determine available storage backends
        self.use_minio = minio_service is not None and minio_service.is_available()
        self.use_local = local_storage is not None and local_storage.is_available()
        
        if self.use_minio:
            self.logger.info("✅ MinIO storage available for cross-node data transfer")
        if self.use_local:
            self.logger.info("✅ Local storage available for fast access")
        
        if not self.use_minio and not self.use_local:
            self.logger.warning("⚠️  No storage backends available - data will pass through memory only")
    
    def is_available(self) -> bool:
        """Check if any storage backend is available"""
        return self.use_minio or self.use_local
    
    def store_data(self, key: str, data: Any, metadata: Optional[Dict] = None) -> StorageResult:
        """
        Store data using available storage backends
        
        Args:
            key: Unique identifier for the data
            data: Data to store (can be dict, str, bytes, etc.)
            metadata: Optional metadata about the data
            
        Returns:
            StorageResult with operation status
        """
        try:
            size_bytes = 0
            stored_successfully = False
            
            # Calculate size if possible
            try:
                if isinstance(data, (str, bytes)):
                    size_bytes = len(data)
                elif isinstance(data, dict):
                    import json
                    size_bytes = len(json.dumps(data).encode('utf-8'))
                else:
                    size_bytes = len(str(data).encode('utf-8'))
            except Exception:
                pass
            
            # Try local storage first (fastest)
            if self.use_local:
                try:
                    self.local_storage.store_data(key, data, metadata)
                    stored_successfully = True
                    self.logger.debug(f"Stored data locally: {key}")
                except Exception as e:
                    self.logger.warning(f"Local storage failed for {key}: {e}")
            
            # Try MinIO storage (for cross-node access)
            if self.use_minio:
                try:
                    self.minio_service.store_data(key, data, metadata)
                    stored_successfully = True
                    self.logger.debug(f"Stored data in MinIO: {key}")
                except Exception as e:
                    self.logger.warning(f"MinIO storage failed for {key}: {e}")
            
            if stored_successfully:
                return StorageResult(
                    success=True,
                    key=key,
                    size_bytes=size_bytes
                )
            else:
                return StorageResult(
                    success=False,
                    error_message="No storage backend succeeded"
                )
                
        except Exception as e:
            self.logger.error(f"Storage operation failed for {key}: {e}")
            return StorageResult(
                success=False,
                error_message=str(e)
            )
    
    def retrieve_data(self, key: str, preferred_source: str = "local") -> Optional[Any]:
        """
        Retrieve data from storage
        
        Args:
            key: Unique identifier for the data
            preferred_source: "local" or "minio" - which source to try first
            
        Returns:
            Retrieved data or None if not found
        """
        try:
            # Try preferred source first
            if preferred_source == "local" and self.use_local:
                try:
                    data = self.local_storage.retrieve_data(key)
                    if data is not None:
                        self.logger.debug(f"Retrieved data from local storage: {key}")
                        return data
                except Exception as e:
                    self.logger.debug(f"Local retrieval failed for {key}: {e}")
            
            elif preferred_source == "minio" and self.use_minio:
                try:
                    data = self.minio_service.retrieve_data(key)
                    if data is not None:
                        self.logger.debug(f"Retrieved data from MinIO: {key}")
                        return data
                except Exception as e:
                    self.logger.debug(f"MinIO retrieval failed for {key}: {e}")
            
            # Fallback to other source
            if preferred_source == "local" and self.use_minio:
                try:
                    data = self.minio_service.retrieve_data(key)
                    if data is not None:
                        self.logger.debug(f"Retrieved data from MinIO (fallback): {key}")
                        return data
                except Exception as e:
                    self.logger.debug(f"MinIO fallback failed for {key}: {e}")
            
            elif preferred_source == "minio" and self.use_local:
                try:
                    data = self.local_storage.retrieve_data(key)
                    if data is not None:
                        self.logger.debug(f"Retrieved data from local storage (fallback): {key}")
                        return data
                except Exception as e:
                    self.logger.debug(f"Local fallback failed for {key}: {e}")
            
            self.logger.warning(f"Data not found in any storage backend: {key}")
            return None
            
        except Exception as e:
            self.logger.error(f"Retrieval operation failed for {key}: {e}")
            return None
    
    def delete_data(self, key: str) -> StorageResult:
        """
        Delete data from all storage backends
        
        Args:
            key: Unique identifier for the data
            
        Returns:
            StorageResult with operation status
        """
        try:
            deleted_from_local = False
            deleted_from_minio = False
            
            # Delete from local storage
            if self.use_local:
                try:
                    self.local_storage.delete_data(key)
                    deleted_from_local = True
                    self.logger.debug(f"Deleted data from local storage: {key}")
                except Exception as e:
                    self.logger.warning(f"Local deletion failed for {key}: {e}")
            
            # Delete from MinIO
            if self.use_minio:
                try:
                    self.minio_service.delete_data(key)
                    deleted_from_minio = True
                    self.logger.debug(f"Deleted data from MinIO: {key}")
                except Exception as e:
                    self.logger.warning(f"MinIO deletion failed for {key}: {e}")
            
            success = deleted_from_local or deleted_from_minio
            
            return StorageResult(
                success=success,
                key=key,
                error_message=None if success else "Failed to delete from any storage backend"
            )
            
        except Exception as e:
            self.logger.error(f"Delete operation failed for {key}: {e}")
            return StorageResult(
                success=False,
                error_message=str(e)
            )
    
    def list_data(self, prefix: str = "") -> Dict[str, list]:
        """
        List available data keys from all storage backends
        
        Args:
            prefix: Optional prefix to filter keys
            
        Returns:
            Dictionary with 'local' and 'minio' keys containing lists of available keys
        """
        result = {
            'local': [],
            'minio': []
        }
        
        try:
            # List from local storage
            if self.use_local:
                try:
                    local_keys = self.local_storage.list_data(prefix)
                    result['local'] = local_keys
                except Exception as e:
                    self.logger.warning(f"Local listing failed: {e}")
            
            # List from MinIO
            if self.use_minio:
                try:
                    minio_keys = self.minio_service.list_data(prefix)
                    result['minio'] = minio_keys
                except Exception as e:
                    self.logger.warning(f"MinIO listing failed: {e}")
            
        except Exception as e:
            self.logger.error(f"Listing operation failed: {e}")
        
        return result
    
    def get_storage_status(self) -> Dict[str, Any]:
        """
        Get status of all storage backends
        
        Returns:
            Dictionary with status information for each backend
        """
        status = {
            'local': {
                'available': self.use_local,
                'service': type(self.local_storage).__name__ if self.local_storage else None
            },
            'minio': {
                'available': self.use_minio,
                'service': type(self.minio_service).__name__ if self.minio_service else None
            },
            'overall_available': self.is_available()
        }
        
        # Add specific status from services if available
        if self.use_local and hasattr(self.local_storage, 'get_status'):
            try:
                status['local']['details'] = self.local_storage.get_status()
            except Exception:
                pass
        
        if self.use_minio and hasattr(self.minio_service, 'get_status'):
            try:
                status['minio']['details'] = self.minio_service.get_status()
            except Exception:
                pass
        
        return status
    
    def clear_all_data(self) -> StorageResult:
        """
        Clear all data from all storage backends
        
        Returns:
            StorageResult with operation status
        """
        try:
            local_cleared = False
            minio_cleared = False
            
            # Clear local storage
            if self.use_local:
                try:
                    self.local_storage.clear_all_data()
                    local_cleared = True
                    self.logger.info("Cleared all data from local storage")
                except Exception as e:
                    self.logger.warning(f"Local clear failed: {e}")
            
            # Clear MinIO
            if self.use_minio:
                try:
                    self.minio_service.clear_all_data()
                    minio_cleared = True
                    self.logger.info("Cleared all data from MinIO")
                except Exception as e:
                    self.logger.warning(f"MinIO clear failed: {e}")
            
            success = local_cleared or minio_cleared
            
            return StorageResult(
                success=success,
                error_message=None if success else "Failed to clear any storage backend"
            )
            
        except Exception as e:
            self.logger.error(f"Clear all operation failed: {e}")
            return StorageResult(
                success=False,
                error_message=str(e)
            )
