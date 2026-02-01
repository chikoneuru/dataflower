"""
Input Retrieval Service for Functions

This service allows functions to retrieve their inputs from either local disk or MinIO storage
without modifying the existing DAG execution logic. Functions can use this service to get
their input data independently.
"""

import logging
import time
import os
import json
import base64
from typing import Any, Dict, Optional, Union

from .local_storage import LocalStorageService
from .minio_service import MinIOService


class InputRetrievalService:
    """
    Service that allows functions to retrieve their inputs from storage.
    
    This service provides a unified interface for functions to get their input data
    from either local disk or MinIO storage, without the orchestrator needing to
    handle data passing through memory.
    """
    
    def __init__(self, minio_service: Optional[MinIOService] = None, 
                 local_storage: Optional[LocalStorageService] = None):
        """
        Initialize input retrieval service
        
        Args:
            minio_service: MinIO service instance
            local_storage: Local storage service instance
        """
        self.minio_service = minio_service
        self.local_storage = local_storage
        self.logger = logging.getLogger("input_retrieval")
        
        # Determine which storage to use
        self.use_minio = (self.minio_service is not None and 
                         self.minio_service.is_available())
        self.use_local = (self.local_storage is not None and 
                         self.local_storage.is_available())
        
        if self.use_minio:
            self.logger.info("Input retrieval service using MinIO")
        elif self.use_local:
            self.logger.info("Input retrieval service using local disk")
        else:
            self.logger.warning("No storage backend available for input retrieval")
    
    def is_available(self) -> bool:
        """Check if any storage backend is available"""
        return self.use_minio or self.use_local
    
    def get_input_data(self, input_key: str, expected_format: str = 'auto') -> Optional[Any]:
        """
        Retrieve input data from storage
        
        Args:
            input_key: Storage key for the input data
            expected_format: Expected format ('image', 'text', 'json', 'auto')
            
        Returns:
            Input data in the appropriate format, or None if not found
        """
        if not self.is_available():
            self.logger.error("No storage backend available for input retrieval")
            return None
        
        try:
            # Colored debug for chosen backend
            if self.use_minio:
                self.logger.info(f"\033[96m[InputRetrieval] MinIO fetch: key={input_key}\033[0m")
            elif self.use_local:
                # We may not know the exact path yet; log key, path resolved in _get_raw_data
                self.logger.info(f"\033[92m[InputRetrieval] Local fetch: key={input_key}\033[0m")
            else:
                self.logger.info(f"\033[93m[InputRetrieval] No storage backend available; cannot fetch key={input_key}\033[0m")
            # Get raw data from storage
            raw_data = self._get_raw_data(input_key)
            if raw_data is None:
                self.logger.error(f"Failed to retrieve data for key: {input_key}")
                return None
            
            # Convert to expected format
            return self._convert_to_format(raw_data, expected_format)
            
        except Exception as e:
            self.logger.error(f"Error retrieving input data for key {input_key}: {e}")
            return None
    
    def get_input_file_path(self, input_key: str) -> Optional[str]:
        """
        Get file path for input data (for functions that work with files)
        
        Args:
            input_key: Storage key for the input data
            
        Returns:
            str: File path to the data, or None if not found
        """
        if self.use_local:
            return self.local_storage.get_data_path(input_key)
        elif self.use_minio:
            return self._download_minio_to_temp_file(input_key)
        else:
            self.logger.error("No storage backend available")
            return None
    
    def get_input_metadata(self, input_key: str) -> Optional[Dict]:
        """
        Get metadata for input data
        
        Args:
            input_key: Storage key for the input data
            
        Returns:
            Dict: Metadata dictionary, or None if not found
        """
        if self.use_minio:
            return {
                'input_key': input_key,
                'storage_type': 'minio',
                'retrieved_time': time.time()
            }
        elif self.use_local:
            return self.local_storage.get_metadata(input_key)
        else:
            return None
    
    def _get_raw_data(self, input_key: str) -> Optional[bytes]:
        """Get raw data from storage"""
        if self.use_minio:
            # Detailed debug at the moment of MinIO access
            self.logger.debug(f"\033[96m[InputRetrieval] Downloading from MinIO: {input_key}\033[0m")
            data, success = self.minio_service.download_data(input_key)
            return data if success else None
        elif self.use_local:
            # Resolve path for visibility
            try:
                path = self.local_storage.get_data_path(input_key)
                if path:
                    self.logger.debug(f"\033[92m[InputRetrieval] Reading from local path: {path}\033[0m")
            except Exception:
                pass
            return self.local_storage.get_data(input_key)
        else:
            return None
    
    def _convert_to_format(self, raw_data: bytes, expected_format: str) -> Any:
        """Convert raw data to expected format"""
        if expected_format == 'auto':
            expected_format = self._detect_format(raw_data)
        
        if expected_format == 'image':
            # Return as base64 encoded string for image functions
            return base64.b64encode(raw_data).decode('utf-8')
        
        elif expected_format == 'text':
            try:
                return raw_data.decode('utf-8')
            except UnicodeDecodeError:
                # Try to parse as JSON
                try:
                    return json.loads(raw_data.decode('utf-8'))
                except json.JSONDecodeError:
                    return raw_data
        
        elif expected_format == 'json':
            try:
                return json.loads(raw_data.decode('utf-8'))
            except json.JSONDecodeError:
                return raw_data
        
        else:
            # Return raw bytes
            return raw_data
    
    def _detect_format(self, data: bytes) -> str:
        """Detect the format of the data"""
        # Check for image signatures
        if data[:4] in [b'\x89PNG', b'\xff\xd8\xff', b'GIF8']:
            return 'image'
        
        # Check for JSON
        try:
            json.loads(data.decode('utf-8'))
            return 'json'
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
        
        # Check for text
        try:
            data.decode('utf-8')
            return 'text'
        except UnicodeDecodeError:
            pass
        
        # Default to raw bytes
        return 'raw'
    
    def _download_minio_to_temp_file(self, input_key: str) -> Optional[str]:
        """Download data from MinIO to a temporary file"""
        try:
            # Download data from MinIO
            data, success = self.minio_service.download_data(input_key)
            if not success or data is None:
                return None
            
            # Create temporary file
            import tempfile
            
            # Create temp file with appropriate extension
            if data[:4] in [b'\x89PNG', b'\xff\xd8\xff', b'GIF8']:
                suffix = '.png' if data[:4] == b'\x89PNG' else '.jpg'
            else:
                suffix = '.bin'
            
            temp_fd, temp_path = tempfile.mkstemp(suffix=suffix)
            
            try:
                os.write(temp_fd, data)
                os.close(temp_fd)
                return temp_path
                
            except Exception as e:
                os.close(temp_fd)
                os.unlink(temp_path)
                raise e
                
        except Exception as e:
            self.logger.error(f"Error downloading MinIO data {input_key} to temp file: {e}")
            return None


class FunctionInputContext:
    """
    Context object that provides input retrieval capabilities to functions.
    
    This object is passed to functions and allows them to retrieve their inputs
    from storage without the orchestrator needing to handle data passing.
    """
    
    def __init__(self, input_retrieval_service: InputRetrievalService, 
                 input_key: Optional[str] = None, 
                 function_name: str = None,
                 request_id: str = None):
        """
        Initialize function input context
        
        Args:
            input_retrieval_service: Service for retrieving inputs
            input_key: Storage key for input data (if any)
            function_name: Name of the function
            request_id: Request identifier
        """
        self.input_retrieval_service = input_retrieval_service
        self.input_key = input_key
        self.function_name = function_name
        self.request_id = request_id
        self.logger = logging.getLogger(f"function_input_context_{function_name}")
        
        # Cache for retrieved data
        self._cached_data = None
        self._cache_key = None
    
    def get_input_data(self, expected_format: str = 'auto') -> Optional[Any]:
        """
        Get input data for this function
        
        Args:
            expected_format: Expected format ('image', 'text', 'json', 'auto')
            
        Returns:
            Input data in the appropriate format, or None if not available
        """
        if not self.input_key:
            self.logger.warning("No input key provided for function")
            return None
        
        # Use cache if available
        if self._cached_data is not None and self._cache_key == self.input_key:
            return self._cached_data
        
        # Retrieve data from storage
        self._cached_data = self.input_retrieval_service.get_input_data(
            self.input_key, expected_format
        )
        self._cache_key = self.input_key
        
        return self._cached_data
    
    def get_input_file_path(self) -> Optional[str]:
        """
        Get file path for input data
        
        Returns:
            str: File path to the data, or None if not available
        """
        if not self.input_key:
            return None
        
        return self.input_retrieval_service.get_input_file_path(self.input_key)
    
    def get_input_metadata(self) -> Optional[Dict]:
        """
        Get metadata for input data
        
        Returns:
            Dict: Metadata dictionary, or None if not available
        """
        if not self.input_key:
            return None
        
        return self.input_retrieval_service.get_input_metadata(self.input_key)
    
    def has_input(self) -> bool:
        """Check if this function has input data available"""
        return self.input_key is not None
    
    def get_input_info(self) -> Dict:
        """Get information about available inputs"""
        return {
            'has_input': self.has_input(),
            'input_key': self.input_key,
            'function_name': self.function_name,
            'request_id': self.request_id,
            'storage_available': self.input_retrieval_service.is_available()
        }
