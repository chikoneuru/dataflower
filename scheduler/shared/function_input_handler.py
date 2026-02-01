"""
Function Input Handler

This module provides utilities for functions to handle input retrieval from storage.
Functions can use these utilities to get their inputs from either local disk or MinIO
without the orchestrator needing to handle data passing through memory.
"""

import logging
import json
import base64
import os
from typing import Any, Dict, Optional


class FunctionInputHandler:
    """
    Handler for functions to retrieve their inputs from storage.
    
    This class provides a simple interface for functions to get their input data
    from storage based on the input context provided by the orchestrator.
    """
    
    def __init__(self, input_context: Dict, storage_config: Dict):
        """
        Initialize function input handler
        
        Args:
            input_context: Input context from orchestrator
            storage_config: Storage configuration from orchestrator
        """
        self.input_context = input_context
        self.storage_config = storage_config
        self.logger = logging.getLogger("function_input_handler")
        
        # Extract configuration
        self.has_input = input_context.get('has_input', False)
        self.input_key = input_context.get('input_key')
        self.function_name = input_context.get('function_name')
        self.request_id = input_context.get('request_id')
        self.storage_available = input_context.get('storage_available', False)
        
        # Storage config (MinIO)
        self.use_minio = bool(storage_config.get('use_minio', False))
        self.use_local = bool(storage_config.get('use_local', True))
        self.minio_host = storage_config.get('minio_host', os.environ.get('MINIO_HOST', 'minio'))
        self.minio_port = int(storage_config.get('minio_port', os.environ.get('MINIO_PORT', 9000)))
        self.minio_access_key = storage_config.get('access_key', os.environ.get('MINIO_ACCESS_KEY', 'dataflower'))
        self.minio_secret_key = storage_config.get('secret_key', os.environ.get('MINIO_SECRET_KEY', 'dataflower123'))
        self.minio_bucket = storage_config.get('bucket_name', os.environ.get('MINIO_BUCKET', 'serverless-data'))
        self.minio_prefix = storage_config.get('prefix', os.environ.get('MINIO_PREFIX', 'dataflower/'))
        self._s3 = None  # lazy
    
    def _ensure_s3(self):
        """Lazily initialize boto3 S3 client for MinIO."""
        if self._s3 is not None:
            return
        if not self.use_minio:
            return
        try:
            import boto3
            from botocore.config import Config
            self._s3 = boto3.client(
                's3',
                endpoint_url=f"http://{self.minio_host}:{self.minio_port}",
                aws_access_key_id=self.minio_access_key,
                aws_secret_access_key=self.minio_secret_key,
                region_name='us-east-1',
                config=Config(signature_version='s3v4'),
                use_ssl=False,
            )
            # Ensure bucket
            try:
                self._s3.head_bucket(Bucket=self.minio_bucket)
            except Exception:
                self._s3.create_bucket(Bucket=self.minio_bucket)
        except Exception as e:
            self.logger.warning(f"Failed to initialize S3 client for MinIO: {e}")
            self._s3 = None
    
    def get_input_data(self, expected_format: str = 'auto') -> Optional[Any]:
        """
        Get input data for this function
        
        Args:
            expected_format: Expected format ('image', 'text', 'json', 'auto')
            
        Returns:
            Input data in the appropriate format, or None if not available
        """
        if not self.has_input or not self.input_key:
            self.logger.info("No input key available for function")
            return None
        
        try:
            # Get raw data from storage
            raw_data = self._get_raw_data(self.input_key)
            if raw_data is None:
                self.logger.error(f"Failed to retrieve data for key: {self.input_key}")
                return None
            
            # Convert to expected format
            return self._convert_to_format(raw_data, expected_format)
            
        except Exception as e:
            self.logger.error(f"Error retrieving input data for key {self.input_key}: {e}")
            return None
    
    def get_input_file_path(self) -> Optional[str]:
        """
        Get file path for input data (for functions that work with files)
        
        Returns:
            str: File path to the data, or None if not available
        """
        if not self.has_input or not self.input_key:
            return None
        
        try:
            # First preference: local shared volume /mnt/node_data/<request_id>/<input_key>_<node>_output.json
            node_id = os.environ.get('HOSTNAME') or __import__('socket').gethostname()
            import pathlib
            shared_volume_root = '/mnt/node_data'
            local_path = pathlib.Path(shared_volume_root) / (self.request_id or 'experiment') / f"{self.input_key}_{node_id}_output.json"
            if local_path.exists():
                # GREEN for local retrieval
                self.logger.info(f"\033[92mInput retrieved from LOCAL shared volume: {local_path}\033[0m")
                return str(local_path)
            # Fallback: download from MinIO to temp file
            if self.use_minio:
                temp = self._download_minio_to_temp_file(self.input_key)
                if temp:
                    # CYAN for MinIO retrieval
                    self.logger.info(f"\033[96mInput retrieved from MINIO to temp file: {temp}\033[0m")
                return temp
            self.logger.error("No storage backend available")
            return None
        except Exception as e:
            self.logger.error(f"Error getting input file path: {e}")
            return None
    
    def get_input_metadata(self) -> Optional[Dict]:
        """
        Get metadata for input data
        
        Returns:
            Dict: Metadata dictionary, or None if not available
        """
        if not self.has_input or not self.input_key:
            return None
        
        try:
            return {
                'input_key': self.input_key,
                'storage_type': 'minio' if self.use_minio else 'local',
                'function_name': self.function_name,
                'request_id': self.request_id
            }
        except Exception as e:
            self.logger.error(f"Error getting input metadata: {e}")
            return None
    
    def _get_raw_data(self, input_key: str) -> Optional[bytes]:
        """Get raw data from storage"""
        try:
            # Prefer local tmp file if present
            node_id = os.environ.get('HOSTNAME') or __import__('socket').gethostname()
            tmp_root = 'tmp'
            import pathlib
            local_path = pathlib.Path(tmp_root) / (self.request_id or 'experiment') / f"{input_key}_{node_id}_output.json"
            if local_path.exists():
                try:
                    data = local_path.read_bytes()
                    # GREEN for local retrieval
                    self.logger.info(f"\033[92mInput read from LOCAL tmp: {local_path}\033[0m")
                    return data
                except Exception as e:
                    self.logger.warning(f"Failed to read local file {local_path}: {e}")
            # Fallback to MinIO
            if self.use_minio:
                self._ensure_s3()
                if self._s3 is None:
                    return None
                key = f"{self.minio_prefix}data/{input_key}.pkl"
                try:
                    obj = self._s3.get_object(Bucket=self.minio_bucket, Key=key)
                    body = obj['Body'].read()
                    # CYAN for MinIO retrieval
                    self.logger.info(f"\033[96mInput fetched from MINIO: s3://{self.minio_bucket}/{key}\033[0m")
                    return body
                except Exception as e:
                    self.logger.debug(f"MinIO get_object failed for {key}: {e}")
                    return None
            return None
        except Exception as e:
            self.logger.error(f"Error getting raw data from storage: {e}")
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
            self._ensure_s3()
            if self._s3 is None:
                return None
            key = f"{self.minio_prefix}data/{input_key}.pkl"
            obj = self._s3.get_object(Bucket=self.minio_bucket, Key=key)
            data = obj['Body'].read()
            
            # Create temporary file
            import tempfile
            import os
            
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
    
    def store_output(self, output_data: Any, output_key: Optional[str] = None) -> Optional[str]:
        """
        Store function output to both local shared volume (node-specific filename) and MinIO (no node suffix).

        Local filename format:
          /mnt/node_data/<request_id>/<request_id>_<function_name>_<node_id>_output.json

        Remote (MinIO) data_id format:
          <request_id>_<function_name>_output

        Args:
            output_data: Output data to store (dict/str/bytes)
            output_key: Optional override for remote data_id

        Returns:
            str: Remote data_id if upload succeeded, else None
        """
        try:
            # Determine identifiers
            node_id = os.environ.get('HOSTNAME') or __import__('socket').gethostname()
            request_id = self.request_id or 'experiment'
            function_name = self.function_name or 'function'

            # Remote key (no node suffix)
            data_id = output_key or f"{request_id}_{function_name}_output"

            # Ensure /mnt/node_data/<request_id>/ exists
            import pathlib
            shared_volume_root = '/mnt/node_data'
            exp_dir = pathlib.Path(shared_volume_root) / request_id
            exp_dir.mkdir(parents=True, exist_ok=True)

            # Build local filename with node suffix
            local_filename = f"{request_id}_{function_name}_{node_id}_output.json"
            local_path = exp_dir / local_filename

            # Normalize output to JSON-serializable
            serializable: Any
            if isinstance(output_data, (str, bytes)):
                if isinstance(output_data, bytes):
                    # Best-effort decode; if fails, base64 encode
                    try:
                        serializable = output_data.decode('utf-8')
                    except UnicodeDecodeError:
                        import base64
                        serializable = {
                            'data_b64': base64.b64encode(output_data).decode('utf-8')
                        }
                else:
                    serializable = output_data
            elif isinstance(output_data, dict):
                serializable = output_data
            else:
                # Fallback to string representation
                serializable = str(output_data)

            # Write local JSON file (node-specific)
            try:
                with open(local_path, 'w', encoding='utf-8') as f:
                    if isinstance(serializable, dict):
                        json.dump(serializable, f, ensure_ascii=False)
                    else:
                        f.write(serializable)
                # GREEN for local storage
                self.logger.info(f"\033[92mWrote local output (local storage): {local_path}\033[0m")
            except Exception as e:
                self.logger.warning(f"Failed writing local output {local_path}: {e}")

            # Prepare bytes for remote upload
            if isinstance(serializable, dict):
                remote_bytes = json.dumps(serializable).encode('utf-8')
            elif isinstance(serializable, str):
                remote_bytes = serializable.encode('utf-8')
            else:
                # Shouldn't happen, but guard anyway
                remote_bytes = str(serializable).encode('utf-8')

            # Upload to MinIO if available
            uploaded = False
            if self.use_minio:
                try:
                    self._ensure_s3()
                    if self._s3 is not None:
                        # Upload as object under prefix data/<data_id>.pkl
                        self._s3.put_object(
                            Bucket=self.minio_bucket,
                            Key=f"{self.minio_prefix}data/{data_id}.pkl",
                            Body=remote_bytes,
                            ContentType='application/json'
                        )
                        uploaded = True
                except Exception as e:
                    self.logger.warning(f"MinIO upload failed for {data_id}: {e}")

            if uploaded:
                # CYAN for MinIO/remote storage
                self.logger.info(f"\033[96mStored remote output (MinIO): {data_id}\033[0m")
                return data_id
            else:
                self.logger.error(f"Failed to store remote output for: {data_id}")
                return None

        except Exception as e:
            self.logger.error(f"Error storing output: {e}")
            return None
    
    def get_input_info(self) -> Dict:
        """Get information about available inputs"""
        return {
            'has_input': self.has_input,
            'input_key': self.input_key,
            'function_name': self.function_name,
            'request_id': self.request_id,
            'storage_available': self.storage_available,
            'use_minio': self.use_minio,
            'use_local': self.use_local
        }


def create_input_handler(request_data: Dict) -> Optional[FunctionInputHandler]:
    """
    Create a function input handler from request data
    
    Args:
        request_data: Request data containing input_context and storage_config
        
    Returns:
        FunctionInputHandler instance or None if not available
    """
    try:
        input_context = request_data.get('input_context', {})
        storage_config = request_data.get('storage_config', {})
        
        if not input_context:
            return None
        
        return FunctionInputHandler(input_context, storage_config)
        
    except Exception as e:
        logging.getLogger("function_input_handler").error(f"Error creating input handler: {e}")
        return None
