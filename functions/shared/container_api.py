#!/usr/bin/env python3
"""
Container-Level API for All Schedulers

This module provides a standardized API that function containers can use
to handle requests with storage support across all DataFlower schedulers.
"""

import json
import logging
import time
import os
from typing import Any, Dict, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if os.getenv('DEBUG', 'false').lower() == 'true' else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Import the input retrieval service
try:
    from input_retrieval_service import retrieve_input
except ImportError:
    # Fallback for other function types
    def retrieve_input(input_ref: Dict, function_name: str = None) -> Union[bytes, Dict]:
        """Fallback input retrieval"""
        return input_ref.get('data', b'')


class ContainerAPI:
    """
    Standardized API for all function containers.
    
    This class provides a unified interface for handling requests with
    storage support across all scheduler types.
    """
    
    def __init__(self, function_name: str):
        """
        Initialize container API for specific function.
        
        Args:
            function_name: Name of the function this container runs
        """
        self.function_name = function_name
        self.logger = logging.getLogger(f"container_api.{function_name}")
    
    def handle_request(self, payload: Dict) -> Dict:
        """
        Handle incoming request with automatic storage support.
        
        Args:
            payload: Request payload containing input data or storage context
            
        Returns:
            Dict: Response with status and result data
        """
        start_time = time.time()
        
        try:
            # Determine input source
            if 'input_ref' in payload:
                # Function has dependencies - retrieve from storage
                input_data = self._retrieve_from_storage(payload['input_ref'])
            elif 'img_b64' in payload:
                # Direct image input
                input_data = payload['img_b64']
            elif 'data' in payload:
                # Direct data input
                input_data = payload['data']
            else:
                # Use entire payload as input
                input_data = payload
            
            # Process the input data
            result = self._process_input(input_data)
            
            # Calculate execution time
            exec_time = (time.time() - start_time) * 1000.0
            
            # Return response with execution metrics
            return {
                'status': 'success',
                'result': result,
                'exec_ms': exec_time,
                'server_timestamps': {
                    'exec_start': start_time,
                    'exec_end': time.time()
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error processing request: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'exec_ms': (time.time() - start_time) * 1000.0
            }
    
    def _retrieve_from_storage(self, input_ref: Dict) -> Any:
        """
        Retrieve input data from storage using the input retrieval service.
        
        Args:
            input_ref: Storage reference containing source and configuration
            
        Returns:
            Any: Retrieved input data
        """
        try:
            self.logger.info(f"Retrieving input from storage: {input_ref.get('src', 'unknown')}")
            return retrieve_input(input_ref, self.function_name)
        except Exception as e:
            self.logger.error(f"Failed to retrieve input from storage: {e}")
            raise
    
    def _process_input(self, input_data: Any) -> Any:
        """
        Process input data. This method should be overridden by specific functions.
        
        Args:
            input_data: Input data to process
            
        Returns:
            Any: Processed result
        """
        # Default implementation - just return the input
        # Functions should override this method with their specific processing logic
        return input_data


class StorageRequestHandler:
    """
    Handle storage-aware requests in containers.
    
    This class provides automatic storage handling for function containers,
    making it easy to integrate storage support into any function.
    """
    
    def __init__(self, function_name: str, process_func: callable):
        """
        Initialize storage request handler.
        
        Args:
            function_name: Name of the function
            process_func: Function to process the input data
        """
        self.function_name = function_name
        self.process_func = process_func
        self.logger = logging.getLogger(f"storage_handler.{function_name}")
    
    def process_request(self, payload: Dict) -> Dict:
        """
        Process request with automatic storage handling.
        
        Args:
            payload: Request payload
            
        Returns:
            Dict: Response with processing result
        """
        start_time = time.time()
        
        try:
            # Extract input data
            input_data = self._extract_input_data(payload)
            
            # Process using the provided function
            result = self.process_func(input_data)
            
            # Calculate execution time
            exec_time = (time.time() - start_time) * 1000.0
            
            return {
                'status': 'success',
                'result': result,
                'exec_ms': exec_time,
                'server_timestamps': {
                    'exec_start': start_time,
                    'exec_end': time.time()
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error processing request: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'exec_ms': (time.time() - start_time) * 1000.0
            }
    
    def _extract_input_data(self, payload: Dict) -> Any:
        """
        Extract input data from payload, handling storage contexts automatically.
        
        Args:
            payload: Request payload
            
        Returns:
            Any: Extracted input data
        """
        if 'input_ref' in payload:
            # Retrieve from storage
            return retrieve_input(payload['input_ref'], self.function_name)
        elif 'img_b64' in payload:
            # Direct image input
            return payload['img_b64']
        elif 'data' in payload:
            # Direct data input
            return payload['data']
        else:
            # Use entire payload
            return payload


# Factory functions for easy integration
def create_container_api(function_name: str) -> ContainerAPI:
    """Create container API for specific function"""
    return ContainerAPI(function_name)


def create_storage_handler(function_name: str, process_func: callable) -> StorageRequestHandler:
    """Create storage request handler for specific function"""
    return StorageRequestHandler(function_name, process_func)


# ==============================================================================
# Local Output Writing Functions
# ==============================================================================

def store_output_locally(function_name: str, output_data: Any, input_ref: Optional[Dict] = None, 
                         payload: Optional[Dict] = None) -> Optional[str]:
    """
    Store output to local shared volume.
    
    Creates a file at: /mnt/node_data/{request_prefix}_{function_name}.json
    
    Args:
        function_name: Name of the function
        output_data: The output data to write (must be JSON serializable)
        input_ref: Optional input reference for extracting request context
        payload: Optional full payload for request context
        
    Returns:
        str: Path to written file on success, None on failure
        
    Example filename: vary-firingrate_FaasPRS_request_rate_per_sec10_workflow_1_recognizer__adult.json
    """
    try:
        import os
        
        # Use function_name from parameter, fallback to env var
        if not function_name:
            function_name = os.getenv("FUNCTION_NAME", "unknown_function")
        
        node_id = os.getenv("NODE_ID", "unknown")
        
        # Try to extract request prefix from payload or input_ref
        request_prefix = None
        context = payload or input_ref
        
        if isinstance(context, dict):
            if "input_ref" in context and isinstance(context["input_ref"], dict):
                # Try to extract from path or input_key
                input_ref_data = context["input_ref"]
                req_dir = input_ref_data.get("path")
                if req_dir:
                    request_prefix = req_dir
                elif input_ref_data.get("input_key"):
                    # input_key format: <prefix>_<previous_function>
                    # Extract everything except the last part (function name)
                    input_key = input_ref_data["input_key"]
                    # Remove the previous function name (last part after last underscore)
                    parts = input_key.rsplit("_", 1)
                    if len(parts) >= 1:
                        request_prefix = parts[0]
                    else:
                        request_prefix = input_key
            elif "request_id" in context:
                request_prefix = context["request_id"]
        
        # If no request_prefix found, log and return error without fallback
        if not request_prefix:
            err = "request_prefix is missing; refusing to fallback to timestamp"
            print(f"[store_output_locally] ✗ ERROR: {err}")
            logging.getLogger('container_api').error(err)
            return None
        
        print(f"[store_output_locally] function_name={function_name}, request_prefix={request_prefix}")
        
        # Create output filename: {request_prefix}_{function_name}.json
        output_dir = '/mnt/node_data'
        print(f"[store_output_locally] Ensuring directory exists: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
        local_path = os.path.join(output_dir, f'{request_prefix}_{function_name}.json')
        
        print(f"[store_output_locally] Writing to: {local_path}")
        print(f"[store_output_locally] Output data type: {type(output_data)}")
        
        # Write JSON output
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        print(f"[store_output_locally] ✓ Successfully wrote to: {local_path}")
        logging.getLogger('container_api').info(f"[{function_name}] Output written to: {local_path}")
        return local_path
        
    except Exception as e:
        import traceback
        error_msg = f"[{function_name}] Failed to write local output: {e}\n{traceback.format_exc()}"
        print(f"[store_output_locally] ✗ ERROR: {error_msg}")
        logging.getLogger('container_api').warning(error_msg)
        return None


def store_output_remotely(function_name: str, output_data: Any, input_ref: Optional[Dict]) -> Optional[str]:
    """
    Store output to MinIO remote storage.
    
    Args:
        function_name: Name of the function
        output_data: The output data to upload
        input_ref: Input reference containing storage config and request context
        
    Returns:
        str: Remote data_id on success, None on failure
    """
    print(f"[store_output_remotely] Starting for function: {function_name}")
    try:
        import os, json, base64
        
        # Extract request_id and storage_config from input_ref
        request_id = None
        storage_config: Dict[str, Any] = {}
        
        print(f"[store_output_remotely] input_ref type: {type(input_ref)}")
        if isinstance(input_ref, dict):
            print(f"[store_output_remotely] input_ref keys: {list(input_ref.keys())}")
            request_id = input_ref.get('request_id')
            storage_config = input_ref.get('storage_config', {})
            
            if not request_id:
                input_key = input_ref.get('input_key')
                if isinstance(input_key, str):
                    if '_recognizer__' in input_key:
                        request_id = input_key.rsplit('_recognizer__', 1)[0]
                    elif '_' in input_key:
                        request_id = input_key.rsplit('_', 1)[0]
        
        if not request_id:
            request_id = 'experiment'
            print(f"[store_output_remotely] No request_id found, using default: {request_id}")
            logging.getLogger('container_api').warning(f"No request_id found for remote storage, using default")
        
        # Prepare data_id
        data_id = f"{request_id}_{function_name}"
        print(f"[store_output_remotely] data_id: {data_id}")
        
        # Serialize to bytes
        if isinstance(output_data, bytes):
            remote_bytes = output_data
        elif isinstance(output_data, (dict, list)):
            remote_bytes = json.dumps(output_data).encode('utf-8')
        elif isinstance(output_data, str):
            remote_bytes = output_data.encode('utf-8')
        else:
            remote_bytes = json.dumps(str(output_data)).encode('utf-8')
        
        # Upload to MinIO
        try:
            # Get MinIO configuration
            def_cfg = {
                'minio_host': 'minio',
                'minio_port': 9000,
                'access_key': 'dataflower',
                'secret_key': 'dataflower123',
                'bucket_name': 'dataflower-storage'
            }
            cfg = {**def_cfg, **storage_config}
            
            import boto3
            from botocore.config import Config
            
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{cfg['minio_host']}:{cfg['minio_port']}",
                aws_access_key_id=cfg['access_key'],
                aws_secret_access_key=cfg['secret_key'],
                region_name='us-east-1',
                config=Config(signature_version='s3v4')
            )
            
            key = f"dataflower/data/{data_id}.pkl"
            print(f"[store_output_remotely] Uploading to MinIO - Bucket: {cfg['bucket_name']}, Key: {key}")
            s3.put_object(
                Bucket=cfg['bucket_name'],
                Key=key,
                Body=remote_bytes,
                ContentType='application/json'
            )
            
            print(f"[store_output_remotely] ✓ Successfully uploaded to MinIO: {data_id}")
            logging.getLogger('container_api').info(f"\033[96mStored remote output (MinIO): {data_id}\033[0m")
            return data_id
            
        except Exception as e:
            import traceback
            error_msg = f"MinIO upload failed for {data_id}: {e}\n{traceback.format_exc()}"
            print(f"[store_output_remotely] ✗ MinIO error: {error_msg}")
            logging.getLogger('container_api').warning(error_msg)
            return None
            
    except Exception as e:
        import traceback
        error_msg = f"Error in store_output_remotely: {e}\n{traceback.format_exc()}"
        print(f"[store_output_remotely] ✗ EXCEPTION: {error_msg}")
        logging.getLogger('container_api').error(error_msg)
        return None


def store_output(function_name: str, output_data: Any, input_ref: Optional[Dict] = None,
                      payload: Optional[Dict] = None) -> Optional[str]:
    """
    Store output to both local shared volume and MinIO remote storage.
    
    This function orchestrates storing output in two places:
    1. Local: Uses store_output_locally() -> writes to /mnt/node_data/
    2. Remote: Uses store_output_remotely() -> uploads to MinIO
    
    Args:
        function_name: Name of the function
        output_data: The output data to store
        input_ref: Optional input reference containing storage context
        payload: Optional full payload for local storage context
        
    Returns:
        str: Remote data_id on success, None on failure
    """
    print(f"[store_output] Called for function: {function_name}")
    print(f"[store_output] input_ref present: {input_ref is not None}, payload present: {payload is not None}")
    try:
        # Store locally (uses local_output_writer module)
        print(f"[store_output] Calling store_output_locally...")
        local_path = store_output_locally(function_name, output_data, input_ref, payload)
        if local_path:
            print(f"[store_output] ✓ Local storage succeeded: {local_path}")
            logging.getLogger('container_api').debug(f"Local storage succeeded: {local_path}")
        else:
            print(f"[store_output] ✗ Local storage returned None")
        
        # Store remotely (uploads to MinIO)
        if input_ref is not None:
            print(f"[store_output] Calling store_output_remotely...")
            remote_id = store_output_remotely(function_name, output_data, input_ref)
            if remote_id:
                print(f"[store_output] ✓ Remote storage succeeded: {remote_id}")
                logging.getLogger('container_api').debug(f"Remote storage succeeded: {remote_id}")
                return remote_id
            else:
                print(f"[store_output] ✗ Remote storage returned None")
                logging.getLogger('container_api').warning("Remote storage failed, but local storage may have succeeded")
                return None
        else:
            print(f"[store_output] No input_ref provided, skipping remote storage")
            logging.getLogger('container_api').debug("No input_ref provided, skipping remote storage")
            return None
            
    except Exception as e:
        import traceback
        error_msg = f"Error in store_output: {e}\n{traceback.format_exc()}"
        print(f"[store_output] ✗ EXCEPTION: {error_msg}")
        logging.getLogger('container_api').error(error_msg)
        return None
