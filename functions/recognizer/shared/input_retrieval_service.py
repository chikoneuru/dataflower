import os
import json
import base64
from typing import Dict, Any, Union

# Import centralized MinIO configuration
try:
    # Try to import from scheduler.shared (when running in orchestrator context)
    from scheduler.shared.minio_config import get_minio_storage_config
except ImportError:
    # Fallback for function containers - define locally if needed
    def get_minio_storage_config():
        """Fallback MinIO configuration for function containers."""
        return {
            'minio_host': '172.26.0.20',  # MinIO IP in Docker network
            'minio_port': 9000,
            'access_key': 'dataflower',
            'secret_key': 'dataflower123',
            'bucket_name': 'dataflower-storage'
        }


def retrieve_input(input_ref: Dict, function_name: str = None) -> Union[bytes, Dict]:
	"""
	Retrieve the actual input from local (shared volume) or remote (MinIO) sources and format it appropriately.

	Args:
		input_ref: A dictionary describing the input location. Expected keys:
		  - src: "local" or "remote"
		  - path: relative path on the shared node volume when src == "local"
		  - input_key: MinIO data key when src == "remote"
		  - storage_config: MinIO configuration when src == "remote"
		function_name: Optional function name to determine expected input format

	Returns:
		Raw bytes for image functions, or formatted dict for text functions.

	Raises:
		ValueError: when the source type is unknown or required keys are missing.
		FileNotFoundError / requests.HTTPError: for underlying IO errors.
	"""
	print(f"DEBUG: retrieve_input called with input_ref: {input_ref}")
	print(f"DEBUG: input_ref type: {type(input_ref)}")
	if isinstance(input_ref, dict):
		print(f"DEBUG: input_ref keys: {list(input_ref.keys())}")
	src = input_ref.get("src")
	print(f"DEBUG: src = {src}")
	print(f"DEBUG: base64 module available: {base64 is not None}")
	
	# Get raw data: always try local first if 'path' is present, then try remote if 'input_key' is present
	raw_data = None
	try_local_first = bool(input_ref.get("path"))
	if try_local_first:
		req_dir = input_ref.get("path")
		input_key = input_ref.get("input_key")
		if req_dir and input_key:
			# Resolve file with current container node id suffix
			node_id = os.environ.get('HOSTNAME') or __import__('socket').gethostname()
			# Local filename pattern is: <request_id>_<producer_fn>_<node>.json
			# input_key is: <request_id>_<producer_fn>
			prefix = input_key
			candidate = os.path.join("/mnt/node_data", req_dir, f"{prefix}_{node_id}.json")
			try:
				with open(candidate, "rb") as f:
					raw_data = f.read()
			except FileNotFoundError:
				raw_data = None
	
	# Fallback to remote if available and nothing loaded locally
	if raw_data is None and input_ref.get("input_key"):
		# Retrieve from MinIO (remote storage)
		input_key = input_ref.get("input_key")
		
		# Use centralized MinIO configuration with fallback to provided config
		default_config = get_minio_storage_config()
		storage_config = input_ref.get("storage_config", {})
		minio_host = storage_config.get("minio_host", default_config["minio_host"])
		minio_port = storage_config.get("minio_port", default_config["minio_port"])
		access_key = storage_config.get("access_key", default_config["access_key"])
		secret_key = storage_config.get("secret_key", default_config["secret_key"])
		bucket_name = storage_config.get("bucket_name", default_config["bucket_name"])
		
		try:
			# Use boto3 to retrieve from MinIO
			import boto3
			from botocore.config import Config
			
			s3_client = boto3.client(
				's3',
				endpoint_url=f'http://{minio_host}:{minio_port}',
				aws_access_key_id=access_key,
				aws_secret_access_key=secret_key,
				region_name='us-east-1',
				config=Config(signature_version='s3v4')
			)
			
			# Retrieve data from MinIO
			data_key = f"dataflower/data/{input_key}.pkl"
			print(f"[{function_name}] Attempting to retrieve from MinIO: Bucket='{bucket_name}', Key='{data_key}'")
			
			response = s3_client.get_object(Bucket=bucket_name, Key=data_key)
			raw_data = response['Body'].read()
			
			print(f"[{function_name}] Successfully retrieved {len(raw_data)} bytes from MinIO key: {data_key}")
			
		except Exception as e:
			error_msg = f"Failed to retrieve data from remote storage (MinIO): {e}\n"
			error_msg += f"  Function: {function_name}\n"
			error_msg += f"  Bucket: {bucket_name}\n"
			error_msg += f"  Key: {data_key}\n"
			error_msg += f"  Input key: {input_key}\n"
			error_msg += f"  MinIO endpoint: http://{minio_host}:{minio_port}"
			print(error_msg)
			raise ValueError(error_msg)
	elif src == "none":
		# No input available (first function or dependencies not ready)
		raise ValueError("No input available - function should receive direct input or dependencies not ready")
	else:
		raise ValueError(f"Unknown source type: {src}. Supported: 'local', 'remote'")
	
	# If it's JSON data, parse it
	try:
		if isinstance(raw_data, bytes):
			decoded = raw_data.decode('utf-8')
			if decoded.strip().startswith('{'):
				parsed_data = json.loads(decoded)
				# Extract the actual result from the stored output
				if isinstance(parsed_data, dict) and 'result' in parsed_data:
					parsed_data = parsed_data['result']
			else:
				parsed_data = None
		else:
			parsed_data = raw_data
	except (json.JSONDecodeError, UnicodeDecodeError):
		parsed_data = None
	
	# Format based on function type
	image_functions = {
		'recognizer__adult',
		'recognizer__violence', 
		'recognizer__extract',
		'recognizer__mosaic',
	}
	text_functions = {
		'recognizer__translate',
		'recognizer__censor',
	}
	
	if function_name in image_functions:
		# For image functions, return bytes or formatted dict with img_b64
		if isinstance(parsed_data, dict):
			# Look for image data in the parsed result
			img_b64 = None
			preferred_keys = ['img_b64', 'img_clean_b64', 'image_b64', 'mosaic_img_b64']
			for key in preferred_keys:
				val = parsed_data.get(key)
				if isinstance(val, str) and val:
					img_b64 = val
					break
			
			if img_b64:
				return {'img_b64': img_b64}
			else:
				# Fallback: return raw bytes if no base64 found
				return raw_data
		else:
			# Return raw bytes
			return raw_data
	
	elif function_name in text_functions:
		# For text functions, return formatted dict with text
		if isinstance(parsed_data, dict):
			text_val = None
			preferred_keys = ['text', 'extracted_text', 'translated_text', 'filtered_text']
			for key in preferred_keys:
				val = parsed_data.get(key)
				if isinstance(val, str):
					text_val = val
					break
			
			if text_val:
				return {'text': text_val}
			else:
				# Fallback: decode raw bytes as text
				try:
					text_content = raw_data.decode('utf-8', errors='ignore')
					return {'text': text_content}
				except:
					return {'text': ''}
		else:
			# Try to decode raw bytes as text
			try:
				text_content = raw_data.decode('utf-8', errors='ignore')
				return {'text': text_content}
			except:
				return {'text': ''}
	
	else:
		# Default: return raw bytes
		return raw_data


