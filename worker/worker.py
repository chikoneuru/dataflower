import base64
import os
from typing import Union
import redis
from fastapi import (FastAPI, Query, Request, HTTPException, Body)
from provider.function_manager import FunctionManager
from provider.remote_storage_adapter import create_storage_adapter
import json
import time

app = FastAPI()

# Redis setup
redis_host = os.getenv("REDIS_HOST", "serverless_redis")
redis_port = int(os.getenv("REDIS_PORT", 6379))
store = redis.Redis(host=redis_host, port=redis_port, decode_responses=False)

fm = FunctionManager()

# MinIO setup for cross-node data transfer
minio_config = {
    'type': 'minio',
    'minio_host': os.getenv('MINIO_HOST', 'minio'),
    'minio_port': int(os.getenv('MINIO_PORT', 9000)),
    'access_key': os.getenv('MINIO_ACCESS_KEY', 'dataflower'),
    'secret_key': os.getenv('MINIO_SECRET_KEY', 'dataflower123'),
    'bucket_name': os.getenv('MINIO_BUCKET', 'dataflower-storage')
}

try:
    minio_adapter = create_storage_adapter('minio', minio_config)
    print("[Worker] MinIO adapter initialized for cross-node data transfer")
except Exception as e:
    print(f"[Worker] Failed to initialize MinIO adapter: {e}")
    minio_adapter = None

@app.get("/")
def health_check():
    """Simple health check endpoint."""
    return {"status": "success", "message": "Worker is running."}

@app.post("/run")
async def run(
    request: Request,
    fn: str = Query(..., description="Function name to invoke")
):
    """
    Accepts input as raw bytes (application/octet-stream) for images,
    or as a JSON body (application/json) for text or structured data.
    Supports MinIO-based cross-node data transfer.
    """
    payload: Union[bytes, dict]
    content_type = request.headers.get('content-type')
    use_minio = False

    if content_type == 'application/octet-stream':
        payload = await request.body()
    elif content_type == 'application/json':
        try:
            payload = await request.json()
            # Check if this is a MinIO-based request
            if isinstance(payload, dict) and payload.get('use_minio') and minio_adapter:
                use_minio = True
                minio_input_key = payload.get('minio_input_key')
                minio_output_key = payload.get('minio_output_key')

                if not minio_input_key:
                    raise HTTPException(status_code=400, detail="MinIO input key missing")

                # Download actual data from MinIO using the input key
                print(f"[Worker] Downloading input data from MinIO (key: {minio_input_key})")
                minio_data = minio_adapter.get_data(minio_input_key)
                
                if minio_data is None:
                    raise HTTPException(status_code=400, detail="Failed to retrieve input data from MinIO")
                
                # Replace the payload with the actual data from MinIO
                payload = minio_data
                print(f"[Worker] Successfully retrieved {len(str(payload))} bytes from MinIO")
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON payload.")
    else:
        # Fallback for other types, e.g. text/plain
        payload = await request.body()
        # Attempt to decode as JSON if it looks like it, otherwise pass raw bytes
        if payload.startswith(b'{') and payload.endswith(b'}'):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                pass # Pass as bytes

    if not payload:
        raise HTTPException(status_code=400, detail="Request body is empty.")

    # Call the chosen function with the determined payload
    try:
        print(f"[Worker] Running function '{fn}' with payload of type {type(payload)}")
        func = fm.get_function(fn)

        # Measure server-side execution (fallback timing)
        t_exec_start = time.time()
        result = func(payload)
        t_exec_end = time.time()

        # Prefer function-provided timestamps if present; otherwise use worker timing
        server_ts = None
        if isinstance(result, dict):
            st = result.get("server_timestamps")
            if isinstance(st, dict):
                server_ts = st
        if not isinstance(server_ts, dict):
            server_ts = {
                "exec_start": t_exec_start,
                "exec_end": t_exec_end,
                "sent": time.time(),
            }
        else:
            # Ensure 'sent' exists for network return calculation
            if 'sent' not in server_ts:
                server_ts['sent'] = time.time()

        # If using MinIO, upload result and return MinIO key (include server_timestamps)
        if use_minio and minio_adapter and minio_output_key:
            print(f"[Worker] Uploading result to MinIO (key: {minio_output_key})")

            # Prepare metadata for result
            from provider.data_manager import DataMetadata
            result_metadata = DataMetadata(
                data_id=minio_output_key,
                node_id=os.getenv('HOSTNAME', 'worker'),
                function_name=fn,
                step_name="output",
                size_bytes=len(result) if hasattr(result, '__len__') else 0,
                created_time=time.time(),
                last_accessed=time.time(),
                access_count=1,
                data_type="output",
                dependencies=[minio_input_key] if minio_input_key else []
            )

            success = minio_adapter.store_data(minio_output_key, result, result_metadata)
            if success:
                return {
                    "status": "success",
                    "message": "Function executed successfully, result uploaded to MinIO",
                    "minio_output_uploaded": True,
                    "minio_output_key": minio_output_key,
                    "server_timestamps": server_ts,
                }
            else:
                print(f"[Worker] Failed to upload result to MinIO, returning directly")
                if isinstance(result, dict):
                    return {**result, "server_timestamps": server_ts}
                return {"result": result, "server_timestamps": server_ts}

        # Standard response for non-MinIO transfers (include server_timestamps)
        if isinstance(result, dict):
            # Merge or overwrite the server_timestamps with preferred (function-provided if any)
            result_out = dict(result)
            result_out["server_timestamps"] = server_ts
            return result_out
        return {"result": result, "server_timestamps": server_ts}
    except Exception as e:
        print(f"[Worker] Error running function '{fn}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error running function '{fn}': {str(e)}")