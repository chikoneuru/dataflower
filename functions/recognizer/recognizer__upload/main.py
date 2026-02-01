#!/usr/bin/env python3
"""
FastAPI server wrapper for recognizer__upload function
"""

import base64
import os
import time
from typing import Dict

import redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

redis_host = os.getenv("REDIS_HOST", "redis")
redis_port = int(os.getenv("REDIS_PORT", 6379))

# Global Redis connection - will be initialized lazily
store = None

def get_redis_connection():
    """Get Redis connection with retry logic"""
    global store
    if store is not None:
        try:
            # Test if connection is still alive
            store.ping()
            return store
        except:
            # Connection is dead, reset it
            store = None
    
    # Try to establish new connection
    max_retries = 3
    for attempt in range(max_retries):
        try:
            store = redis.Redis(host=redis_host, port=redis_port, decode_responses=False, socket_connect_timeout=5, socket_timeout=5)
            store.ping()
            print(f"Successfully connected to Redis at {redis_host}:{redis_port}")
            return store
        except Exception as e:
            print(f"Redis connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait before retry
            else:
                print(f"Failed to connect to Redis at {redis_host}:{redis_port} after {max_retries} attempts")
                store = None
                return None

def save_image_to_redis(img_bytes: bytes) -> Dict:
    store = get_redis_connection()
    img_b64 = base64.b64encode(img_bytes).decode('utf-8')

    # Graceful fallback: if Redis is unavailable, still return the payload inline
    if store is None:
        return {
            "status": "success",
            "message": "Redis connection not available; returning inline payload",
            "img_clean_b64": img_b64,
            "redis_used": False,
        }
    
    try:
        store.set('start', img_bytes)
        return {
            "status": "success", 
            "message": f"Stored {len(img_bytes)} bytes under key 'start'",
            "img_clean_b64": img_b64,
            "redis_used": True,
        }
    except redis.RedisError as e:
        # Degrade gracefully to inline payload if Redis write fails
        return {
            "status": "success",
            "message": f"Redis error on SET: {e}; returning inline payload",
            "img_clean_b64": img_b64,
            "redis_used": False,
        }

def run(_payload: bytes) -> Dict:
    """
    _payload is expected to contain image bytes.
    This function saves the image bytes to the Redis key 'start'.
    """
    
    if not isinstance(_payload, bytes):
        return {"status": "error", "message": "Invalid payload: input must be bytes."}
    
    if _payload is None:
        return {"status": "error", "message": "No payload provided"}

    t_exec_start = time.time()
    resp = save_image_to_redis(_payload)
    t_exec_end = time.time()
    exec_ms = (t_exec_end - t_exec_start) * 1000.0
    if isinstance(resp, dict):
        resp = {**resp, "exec_ms": exec_ms, "server_timestamps": {"exec_start": t_exec_start, "exec_end": t_exec_end, "sent": time.time()}}
    return resp

# Create FastAPI app instance
app = FastAPI(title="Recognizer Upload Service", version="1.0.0")

@app.get("/")
async def health_check():
    """Health check endpoint"""
    test_store = get_redis_connection()
    redis_status = "connected" if test_store is not None else "disconnected"
    return {
        "status": "healthy", 
        "service": "recognizer__upload",
        "redis_status": redis_status,
        "redis_host": redis_host,
        "redis_port": redis_port
    }

@app.post("/invoke")
async def invoke_function(payload: Dict):
    """Invoke the upload function"""
    try:
        print(f"DEBUG: Received payload keys: {list(payload.keys()) if isinstance(payload, dict) else 'Not a dict'}")
        print(f"DEBUG: Payload type: {type(payload)}")
        
        # Convert payload to bytes if needed
        img_bytes = None
        if "img_b64" in payload:
            # Handle the format sent by orchestrator
            data = payload["img_b64"]
            print(f"DEBUG: Found img_b64, data type: {type(data)}, length: {len(data) if hasattr(data, '__len__') else 'unknown'}")
            if isinstance(data, str):
                img_bytes = base64.b64decode(data)
            else:
                img_bytes = data
        elif "data" in payload:
            # Handle legacy format
            data = payload["data"]
            print(f"DEBUG: Found data field, data type: {type(data)}")
            if isinstance(data, str):
                img_bytes = base64.b64decode(data)
            else:
                img_bytes = data
        else:
            print(f"DEBUG: ERROR - Missing 'img_b64' or 'data' field in payload")
            raise HTTPException(status_code=400, detail="Missing 'img_b64' or 'data' field in payload")
        
        
        print(f"DEBUG: About to call run() with img_bytes type: {type(img_bytes)}, length: {len(img_bytes) if hasattr(img_bytes, '__len__') else 'unknown'}")
        result = run(img_bytes)
        
        # Store output to both local and MinIO if input_ref was provided
        if isinstance(payload, dict) and "input_ref" in payload:
            try:
                from container_api import store_output
                data_id = store_output("recognizer__upload", result, 
                                      input_ref=payload.get("input_ref"),
                                      payload=payload)
                if data_id:
                    result["storage_saved"] = True
                    result["storage_data_id"] = data_id
                    print(f"DEBUG: Stored output to MinIO: {data_id}")
            except Exception as store_err:
                print(f"WARNING: Failed to store output: {store_err}")
                result["storage_saved"] = False
        else:
            # Still store locally even without input_ref
            try:
                from container_api import store_output
                store_output("recognizer__upload", result, payload=payload if isinstance(payload, dict) else None)
            except Exception as e:
                print(f"WARNING: Failed to store output locally: {e}")
        
        return JSONResponse(content=result)
    except Exception as e:
        print(f"DEBUG: Exception caught: {type(e).__name__}: {str(e)}")
        import traceback
        print(f"DEBUG: Traceback: {traceback.format_exc()}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Function execution failed: {str(e)}"}
        )