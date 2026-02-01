import base64
import os
from typing import Union
import redis
from fastapi import (FastAPI, Query, Request, HTTPException, Body)
from provider.function_manager import FunctionManager
import json

app = FastAPI()

# Redis setup
redis_host = os.getenv("REDIS_HOST", "serverless_redis")
redis_port = int(os.getenv("REDIS_PORT", 6379))
store = redis.Redis(host=redis_host, port=redis_port, decode_responses=False)

fm = FunctionManager()

@app.get("/")
def health_check():
    """Simple health check endpoint."""
    return {"status": "ok", "message": "Worker is running."}

@app.post("/run")
async def run(
    request: Request,
    fn: str = Query(..., description="Function name to invoke")
):
    """
    Accepts input as raw bytes (application/octet-stream) for images,
    or as a JSON body (application/json) for text or structured data.
    The worker determines the type and passes it to the target function.
    """
    payload: Union[bytes, dict]
    content_type = request.headers.get('content-type')

    if content_type == 'application/octet-stream':
        payload = await request.body()
    elif content_type == 'application/json':
        try:
            payload = await request.json()
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
        result = func(payload)
        return {"result": result}
    except Exception as e:
        print(f"[Worker] Error running function '{fn}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error running function '{fn}': {str(e)}")
