#!/usr/bin/env python3
"""
FastAPI server wrapper for recognizer__upload function
"""

import os
import redis
import base64
from typing import Dict

redis_host = os.getenv("REDIS_HOST", "redis")
redis_port = int(os.getenv("REDIS_PORT", 6379))

store = redis.Redis(host=redis_host, port=redis_port, decode_responses=False)

def save_image_to_redis(img_bytes: bytes) -> Dict:
    try:
        store.set('start', img_bytes)
        img_b64 = base64.b64encode(img_bytes).decode('utf-8')
        return {
            "status": "success", 
            "message": f"Stored {len(img_bytes)} bytes under key 'start'",
            "img_clean_b64": img_b64
        }
    except redis.RedisError as e:
        return {"status": "error", "message": f"Redis error on SET: {e}"}

def run(_payload: bytes) -> Dict:
    """
    _payload is expected to contain image bytes.
    This function saves the image bytes to the Redis key 'start'.
    """
    
    if not isinstance(_payload, bytes):
        return {"status": "error", "message": "Invalid payload: input must be bytes."}
    
    if _payload is None:
        return {"status": "error", "message": "No payload provided"}

    return save_image_to_redis(_payload)