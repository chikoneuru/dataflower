# -*- coding: utf-8 -*-
"""
Simplified translate function.

Inputs:
  - text: str               # text to translate (required)
  - target_lang: str        # optional, e.g., 'en', default 'en'

Outputs:
  - status: "success" | "error"
  - translated_text: str
  - target_lang: str
"""

import time
from typing import Dict, Union

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from input_retrieval_service import retrieve_input
from container_api import store_output


def run(payload: Union[Dict, bytes]) -> Dict:
    t_exec_start = time.time()
    
    # Handle input_ref for shared volume/MinIO retrieval
    if isinstance(payload, dict) and "input_ref" in payload and isinstance(payload["input_ref"], dict):
        try:
            retrieved_data = retrieve_input(payload["input_ref"], "recognizer__translate")  # I/O inside container
            if isinstance(retrieved_data, dict) and "text" in retrieved_data:
                text = retrieved_data["text"]
            elif isinstance(retrieved_data, bytes):
                text = retrieved_data.decode('utf-8', errors='ignore')
            else:
                text = ""
        except Exception as e:
            return {"status": "error", "message": f"Failed to retrieve input: {str(e)}"}
    elif isinstance(payload, dict):
        text = payload.get("text")
    elif isinstance(payload, bytes):
        # Handle raw bytes as text
        text = payload.decode('utf-8', errors='ignore')
    else:
        return {
            "status": "error",
            "message": "Invalid payload: expected dict with 'text' or 'input_ref', or raw bytes.",
            "translated_text": "",
            "target_lang": "en",
        }

    if not isinstance(text, str):
        return {"status": "error", 
                "message": "text must be provided as a string", 
                "text": text}

    target_lang = payload.get("target_lang", "en")

    # NOTE:
    # In the original template, googletrans was intended but commented out.
    # For reproducibility without external network calls, we do a no-op "translation":
    # simply echo the original text. Replace the next line with a real translator if needed.
    translated_text = text

    t_exec_end = time.time()
    exec_ms = (t_exec_end - t_exec_start) * 1000.0
    out = {
        "status": "success",
        "message": "Translation successful",
        "translated_text": translated_text,
        "target_lang": target_lang,
        "exec_ms": exec_ms,
        "server_timestamps": {"exec_start": t_exec_start, "exec_end": t_exec_end, "sent": time.time()}
    }
    # If input_ref was used, store output to both local and MinIO
    # Store output to both local and remote storage
    try:
        data_id = store_output("recognizer__translate", out,
                              input_ref=payload.get("input_ref") if isinstance(payload, dict) else None,
                              payload=payload if isinstance(payload, dict) else None)
        if data_id:
            out["storage_saved"] = True
            out["storage_key"] = data_id
        else:
            out["storage_saved"] = False
    except Exception as e:
        print(f"store_output error: {e}")
        out["storage_saved"] = False
        out["storage_save_error"] = str(e)
    
    return out

# Create FastAPI app instance
app = FastAPI(title="Recognizer Translate Service", version="1.0.0")

@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "recognizer__translate"}

@app.post("/invoke")
async def invoke_function(payload: Dict):
    """Invoke the translate function"""
    try:
        result = run(payload)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Function execution failed: {str(e)}"}
        )