import base64
import io
import os
import time
from typing import Dict, Union

import cv2
import numpy as np
import pytesseract
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from PIL import Image
from input_retrieval_service import retrieve_input
from container_api import store_output
Image.MAX_IMAGE_PIXELS = 300000000  # e.g., 300 million pixels


def _img_from_payload(payload: Union[bytes, Dict]) -> Image.Image | None:
    # If the payload is raw bytes, load it directly. This is the expected path for the workflow.
    if isinstance(payload, bytes):
        try:
            return Image.open(io.BytesIO(payload)).convert("RGB")
        except Exception as e:
            print(f"Error loading image from bytes: {e}")
            return None

    # The following checks are for backward compatibility or direct invocation with a dict payload.
    if "img_b64" in payload:
        try:
            data = base64.b64decode(payload["img_b64"])
            return Image.open(io.BytesIO(data)).convert("RGB")
        except Exception:
            return None
    if "img_path" in payload:
        try:
            return Image.open(payload["img_path"]).convert("RGB")
        except Exception:
            return None
    if "img_clean" in payload:
        # accept either base64 or filesystem path
        val = payload["img_clean"]
        try:
            data = base64.b64decode(val)
            return Image.open(io.BytesIO(data)).convert("RGB")
        except Exception:
            try:
                return Image.open(val).convert("RGB")
            except Exception:
                return None
    return None

def _preprocess_for_ocr(pil_img: Image.Image) -> np.ndarray:
    # to OpenCV BGR
    img = np.array(pil_img)[:, :, ::-1]
    # resize smaller side ~ (scale down heavy images)
    h, w = img.shape[:2]
    scale = 0.1 if max(h, w) > 2000 else 1.0
    if scale != 1.0:
        img = cv2.resize(img, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)
    # grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # light denoise: dilate then erode
    kernel = np.ones((1, 1), np.uint8)
    den = cv2.dilate(gray, kernel, iterations=1)
    den = cv2.erode(den, kernel, iterations=1)
    return den

def _ocr(img_cv_gray: np.ndarray) -> str:
    # pytesseract works on PIL or filepath; use PIL to avoid temp files
    pil = Image.fromarray(img_cv_gray)
    return pytesseract.image_to_string(pil)

def _chunk_text(text: str) -> list[str]:
    # simple sentence-ish split; fallback to words if single chunk
    parts = [s.strip() for s in text.replace("\r", " ").split("\n") if s.strip()]
    if not parts:
        return []
    # if everything is one big line, split by periods
    if len(parts) == 1:
        raw = [p.strip() for p in parts[0].split(".") if p.strip()]
        return raw if raw else parts
    return parts

def run(payload: Union[bytes, Dict]) -> dict:
    """
    Inputs (one of):
      - raw image bytes (preferred for workflow)
      - a dict containing one of:
        - input_ref: storage reference for input retrieval
        - img_b64: base64-encoded image
        - img_path: filesystem path
        - img_clean: base64 or path (from previous step)

    Outputs:
      - status: "success" | "error"
      - text: extracted string
      - text_chunks: list[str]
    """
    t_exec_start = time.time()
    
    # Handle input retrieval from storage
    if isinstance(payload, dict) and "input_ref" in payload:
        try:
            retrieved_data = retrieve_input(payload["input_ref"], "recognizer__extract")
            if isinstance(retrieved_data, dict) and "img_b64" in retrieved_data:
                img_bytes = base64.b64decode(retrieved_data["img_b64"])
                pil = Image.open(io.BytesIO(img_bytes)).convert("RGB")
            elif isinstance(retrieved_data, bytes):
                pil = Image.open(io.BytesIO(retrieved_data)).convert("RGB")
            else:
                return {"status": "error", "message": "Failed to extract image data from retrieved input"}
        except Exception as e:
            return {"status": "error", "message": f"Failed to retrieve input: {str(e)}"}
    else:
        pil = _img_from_payload(payload)
    
    if pil is None:
        return {"status": "error", "message": "no image provided or image format is invalid"}

    proc = _preprocess_for_ocr(pil)
    try:
        text = _ocr(proc)
    except Exception as e:
        return {"status": "error", "message": f"ocr_failed: {e}"}

    chunks = _chunk_text(text)
    t_exec_end = time.time()
    exec_ms = (t_exec_end - t_exec_start) * 1000.0
    out = {
        "status": "success", 
        "message": "Text extraction successful",
        "text": text, 
        "text_chunks": chunks,
        "exec_ms": exec_ms,
        "server_timestamps": 
            {"exec_start": t_exec_start, 
            "exec_end": t_exec_end, 
            "sent": time.time()}
    }
    # Store output to both local and remote storage
    try:
        data_id = store_output("recognizer__extract", out,
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
app = FastAPI(title="Recognizer Extract Service", version="1.0.0")

@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "recognizer__extract"}

@app.post("/invoke")
async def invoke_function(payload: Dict):
    """Invoke the extract function"""
    try:
        result = run(payload)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Function execution failed: {str(e)}"}
        )