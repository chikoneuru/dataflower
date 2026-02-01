import base64
import io
import os
import time
from typing import Dict, Union

import cv2
import numpy as np
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from input_retrieval_service import retrieve_input
from PIL import Image, ImageOps
Image.MAX_IMAGE_PIXELS = 300000000  # e.g., 300 million pixels


MOSAIC_SIZE = (
    int(os.getenv("MOSAIC_SIZE_W", "1024")),
    int(os.getenv("MOSAIC_SIZE_H", "1024")),
)


def _resample_pil(img: Image.Image) -> Image.Image:
    # Resize/crop to target size using high-quality Lanczos resampling
    try:
        return ImageOps.fit(img, MOSAIC_SIZE, method=Image.Resampling.LANCZOS)
    except Exception:
        return img


def _img_from_payload(payload: Union[bytes, Dict]) -> np.ndarray | None:
    # Returns OpenCV BGR ndarray
    def pil_to_bgr(pil_img: Image.Image) -> np.ndarray:
        arr = np.array(pil_img.convert("RGB"))
        return arr[:, :, ::-1]  # RGB -> BGR

    if isinstance(payload, bytes):
        try:
            pil = Image.open(io.BytesIO(payload)).convert("RGB")
            pil = _resample_pil(pil)
            return pil_to_bgr(pil)
        except Exception:
            return None

    # Handle input_ref for shared volume/MinIO retrieval
    if isinstance(payload, dict) and "input_ref" in payload and isinstance(payload["input_ref"], dict):
        try:
            retrieved_data = retrieve_input(payload["input_ref"], "recognizer__mosaic")  # I/O inside container
            if isinstance(retrieved_data, dict) and "img_b64" in retrieved_data:
                img_bytes = base64.b64decode(retrieved_data["img_b64"])
            elif isinstance(retrieved_data, bytes):
                img_bytes = retrieved_data
            else:
                return None
            
            pil = Image.open(io.BytesIO(img_bytes)).convert("RGB")
            pil = _resample_pil(pil)
            return pil_to_bgr(pil)
        except Exception:
            return None

    if "img_b64" in payload:
        try:
            data = base64.b64decode(payload["img_b64"])
            pil = Image.open(io.BytesIO(data)).convert("RGB")
            pil = _resample_pil(pil)
            return pil_to_bgr(pil)
        except Exception:
            return None

    if "img_path" in payload:
        try:
            pil = Image.open(payload["img_path"]).convert("RGB")
            pil = _resample_pil(pil)
            return pil_to_bgr(pil)
        except Exception:
            return None

    if "img_clean" in payload:
        val = payload["img_clean"]
        # try base64 first, then path
        try:
            data = base64.b64decode(val)
            pil = Image.open(io.BytesIO(data)).convert("RGB")
            pil = _resample_pil(pil)
            return pil_to_bgr(pil)
        except Exception:
            try:
                pil = Image.open(val).convert("RGB")
                pil = _resample_pil(pil)
                return pil_to_bgr(pil)
            except Exception:
                return None

    return None

def _pixelate(img_bgr: np.ndarray, block: int = 8, scale_large: bool = False) -> np.ndarray:
    h, w = img_bgr.shape[:2]
    if scale_large and max(h, w) > 2000:
        img_bgr = cv2.resize(img_bgr, None, fx=0.1, fy=0.1, interpolation=cv2.INTER_AREA)
        h, w = img_bgr.shape[:2]

    # Apply mosaic by setting each block to the block's top-left pixel
    out = img_bgr.copy()
    for y in range(0, h - block, block):
        for x in range(0, w - block, block):
            b, g, r = img_bgr[y, x]
            out[y:y+block, x:x+block] = (b, g, r)
    return out

def _to_jpg_b64(img_bgr: np.ndarray) -> str:
    ok, enc = cv2.imencode(".jpg", img_bgr, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
    if not ok:
        raise RuntimeError("encode_failed")
    return base64.b64encode(enc.tobytes()).decode("ascii")

def run(payload: Union[bytes, Dict]) -> dict:
    """
    Inputs (one of):
      - raw image bytes (preferred for workflow)
      - a dict containing one of:
        - img_b64: base64-encoded image
        - img_path: filesystem path
      - img_clean: base64 or path (from previous step)

    Outputs:
      - status: "success" | "error"
      - mosaic_img_b64: base64 JPEG of pixelated image (if ok)
    """
    t_exec_start = time.time()
    img = _img_from_payload(payload)
    if img is None:
        return {"status": "error", "message": "no image provided"}

    try:
        mosaic = _pixelate(img, block=8, scale_large=True)
        out_b64 = _to_jpg_b64(mosaic)
    except Exception as e:
        return {"status": "error", "message": f"mosaic_failed: {e}"}

    t_exec_end = time.time()
    exec_ms = (t_exec_end - t_exec_start) * 1000.0
    return {
        "status": "success", 
        "message": "Mosaic successful",
        "mosaic_img_b64": out_b64,
        "exec_ms": exec_ms,
        "server_timestamps": {
            "exec_start": t_exec_start, 
            "exec_end": t_exec_end, 
            "sent": time.time()}}

# Create FastAPI app instance
app = FastAPI(title="Recognizer Mosaic Service", version="1.0.0")

@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "recognizer__mosaic"}

@app.post("/invoke")
async def invoke_function(payload: Dict):
    """Invoke the mosaic function"""
    try:
        result = run(payload)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Function execution failed: {str(e)}"}
        )