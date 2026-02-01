import os
import io
import base64
import cv2
import numpy as np
from PIL import Image
import pytesseract
from typing import Union, Dict

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
        - img_b64: base64-encoded image
        - img_path: filesystem path
        - img_clean: base64 or path (from previous step)

    Outputs:
      - status: "ok" | "error"
      - text: extracted string
      - text_chunks: list[str]
    """
    pil = _img_from_payload(payload)
    if pil is None:
        return {"status": "error", "error": "no image provided or image format is invalid"}

    proc = _preprocess_for_ocr(pil)
    try:
        text = _ocr(proc)
    except Exception as e:
        return {"status": "error", "error": f"ocr_failed: {e}"}

    chunks = _chunk_text(text)
    return {"status": "ok", "text": text, "text_chunks": chunks}