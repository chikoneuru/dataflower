import os
import io
import base64
import numpy as np
import cv2
from PIL import Image
from typing import Union, Dict

def _img_from_payload(payload: Union[bytes, Dict]) -> np.ndarray | None:
    # Returns OpenCV BGR ndarray
    def pil_to_bgr(pil_img: Image.Image) -> np.ndarray:
        arr = np.array(pil_img.convert("RGB"))
        return arr[:, :, ::-1]  # RGB -> BGR

    if isinstance(payload, bytes):
        try:
            pil = Image.open(io.BytesIO(payload)).convert("RGB")
            return pil_to_bgr(pil)
        except Exception:
            return None

    if "img_b64" in payload:
        try:
            data = base64.b64decode(payload["img_b64"])
            pil = Image.open(io.BytesIO(data)).convert("RGB")
            return pil_to_bgr(pil)
        except Exception:
            return None

    if "img_path" in payload:
        try:
            pil = Image.open(payload["img_path"]).convert("RGB")
            return pil_to_bgr(pil)
        except Exception:
            return None

    if "img_clean" in payload:
        val = payload["img_clean"]
        # try base64 first, then path
        try:
            data = base64.b64decode(val)
            pil = Image.open(io.BytesIO(data)).convert("RGB")
            return pil_to_bgr(pil)
        except Exception:
            try:
                pil = Image.open(val).convert("RGB")
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
      - status: "ok" | "error"
      - mosaic_img_b64: base64 JPEG of pixelated image (if ok)
    """
    img = _img_from_payload(payload)
    if img is None:
        return {"status": "error", "error": "no image provided"}

    try:
        mosaic = _pixelate(img, block=8, scale_large=True)
        out_b64 = _to_jpg_b64(mosaic)
    except Exception as e:
        return {"status": "error", "error": f"mosaic_failed: {e}"}

    return {"status": "ok", "mosaic_img_b64": out_b64}