import base64
import io
import os
import time
from typing import Dict, Union

import numpy as np
from PIL import Image, ImageOps
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image as keras_image
from input_retrieval_service import retrieve_input
from container_api import store_output

SIZE = (224, 224)
# Build an absolute path to the model file relative to this script's location
MODEL_PATH = os.path.join(os.path.dirname(__file__), 'resnet50_final_adult.h5')

# Pre-load the model to improve performance and catch errors early
model = None
if os.path.exists(MODEL_PATH):
    try:
        model = load_model(MODEL_PATH, compile=False)
    except Exception as e:
        print(f"Error loading adult detection model: {e}")

def run(payload: Union[bytes, Dict]) -> Dict:
    """
    _payload is expected to contain image bytes or dict with img_b64.
    Run inference on the image to detect adult content.
    """
    global model
    if model is None:
        return {"status": "error", "message": f"Model file not found or failed to load from {MODEL_PATH}"}

    # Handle different payload formats
    img_bytes = None
    if isinstance(payload, bytes):
        img_bytes = payload
    elif isinstance(payload, dict):
        if "input_ref" in payload and isinstance(payload["input_ref"], dict):
            try:
                retrieved_data = retrieve_input(payload["input_ref"], "recognizer__adult")  # I/O inside container
                if isinstance(retrieved_data, dict) and "img_b64" in retrieved_data:
                    img_bytes = base64.b64decode(retrieved_data["img_b64"])
                elif isinstance(retrieved_data, bytes):
                    img_bytes = retrieved_data
                else:
                    return {"status": "error", "message": "Failed to extract image data from retrieved input"}
            except Exception as e:
                return {"status": "error", "message": f"Failed to retrieve input: {str(e)}"}
        elif "img_b64" in payload:
            try:
                img_bytes = base64.b64decode(payload["img_b64"])
            except Exception as e:
                return {"status": "error", "message": f"Failed to decode base64 image: {str(e)}"}
        else:
            return {"status": "error", "message": "Invalid payload: expected 'input_ref', raw bytes, or 'img_b64'."}
    else:
        return {"status": "error", "message": "Invalid payload type."}
        
    try:
        t_exec_start = time.time()
        # Load and resize image to avoid decompression bomb on large images
        img = Image.open(io.BytesIO(img_bytes))  # Lazy decode
        img = ImageOps.fit(img, SIZE, method=Image.Resampling.LANCZOS)  # Force to 224x224
        
        # Convert to array for model processing
        input_x = keras_image.img_to_array(img)
        input_x = np.expand_dims(input_x, axis=0)
        
        # Normalize the image (important for ResNet models)
        input_x = input_x / 255.0

        # Prediction
        preds = model.predict(input_x, verbose=0)  # Set verbose=0 to reduce output
        
        print(f"preds.tolist(): {preds.tolist()}")
        
        # Assuming binary classification (0: safe, 1: adult)
        # You may need to adjust this based on your model's output format
        prediction = preds[0][0] if len(preds.shape) > 1 else preds[0]
        is_adult = bool(prediction > 0.5)  # Threshold at 0.5
        
        t_exec_end = time.time()
        exec_ms = (t_exec_end - t_exec_start) * 1000.0
        out = {
            "status": "success",
            "predictions": preds.tolist(),
            "is_adult": is_adult,
            "confidence": float(prediction),
            "message": "Done processing request",
            "exec_ms": exec_ms,
            "server_timestamps": {
                "exec_start": t_exec_start,
                "exec_end": t_exec_end,
                "sent": time.time(),
            }
        }
        # Store output to both local and remote storage
        try:
            data_id = store_output("recognizer__adult", out,
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
        
    except Exception as e:
        print(f"Error in recognizer__adult: {str(e)}")
        return {
            "status": "error", 
            "message": f"Error processing image: {str(e)}"
        }

# Create FastAPI app instance
app = FastAPI(title="Recognizer Adult Service", version="1.0.0")

@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "recognizer__adult"}

@app.post("/invoke")
async def invoke_function(payload: Dict):
    """Invoke the adult recognition function"""
    try:
        result = run(payload)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Function execution failed: {str(e)}"}
        )
