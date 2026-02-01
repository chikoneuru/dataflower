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

# --- Configuration ---
SIZE = (224, 224)
# Build a reliable, absolute path to the model file.
# This works correctly regardless of where the script is called from.
MODEL_PATH = os.path.join(os.path.dirname(__file__), 'resnet50_final_violence.h5')

# --- Model Loading ---
# Pre-load the model when the module is first imported for better performance.
# This also allows us to catch loading errors early.
model = None
if os.path.exists(MODEL_PATH):
    try:
        model = load_model(MODEL_PATH, compile=False)
        print("Violence detection model loaded successfully.")
    except Exception as e:
        print(f"CRITICAL: Error loading violence detection model from {MODEL_PATH}: {e}")
else:
    print(f"CRITICAL: Violence detection model file not found at {MODEL_PATH}")

def run(payload: Union[bytes, Dict]) -> Dict:
    """
    Analyzes an image to detect violent content.

    Args:
        payload: The input image as raw bytes or dict with img_b64.

    Returns:
        A dictionary containing the analysis results, including
        an 'illegal' flag and a confidence score.
    """
    # --- Input Validation ---
    global model
    if model is None:
        return {"status": "error", "message": "Violence detection model is not available."}

    # Handle different payload formats
    img_bytes = None
    if isinstance(payload, bytes):
        img_bytes = payload
    elif isinstance(payload, dict):
        if "input_ref" in payload and isinstance(payload["input_ref"], dict):
            try:
                retrieved_data = retrieve_input(payload["input_ref"], "recognizer__violence")  # I/O inside container
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
    
    # --- Image Processing and Prediction ---
    try:
        t_exec_start = time.time()
        # Load image directly from the in-memory byte payload
        print(f"Payload type: {type(img_bytes)}, length: {len(img_bytes) if img_bytes else 0}")
        print(f"First 100 bytes: {img_bytes[:100] if img_bytes else None}")

        # Load image and resize if too large to prevent decompression bomb
        img = Image.open(io.BytesIO(img_bytes))
        
        # If image is larger than 3000x3000, resize it down first to reduce memory usage
        if max(img.size) > 3000:
            scale = 3000.0 / max(img.size)
            new_size = (int(img.size[0] * scale), int(img.size[1] * scale))
            img = img.resize(new_size, Image.Resampling.LANCZOS)
        
        # Final resize to model input size
        img = img.resize(SIZE, Image.Resampling.LANCZOS)
        
        # Convert to array for model processing
        input_x = keras_image.img_to_array(img)
        input_x = np.expand_dims(input_x, axis=0)
        
        # The model expects pixel values to be in a certain range.
        # If your original code didn't normalize, we omit it. 
        # If it did, we should add: input_x = input_x / 255.0

        # Run prediction with reduced retracing
        # Force static shapes to prevent TensorFlow retracing
        input_x = np.array(input_x, dtype=np.float32)
        preds = model.predict(input_x, verbose=0, batch_size=1)
        
        # --- Result Formatting ---
        # Extract the confidence score from the prediction result
        confidence = float(preds[0][0])
        
        # Determine if the content is illegal based on a threshold
        is_illegal = bool(confidence > 0.95)
        
        t_exec_end = time.time()
        exec_ms = (t_exec_end - t_exec_start) * 1000.0
        out = {
            "status": "success",
            "message": "Violence detection successful",
            "illegal": is_illegal,
            "confidence": confidence,
            "exec_ms": exec_ms,
            "server_timestamps": {"exec_start": t_exec_start, "exec_end": t_exec_end, "sent": time.time()}
        }
        # Store output to both local and remote storage
        try:
            data_id = store_output("recognizer__violence", out,
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
        print(f"Error during violence detection: {e}")
        return {
            "status": "error", 
            "message": f"An error occurred while processing the image: {str(e)}"
        }

# Create FastAPI app instance
app = FastAPI(title="Recognizer Violence Service", version="1.0.0")

@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "recognizer__violence"}

@app.post("/invoke")
async def invoke_function(payload: Dict):
    """Invoke the violence recognition function"""
    try:
        result = run(payload)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Function execution failed: {str(e)}"}
        )