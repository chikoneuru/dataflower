import io
import os
import numpy as np
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image
from typing import Union, Dict

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
        payload: The input image as raw bytes.

    Returns:
        A dictionary containing the analysis results, including
        an 'illegal' flag and a confidence score.
    """
    # --- Input Validation ---
    global model
    if model is None:
        return {"status": "error", "message": "Violence detection model is not available."}

    if not isinstance(payload, bytes):
        return {"status": "error", "message": "Invalid payload: input must be image bytes."}
    
    # --- Image Processing and Prediction ---
    try:
        # Load image directly from the in-memory byte payload
        img = image.load_img(io.BytesIO(payload), target_size=SIZE)
        input_x = image.img_to_array(img)
        input_x = np.expand_dims(input_x, axis=0)
        
        # The model expects pixel values to be in a certain range.
        # If your original code didn't normalize, we omit it. 
        # If it did, we should add: input_x = input_x / 255.0

        # Run prediction
        preds = model.predict(input_x, verbose=0)
        
        # --- Result Formatting ---
        # Extract the confidence score from the prediction result
        confidence = float(preds[0][0])
        
        # Determine if the content is illegal based on a threshold
        is_illegal = bool(confidence > 0.95)
        
        return {
            "status": "success",
            "illegal": is_illegal,
            "confidence": confidence
        }
        
    except Exception as e:
        print(f"Error during violence detection: {e}")
        return {
            "status": "error", 
            "message": f"An error occurred while processing the image: {str(e)}"
        }