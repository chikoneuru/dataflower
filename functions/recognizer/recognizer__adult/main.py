import io
import numpy as np
import os
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image
from typing import Union, Dict

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
    _payload is expected to contain image bytes.
    Run inference on the image to detect adult content.
    """
    global model
    if model is None:
        return {"status": "error", "message": f"Model file not found or failed to load from {MODEL_PATH}"}

    if not isinstance(payload, bytes):
        return {"status": "error", "message": "Invalid payload: input must be bytes."}
        
    try:
        # Load image from bytes
        img = image.load_img(io.BytesIO(payload), target_size=SIZE)
        input_x = image.img_to_array(img)
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
        
        return {
            "status": "success",
            "predictions": preds.tolist(),
            "is_adult": is_adult,
            "confidence": float(prediction),
            "message": "Done processing request"
        }
        
    except Exception as e:
        print(f"Error in recognizer__adult: {str(e)}")
        return {
            "status": "error", 
            "message": f"Error processing image: {str(e)}"
        }
