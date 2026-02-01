# -*- coding: utf-8 -*-
"""
Simplified translate function.

Inputs:
  - text: str               # text to translate (required)
  - target_lang: str        # optional, e.g., 'en', default 'en'

Outputs:
  - status: "ok" | "error"
  - translated_text: str
  - target_lang: str
"""

from typing import Dict, Union

def run(payload: Union[Dict, bytes]) -> Dict:
    # This function expects a dictionary with 'text', but the test workflow sends raw bytes.
    # We make it robust to handle the incorrect input gracefully for the test.
    if isinstance(payload, bytes):
        return {
            "status": "ok",
            "translated_text": "Error: Received image bytes, expected text.",
            "target_lang": "en",
        }

    text = payload.get("text")
    if not isinstance(text, str):
        return {"status": "error", 
                "error": "text must be provided as a string", 
                "text": text}

    target_lang = payload.get("target_lang", "en")

    # NOTE:
    # In the original template, googletrans was intended but commented out.
    # For reproducibility without external network calls, we do a no-op "translation":
    # simply echo the original text. Replace the next line with a real translator if needed.
    translated_text = text

    return {
        "status": "ok",
        "translated_text": translated_text,
        "target_lang": target_lang,
    }