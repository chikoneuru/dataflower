# functions/recognizer/recognizer_censor/main.py
# -*- coding: utf-8 -*-
import os
import time
from typing import Dict

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from input_retrieval_service import retrieve_input


class DFAFilter:
    def __init__(self):
        self.keyword_chains = {}
        self.delimit = '\x00'

    def add(self, keyword: str):
        if not isinstance(keyword, str):
            keyword = keyword.decode('utf-8')
        keyword = keyword.lower().strip()
        if not keyword:
            return
        level = self.keyword_chains
        for i, ch in enumerate(keyword):
            if ch in level:
                level = level[ch]
            else:
                if not isinstance(level, dict):
                    break
                last_level = None
                last_char = None
                for j in range(i, len(keyword)):
                    level[keyword[j]] = {}
                    last_level, last_char = level, keyword[j]
                    level = level[keyword[j]]
                last_level[last_char] = {self.delimit: 0}
                break
        if i == len(keyword) - 1:
            level[self.delimit] = 0

    def parse(self, path: str):
        if not os.path.exists(path):
            return
        with open(path, 'r', encoding='utf-8') as f:
            for keyword in f:
                self.add(keyword.strip())

    def filter(self, message: str, repl: str = "*"):
        if not isinstance(message, str):
            message = message.decode('utf-8')
        message = message.lower()
        ret = []
        replaced = 0
        start = 0
        while start < len(message):
            level = self.keyword_chains
            step_ins = 0
            for ch in message[start:]:
                if ch in level:
                    step_ins += 1
                    if self.delimit not in level[ch]:
                        level = level[ch]
                    else:
                        ret.append(repl * step_ins)
                        replaced += 1
                        start += step_ins - 1
                        break
                else:
                    ret.append(message[start])
                    break
            else:
                ret.append(message[start])
            start += 1
        return ''.join(ret), replaced


# Initialize DFA filter with local keywords file
_BASE_DIR = os.path.dirname(__file__)
_KEYWORDS_PATH = os.path.join(_BASE_DIR, "spooky_keywords")
_gfw = DFAFilter()
_gfw.parse(_KEYWORDS_PATH)


def run(payload: dict) -> dict:
    t_exec_start = time.time()
    
    # Handle input_ref for shared volume/MinIO retrieval
    if isinstance(payload, dict) and "input_ref" in payload and isinstance(payload["input_ref"], dict):
        try:
            retrieved_data = retrieve_input(payload["input_ref"], "recognizer__censor")  # I/O inside container
            if isinstance(retrieved_data, dict) and "text" in retrieved_data:
                text = retrieved_data["text"]
            elif isinstance(retrieved_data, bytes):
                text = retrieved_data.decode('utf-8', errors='ignore')
            else:
                text = ""
        except Exception as e:
            return {"status": "error", "message": f"Failed to retrieve input: {str(e)}"}
    elif isinstance(payload, dict):
        text = payload.get("text") or payload.get("translated_text")
    elif isinstance(payload, bytes):
        # Handle raw bytes as text
        text = payload.decode('utf-8', errors='ignore')
    else:
        return {
            "status": "error",
            "message": "Invalid payload: expected dict with 'text' or 'input_ref', or raw bytes.",
            "illegal": False,
            "filtered_text": "",
            "filter_count": 0,
        }
    """
    Inputs (any one):
      - text: str            # preferred
      - translated_text: str # backward-compat
    Outputs:
      - status: "success" | "error"
      - illegal: bool
      - filtered_text: str
      - filter_count: int
    """
    # Remove duplicate t_exec_start since it's already set at the beginning of the function
    text = payload.get("text") or payload.get("translated_text") or ""
    if not isinstance(text, str):
        return {"status": "error", "message": "text must be a string"}

    filtered, count = _gfw.filter(text, "*")
    illegal = count >= 1

    t_exec_end = time.time()
    exec_ms = (t_exec_end - t_exec_start) * 1000.0
    out = {
        "status": "success",
        "message": "Censor successful",
        "illegal": illegal,
        "filtered_text": filtered,
        "filter_count": count,
        "exec_ms": exec_ms,
        "server_timestamps": {"exec_start": t_exec_start, "exec_end": t_exec_end, "sent": time.time()}
    }
    return out

# Create FastAPI app instance
app = FastAPI(title="Recognizer Censor Service", version="1.0.0")

@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "recognizer__censor"}

@app.post("/invoke")
async def invoke_function(payload: Dict):
    """Invoke the censor function"""
    try:
        result = run(payload)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Function execution failed: {str(e)}"}
        )