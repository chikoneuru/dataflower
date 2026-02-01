import os
import time

import requests
from fastapi import FastAPI, File, HTTPException, Request, UploadFile

from provider.container_manager import ContainerManager
from scheduler.ours.metrics import Metrics
from scheduler.ours.scheduler import Scheduler
from scheduler.ours.cost_models import CostModels
from scheduler.ours.mode_classifier import ModeClassifier

app = FastAPI()

# Initialize required components
cost_models = CostModels()
mode_classifier = ModeClassifier(threshold=1.0)  # Default threshold
cm = ContainerManager()
scheduler = Scheduler(cost_models, mode_classifier)
metrics = Metrics()

def get_host_port(container, internal_port="5000/tcp"):
    """Retrieve the host port mapped to the container's internal port."""
    container.reload()
    ports = container.attrs["NetworkSettings"]["Ports"]
    if internal_port in ports and ports[internal_port]:
        return ports[internal_port][0]["HostPort"]
    return None

@app.post("/user/invoke/{fn}")
async def invoke(fn: str, request: Request, file: UploadFile = File(None)):
    print(f"[Gateway] Invoking function: {fn}")

    containers = cm.containers.get(fn, [])
    container = scheduler.select_container(containers)

    if not container:
        print(f"[Gateway] Cold start for '{fn}'...")
        cm.add_container(fn)
        containers = cm.containers.get(fn, [])
        container = scheduler.select_container(containers)
        if not container:
            raise HTTPException(status_code=500, detail=f"Failed to start container for '{fn}'")
        time.sleep(2)

    start = time.time()
    print(f"[Gateway] Assigned {fn} → {container.short_id}")

    # try:
    port = get_host_port(container)
    if not port:
        raise RuntimeError("No port mapping found for container")

    worker_url = f"http://localhost:{port}/run?fn={fn}"
    print(f"[Gateway] Forwarding request to worker at {worker_url}")
    print(f"[Gateway] Request file: {file.filename if file else 'None'}")
    if file is not None:
        file_content = await file.read()
        print(f"[Gateway] File content size: {len(file_content)} bytes")
        files = {"img": (file.filename, file_content, file.content_type or "application/octet-stream")}
        resp = requests.post(worker_url, files=files, timeout=30)
    else:
        json_payload = await request.json()
        print(f"[Gateway] JSON payload: {json_payload}")
        resp = requests.post(worker_url, json=json_payload, timeout=30)
    print(f"[Gateway] Response status: {resp.status_code}")
    print(f"[Gateway] Response text: {resp.text}")
    
    # Check if the response was successful
    if resp.status_code != 200:
        print(f"[Gateway] Worker returned error status: {resp.status_code}")
        # Try to parse error response as JSON, fallback to text
        try:
            error_data = resp.json()
            error_message = error_data.get('detail', resp.text)
        except:
            error_message = resp.text
        
        raise HTTPException(
            status_code=resp.status_code, 
            detail=f"Worker error: {error_message}"
        )
    
    # Parse successful response
    try:
        result = resp.json()['result']
    except Exception as e:
        print(f"[Gateway] Error parsing worker response: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Error parsing worker response: {e}"
        )
    end = time.time()
    metric = metrics.get_summary()
    print(f"[Gateway] Response from worker: {result}")
    print(f"[Gateway] Metrics: {metric}")

    return {"status": f"{result['status']}", "container": container.short_id, "result": result}

    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=f"Error invoking worker: {e}")

@app.post("/admin/add_container/{fn}")
def add_container(fn: str):
    cid = cm.add_container(fn)
    return {"status": f"Container {cid} started for '{fn}'"}

@app.post("/admin/remove_container/{fn}")
def remove_container(fn: str):
    cid = cm.remove_container(fn)
    return {"status": f"Container {cid} stopped for '{fn}'"}

@app.get("/admin/status")
def status():
    return {"containers": cm.list_containers()}

@app.post("/admin/start_redis")
def start_redis():
    cid = cm.start_redis()
    if cid:
        return {"status": f"Redis container started with id {cid}"}
    else:
        return {"status": "Failed to start Redis container"}

@app.post("/admin/stop_redis")
def stop_redis():
    success = cm.stop_redis()
    if success:
        return {"status": "Redis container stopped"}
    else:
        return {"status": "Redis container was not running or failed to stop"}
