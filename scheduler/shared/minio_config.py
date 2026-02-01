#!/usr/bin/env python3
"""
Centralized MinIO configuration for DataFlower.
Works for:
- Containers inside Docker
- Host machine
- Host machine over Tailscale
"""

import os

def get_minio_config():
    """
    Determine correct MinIO host based on environment.
    """
    access_key = "dataflower"
    secret_key = "dataflower123"
    bucket = "dataflower-storage"

    # Inside Docker → use container DNS name + internal port
    if os.path.exists("/.dockerenv"):
        return {
            "host": "minio",        # IMPORTANT: use the docker-compose service name
            "port": 9000,
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket_name": bucket,
            "context": "docker_container",
        }

    # Host but using Tailscale IP
    tailscale_ip = os.getenv("MINIO_TAILSCALE_IP")
    if tailscale_ip:
        return {
            "host": tailscale_ip,
            "port": 19000,          # host → container port mapping
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket_name": bucket,
            "context": "host_tailscale_ip",
        }

    # Host machine (normal localhost)
    return {
        "host": "localhost",
        "port": 19000,              # host → container port mapping
        "access_key": access_key,
        "secret_key": secret_key,
        "bucket_name": bucket,
        "context": "host_local",
    }


def get_minio_endpoint_url():
    c = get_minio_config()
    return f"http://{c['host']}:{c['port']}"


def get_minio_storage_config():
    c = get_minio_config()
    return {
        "minio_host": c["host"],
        "minio_port": c["port"],
        "access_key": c["access_key"],
        "secret_key": c["secret_key"],
        "bucket_name": c["bucket_name"],
    }


MINIO_CONFIG = get_minio_config()
MINIO_ENDPOINT_URL = get_minio_endpoint_url()
MINIO_STORAGE_CONFIG = get_minio_storage_config()

if __name__ == "__main__":
    config = get_minio_config()
    print("MinIO Configuration:")
    print(f"  Host: {config['host']}")
    print(f"  Port: {config['port']}")
    print(f"  Context: {config['context']}")
    print(f"  Endpoint URL: {get_minio_endpoint_url()}")
    print(f"  Storage Config: {get_minio_storage_config()}")