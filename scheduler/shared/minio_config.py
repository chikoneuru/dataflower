#!/usr/bin/env python3
"""
Centralized MinIO configuration for DataFlower.
This file provides consistent MinIO connection settings across all components.
"""

import socket
import os

def get_minio_config():
    """
    Get MinIO configuration based on the current environment.
    Automatically detects MinIO IP address from Docker network.
    
    Returns:
        dict: MinIO configuration with host, port, credentials, and bucket info
    """
    
    # Default MinIO settings
    config = {
        'port': 9000,
        'access_key': 'dataflower',
        'secret_key': 'dataflower123',
        'bucket_name': 'dataflower-storage'
    }
    
    # Determine the appropriate MinIO host based on environment
    try:
        # Check if we're running inside a Docker container
        if os.path.exists('/.dockerenv'):
            # We're inside a Docker container - use Docker network IP
            config['host'] = '172.26.0.20'  # MinIO's IP in docker_dataflower-shared network
            config['context'] = 'docker_container'
        else:
            # We're running on the host - try to get actual MinIO IP from Docker
            minio_ip = _get_minio_ip_from_docker()
            if minio_ip:
                config['host'] = minio_ip
                config['context'] = 'host_docker_ip'
            else:
                # Fallback to localhost if we can't detect MinIO IP
                config['host'] = 'localhost'
                config['context'] = 'host_localhost'
            
    except Exception:
        # Fallback to localhost if detection fails
        config['host'] = 'localhost'
        config['context'] = 'fallback'
    
    return config

def _get_minio_ip_from_docker():
    """
    Get MinIO IP address from Docker network inspection.
    
    Returns:
        str: MinIO IP address or None if not found
    """
    try:
        import subprocess
        import json
        
        # Get MinIO container IP from docker network inspect
        result = subprocess.run([
            'docker', 'network', 'inspect', 'docker_dataflower-shared',
            '--format', '{{json .Containers}}'
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            containers = json.loads(result.stdout)
            
            # Look for MinIO container
            for container_id, container_info in containers.items():
                if container_info.get('Name') == 'dataflower_minio':
                    ip_address = container_info.get('IPv4Address', '')
                    # Extract IP from CIDR format (e.g., "172.26.0.20/24")
                    if '/' in ip_address:
                        ip_address = ip_address.split('/')[0]
                    return ip_address
                    
    except Exception as e:
        print(f"Warning: Could not detect MinIO IP from Docker: {e}")
    
    return None

def get_minio_endpoint_url():
    """Get the complete MinIO endpoint URL."""
    config = get_minio_config()
    return f"http://{config['host']}:{config['port']}"

def get_minio_storage_config():
    """Get MinIO storage configuration for use in storage contexts."""
    config = get_minio_config()
    return {
        'minio_host': config['host'],
        'minio_port': config['port'],
        'access_key': config['access_key'],
        'secret_key': config['secret_key'],
        'bucket_name': config['bucket_name']
    }

# For backward compatibility and easy imports
MINIO_CONFIG = get_minio_config()
MINIO_ENDPOINT_URL = get_minio_endpoint_url()
MINIO_STORAGE_CONFIG = get_minio_storage_config()

if __name__ == "__main__":
    # Test the configuration
    config = get_minio_config()
    print(f"MinIO Configuration:")
    print(f"  Host: {config['host']}")
    print(f"  Port: {config['port']}")
    print(f"  Context: {config['context']}")
    print(f"  Endpoint URL: {get_minio_endpoint_url()}")
    print(f"  Storage Config: {get_minio_storage_config()}")
