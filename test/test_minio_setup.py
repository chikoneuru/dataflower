#!/usr/bin/env python3
"""
Standalone script to start MinIO for testing

Usage:
    python test/test_minio_setup.py start    # Start MinIO
    python test/test_minio_setup.py stop     # Stop MinIO  
    python test/test_minio_setup.py status   # Check status
    python test/test_minio_setup.py manual   # Show manual setup instructions
"""

import sys
import os
import subprocess
import time
import requests
from pathlib import Path


def check_minio_status():
    """Check if MinIO is running"""
    try:
        response = requests.get('http://localhost:9000/minio/health/live', timeout=5)
        return response.status_code == 200
    except:
        return False


def start_minio():
    """Start MinIO using docker-compose or direct docker"""
    compose_file = Path(__file__).parent.parent / 'docker' / 'docker-compose-minio.yml'
    
    if not compose_file.exists():
        print(f"❌ Docker compose file not found: {compose_file}")
        return try_start_minio_direct()
    
    # First try docker-compose
    if try_start_with_docker_compose(compose_file):
        return True
    
    # Fallback to direct docker commands
    print("📦 Trying direct Docker commands as fallback...")
    return try_start_minio_direct()


def try_start_with_docker_compose(compose_file):
    """Try to start MinIO using docker-compose"""
    try:
        # Check if docker-compose works
        print("🔍 Checking docker-compose...")
        result = subprocess.run(['docker-compose', '--version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            print("⚠️  docker-compose not working properly")
            return False
        
        # Start MinIO
        print("🚀 Starting MinIO with docker-compose...")
        result = subprocess.run([
            'docker-compose', '-f', str(compose_file), 'up', '-d'
        ], capture_output=True, text=True, timeout=60)
        
        if result.returncode != 0:
            print(f"⚠️  docker-compose failed: {result.stderr}")
            return False
        
        return wait_for_minio_ready()
        
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        print(f"⚠️  docker-compose issue: {e}")
        return False
    except Exception as e:
        print(f"⚠️  docker-compose error: {e}")
        return False


def try_start_minio_direct():
    """Try to start MinIO using direct docker commands"""
    try:
        # Check if docker works
        print("🔍 Checking Docker...")
        result = subprocess.run(['docker', '--version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            print("❌ Docker not available")
            return False
        
        # Stop existing MinIO container if it exists
        print("🔄 Cleaning up existing containers...")
        subprocess.run(['docker', 'stop', 'dataflower_minio'], 
                      capture_output=True, timeout=10)
        subprocess.run(['docker', 'rm', 'dataflower_minio'], 
                      capture_output=True, timeout=10)
        
        # Pull MinIO image first (this might take time)
        print("📥 Pulling MinIO image (this may take a few minutes on first run)...")
        pull_result = subprocess.run(['docker', 'pull', 'minio/minio:latest'], 
                                   capture_output=True, text=True, timeout=300)  # 5 minutes
        
        if pull_result.returncode != 0:
            print(f"⚠️  Image pull had issues: {pull_result.stderr}")
            print("   Continuing anyway - image might already exist...")
        else:
            print("✅ MinIO image ready")
        
        # Start MinIO container (should be faster now)
        print("🚀 Starting MinIO container...")
        cmd = [
            'docker', 'run', '-d',
            '--name', 'dataflower_minio',
            '-p', '9000:9000',
            '-p', '9001:9001',
            '-e', 'MINIO_ROOT_USER=dataflower',
            '-e', 'MINIO_ROOT_PASSWORD=dataflower123',
            'minio/minio:latest',
            'server', '/data', '--console-address', ':9001'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode != 0:
            print(f"❌ Failed to start MinIO container: {result.stderr}")
            return False
        
        print(f"✅ Container started: {result.stdout.strip()}")
        
        # Create bucket using docker exec
        if wait_for_minio_ready():
            create_test_bucket()
            return True
        
        return False
        
    except subprocess.TimeoutExpired as e:
        print(f"⏱️  Docker operation timed out: {e}")
        print("   This might happen on slow internet or first-time image download")
        print("\n💡 Try manually:")
        print("   docker pull minio/minio:latest")
        print("   docker run -d --name dataflower_minio \\")
        print("     -p 9000:9000 -p 9001:9001 \\")
        print("     -e MINIO_ROOT_USER=dataflower \\")
        print("     -e MINIO_ROOT_PASSWORD=dataflower123 \\")
        print("     minio/minio:latest server /data --console-address :9001")
        return False
    except Exception as e:
        print(f"❌ Docker error: {e}")
        print("\n💡 Manual setup instructions:")
        print("   docker run -d --name dataflower_minio \\")
        print("     -p 9000:9000 -p 9001:9001 \\")
        print("     -e MINIO_ROOT_USER=dataflower \\")
        print("     -e MINIO_ROOT_PASSWORD=dataflower123 \\")
        print("     minio/minio:latest server /data --console-address :9001")
        return False


def wait_for_minio_ready():
    """Wait for MinIO to be ready"""
    print("⏳ Waiting for MinIO to be ready...")
    for i in range(30):  # Wait up to 60 seconds
        if check_minio_status():
            print("✅ MinIO is ready!")
            print("🌐 MinIO Console: http://localhost:9001")
            print("🔑 Username: dataflower")
            print("🔑 Password: dataflower123")
            return True
        print(f"   Attempt {i+1}/30...")
        time.sleep(2)
    
    print("❌ MinIO did not start within 60 seconds")
    return False


def create_test_bucket():
    """Create test bucket using MinIO client or direct API"""
    try:
        # Try using docker exec with mc (MinIO client)
        print("🪣 Creating test bucket...")
        
        # First, try to install mc in the container
        subprocess.run([
            'docker', 'exec', 'dataflower_minio', 'sh', '-c',
            'curl -o /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x /usr/local/bin/mc'
        ], capture_output=True, timeout=30)
        
        # Configure mc and create bucket
        subprocess.run([
            'docker', 'exec', 'dataflower_minio', 'sh', '-c',
            '/usr/local/bin/mc alias set myminio http://localhost:9000 dataflower dataflower123 && ' +
            '/usr/local/bin/mc mb myminio/dataflower-storage --ignore-existing && ' +
            '/usr/local/bin/mc mb myminio/test-bucket --ignore-existing'
        ], capture_output=True, timeout=20)
        
        print("✅ Test buckets created")
        
    except Exception as e:
        print(f"⚠️  Could not create buckets automatically: {e}")
        print("   Buckets will be created automatically when first used")


def stop_minio():
    """Stop MinIO using docker-compose or direct docker"""
    compose_file = Path(__file__).parent.parent / 'docker' / 'docker-compose-minio.yml'
    
    # First try docker-compose
    if try_stop_with_docker_compose(compose_file):
        return True
    
    # Fallback to direct docker commands
    return try_stop_minio_direct()


def try_stop_with_docker_compose(compose_file):
    """Try to stop MinIO using docker-compose"""
    try:
        print("🛑 Stopping MinIO with docker-compose...")
        result = subprocess.run([
            'docker-compose', '-f', str(compose_file), 'down'
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ MinIO stopped successfully")
            return True
        else:
            print(f"⚠️  docker-compose stop failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"⚠️  docker-compose stop error: {e}")
        return False


def try_stop_minio_direct():
    """Try to stop MinIO using direct docker commands"""
    try:
        print("🛑 Stopping MinIO container...")
        
        # Stop the container
        result = subprocess.run(['docker', 'stop', 'dataflower_minio'], 
                              capture_output=True, text=True, timeout=20)
        
        # Remove the container
        subprocess.run(['docker', 'rm', 'dataflower_minio'], 
                      capture_output=True, text=True, timeout=10)
        
        print("✅ MinIO stopped successfully")
        return True
        
    except Exception as e:
        print(f"⚠️  Could not stop MinIO container: {e}")
        print("💡 Manual stop: docker stop dataflower_minio && docker rm dataflower_minio")
        return False


def show_manual_instructions():
    """Show manual MinIO setup instructions"""
    print("📖 Manual MinIO Setup Instructions")
    print("="*50)
    print()
    print("🔧 Option 1: Direct Docker Command")
    print("   docker run -d --name dataflower_minio \\")
    print("     -p 9000:9000 -p 9001:9001 \\")
    print("     -e MINIO_ROOT_USER=dataflower \\")
    print("     -e MINIO_ROOT_PASSWORD=dataflower123 \\")
    print("     minio/minio:latest server /data --console-address :9001")
    print()
    print("🔧 Option 2: Docker Compose (if working)")
    print("   docker-compose -f docker/docker-compose-minio.yml up -d")
    print()
    print("🔧 Option 3: Local MinIO Binary")
    print("   1. Download: https://min.io/download")
    print("   2. Set environment:")
    print("      export MINIO_ROOT_USER=dataflower")
    print("      export MINIO_ROOT_PASSWORD=dataflower123") 
    print("   3. Run: ./minio server /tmp/minio-data --console-address :9001")
    print()
    print("🌐 Access MinIO Console: http://localhost:9001")
    print("🔑 Username: dataflower")
    print("🔑 Password: dataflower123")
    print()
    print("🧪 After starting, test with:")
    print("   python test/test_minio_setup.py status")


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == 'start':
        success = start_minio()
        sys.exit(0 if success else 1)
    elif command == 'stop':
        success = stop_minio()
        sys.exit(0 if success else 1)
    elif command == 'status':
        if check_minio_status():
            print("✅ MinIO is running")
            sys.exit(0)
        else:
            print("❌ MinIO is not running")
            sys.exit(1)
    elif command == 'manual':
        show_manual_instructions()
        sys.exit(0)
    else:
        print(f"❌ Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == '__main__':
    main()
