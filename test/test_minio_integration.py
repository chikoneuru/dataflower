"""
MinIO Integration Test - Data Locality with Large Files

This is an interactive demo (not a traditional unit test) that showcases:
- MinIO storage for large files (images, videos, text, npy arrays)
- Data locality management with caching
- Cross-node data sharing scenarios

Prerequisites:
1. Start MinIO: ./scripts/setup-minio.sh
2. Run test: python -m test.test_minio_integration

The demo will:
1. Store various data types in MinIO
2. Demonstrate retrieval with caching
3. Show workflow-style data processing
4. Display storage statistics
"""

import numpy as np
import time
from pathlib import Path

from provider.data_locality_config import DataLocalityPresets
from provider.remote_storage_adapter import create_storage_adapter
from provider.data_manager import DataManager, DataMetadata


def demo_large_files_with_minio():
    """Demonstrate MinIO storage for different large file types"""
    print("=== MinIO Large File Storage Demo ===")
    
    # 1. Setup MinIO configuration
    config = DataLocalityPresets.development("node1")

    # Override MinIO host for local testing (from host machine)
    # Container uses "minio", host machine uses "localhost"
    if config.remote_storage.minio_host == "minio":
        config.remote_storage.minio_host = "localhost"
        print("🔧 Adjusted MinIO host to 'localhost' for local testing")

    print(f"Using storage type: {config.remote_storage.type}")
    print(f"MinIO endpoint: http://{config.remote_storage.minio_host}:{config.remote_storage.minio_port}")

    try:
        # 2. Initialize MinIO storage
        storage = create_storage_adapter(
            config.remote_storage.type,
            config.remote_storage.__dict__
        )
        
        locality_manager = DataManager({
            'node_id': 'node1',
            'cache_dir': '/tmp/dataflower_minio_test'
        })
        locality_manager.set_remote_storage_adapter(storage)
        
        print("✅ MinIO storage initialized successfully")
        
        # 3. Test different data types
        test_cases = [
            ("image_data", create_mock_image(), "Binary image data"),
            ("numpy_array", create_mock_numpy_array(), "NumPy array"),
            ("text_data", create_mock_text_data(), "Text content"),
            ("json_data", create_mock_json_data(), "JSON data"),
            ("video_chunk", create_mock_video_chunk(), "Video chunk")
        ]
        
        print("\n📁 Testing different data types:")
        
        for data_name, data, description in test_cases:
            print(f"\n  Testing {data_name} ({description})")
            
            # Generate data ID
            data_id = locality_manager.generate_data_id("test_function", data_name)
            
            # Create metadata
            metadata = DataMetadata(
                data_id=data_id,
                node_id="node1",
                function_name="test_function",
                step_name=data_name,
                size_bytes=get_data_size(data),
                created_time=time.time(),
                last_accessed=time.time(),
                access_count=0,
                data_type="intermediate",
                dependencies=[],
                cache_priority=3
            )
            
            # Store data
            print(f"    📤 Storing {data_name} ({get_data_size(data)} bytes)")
            success = locality_manager.store_data(data_id, data, metadata)
            
            if success:
                print(f"    ✅ Stored successfully")
                
                # Retrieve data
                print(f"    📥 Retrieving {data_name}")
                from .data_manager import DataAccessRequest
                request = DataAccessRequest(
                    data_id=data_id,
                    requesting_function="test_retrieval",
                    requesting_node="node1",
                    access_type="read"
                )
                
                retrieved_data, location = locality_manager.get_data(request)
                
                if retrieved_data is not None:
                    print(f"    ✅ Retrieved successfully from {location}")
                    
                    # Verify data integrity
                    if verify_data_integrity(data, retrieved_data, data_name):
                        print(f"    ✅ Data integrity verified")
                    else:
                        print(f"    ❌ Data integrity check failed")
                else:
                    print(f"    ❌ Failed to retrieve data")
            else:
                print(f"    ❌ Failed to store data")
        
        # 4. Show storage statistics
        print(f"\n📊 Storage Statistics:")
        stats = locality_manager.get_cache_stats()
        print(f"  Cache size: {stats.get('cache_size', 0)} items")
        print(f"  Hit rate: {stats.get('hit_rate', 0):.2%}")
        print(f"  Remote fetches: {stats.get('remote_fetches', 0)}")
        
        print("\n✅ MinIO demo completed successfully!")
        
    except Exception as e:
        print(f"❌ MinIO demo failed: {e}")
        print("Make sure MinIO is running with: docker-compose -f docker-compose-minio.yml up -d")


def create_mock_image():
    """Create mock image data (bytes)"""
    # Simulate a small image file
    return b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR' + b'\x00' * 1000


def create_mock_numpy_array():
    """Create mock NumPy array"""
    return np.random.random((100, 100, 3)).astype(np.float32)


def create_mock_text_data():
    """Create mock text data"""
    return """
    This is a sample text file content that could represent:
    - Log files
    - Configuration files
    - Processing results
    - Intermediate text outputs
    """ * 50  # Make it reasonably large


def create_mock_json_data():
    """Create mock JSON data"""
    return {
        "workflow_id": "test_workflow_001",
        "processing_results": [
            {"step": "preprocessing", "duration": 1.23, "status": "completed"},
            {"step": "feature_extraction", "duration": 2.45, "status": "completed"},
            {"step": "classification", "duration": 0.87, "status": "completed"}
        ],
        "metadata": {
            "input_size": [1920, 1080],
            "processing_node": "node1",
            "timestamp": "2024-01-01T12:00:00Z"
        },
        "large_array": list(range(1000))  # Simulate some bulk data
    }


def create_mock_video_chunk():
    """Create mock video chunk data"""
    # Simulate video chunk (could be from ffmpeg processing)
    return b'ftypisom\x00\x00\x02\x00isomiso2mp41' + b'\x00' * 5000


def get_data_size(data):
    """Get size of data in bytes"""
    if isinstance(data, bytes):
        return len(data)
    elif isinstance(data, str):
        return len(data.encode('utf-8'))
    elif isinstance(data, np.ndarray):
        return data.nbytes
    else:
        # Estimate for other types
        import sys
        return sys.getsizeof(data)


def verify_data_integrity(original, retrieved, data_type):
    """Verify that retrieved data matches original"""
    if data_type == "numpy_array":
        return np.array_equal(original, retrieved)
    elif isinstance(original, bytes):
        return original == retrieved
    elif isinstance(original, str):
        return original == retrieved
    elif isinstance(original, dict):
        return original == retrieved
    else:
        return str(original) == str(retrieved)


def demo_workflow_with_large_files():
    """Demonstrate workflow processing with large files"""
    print("\n=== Workflow with Large Files Demo ===")
    
    # Simulate an image processing workflow
    workflow_steps = [
        ("load_image", create_mock_image()),
        ("extract_features", create_mock_numpy_array()),
        ("process_results", create_mock_json_data()),
        ("save_output", create_mock_text_data())
    ]
    
    # Use development configuration for MinIO testing
    config = DataLocalityPresets.development("node1")

    # Override MinIO host for local testing (from host machine)
    if config.remote_storage.minio_host == "minio":
        config.remote_storage.minio_host = "localhost"
        print("🔧 Adjusted MinIO host to 'localhost' for workflow demo")

    try:
        storage = create_storage_adapter(
            config.remote_storage.type,
            config.remote_storage.__dict__
        )
        
        locality_manager = DataManager({
            'node_id': 'node1',
            'cache_dir': '/tmp/dataflower_workflow_test'
        })
        locality_manager.set_remote_storage_adapter(storage)
        
        print("📋 Simulating image processing workflow:")
        
        stored_data_ids = []
        
        for i, (step_name, data) in enumerate(workflow_steps):
            print(f"\n  Step {i+1}: {step_name}")
            
            data_id = locality_manager.generate_data_id("image_processing", step_name)
            
            metadata = DataMetadata(
                data_id=data_id,
                node_id="node1",
                function_name="image_processing",
                step_name=step_name,
                size_bytes=get_data_size(data),
                created_time=time.time(),
                last_accessed=time.time(),
                access_count=0,
                data_type="intermediate",
                dependencies=stored_data_ids.copy(),  # Depend on previous steps
                cache_priority=4  # High priority for workflow data
            )
            
            print(f"    📤 Storing {step_name} output ({get_data_size(data)} bytes)")
            success = locality_manager.store_data(data_id, data, metadata)
            
            if success:
                stored_data_ids.append(data_id)
                print(f"    ✅ Step {step_name} completed")
            else:
                print(f"    ❌ Step {step_name} failed")
                break
        
        print(f"\n✅ Workflow completed! Stored {len(stored_data_ids)} intermediate results")
        
    except Exception as e:
        print(f"❌ Workflow demo failed: {e}")


if __name__ == "__main__":
    print("🚀 MinIO Data Locality Examples")
    print("=" * 50)
    
    try:
        demo_large_files_with_minio()
        demo_workflow_with_large_files()
        
        print("\n" + "=" * 50)
        print("✅ All MinIO demos completed!")
        print("\n📝 Next steps:")
        print("1. Start MinIO: docker-compose -f docker-compose-minio.yml up -d")
        print("2. Access MinIO Console: http://localhost:9001 (dataflower/dataflower123)")
        print("3. Use MinIO for your large file workflows!")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
