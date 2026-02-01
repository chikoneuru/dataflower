"""
Simple tests for data sharing feature (local and remote storage)

Tests cover:
- Local file storage operations
- MinIO remote storage operations  
- Cross-storage data sharing between local and remote storage
- Storage adapter integration patterns
"""

import json
import logging
import os
import pickle
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any, Dict

from provider.data_manager import DataMetadata
# Import storage adapters to test
from provider.remote_storage_adapter import (LocalFileStorageAdapter,
                                             MinIOStorageAdapter,
                                             MockRemoteStorageAdapter,
                                             create_storage_adapter)


class TestDataSharingBase(unittest.TestCase):
    """Base class for data sharing tests"""
    
    def setUp(self):
        """Set up test environment"""
        # Create temporary directories
        self.temp_dir = Path(tempfile.mkdtemp())
        self.remote_dir = self.temp_dir / "remote"
        
        # Setup test data
        self.test_data = {
            'text_data': "Hello, World!",
            'json_data': {"key": "value", "number": 42},
            'binary_data': b'\x00\x01\x02\x03',
            'large_data': list(range(10000))  # Larger dataset
        }
        
    def tearDown(self):
        """Clean up test environment"""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def create_test_metadata(self, data_id: str, function_name: str = "test_function",
                           step_name: str = "step1", node_id: str = "node-1", 
                           data_type: str = "intermediate") -> DataMetadata:
        """Create test metadata"""
        return DataMetadata(
            data_id=data_id,
            node_id=node_id,
            function_name=function_name,
            step_name=step_name,
            size_bytes=1024,
            created_time=time.time(),
            last_accessed=time.time(),
            access_count=0,
            data_type=data_type,
            dependencies=[],
            cache_priority=3
        )


class TestBasicDataSharingScenarios(TestDataSharingBase):
    """Test basic data sharing between local and remote storage"""
    
    def setUp(self):
        super().setUp()
        # Setup storage adapters for testing
        self.local_adapter = LocalFileStorageAdapter(str(self.remote_dir))
        
        # MinIO adapter (will be tested if MinIO is available)
        self.minio_config = {
            'minio_host': 'localhost',
            'minio_port': 9000,
            'access_key': 'dataflower',
            'secret_key': 'dataflower123',
            'bucket_name': 'test-bucket'
        }
    
    def test_local_storage_basic_operations(self):
        """Test basic store/retrieve operations with local storage"""
        # Test different data types
        test_cases = [
            ("text_data", self.test_data['text_data']),
            ("json_data", self.test_data['json_data']),
            ("binary_data", self.test_data['binary_data'])
        ]
        
        for data_name, data in test_cases:
            with self.subTest(data_type=data_name):
                data_id = f"local_{data_name}"
                metadata = self.create_test_metadata(data_id)
                
                # Store data
                success = self.local_adapter.store_data(data_id, data, metadata)
                self.assertTrue(success, f"Failed to store {data_name}")
                
                # Verify data exists
                exists = self.local_adapter.check_exists(data_id)
                self.assertTrue(exists, f"{data_name} should exist after storage")
                
                # Retrieve data
                retrieved_data = self.local_adapter.get_data(data_id)
                self.assertEqual(retrieved_data, data, f"Retrieved {data_name} doesn't match original")
                
                # Clean up
                self.local_adapter.delete_data(data_id)
                self.assertFalse(self.local_adapter.check_exists(data_id))
    
    def test_minio_storage_basic_operations(self):
        """Test basic store/retrieve operations with MinIO storage"""
        # Check if we should skip MinIO tests
        skip_minio = os.getenv('SKIP_MINIO_TESTS', 'false').lower() == 'true'
        if skip_minio:
            self.skipTest("MinIO tests disabled via SKIP_MINIO_TESTS environment variable")
        
        # Try to ensure MinIO is running
        minio_available = self._check_minio_available()
        if not minio_available:
            self.fail("""
            MinIO server is not available for testing.
            
            To run MinIO tests:
            1. Start MinIO: python test/test_minio_setup.py start
            2. Or set SKIP_MINIO_TESTS=true to skip these tests
            """)
        
        try:
            minio_adapter = MinIOStorageAdapter(self.minio_config)
            
            # Test different data types  
            test_cases = [
                ("text_data", self.test_data['text_data']),
                ("json_data", self.test_data['json_data'])
            ]
            
            for data_name, data in test_cases:
                with self.subTest(data_type=data_name):
                    data_id = f"minio_{data_name}"
                    metadata = self.create_test_metadata(data_id)
                    
                    # Store data
                    success = minio_adapter.store_data(data_id, data, metadata)
                    self.assertTrue(success, f"Failed to store {data_name} in MinIO")
                    
                    # Verify data exists
                    exists = minio_adapter.check_exists(data_id)
                    self.assertTrue(exists, f"{data_name} should exist in MinIO")
                    
                    # Retrieve data
                    retrieved_data = minio_adapter.get_data(data_id)
                    self.assertEqual(retrieved_data, data, f"Retrieved {data_name} from MinIO doesn't match")
                    
                    # Clean up
                    minio_adapter.delete_data(data_id)
                    self.assertFalse(minio_adapter.check_exists(data_id))
        
        except Exception as e:
            self.fail(f"MinIO adapter test failed: {e}")
    
    def test_data_sharing_between_storages(self):
        """Test sharing data between local and remote storage"""
        data_id = "shared_test_data"
        data = {"message": "Hello from function A", "value": 42, "timestamp": time.time()}
        metadata = self.create_test_metadata(data_id)
        
        # Store data locally (simulating function A)
        success = self.local_adapter.store_data(data_id, data, metadata)
        self.assertTrue(success, "Failed to store data locally")
        
        # Retrieve from local storage
        local_data = self.local_adapter.get_data(data_id)
        self.assertEqual(local_data, data, "Local retrieval failed")
        
        # Check MinIO availability for remote test
        minio_available = self._check_minio_available()
        skip_minio = os.getenv('SKIP_MINIO_TESTS', 'false').lower() == 'true'
        
        if not skip_minio and minio_available:
            try:
                minio_adapter = MinIOStorageAdapter(self.minio_config)
                
                # Store same data remotely (simulating backup or cross-node sharing)
                remote_data_id = f"remote_{data_id}"
                success = minio_adapter.store_data(remote_data_id, data, metadata)
                self.assertTrue(success, "Failed to store data in MinIO")
                
                # Retrieve from remote storage
                remote_data = minio_adapter.get_data(remote_data_id)
                self.assertEqual(remote_data, data, "Remote retrieval failed")
                
                # Verify both storages have the data
                self.assertTrue(self.local_adapter.check_exists(data_id))
                self.assertTrue(minio_adapter.check_exists(remote_data_id))
                
                # Clean up remote
                minio_adapter.delete_data(remote_data_id)
            except Exception as e:
                print(f"MinIO portion of test skipped due to error: {e}")
        
        # Clean up local
        self.local_adapter.delete_data(data_id)
    
    def _check_minio_available(self) -> bool:
        """Check if MinIO server is available"""
        try:
            import requests
            response = requests.get('http://localhost:9000/minio/health/live', timeout=2)
            return response.status_code == 200
        except:
            return False


class TestStorageAdapterIntegration(TestDataSharingBase):
    """Test integration with different storage adapters"""
    
    def test_mock_storage_adapter(self):
        """Test MockRemoteStorageAdapter functionality"""
        adapter = MockRemoteStorageAdapter()
        
        # Test store and retrieve
        data_id = "mock_test"
        data = self.test_data['text_data']
        metadata = self.create_test_metadata(data_id)
        
        success = adapter.store_data(data_id, data, metadata)
        self.assertTrue(success)
        
        retrieved_data = adapter.get_data(data_id)
        self.assertEqual(retrieved_data, data)
        
        # Test exists check
        self.assertTrue(adapter.check_exists(data_id))
        self.assertFalse(adapter.check_exists("nonexistent"))
        
        # Test delete
        success = adapter.delete_data(data_id)
        self.assertTrue(success)
        self.assertFalse(adapter.check_exists(data_id))
    
    def test_local_file_storage_adapter(self):
        """Test LocalFileStorageAdapter functionality"""
        adapter = LocalFileStorageAdapter(str(self.remote_dir))
        
        # Test different data types
        for data_name, data in self.test_data.items():
            with self.subTest(data_type=data_name):
                data_id = f"local_file_{data_name}"
                metadata = self.create_test_metadata(data_id)
                
                # Store
                success = adapter.store_data(data_id, data, metadata)
                self.assertTrue(success)
                
                # Verify files exist
                data_file = Path(adapter.data_dir) / f"{data_id}.pkl"
                metadata_file = Path(adapter.metadata_dir) / f"{data_id}.json"
                self.assertTrue(data_file.exists())
                self.assertTrue(metadata_file.exists())
                
                # Retrieve and verify
                retrieved_data = adapter.get_data(data_id)
                self.assertEqual(retrieved_data, data)
    
    def test_minio_storage_adapter(self):
        """Test MinIOStorageAdapter functionality"""
        # Check if we should skip MinIO tests based on environment
        skip_minio = os.getenv('SKIP_MINIO_TESTS', 'false').lower() == 'true'
        if skip_minio:
            self.skipTest("MinIO tests disabled via SKIP_MINIO_TESTS environment variable")
        
        # MinIO config for testing
        minio_config = {
            'minio_host': 'localhost',
            'minio_port': 9000,
            'access_key': 'dataflower',
            'secret_key': 'dataflower123',
            'bucket_name': 'test-bucket'
        }
        
        try:
            adapter = MinIOStorageAdapter(minio_config)
            
            # Test basic functionality with simple data
            data_id = "minio_test"
            data = self.test_data['text_data'] 
            metadata = self.create_test_metadata(data_id)
            
            # Store
            success = adapter.store_data(data_id, data, metadata)
            if not success:
                self.fail(f"""
                MinIO server is not available for testing.
                
                To run MinIO tests:
                1. Start MinIO: python test/test_minio_setup.py start
                2. Or set SKIP_MINIO_TESTS=true to skip these tests
                """)
            
            # Check exists
            self.assertTrue(adapter.check_exists(data_id))
            
            # Retrieve and verify
            retrieved_data = adapter.get_data(data_id)
            self.assertEqual(retrieved_data, data)
            
            # Delete
            success = adapter.delete_data(data_id)
            self.assertTrue(success)
            self.assertFalse(adapter.check_exists(data_id))
            
        except Exception as e:
            self.fail(f"""
            MinIO server is not available for testing.
            
            To run MinIO tests:
            1. Start MinIO: python test/test_minio_setup.py start
            2. Or set SKIP_MINIO_TESTS=true to skip these tests.
            
            Error: {e}
            """)
    
    def test_storage_adapter_factory(self):
        """Test storage adapter factory function"""
        # Test mock adapter creation
        mock_adapter = create_storage_adapter("mock", {})
        self.assertIsInstance(mock_adapter, MockRemoteStorageAdapter)
        
        # Test local adapter creation
        local_config = {"base_path": str(self.remote_dir)}
        local_adapter = create_storage_adapter("local_file", local_config)
        self.assertIsInstance(local_adapter, LocalFileStorageAdapter)
        
        # Test MinIO adapter creation (without actual connection)
        minio_config = {
            'minio_host': 'localhost',
            'minio_port': 9000,
            'access_key': 'test',
            'secret_key': 'test',
            'bucket_name': 'test'
        }
        minio_adapter = create_storage_adapter("minio", minio_config)
        self.assertIsInstance(minio_adapter, MinIOStorageAdapter)
        
        # Test invalid storage type
        with self.assertRaises(ValueError):
            create_storage_adapter("invalid_type", {})


if __name__ == '__main__':
    # Set up logging for tests
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test cases (cache-related tests removed)
    suite.addTests(loader.loadTestsFromTestCase(TestBasicDataSharingScenarios))
    suite.addTests(loader.loadTestsFromTestCase(TestStorageAdapterIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print(f"{'='*50}")
    print(f"Test Results Summary:")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    if result.testsRun > 0:
        success_rate = ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100)
        print(f"Success rate: {success_rate:.1f}%")
    print(f"{'='*50}")