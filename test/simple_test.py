#!/usr/bin/env python3
"""
Simple Multi-RPM Test Runner
"""

import time
import sys
import os
from test.async_test import test_to_one

def run_test(test_type, rpm, duration=60, clear_db=False):
    """Run async_test directly with specified RPM"""
    print(f"🔧 Running {rpm} RPM {test_type}...")
    
    # Backup original sys.argv
    original_argv = sys.argv.copy()
    
    try:
        test_to_one('wordcount', rpm, duration, clear_db=clear_db, test_type=test_type)
        print(f"✅ {rpm} RPM - SUCCESS")
        return True
        
    except Exception as e:
        print(f"❌ {rpm} RPM - ERROR: {e}")
        return False
    finally:
        # Restore original sys.argv
        sys.argv = original_argv

def main():
    # Test rates
    rates = [5, 10, 20, 60, 120] if len(sys.argv) == 1 else [int(x) for x in sys.argv[1].split(',')]
    duration = 60
    test_type = "rate_test" # rate test, input_size_test, or parallel_test
    
    print(f"🧪 Test type: {test_type}")
    print(f"🚀 Testing RPM rates: {rates}")
    print(f"⏱️  Duration: {duration}s each")
    print("-" * 40)
    
    results = {}
    start = time.time()
    
    for i, rpm in enumerate(rates):
        scenario_start = time.time()
        success = run_test(test_type=test_type, rpm=rpm, duration=duration, clear_db=i==0)
        scenario_duration = time.time() - scenario_start
        results[rpm] = 'SUCCESS' if success else 'FAILED'
        results[rpm] += f" ({scenario_duration:.1f}s)"
        
        # Wait between tests (except for last test)
        if i < len(rates) - 1:
            print("⏳ Waiting 30s...")
            time.sleep(30)
    
    # Summary
    total_time = time.time() - start
    print("\n📊 SUMMARY")
    print("-" * 40)
    for rpm, status in results.items():
        print(f"{rpm:3d} RPM: {status}")
    print(f"Total time: {total_time/60:.1f} min")

if __name__ == "__main__":
    main()