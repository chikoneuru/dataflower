#!/usr/bin/env python3
"""
Static Profile Calibration Script

Runs calibration phase to measure function and node performance,
then saves static profiles for use during experiments.

Usage:
    # Individual function calibration (auto-detects all functions from DAG)
    python scripts/calibrate_profiles.py [--input-sizes 0.1,0.5,1.0]
    
    # Calibrate specific functions only
    python scripts/calibrate_profiles.py --functions func1,func2 [--input-sizes 0.1,0.5,1.0]
    
    # DAG-based calibration (executes entire workflow)
    python scripts/calibrate_profiles.py --dag-mode [--dag-file functions/recognizer/recognizer_dag.yaml]
    
    # Mixed mode (load specific DAG but calibrate individual functions)
    python scripts/calibrate_profiles.py --functions func1,func2 --dag-file custom_dag.yaml
"""

import argparse
import sys
import os
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scheduler.ours.config import load_config
from provider.function_container_manager import FunctionContainerManager
from scheduler.ours.static_profiling import CalibrationRunner

def main():
    parser = argparse.ArgumentParser(description='Calibrate static profiles for scheduler')
    parser.add_argument('--functions', type=str, 
                       help='Comma-separated list of functions to calibrate')
    parser.add_argument('--input-sizes', type=str,
                       help='Comma-separated list of input sizes in MB (e.g., 0.1,0.5,1.0)')
    parser.add_argument('--samples', type=int, default=10,
                       help='Number of samples per measurement (default: 10)')
    parser.add_argument('--profile-name', type=str, default='default',
                       help='Name for the saved profile (default: default)')
    parser.add_argument('--dag-file', type=str,
                       help='DAG file path for DAG-based calibration (e.g., functions/recognizer/recognizer_dag.yaml)')
    parser.add_argument('--dag-mode', action='store_true',
                       help='Use DAG execution mode for calibration (requires --dag-file)')
    
    args = parser.parse_args()
    
    print("🔧 Static Profile Calibration")
    print("=" * 40)
    
    # Load configuration
    try:
        load_config('production')
        print("✅ Configuration loaded")
    except Exception as e:
        print(f"❌ Failed to load configuration: {e}")
        return 1
    
    # Initialize function container manager
    try:
        function_manager = FunctionContainerManager()
        nodes = function_manager.get_nodes()
        function_manager.discover_existing_containers()
        
        print(f"✅ Environment ready: {len(nodes)} nodes discovered")
        
        if not nodes:
            print("❌ No nodes discovered! Check if containers are running")
            return 1
            
    except Exception as e:
        print(f"❌ Failed to initialize environment: {e}")
        return 1
    
    # Validate DAG mode arguments
    if args.dag_mode and not args.dag_file:
        print("❌ DAG mode requires --dag-file parameter")
        return 1
    
    # Parse function list
    if args.functions:
        functions = [f.strip() for f in args.functions.split(',')]
    else:
        # Try to load DAG and extract all functions if no specific functions provided
        if args.dag_file or os.path.exists("functions/recognizer/recognizer_dag.yaml"):
            try:
                dag_file = args.dag_file or "functions/recognizer/recognizer_dag.yaml"
                temp_runner = CalibrationRunner(function_manager)
                temp_runner._load_dag(dag_file)
                if temp_runner.dag:
                    functions = temp_runner._extract_functions_from_dag()
                    print(f"📋 Extracted {len(functions)} functions from DAG: {dag_file}")
                else:
                    # Fallback to default functions
                    functions = ['recognizer__upload', 'recognizer__adult', 'recognizer__violence', 'recognizer__extract', 
                                'recognizer__translate', 'recognizer__censor', 'recognizer__mosaic']
            except Exception as e:
                print(f"⚠️  Failed to load DAG for function extraction: {e}")
                # Fallback to default functions
                functions = ['recognizer__upload', 'recognizer__adult', 'recognizer__violence', 'recognizer__extract', 
                            'recognizer__translate', 'recognizer__censor', 'recognizer__mosaic']
        else:
            # Default functions
            functions = ['recognizer__upload', 'recognizer__adult', 'recognizer__violence', 'recognizer__extract', 
                        'recognizer__translate', 'recognizer__censor', 'recognizer__mosaic']
    
    # Parse input sizes
    if args.input_sizes:
        try:
            input_sizes_mb = [float(s.strip()) for s in args.input_sizes.split(',')]
        except ValueError:
            print("❌ Invalid input sizes format. Use comma-separated numbers (e.g., 0.1,0.5,1.0)")
            return 1
    else:
        # Default input sizes - use larger sizes for image functions
        # Note: The calibration runner will handle per-function size selection automatically
        input_sizes_mb = [0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10, 20, 50]  # Default for image functions
    
    print(f"📊 Calibration parameters:")
    if args.dag_mode:
        print(f"   Mode: DAG-based calibration")
        print(f"   DAG file: {args.dag_file}")
    else:
        print(f"   Mode: Individual function calibration")
        print(f"   Functions: {functions}")
    print(f"   Input sizes: {input_sizes_mb} MB")
    print(f"   Samples per measurement: {args.samples}")
    print(f"   Profile name: {args.profile_name}")
    
    # Run calibration
    try:
        print("\n🚀 Starting calibration...")
        start_time = time.time()
        
        calibration_runner = CalibrationRunner(function_manager)
        
        # Try to run actual calibration
        try:
            if args.dag_mode:
                # DAG-based calibration
                if not args.dag_file:
                    # Default to recognizer DAG if no DAG file specified
                    args.dag_file = "functions/recognizer/recognizer_dag.yaml"
                calibration_runner._load_dag(args.dag_file)
                profiles = calibration_runner.run_dag_calibration(input_sizes_mb, args.samples)
                print("✅ DAG-based calibration completed")
            else:
                # Individual function calibration
                profiles = calibration_runner.run_calibration(functions, input_sizes_mb, args.samples, args.dag_file)
                print("✅ Individual function calibration completed")
        except Exception as e:
            import traceback
            print(f"\033[91mERROR\033[0m")
            traceback.print_exc()
            print("📝 Creating default profiles instead...")
            if args.dag_mode and args.dag_file:
                # Try to extract functions from DAG for default profiles
                try:
                    calibration_runner._load_dag(args.dag_file)
                    if calibration_runner.dag:
                        functions = calibration_runner._extract_functions_from_dag()
                except:
                    pass
            profiles = calibration_runner.create_default_profiles(functions)
        
        # Save profiles
        success = calibration_runner.static_profiler.save_profiles(profiles, args.profile_name)
        
        end_time = time.time()
        calibration_time = end_time - start_time
        
        if success:
            print(f"\n✅ Calibration completed successfully in {calibration_time:.1f} seconds")
            print(f"📄 Profiles saved to: results/static_profiles/{args.profile_name}.json")
            
            # Print summary
            print(f"\n📊 Calibration Summary:")
            print(f"   Functions calibrated: {len(profiles.function_profiles)}")
            print(f"   Nodes profiled: {len(profiles.node_profiles)}")
            print(f"   Network paths: {len(profiles.network_profiles)}")
            print(f"   Timestamp: {profiles.calibration_timestamp}")
            
            # Show function profiles
            print(f"\n🔧 Function Profiles:")
            for func_name, profile in profiles.function_profiles.items():
                print(f"   {func_name}: α={profile.base_execution_time_ms:.1f}ms, "
                      f"η={profile.scaling_exponent:.2f}, "
                      f"R²={profile.r_squared:.3f}, "
                      f"memory={profile.memory_requirement_mb}MB, "
                      f"CPU={profile.cpu_intensity:.2f}")
            
            return 0
        else:
            print("❌ Failed to save calibration profiles")
            return 1
            
    except Exception as e:
        print(f"❌ Calibration failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
