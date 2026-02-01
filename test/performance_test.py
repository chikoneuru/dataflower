#!/usr/bin/env python3
"""
Performance Testing Script for DataFlower Serverless Platform
Tests 3 scenarios:
1. Request Rate Scaling (RPM increase)
2. Input Size Scaling (data size increase) 
3. Parallelism Scaling (concurrent requests)
"""

import datetime
import json
import os
import sys
import time
import threading
import base64
import random
import string
from collections import defaultdict
from gevent import monkey

monkey.patch_all()
import gevent
import requests
import numpy as np
from src.workflow_manager.repository import Repository
from config import config

# Configuration
GATEWAY_URL = f'http://{config.GATEWAY_IP}:7000'
repo = Repository()


class TestConfig:
    """Configuration class for test parameters"""
    
    def __init__(self):
        # Default test configurations
        self.default_rate_tests = [5, 10, 20, 50]
        self.default_size_tests = [(0.1, 'text'), (1, 'text'), (5, 'text')]
        self.default_parallel_levels = [1, 5, 10, 20]
        
        # Test durations
        self.default_test_duration = 60
        self.max_test_duration = 300
        
        # Data generation defaults
        self.default_input_size_mb = 0.1
        self.default_data_type = 'text'
        
        # Parallelism defaults
        self.default_burst_interval = 10
        
        # Timeouts
        self.request_timeout = 90
        self.join_timeout = 60
        
    def get_rate_test_config(self, custom_rates=None):
        """Get rate test configuration"""
        rates = custom_rates if custom_rates is not None else self.default_rate_tests
        return {
            'test_rates': rates,
            'test_duration': self.default_test_duration,
            'input_size_mb': self.default_input_size_mb,
            'data_type': self.default_data_type
        }
    
    def get_size_test_config(self, custom_sizes=None):
        """Get size test configuration"""
        sizes = custom_sizes if custom_sizes is not None else self.default_size_tests
        return {
            'size_tests': sizes,
            'test_duration': self.default_test_duration
        }
    
    def get_parallel_test_config(self, custom_levels=None):
        """Get parallelism test configuration"""
        levels = custom_levels if custom_levels is not None else self.default_parallel_levels
        return {
            'parallelism_levels': levels,
            'test_duration': self.default_test_duration,
            'input_size_mb': 0.5,
            'data_type': self.default_data_type,
            'burst_interval': self.default_burst_interval
        }


class MetricTracker:
    """Centralized metric collection and analysis"""
    
    def __init__(self):
        self.start_times = {}
        self.durations = defaultdict(list)
        self.values = defaultdict(list)
        self.request_details = []

    def start(self, label):
        """Start timing for a label"""
        self.start_times[label] = time.time()

    def stop(self, label):
        """Stop timing for a label and record duration"""
        if label in self.start_times:
            elapsed = time.time() - self.start_times[label]
            self.durations[label].append(elapsed)
            del self.start_times[label]
            return elapsed
        return None

    def record(self, label, value):
        """Record a numeric value"""
        self.values[label].append(value)

    def add_request_detail(self, request_info):
        """Add detailed request information"""
        self.request_details.append(request_info)

    def summary(self):
        """Generate comprehensive summary of all metrics"""
        summary_data = {}
        
        # Process durations
        for label, times in self.durations.items():
            if times:
                summary_data[label] = {
                    "count": len(times),
                    "avg": np.mean(times),
                    "min": np.min(times),
                    "max": np.max(times),
                    "p50": np.percentile(times, 50),
                    "p95": np.percentile(times, 95),
                    "p99": np.percentile(times, 99),
                    "std": np.std(times),
                    "values": times
                }
        
        # Process values
        for label, vals in self.values.items():
            if vals:
                summary_data[label] = {
                    "count": len(vals),
                    "avg": np.mean(vals),
                    "min": np.min(vals),
                    "max": np.max(vals),
                    "p50": np.percentile(vals, 50),
                    "p95": np.percentile(vals, 95),
                    "p99": np.percentile(vals, 99),
                    "std": np.std(vals),
                    "values": vals
                }
        
        return summary_data

    def get_processing_delays(self):
        """Extract processing delay metrics (time between request start and completion)"""
        processing_delays = []
        for req in self.request_details:
            if req.get('status') == 'success' and 'processing_start' in req and 'processing_end' in req:
                delay = req['processing_end'] - req['processing_start']
                processing_delays.append(delay)
        return processing_delays

    def get_scenario_duration(self):
        """Get total scenario duration"""
        if 'scenario_start' in self.start_times:
            return time.time() - self.start_times['scenario_start']
        return None


class PerformanceTest:
    def __init__(self, test_config=None):
        self.metrics = MetricTracker()
        self.pre_time = 60  # Pre-warming time in seconds
        self.test_config = test_config if test_config else TestConfig()
        
    def setup_test_environment(self):
        """Clean environment before testing"""
        print("🧹 Cleaning test environment...")
        
        # Clear databases
        repo.clear_couchdb_workflow_latency()
        repo.clear_couchdb_results()
        
        # Clear gateway
        try:
            r = requests.post(f'{GATEWAY_URL}/clear')
            if r.status_code == 200:
                print("✅ Gateway cleared")
        except Exception as e:
            print(f"⚠️  Gateway clear failed: {e}")
        
        # Clear workers
        threads_ = []
        for addr in config.WORKER_ADDRS:
            t = threading.Thread(target=self._clean_worker, args=(addr,))
            threads_.append(t)
            t.start()
        for t in threads_:
            t.join()
    
    def _clean_worker(self, addr):
        """Clean individual worker"""
        try:
            r = requests.post(f'http://{addr}:7999/clear')
            if r.status_code == 200:
                print(f"✅ Worker {addr} cleared")
        except Exception as e:
            print(f"⚠️  Worker {addr} clear failed: {e}")
    
    def generate_test_data(self, size_mb=1, data_type='text'):
        """Generate test data of specified size"""
        if data_type == 'text':
            return self._generate_text_data(size_mb)
        elif data_type == 'image':
            return self._generate_image_data(size_mb)
        elif data_type == 'json':
            return self._generate_json_data(size_mb)
        else:
            return None
    
    def _generate_text_data(self, size_mb):
        """Generate large text data"""
        words = ['the', 'quick', 'brown', 'fox', 'jumps', 'over', 'lazy', 'dog', 
                'serverless', 'computing', 'dataflow', 'workflow', 'function'] * 100
        
        target_size = size_mb * 1024 * 1024
        text_parts = []
        current_size = 0
        
        while current_size < target_size:
            sentence = ' '.join(random.choices(words, k=random.randint(10, 20))) + '. '
            text_parts.append(sentence)
            current_size += len(sentence)
        
        return ''.join(text_parts)
    
    def _generate_image_data(self, size_mb):
        """Generate fake image data (base64)"""
        raw_size = int(size_mb * 1024 * 1024 * 0.75)
        fake_binary = ''.join(random.choices(string.ascii_letters + string.digits, k=raw_size))
        return base64.b64encode(fake_binary.encode()).decode()
    
    def _generate_json_data(self, size_mb):
        """Generate large JSON data"""
        target_size = size_mb * 1024 * 1024
        data = {'items': []}
        
        item_template = {
            'id': 0,
            'name': 'item_name',
            'description': 'A' * 100,
            'metadata': {
                'created': time.time(),
                'data': 'x' * 200
            }
        }
        
        current_size = 0
        item_id = 0
        
        while current_size < target_size:
            item = item_template.copy()
            item['id'] = item_id
            item['name'] = f'item_{item_id}'
            data['items'].append(item)
            current_size += len(json.dumps(item))
            item_id += 1
        
        return json.dumps(data)

    def post_request(self, request_id, workflow_name, input_data, input_size_mb=0):
        """Send single request and collect metrics"""
        request_info = {
            'request_id': request_id,
            'workflow_name': workflow_name,
            'input_datas': {
                '$USER.start': {
                    'datatype': 'entity', 
                    'val': input_data, 
                    'output_type': 'NORMAL'
                }
            }
        }
        
        # Start tracking request
        self.metrics.start(f'request_{request_id}')
        request_start = time.time()
        
        request_detail = {
            'request_id': request_id,
            'workflow_name': workflow_name,
            'input_size_mb': input_size_mb,
            'request_start': request_start,
            'status': 'started',
            'phase': 'pre_warming' if request_start <= time.time() - self.pre_time else 'actual_test'
        }
        
        try:
            # Send HTTP request
            self.metrics.start(f'http_request_{request_id}')
            r = requests.post(f'{GATEWAY_URL}/run', json=request_info, timeout=90)
            http_duration = self.metrics.stop(f'http_request_{request_id}')
            
            request_detail.update({
                'http_duration': http_duration,
                'http_status_code': r.status_code
            })
            
            if r.status_code != 200:
                request_detail.update({
                    'status': 'http_error',
                    'error_message': f'HTTP {r.status_code}: {r.text[:200]}'
                })
                self.metrics.add_request_detail(request_detail)
                self.metrics.stop(f'request_{request_id}')
                return
            
            # Parse response
            self.metrics.start(f'parse_response_{request_id}')
            response_json = r.json()
            parse_duration = self.metrics.stop(f'parse_response_{request_id}')
            
            request_detail.update({
                'response_data': response_json,
                'parse_duration': parse_duration
            })
            
            if 'latency' not in response_json:
                request_detail.update({
                    'status': 'missing_latency',
                    'error_message': 'Response missing latency field'
                })
                self.metrics.add_request_detail(request_detail)
                self.metrics.stop(f'request_{request_id}')
                return
            
            # Success case - record processing delay
            request_end = time.time()
            processing_start = request_start  # Processing starts when we send the request
            processing_end = request_end      # Processing ends when we get the response
            
            request_detail.update({
                'status': 'success',
                'request_end': request_end,
                'processing_start': processing_start,
                'processing_end': processing_end,
                'processing_delay': processing_end - processing_start,
                'gateway_latency': response_json.get('latency', 0),
                'total_duration': request_end - request_start
            })
            
            # Record processing delay metric
            self.metrics.record('processing_delays', processing_end - processing_start)
            
            # Only count requests after pre-warming period
            if request_start > time.time() - self.pre_time:
                request_detail['counted'] = True
                self.metrics.record('counted_processing_delays', processing_end - processing_start)
            
            self.metrics.add_request_detail(request_detail)
            
        except requests.exceptions.Timeout:
            request_end = time.time()
            request_detail.update({
                'status': 'timeout',
                'error_message': 'Request timeout after 90s',
                'processing_start': request_start,
                'processing_end': request_end,
                'processing_delay': request_end - request_start
            })
            self.metrics.add_request_detail(request_detail)
            
        except Exception as e:
            request_end = time.time()
            request_detail.update({
                'status': 'error',
                'error_message': f'Unexpected error: {str(e)}',
                'processing_start': request_start,
                'processing_end': request_end,
                'processing_delay': request_end - request_start
            })
            self.metrics.add_request_detail(request_detail)
        
        self.metrics.stop(f'request_{request_id}')

    def run_scenario(self, scenario_name, test_config, request_generator):
        """Generic scenario runner with unified metric collection"""
        print(f"\n🚀 SCENARIO: {scenario_name}")
        print("=" * 50)
        print(f"🕒 Scenario started at: {datetime.datetime.now().strftime('%H:%M:%S')}")
        
        # Start scenario timing
        self.metrics.start('scenario_start')
        scenario_start = time.time()
        
        results = {}
        
        for test_param, config in test_config.items():
            print(f"\n📊 Testing {test_param}...")
            self.setup_test_environment()
            time.sleep(2)
            
            # Reset metrics for this test
            test_metrics = MetricTracker()
            test_metrics.start('test_start')
            
            # Generate and send requests
            request_generator(test_param, config, test_metrics)
            
            # Wait for completion
            test_metrics.stop('test_start')
            test_duration = test_metrics.get_scenario_duration()
            
            # Calculate results
            successful_requests = [req for req in test_metrics.request_details 
                                 if req.get('status') == 'success' and req.get('counted')]
            
            results[test_param] = {
                'test_config': config,
                'test_duration': test_duration,
                'summary_stats': {
                    'total_requests': len(test_metrics.request_details),
                    'successful_counted': len(successful_requests),
                    'success_rate': len(successful_requests) / len([r for r in test_metrics.request_details if r.get('counted')]) * 100 if len([r for r in test_metrics.request_details if r.get('counted')]) > 0 else 0
                },
                'performance_metrics': test_metrics.summary(),
                'request_details': test_metrics.request_details
            }
            
            if successful_requests:
                processing_delays = [req['processing_delay'] for req in successful_requests]
                print(f"   ✅ {test_param}: {len(successful_requests)} success")
                print(f"      Processing Delay: avg {np.mean(processing_delays)*1000:.1f}ms, p95 {np.percentile(processing_delays, 95)*1000:.1f}ms")
                print(f"      Test Duration: {test_duration:.1f}s")
            else:
                print(f"   ❌ {test_param}: All requests failed")
            
            # Save individual test results
            self._save_results(f'{scenario_name}_{test_param}', results[test_param])
        
        # Calculate total scenario duration
        scenario_duration = self.metrics.get_scenario_duration()
        
        results['_scenario_timing'] = {
            'scenario_start': scenario_start,
            'scenario_end': time.time(),
            'total_duration': scenario_duration,
            'scenario_name': scenario_name
        }
        
        print(f"\n🕒 {scenario_name} SCENARIO COMPLETED")
        print(f"   Total Duration: {scenario_duration:.1f}s ({scenario_duration/60:.1f} min)")
        
        return results

    def test_request_rate_scaling(self, workflow_name='wordcount', max_duration=300, test_rates=None):
        """Test Scenario 1: Request Rate Scaling"""

        # Get configuration
        config = self.test_config.get_rate_test_config(test_rates)
        test_rates = config['test_rates']
        test_duration = min(config['test_duration'], max_duration)

        print(f"\n🚀 SCENARIO: REQUEST_RATE_SCALING")
        print("=" * 50)
        print(f"🕒 Scenario started at: {datetime.datetime.now().strftime('%H:%M:%S')}")

        self.metrics.start('scenario_start')
        scenario_start = time.time()
        results = {}

        for rpm in test_rates:
            test_param = f"{rpm}_rpm"
            print(f"\n📊 Testing {test_param}...")

            # Reset metrics
            test_metrics = MetricTracker()
            test_start_time = time.time()
            test_metrics.start('test_start') 
            
            self.setup_test_environment()
            time.sleep(2)

            # Prepare request data
            input_data = self.generate_test_data(
                size_mb=config.get('input_size_mb', 0.1),
                data_type=config.get('data_type', 'text')
            )
            input_size_mb = len(str(input_data)) / (1024 * 1024)

            idx = 0
            active_greenlets = []
            total_duration = self.pre_time + test_duration

            while time.time() - test_start_time < total_duration:
                greenlet = gevent.spawn(
                    self.post_request,
                    f'rate_test_{rpm}rpm_{idx:04d}',
                    workflow_name,
                    input_data,
                    input_size_mb
                )
                active_greenlets.append(greenlet)
                idx += 1

                # Dynamic rate adjustment
                delta = time.time() - test_start_time
                period = int(delta / 20)
                f = max(1, 4 - period)
                gevent.sleep(60 / rpm * f)

            gevent.joinall(active_greenlets, timeout=self.test_config.join_timeout)

            # End metrics for this test
            test_metrics.stop('test_start')
            test_duration_actual = test_metrics.get_scenario_duration()

            # Calculate results
            successful_requests = [
                req for req in test_metrics.request_details
                if req.get('status') == 'success' and req.get('counted')
            ]

            results[test_param] = {
                'test_config': {
                    'rpm': rpm,
                    'workflow': workflow_name,
                    'input_size_mb': config['input_size_mb'],
                    'data_type': config['data_type']
                },
                'test_duration': test_duration_actual,
                'summary_stats': {
                    'total_requests': len(test_metrics.request_details),
                    'successful_counted': len(successful_requests),
                    'success_rate': (
                        len(successful_requests) /
                        len([r for r in test_metrics.request_details if r.get('counted')]) * 100
                        if any(r.get('counted') for r in test_metrics.request_details)
                        else 0
                    )
                },
                'performance_metrics': test_metrics.summary(),
                'request_details': test_metrics.request_details
            }

            if successful_requests:
                processing_delays = [req['processing_delay'] for req in successful_requests]
                print(f"   ✅ {test_param}: {len(successful_requests)} success")
                print(f"      Processing Delay: avg {np.mean(processing_delays)*1000:.1f}ms, "
                    f"p95 {np.percentile(processing_delays, 95)*1000:.1f}ms")
                print(f"      Test Duration: {test_duration_actual:.1f}s")
            else:
                print(f"   ❌ {test_param}: All requests failed")

            self._save_results(f'REQUEST_RATE_SCALING_{test_param}', results[test_param])

        # Scenario duration
        scenario_duration = time.time() - scenario_start
        results['_scenario_timing'] = {
            'scenario_start': scenario_start,
            'scenario_end': time.time(),
            'total_duration': scenario_duration,
            'scenario_name': 'REQUEST_RATE_SCALING'
        }

        print(f"\n🕒 REQUEST_RATE_SCALING SCENARIO COMPLETED")
        print(f"   Total Duration: {scenario_duration:.1f}s ({scenario_duration/60:.1f} min)")

        return results


    def test_input_size_scaling(self, workflow_name='wordcount', rpm=10, size_tests=None):
        """Test Scenario 2: Input Size Scaling"""
        
        # Get configuration
        config = self.test_config.get_size_test_config(size_tests)
        size_tests = config['size_tests']
        test_duration = config['test_duration']
        
        def request_generator(test_param, config, metrics):
            total_duration = self.pre_time + test_duration
            input_data = self.generate_test_data(size_mb=config['size_mb'], data_type=config['data_type'])
            input_size_mb = len(str(input_data)) / (1024 * 1024)
            
            # Extract RPM value from parameter
            test_rpm = rpm  # Use the rpm parameter from the outer function
            
            idx = 0
            active_greenlets = []
            
            while time.time() - metrics.start_times.get('test_start', time.time()) < total_duration:
                greenlet = gevent.spawn(self.post_request, 
                                      f'size_test_{config["size_mb"]}mb_{config["data_type"]}_{idx:04d}', 
                                      workflow_name, input_data, input_size_mb)
                active_greenlets.append(greenlet)
                idx += 1
                
                # Dynamic rate adjustment
                delta = time.time() - metrics.start_times.get('test_start', time.time())
                period = int(delta / 20)
                f = max(1, 4 - period)
                
                gevent.sleep(60 / test_rpm * f)
            
            gevent.joinall(active_greenlets, timeout=self.test_config.join_timeout * 2)  # Longer timeout for size tests
        
        # Build test config dynamically
        test_config = {}
        for size_mb, data_type in size_tests:
            test_config[f'{size_mb}MB_{data_type}'] = {
                'size_mb': size_mb, 
                'data_type': data_type, 
                'workflow': workflow_name,
                'test_duration': test_duration
            }
        
        return self.run_scenario('INPUT_SIZE_SCALING', test_config, request_generator)

    def test_parallelism_scaling(self, workflow_name='wordcount', parallelism_levels=None):
        """Test Scenario 3: Parallelism Scaling"""
        
        # Get configuration
        config = self.test_config.get_parallel_test_config(parallelism_levels)
        parallelism_levels = config['parallelism_levels']
        test_duration = config['test_duration']
        
        def request_generator(test_param, config, metrics):
            total_duration = self.pre_time + test_duration
            input_data = self.generate_test_data(size_mb=config.get('input_size_mb', 0.5), 
                                               data_type=config.get('data_type', 'text'))
            input_size_mb = len(str(input_data)) / (1024 * 1024)
            
            idx = 0
            active_greenlets = []
            
            while time.time() - metrics.start_times.get('test_start', time.time()) < total_duration:
                # Send a burst of parallel requests
                burst_greenlets = []
                for i in range(config['parallel_count']):
                    greenlet = gevent.spawn(self.post_request, 
                                          f'parallel_test_{config["parallel_count"]}p_{idx:04d}_{i:02d}', 
                                          workflow_name, input_data, input_size_mb)
                    burst_greenlets.append(greenlet)
                    active_greenlets.append(greenlet)
                    idx += 1
                
                # Wait between bursts
                gevent.sleep(config.get('burst_interval', 10))
            
            gevent.joinall(active_greenlets, timeout=self.test_config.join_timeout * 3)  # Longer timeout for parallel tests
        
        # Build test config dynamically
        test_config = {}
        for parallel_count in parallelism_levels:
            test_config[f'{parallel_count}_parallel'] = {
                'parallel_count': parallel_count, 
                'workflow': workflow_name,
                'input_size_mb': config['input_size_mb'],
                'data_type': config['data_type'],
                'test_duration': test_duration,
                'burst_interval': config['burst_interval']
            }
        
        return self.run_scenario('PARALLELISM_SCALING', test_config, request_generator)
    
    def _save_results(self, test_name, results):
        """Save test results to file"""
        results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results')
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = os.path.join(results_dir, f'{test_name}_{timestamp}.json')
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"📁 Results saved to: {filename}")
    
    def run_all_tests(self, workflow_name='wordcount'):
        """Run all three test scenarios"""
        print("🎯 DATAFLOWER PERFORMANCE TEST SUITE")
        print("=" * 60)
        print(f"Testing workflow: {workflow_name}")
        print(f"Gateway: {GATEWAY_URL}")
        print("=" * 60)
        
        try:
            # Start overall timing
            self.metrics.start('overall_start')
            
            rate_results = self.test_request_rate_scaling(workflow_name, max_duration=180)
            size_results = self.test_input_size_scaling(workflow_name)
            parallel_results = self.test_parallelism_scaling(workflow_name)
            
            # Calculate overall duration
            overall_duration = self.metrics.get_scenario_duration()
            
            # Save combined results
            combined_results = {
                'test_info': {
                    'workflow_name': workflow_name,
                    'test_time': datetime.datetime.now().isoformat(),
                    'gateway_url': GATEWAY_URL,
                    'overall_duration': overall_duration
                },
                'request_rate_scaling': rate_results,
                'input_size_scaling': size_results,
                'parallelism_scaling': parallel_results
            }
            self._save_results('complete_performance_test', combined_results)
            
            # Print summary
            self._print_summary(combined_results)
            
        except KeyboardInterrupt:
            print("\n⚠️  Test interrupted by user")
            print("💾 Partial results have been saved individually")
    
    def _print_summary(self, results):
        """Print test summary focusing on key metrics"""
        print("\n" + "=" * 60)
        print("📊 PERFORMANCE TEST SUMMARY")
        print("=" * 60)
        
        overall_duration = results['test_info'].get('overall_duration', 0)
        print(f"🕒 Overall Test Duration: {overall_duration:.1f}s ({overall_duration/60:.1f} min)")
        
        for test_type, test_results in results.items():
            if test_type == 'test_info':
                continue
                
            print(f"\n{test_type.replace('_', ' ').title()}:")
            
            if '_scenario_timing' in test_results:
                scenario_duration = test_results['_scenario_timing']['total_duration']
                print(f"   Scenario Duration: {scenario_duration:.1f}s")
            
            for key, data in test_results.items():
                if key == '_scenario_timing':
                    continue
                    
                if 'performance_metrics' in data and 'processing_delays' in data['performance_metrics']:
                    delays = data['performance_metrics']['processing_delays']
                    success_count = data['summary_stats']['successful_counted']
                    avg_delay = delays['avg'] * 1000  # Convert to ms
                    p95_delay = delays['p95'] * 1000  # Convert to ms
                    print(f"   {key}: {success_count} success, avg processing delay {avg_delay:.1f}ms (p95: {p95_delay:.1f}ms)")
                elif 'summary_stats' in data:
                    success_count = data['summary_stats']['successful_counted']
                    print(f"   {key}: {success_count} success")


def main():
    """Main function with command line options"""
    
    # Create custom test configuration
    custom_config = TestConfig()
    custom_config.default_rate_tests = [2, 5, 15, 30]  # Custom RPM values
    custom_config.default_size_tests = [(0.05, 'text'), (0.5, 'text'), (2, 'text'), (10, 'json')]  # Custom size/type combinations
    custom_config.default_parallel_levels = [2, 8, 15, 25]  # Custom parallelism levels
    custom_config.default_test_duration = 45  # Shorter tests
    
    test = PerformanceTest(test_config=custom_config)
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--rate':
            # Use custom rate tests
            test.test_request_rate_scaling()
        elif sys.argv[1] == '--size':
            # Use custom size tests
            test.test_input_size_scaling()
        elif sys.argv[1] == '--parallel':
            # Use custom parallelism levels
            test.test_parallelism_scaling()
        elif sys.argv[1] == '--custom':
            # Run with all custom configurations
            print("Running with custom test configurations...")
            test.run_all_tests()
        elif sys.argv[1] == '--default':
            # Run with default configurations
            default_test = PerformanceTest()
            default_test.run_all_tests()
        else:
            test.run_all_tests()
    else:
        test.run_all_tests()


if __name__ == "__main__":
    main()