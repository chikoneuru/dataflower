import datetime
import json
import os
import os.path
import shutil
import threading

from gevent import monkey

monkey.patch_all()
import sys
import time
import gevent
import pandas as pd
import numpy as np
import requests
from src.workflow_manager.repository import Repository
from config import config

repo = Repository()
gateway_url = 'http://' + config.GATEWAY_URL + '/{}'
slow_threshold = 1000
pre_time = 1 * 60
latencies = []
request_infos = {}
ids = {}
# Track only requests that matter for statistics
counted_greenlets = []


def post_request(request_id, workflow_name):
    request_info = {'request_id': request_id,
                    'workflow_name': workflow_name,
                    'input_datas': {'$USER.start': {'datatype': 'entity', 'val': None, 'output_type': 'NORMAL'}}}
    
    # Debug: Print request details
    print(f'\n--firing-- {request_id} at {time.time() - test_start:.1f}s')
    st = time.time()
    try:
        # Test gateway connectivity first
        print(f'Testing gateway connectivity for {request_id}...')
        r = requests.post(gateway_url.format('run'), json=request_info, timeout=90)
        ed = time.time()
        
        elapsed_at_start = st - test_start
        print(f'\nRequest {request_id} completed in {ed - st:.3f}s, status: {r.status_code}')
        
        # Check if response is valid
        if r.status_code != 200:
            print(f'\nERROR: Gateway returned status {r.status_code} for request {request_id}')
            print(f'Response content: {r.text}')
            return
            
        # Try to parse response
        try:
            response_json = r.json()
            # print(f'Response JSON: {response_json}')
        except json.JSONDecodeError as e:
            print(f'ERROR: Invalid JSON response for request {request_id}: {e}')
            print(f'Response text: {r.text}')
            return
            
        # Check if response has required fields
        if 'latency' not in response_json:
            print(f'ERROR: Response missing latency field for request {request_id}')
            print(f'Available fields: {list(response_json.keys())}')
            return
        
        print(f'SUCCESS: Request {request_id} completed successfully with latency: {response_json["latency"]}')
        
        if st > test_start + pre_time:
            print(f'*** COUNTING REQUEST {request_id} ***')
            duration = ed - st
            ids[request_id] = {'time': duration, 'st': st, 'ed': ed, 'latency': response_json['latency']}
            latencies.append(response_json['latency'])
            
            if len(latencies) == 1:
                print(f'*** FIRST COUNTED REQUEST: {request_id} at {elapsed_at_start:.1f}s ***')
            
            global counted_greenlets
            current_greenlet = gevent.getcurrent()
            counted_greenlets.append(current_greenlet)
        else:
            print(f'Request {request_id} in pre-warming phase (not counted)')
            
    except requests.exceptions.ConnectionError as e:
        print(f'ERROR: Connection failed for request {request_id}')
        print(f'This indicates the gateway service is not running or not accessible')
        print(f'Error details: {e}')
        print(f'Gateway URL: {gateway_url.format("run")}')
        print(f'Please check if the gateway service is running on {config.GATEWAY_IP}:7000')
        
    except requests.exceptions.Timeout as e:
        print(f'ERROR: Request timeout for {request_id} after 30 seconds')
        print(f'This could indicate network issues or the gateway is overloaded')
        print(f'Error details: {e}')
        
    except requests.exceptions.RequestException as e:
        print(f'ERROR: Request failed for {request_id}')
        print(f'This could indicate network connectivity issues')
        print(f'Error details: {e}')
        
    except Exception as e:
        print(f'ERROR: Unexpected error for request {request_id}: {e}')
        print(f'Error type: {type(e).__name__}')
        import traceback
        traceback.print_exc()


def end_loop(idx, workflow_name, parallel, duration, test_type="rate_test"):
    print(f"Ending loop!!!")
    while time.time() - test_start < pre_time + duration:
        # Format RPM with zero-padding to 3 digits
        request_id = f'{test_type}_{str(idx).zfill(3)}rpm_request_' + str(idx).rjust(4, '0')
        post_request(request_id, workflow_name)
        idx += parallel


input_args = ''.join(sys.argv[1:])


def get_use_container_log(workflow_name, rpm, tests_duration):
    cnt = {}
    avg = {}
    GB_s = 0
    requests_logs = repo.get_latencies('use_container')
    save_logs = {}
    for request_id in ids:
        start_time = ids[request_id]['st']
        duration = ids[request_id]['time']
        slow = False
        if duration > slow_threshold:
            slow = True
        logs = requests_logs[request_id]
        save_logs[request_id] = {}
        save_logs[request_id]['logs'] = []
        save_logs[request_id]['fire_time'] = start_time
        save_logs[request_id]['latency'] = ids[request_id]['latency']
        if slow:
            print(ids[request_id])
        current_request_cnt = {}
        current_request_function_max_log = {}
        for log in logs:
            function_name = log['template_name'] + '_' + log['block_name']
            save_logs[request_id]['logs'].append({'time': log['time'], 'function_name': function_name, 'st': log['st'],
                                                  'ed': log['ed'], 'cpu': log['cpu']})
            GB_s += log['time'] * log['cpu'] * 1280 / 1024
            if function_name not in current_request_cnt:
                current_request_cnt[function_name] = 0
            current_request_cnt[function_name] += 1
            if current_request_cnt[function_name] == 1:
                current_request_function_max_log[function_name] = log
            else:
                if log['time'] > current_request_function_max_log[function_name]['time']:
                    current_request_function_max_log[function_name] = log
            if function_name not in cnt:
                cnt[function_name] = 0
            cnt[function_name] += 1
            if function_name not in avg:
                avg[function_name] = [0, 0, 0]
            avg[function_name][0] += log['time']
            avg[function_name][1] += log['st'] - start_time
            avg[function_name][2] += log['ed'] - start_time
            if slow:
                print(function_name, "%0.3f" % log['time'], "%0.3f" % (log['st'] - start_time),
                      "%0.3f" % (log['ed'] - start_time))

        for func in current_request_cnt:
            if current_request_cnt[func] > 1:
                function_name = func + '_longest'
                log = current_request_function_max_log[func]
                if function_name not in cnt:
                    cnt[function_name] = 0
                cnt[function_name] += 1
                if function_name not in avg:
                    avg[function_name] = [0, 0, 0]
                avg[function_name][0] += log['time']
                avg[function_name][1] += log['st'] - start_time
                avg[function_name][2] += log['ed'] - start_time

    for function_name in cnt:
        print(function_name, end=' ')
        for v in avg[function_name]:
            print("%0.3f" % (v / cnt[function_name]), end=' ')
        print()
    print('Container_GB-s:', format(GB_s / len(latencies), '.3f'))
    nowtime = str(datetime.datetime.now())
    result_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'result')
    if not os.path.exists(result_dir):
        os.mkdir(result_dir)
    suffix = 'async_' + workflow_name + '_' + str(rpm) + '_' + str(tests_duration)

    filepath = os.path.join(result_dir, suffix + '.json')
    with open(filepath, 'w') as f:
        json.dump(save_logs, f)


def cal_percentile():
    percents = [50, 90, 95, 99]
    for percent in percents:
        print(f'P{percent}: ', format(np.percentile(latencies, percent), '.3f'))


def clean_worker(addr):
    r = requests.post(f'http://{addr}:7999/clear')
    assert r.status_code == 200


def finish_worker(addr):
    r = requests.post(f'http://{addr}:8000/finish')
    assert r.status_code == 200

def test_to_one(workflow_name, rpm, duration, clear_db=True, test_type="rate_test"):

    if clear_db:
        print('Clearing database...')
        repo.clear_all_couchdb()

    r = requests.post(f'http://{config.GATEWAY_IP}:7000/clear')
    assert r.status_code == 200
    threads_ = []
    for addr in config.WORKER_ADDRS:
        t = threading.Thread(target=clean_worker, args=(addr, ))
        threads_.append(t)
        t.start()
    for t in threads_:
        t.join()
    global ids, latencies, counted_greenlets
    ids = {}
    latencies = []
    counted_greenlets = []  
    
    print(f'firing {workflow_name} with rpm {rpm} for {duration} s (test_type: {test_type})')
    
    global test_start
    test_start = time.time()
    print(f'=== TEST STARTED at {datetime.datetime.fromtimestamp(test_start).strftime("%H:%M:%S")} ===')
    print(f'=== PRE-WARMING PHASE: 0-{pre_time}s (requests fired but NOT counted) ===')

    idx = 0
    pre_warming_done = False
    actual_test_done = False

    active_greenlets = []

    while time.time() - test_start < pre_time + duration:
        current_time = time.time()
        elapsed = current_time - test_start

        # Check if we've entered the actual test phase
        if not pre_warming_done and elapsed >= pre_time:
            print(f'=== ACTUAL TEST PHASE BEGINS at {elapsed:.1f}s (requests fired AND counted) ===')
            pre_warming_done = True

        # Check if we've finished the actual test phase
        if pre_warming_done and not actual_test_done and elapsed >= pre_time + duration:
            print(f'=== ACTUAL TEST PHASE ENDS at {elapsed:.1f}s ===')
            actual_test_done = True

        # Updated request_id format with test_type prefix and zero-padded RPM
        request_id = f'{test_type}_{str(rpm).zfill(3)}rpm_request_' + str(idx).rjust(4, '0')
        greenlet = gevent.spawn(post_request, request_id, workflow_name)
        active_greenlets.append(greenlet)
        idx += 1
        delta = time.time() - test_start
        period = int(delta / 20)
        f = max(1, 4 - period)

        # Print phase info every 30 seconds
        if idx % 30 == 0:
            if elapsed < pre_time:
                print(f'PRE-WARMING: {elapsed:.1f}s/{pre_time}s - Request #{idx} (rate factor: {f}x)')
            else:
                test_elapsed = elapsed - pre_time
                print(f'ACTUAL TEST: {test_elapsed:.1f}s/{duration}s - Request #{idx} (rate factor: {f}x)')

        gevent.sleep(60 / rpm * f)

    print(f'=== REQUEST GENERATION STOPPED at {time.time() - test_start:.1f}s ===')
    print(f'=== WAITING FOR {len(counted_greenlets)} COUNTED REQUESTS TO COMPLETE ===')
    gevent.joinall(counted_greenlets, timeout=30)
    completed_counted = sum(1 for g in counted_greenlets if g.ready())
    print(f'=== {completed_counted}/{len(counted_greenlets)} COUNTED REQUESTS COMPLETED ===')
    print(f'=== IGNORING {len(active_greenlets) - len(counted_greenlets)} PRE-WARMING REQUESTS ===')
    time.sleep(5)
    # print(f'=== REQUEST GENERATION STOPPED at {time.time() - test_start:.1f}s ===')
    # print(f'=== WAITING FOR {len(active_greenlets)} REQUESTS TO COMPLETE ===')
    # gevent.joinall(active_greenlets, timeout=60)
    # completed = sum(1 for g in active_greenlets if g.ready())
    # print(f'=== {completed}/{len(active_greenlets)} REQUESTS COMPLETED ===')
    # print(f'=== ALL REQUESTS COMPLETED - sleeping 5 seconds ===')
    # time.sleep(5)

    print('total requests count:', len(latencies))
    
    # Add safety check for division by zero
    if len(latencies) == 0:
        print('ERROR: No requests were counted. Cannot calculate statistics.')
        return
        
    # print(f"Skip container log retrieval for now")
    get_use_container_log(workflow_name, rpm, duration)
    print('avg:', format(sum(latencies) / len(latencies), '.3f'))
    cal_percentile()


def test_to_all():
    print(f"input_args: {input_args}")
    # target_workflow = {'recognizer': {1: 10, 2: 10, 4: 10, 7: 10, 8: 10, 9: 10, 10: 10},
    #                    'wordcount': {1: 5, 2: 5, 4: 5, 8: 5, 16: 5, 19: 5, 20: 5, 21: 5, 22: 5},
    #                    'video': {1: 10, 2: 10, 4: 10, 8: 10, 9: 10, 10: 10, 11: 10}}
    # target_workflow = {
    #     'svd': {10: 5, 20: 5, 40: 5, 60: 5, 80: 5, 100: 5}
    #     }
    # target_workflow = {'recognizer': {10: 1},
    #                    'wordcount': {10: 1},
    #                    'svd': {10: 1},
    #                    'video': {4: 1}}
    target_workflow = {'wordcount': {10: 1},}
    for workflow_name in target_workflow:
        for rpm in target_workflow[workflow_name]:
            test_to_one(workflow_name, rpm, 60 * target_workflow[workflow_name][rpm], test_type="rate_test")

if __name__ == "__main__":
    test_to_all()
