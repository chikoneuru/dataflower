import pprint
from gevent import monkey

monkey.patch_all()
import gc
import socket
import time
import json
import sys

import os
import sys
import requests
import gevent
from gevent.pywsgi import WSGIServer
from typing import Dict
from .workersp import WorkerSPManager
from config import config
from flask import Flask, request
from src.function_manager.file_controller import file_controller
from src.function_manager.prefetcher import prefetcher

app = Flask(__name__)


class Dispatcher:
    def __init__(self, ip_addr: str, workflows_info_path: dict, functions_info_path: str):
        self.manager = WorkerSPManager(ip_addr, config.GATEWAY_URL,
                                       workflows_info_path, functions_info_path)

    def get_state(self, request_id):
        return self.manager.get_state(request_id)

    # def trigger_function(self, state, function_name):
    #     self.manager.trigger_function(state, function_name)

    def receive_incoming_data(self, request_id, workflow_name, template_name, block_name, datas: dict,
                              from_local: bool, from_virtual=None):
        self.manager.receive_incoming_data(request_id, workflow_name, template_name, block_name, datas, from_local,
                                           from_virtual)

    def receive_incoming_request(self, request_id, workflow_name, templates_info):
        self.manager.init_incoming_request(request_id, workflow_name, templates_info)


print(config.WORKFLOWS_INFO_PATH)
print(config.FUNCTIONS_INFO_PATH)
# dispatcher = Dispatcher(config.WORKFLOWS_INFO_PATH, config.FUNCTIONS_INFO_PATH)

gc_interval = 20


def regular_clear_gc():
    gevent.spawn_later(gc_interval, regular_clear_gc)
    gc.collect()


def socket_server():
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', 5999))
    s.listen(100)
    while True:
        c, addr = s.accept()
        client_data = c.recv(64 * 1024)
        c.close()
        # time.sleep(0.005)
        data = json.loads(client_data)
        dispatcher.receive_incoming_data(data['request_id'], data['workflow_name'], data['template_name'],
                                         data['block_name'], data['datas'], from_local=True)



@app.route('/commit_inter_data', methods=['POST'])
def handle_inter_data_commit():
    data = request.get_json(force=True, silent=True)

    print(f"🏁 FINISHED BLOCK: {data['template_name']}.{data['block_name']}")
    workflow_info = dispatcher.manager.workflows_info[data['workflow_name']]

    # Look up successors
    block_key = (data['template_name'], data['block_name'])
    successors = workflow_info.block_successors.get(block_key, {})

    if not successors:  # ✅ No successors → final block
        print(f"🎯 FINAL BLOCK REACHED: {data['template_name']}.{data['block_name']}")
        
        result_data = {
            'latency': time.time() - data['st'],   # or use measured exec time
            'result': f"{data['workflow_name']}_completed",
            'status': 'success',
            'timestamp': time.time()
        }
        send_result_to_gateway(data['request_id'], result_data)

    # Normal dispatching
    dispatcher.receive_incoming_data(
        data['request_id'], data['workflow_name'],
        data['template_name'], data['block_name'],
        data['datas'], from_local=True
    )
    return 'OK', 200

@app.route('/transfer_data', methods=['POST'])
def transfer_data():
    try:
        data = request.get_json(force=True, silent=True)
        from_virtual = None
        if 'from_virtual' in data:
            from_virtual = data['from_virtual']
        
        print(f"📥 TRANSFER_DATA: {data.get('request_id')} - {data.get('template_name')}.{data.get('block_name')}")
        
        dispatcher.receive_incoming_data(
            data['request_id'], data['workflow_name'], data['template_name'],
            data['block_name'], data['datas'], from_local=False, from_virtual=from_virtual
        )
        
        return json.dumps({'status': 'ok'})
        
    except Exception as e:
        print(f"❌ ERROR in transfer_data: {e}")
        import traceback
        traceback.print_exc()
        return json.dumps({'status': 'error', 'message': str(e)}), 500



@app.route('/request_info', methods=['POST'])
def req():
    data = request.get_json(force=True, silent=True)
    print(f"Received request_info: {data}")
    request_id = data['request_id']
    workflow_name = data['workflow_name']
    templates_info = data['templates_info']
    dispatcher.receive_incoming_request(request_id, workflow_name, templates_info)
    return json.dumps({'status': 'ok'})


@app.route('/test_send_data', methods=['POST'])
def test_send_data():
    data = request.get_json(force=True, silent=True)
    print(type(data))
    return json.dumps({'status': 'ok'})


@app.route('/clear', methods=['POST'])
def clear():
    file_controller.init(config.FILE_CONTROLLER_PATH)
    prefetcher.init(config.PREFETCH_POOL_PATH)
    global dispatcher
    dispatcher = Dispatcher(config.WORKFLOWS_INFO_PATH, config.FUNCTIONS_INFO_PATH)
    time.sleep(10)
    return json.dumps({'status': 'ok'})


@app.route('/finish', methods=['POST'])
def finish():
    dispatcher.manager.flow_monitor.upload_all_logs()
    return 'OK', 200

@app.before_request
def log_request_info():
    print(f"\nReceived {request.method} request for {request.path}")

@app.after_request
def log_response_info(response):
    print(f"Request completed. Response status: {response.status}")
    return response

def send_result_to_gateway(request_id, result_data, gateway_ip='127.0.0.1', gateway_port=7000):
    """Send workflow results back to gateway"""
    gateway_url = f'http://{gateway_ip}:{gateway_port}/post_user_data'
    
    payload = {
        'request_id': request_id,
        'datas': result_data
    }
    
    print(f"Sending result to gateway: {gateway_url}")
    print(f"Result payload: {payload}")
    
    try:
        response = requests.post(gateway_url, json=payload, timeout=10)
        print(f"Gateway response: {response.status_code}")
        if response.status_code == 200:
            print(f"SUCCESS: Result sent to gateway for {request_id}")
        else:
            print(f"ERROR: Gateway rejected result for {request_id}: {response.text}")
    except Exception as e:
        print(f"ERROR: Failed to send result to gateway for {request_id}: {e}")

def store_transfer_result_to_db(request_id, workflow_name, template_name, block_name, execution_time, input_datas):
    """Store transfer data execution results in results database"""
    from src.workflow_manager.repository import Repository
    repo = Repository()
    
    try:
        current_time = time.time()
        
        # Get or create result document
        if request_id in repo.couchdb['results']:
            result_doc = repo.couchdb['results'][request_id]
        else:
            result_doc = {'_id': request_id}
        
        # Initialize arrays if not exists
        if 'transfers' not in result_doc:
            result_doc['transfers'] = []
        if 'executions' not in result_doc:
            result_doc['executions'] = []
        
        # Add transfer execution details
        transfer_record = {
            'template_name': template_name,
            'block_name': block_name,
            'execution_time': execution_time,
            'timestamp': current_time,
            'input_data_keys': list(input_datas.keys()) if input_datas else [],
            'data_size': len(str(input_datas)) if input_datas else 0,
            'status': 'completed',
            'event': 'data_transfer'
        }
        
        result_doc['transfers'].append(transfer_record)
        
        # Update document metadata
        result_doc.update({
            'workflow_name': workflow_name,
            'last_updated': current_time,
            'total_transfers': len(result_doc['transfers']),
            'total_transfer_time': sum(transfer['execution_time'] for transfer in result_doc['transfers']),
            'status': 'processing'
        })
        
        # # Generate workflow-specific results (like your mock worker)
        # if workflow_name == 'wordcount':
        #     result_doc['workflow_results'] = generate_wordcount_results(request_id, input_datas)
        
        # Save to database
        repo.couchdb['results'][request_id] = result_doc
        print(f"✅ Stored transfer result for {request_id} - {template_name}.{block_name}")
        
    except Exception as e:
        print(f"❌ Error storing transfer result for {request_id}: {e}")

def store_transfer_error_to_db(request_id, workflow_name, template_name, block_name, execution_time, error_msg):
    """Store transfer error in results database"""
    from src.workflow_manager.repository import Repository
    repo = Repository()
    
    try:
        current_time = time.time()
        
        # Get or create result document
        if request_id in repo.couchdb['results']:
            result_doc = repo.couchdb['results'][request_id]
        else:
            result_doc = {'_id': request_id}
        
        # Add error details
        if 'errors' not in result_doc:
            result_doc['errors'] = []
        
        error_record = {
            'template_name': template_name,
            'block_name': block_name,
            'execution_time': execution_time,
            'timestamp': current_time,
            'error': error_msg,
            'status': 'failed',
            'event': 'transfer_error'
        }
        
        result_doc['errors'].append(error_record)
        result_doc.update({
            'workflow_name': workflow_name,
            'last_updated': current_time,
            'status': 'failed'
        })
        
        # Save to database
        repo.couchdb['results'][request_id] = result_doc
        print(f"❌ Stored transfer error for {request_id}")
        
    except Exception as e:
        print(f"❌ Error storing transfer error for {request_id}: {e}")

def generate_wordcount_results(request_id, input_datas):
    """Generate realistic wordcount results (similar to mock worker)"""
    import random
    
    sample_words = ['the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by']
    word_counts = {}
    
    for word in sample_words:
        word_counts[word] = random.randint(1, 100)
    
    return {
        'word_counts': word_counts,
        'total_words': sum(word_counts.values()),
        'unique_words': len(word_counts),
        'processed_by': request_id,
        'input_data': str(input_datas)[:100] + '...' if len(str(input_datas)) > 100 else str(input_datas)
    }

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(f"Usage: python -m src.workflow_manager.proxy <ip_addr> <port>")
        sys.exit(1)

    ip_addr = sys.argv[1]
    port = int(sys.argv[2])

    dispatcher = Dispatcher(ip_addr, config.WORKFLOWS_INFO_PATH, config.FUNCTIONS_INFO_PATH)

    print(f'Starting the proxy, listening on port {port}...')
    gc.disable()
    gevent.spawn_later(gc_interval, regular_clear_gc)
    file_controller.init(config.FILE_CONTROLLER_PATH)
    prefetcher.init(config.PREFETCH_POOL_PATH)
    gevent.spawn(socket_server)
    server = WSGIServer(('0.0.0.0', int(sys.argv[2])), app, log=None)
    server.serve_forever()
    print(f'Server started on port {int(sys.argv[2])}')