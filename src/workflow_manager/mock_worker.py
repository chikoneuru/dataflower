# mock_worker.py
from flask import Flask, request, jsonify
import json
import time
import random
import traceback


# Import your repository to write to database
import sys
import os
from src.workflow_manager.repository import Repository

app = Flask(__name__)

repo = Repository()

@app.route('/request_info', methods=['POST'])
def request_info():
    try:
        print("Mock worker received request_info:")
        data = request.get_json()
        if data:
            print(json.dumps(data, indent=2))
        else:
            print("No JSON data received")
        return jsonify({'status': 'OK', 'message': 'request_info received'}), 200
    except Exception as e:
        print(f"Error in request_info: {e}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/transfer_data', methods=['POST'])
def transfer_data():
    try:
        print("Mock worker received transfer_data:")
        data = request.get_json()
        
        if data and 'request_id' in data:
            request_id = data['request_id']
            workflow_name = data.get('workflow_name', 'wordcount')
            
            # Generate realistic mock execution logs
            current_time = time.time()
            mock_logs = [
                {
                    'phase': 'use_container',
                    'request_id': request_id,
                    'template_name': 'wordcount',
                    'block_name': 'start',
                    'time': random.uniform(0.001, 0.003),
                    'st': current_time,
                    'ed': current_time + random.uniform(0.001, 0.003),
                    'cpu': random.uniform(0.3, 0.7),
                    'memory': random.uniform(50, 100)
                },
                {
                    'phase': 'use_container',
                    'request_id': request_id,
                    'template_name': 'wordcount',
                    'block_name': 'count', 
                    'time': random.uniform(0.002, 0.008),
                    'st': current_time + 0.003,
                    'ed': current_time + 0.003 + random.uniform(0.002, 0.008),
                    'cpu': random.uniform(0.6, 0.9),
                    'memory': random.uniform(80, 150)
                },
                {
                    'phase': 'use_container',
                    'request_id': request_id,
                    'template_name': 'wordcount',
                    'block_name': 'merge',
                    'time': random.uniform(0.001, 0.004),
                    'st': current_time + 0.008,
                    'ed': current_time + 0.008 + random.uniform(0.001, 0.004),
                    'cpu': random.uniform(0.2, 0.5),
                    'memory': random.uniform(40, 80)
                }
            ]
            
            # Store execution logs in workflow_latency database
            try:
                print(f"Storing {len(mock_logs)} mock logs for {request_id} in workflow_latency database...")
                
                for log in mock_logs:
                    repo.save_latency(log)
                
                print(f"✓ Successfully stored execution logs for {request_id}")
                
            except Exception as e:
                print(f"✗ Error storing logs to workflow_latency database: {e}")
                traceback.print_exc()
            
            # Generate realistic workflow results
            workflow_results = generate_workflow_results(workflow_name, request_id)
            
            # Store results in results database
            try:
                print(f"Storing workflow results for {request_id} in results database...")
                
                total_execution_time = sum(log['time'] for log in mock_logs)
                
                result_doc = {
                    '_id': request_id,
                    'workflow_name': workflow_name,
                    'execution_time': total_execution_time,
                    'timestamp': current_time,
                    'status': 'completed',
                    'results': workflow_results,
                    'resource_usage': {
                        'total_cpu_time': sum(log['time'] * log['cpu'] for log in mock_logs),
                        'peak_memory': max(log['memory'] for log in mock_logs),
                        'total_execution_time': total_execution_time
                    }
                }
                
                # Try multiple methods to store results
                try:
                    repo.results_db.save(result_doc)
                    print(f"✓ Successfully stored results for {request_id}")
                except Exception as e:
                    print(f"Method 1 failed: {e}")
                    try:
                        repo.couchdb['results'][request_id] = result_doc
                        print(f"✓ Successfully stored results using alternative method for {request_id}")
                    except Exception as e2:
                        print(f"✗ All storage methods failed: {e2}")
                
            except Exception as e:
                print(f"✗ Error storing results to database: {e}")
                traceback.print_exc()
            
            # Send result back to gateway
            result_data = {
                'request_id': request_id,
                'datas': workflow_results
            }
            
            try:
                import requests
                gateway_response = requests.post(
                    'http://127.0.0.1:7000/post_user_data',
                    json=result_data,
                    timeout=5
                )
                print(f"Sent result to gateway: {gateway_response.status_code}")
            except Exception as e:
                print(f"Failed to send result to gateway: {e}")
        
        return jsonify({'status': 'OK', 'message': 'transfer_data received'}), 200
        
    except Exception as e:
        print(f"Error in transfer_data: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

def generate_workflow_results(workflow_name, request_id):
    """Generate realistic results based on workflow type"""
    
    if workflow_name == 'wordcount':
        sample_words = ['the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by']
        word_counts = {}
        
        for word in sample_words:
            word_counts[word] = random.randint(1, 100)
        
        return {
            'word_counts': word_counts,
            'total_words': sum(word_counts.values()),
            'unique_words': len(word_counts),
            'processed_by': request_id
        }
    else:
        return {
            'status': 'completed',
            'execution_details': f'Mock execution of {workflow_name}',
            'processed_by': request_id,
            'mock_data': True
        }

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({'status': 'Mock worker is running'}), 200

@app.route('/clear', methods=['POST'])
def clear():
    print("Mock worker received clear request")
    return jsonify({'status': 'cleared'}), 200

@app.route('/finish', methods=['POST'])  
def finish():
    print("Mock worker received finish request")
    return jsonify({'status': 'finished'}), 200

if __name__ == '__main__':
    print("Starting mock worker on http://127.0.0.1:8000")
    app.run(host='127.0.0.1', port=8000, debug=True)