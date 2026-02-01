import os
import sys
import subprocess
import time
import uuid
import yaml
from typing import List


from flask import Flask, request
from config import config

proxy = Flask(__name__)
processes = [None, None, None]


def sensitivity_set(cpu):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.abspath(os.path.join(script_dir, '../../benchmark/templates_info.yaml'))
    with open(file_path, 'r') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    for entry in data['templates']:
        if 'wordcount' in entry['template_name']:
            entry['cpus'] = cpu
    with open(file_path, 'w') as f:
        yaml.dump(data, f)


@proxy.route('/clear', methods=['post'])
def start():
    global processes
    for p in processes:
        if p is not None:
            p.kill()

    inp = request.get_json(force=True, silent=True)
    wc_cpu = 0.2
    if inp is not None:
        wc_cpu = inp['wc_cpu']
    sensitivity_set(wc_cpu)

    # Remove existing Docker containers
    print('Removing existing Docker containers...')
    os.system('docker rm -f $(docker ps -aq --filter label=workflow)')
    # os.system('service docker restart')
    
    # Start CouchDB first (required by the system)
    print('Starting CouchDB...')
    os.system('docker run -itd -p 5984:5984 -e COUCHDB_USER=openwhisk -e COUCHDB_PASSWORD=openwhisk --name couchdb couchdb')
    time.sleep(10)  # Give CouchDB time to start
    # Start DB starter
    print('Creating DBs...')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        db_starter_path = os.path.abspath(os.path.join(script_dir, '../../scripts/couchdb_starter.py'))
        os.system(f'python "{db_starter_path}"')
    except Exception as e:
        print(f"❌ Error starting CouchDB: {e}")
    time.sleep(10)  # Give DB starter time to initialize
    
    # Start Kafka and Zookeeper BEFORE gateway (gateway needs Kafka to be running)
    print('Starting Kafka and Zookeeper...')
    kafka_compose_path = os.path.abspath(os.path.join(script_dir, '../../scripts/kafka'))
    docker_compose_file = os.path.join(kafka_compose_path, 'docker-compose.yml')
    print(f"docker_compose_file: {docker_compose_file}")
    os.system(f'docker compose -f "{docker_compose_file}" up -d')
    time.sleep(20)  # Give Kafka time to start properly
    
    # Check if Kafka broker is running
    import socket
    def is_kafka_running(host='localhost', port=9092, timeout=5):
        try:
            with socket.create_connection((host, port), timeout=timeout):
                print('Kafka broker is running.')
                return True
        except Exception as e:
            print(f'Kafka broker is NOT running: {e}')
            return False

    if not is_kafka_running():
        raise RuntimeError('Kafka broker failed to start. Please check docker-compose logs.')

    # Start Redis
    print('Starting Redis...')
    os.system('docker run -itd -p 6379:6379 --name redis redis')
    time.sleep(10)  # Give Redis more time to start


    time.sleep(5)
    print('Starting Proxy, Disk Reader, and Prefetcher...')   
    script_dir = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(script_dir, '..', '..'))
    
    port = 8000
    # Kill existing processes
    result = subprocess.run(
        ['lsof', '-i', f':{port}'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if result.stdout:  # Port is in use
        print(f"Port {port} in use — killing {process_pattern}...")
        subprocess.run(['pkill', '-f', 'src.workflow_manager.proxy'])
        subprocess.run(['pkill', '-f', 'src.workflow_manager.disk_reader'])
        subprocess.run(['pkill', '-f', 'src.workflow_manager.prefetcher'])
    
    subprocess.Popen(
        ['python3', '-m', 'src.workflow_manager.proxy', addr, '8000'],
        cwd=PROJECT_ROOT
    )
    # processes[0] = subprocess.Popen(['python3', os.path.join(script_dir, 'proxy.py'), addr, '8000'])
    time.sleep(20)
    processes[1] = subprocess.Popen(
        ['python3', '-m', 'src.workflow_manager.disk_reader'],
        cwd=PROJECT_ROOT
    )

    processes[2] = subprocess.Popen(
        ['python3', '-m', 'src.workflow_manager.prefetcher'],
        cwd=PROJECT_ROOT
    )
    time.sleep(1)
    return 'OK', 200


if __name__ == '__main__':
    addr = sys.argv[1]
    proxy.run('0.0.0.0', 7999, threaded=True)
