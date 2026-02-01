# In template, functions are across different workflows.
import os.path

KAFKA_IP = '127.0.0.1'
GATEWAY_IP = '127.0.0.1'
COUCHDB_IP = '127.0.0.1'
WORKER_ADDRS = ['127.0.0.1']

# Original Config
# KAFKA_IP = '10.10.1.2'
# GATEWAY_IP = '10.10.1.1'
# COUCHDB_IP = '10.10.1.2'
# WORKER_ADDRS = ['10.10.1.3', '10.10.1.4', '10.10.1.5']

COUCHDB_URL = f'http://openwhisk:openwhisk@{COUCHDB_IP}:5984/'
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
REDIS_DB = 0
# RESOURCE_MONITOR_URL = 'http://127.0.0.1:7998/{}'
KAFKA_URL = f'{KAFKA_IP}:9092'
PREFETCHER_URL = 'http://127.0.0.1:8002/{}'
GATEWAY_URL = f'{GATEWAY_IP}:7000'

FASSFLOW_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(f"FASSFLOW_ROOT: {FASSFLOW_ROOT}")
FUNCTIONS_INFO_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../benchmark'))
WORKFLOWS_INFO_PATH = {
                       'video': os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'benchmark/video')),
                       'wordcount': os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'benchmark/wordcount')),
                       'recognizer': os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'benchmark/recognizer')),
                       'svd': os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'benchmark/svd'))}
if os.path.exists('/state/partition2/FaaSFlow'):
    PREFETCH_POOL_PATH = '/state/partition2/FaaSFlow/prefetch_pool'
    FILE_CONTROLLER_PATH = '/state/partition2/FaaSFlow/file_controller'
else:
    PREFETCH_POOL_PATH = os.path.join(FASSFLOW_ROOT, 'prefetch_pool')
    FILE_CONTROLLER_PATH = os.path.join(FASSFLOW_ROOT, 'file_controller')
CHUNK_SIZE = 1 * 1024 * 1024

DOCKER_CPU_QUOTA = 100000

REDIS_EXPIRE_SECONDS = 100
# COLLECT_CONTAINER_RESOURCE = False
KAFKA_CHUNK_TEST = False
DISABLE_PRESSURE_AWARE = False
