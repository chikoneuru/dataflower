import gevent

from config import config
import couchdb

couchdb_url = config.COUCHDB_URL


class Repository:
    def __init__(self):
        self.couchdb = couchdb.Server(couchdb_url)
        self.waiting_logs = []
        for db_name in self.couchdb:
            print(f"db_name: {db_name}")
        print("--------------------------------")
        if 'workflow_latency' not in self.couchdb:
            self.couchdb.create('workflow_latency')
            print("workflow_latency created")
        if 'results' not in self.couchdb:
            self.couchdb.create('results')
            print("results created")
        if 'logs' not in self.couchdb:
            self.couchdb.create('logs')
            print("logs created")

    def create_request_doc(self, request_id: str):
        if request_id in self.couchdb['results']:
            # self.couchdb['results'].delete(self.couchdb['results'][request_id])
            self.couchdb['results'].purge([self.couchdb['results'][request_id]])
        self.couchdb['results'][request_id] = {}

    def clear_couchdb_workflow_latency(self):
        self.couchdb.delete('workflow_latency')
        self.couchdb.create('workflow_latency')

    def clear_couchdb_results(self):
        self.couchdb.delete('results')
        self.couchdb.create('results')

    def clear_couchdb_logs(self):
        """Clear the logs database"""
        self.couchdb.delete('logs')
        self.couchdb.create('logs')

    def clear_all_couchdb(self):
        """Clear all databases (workflow_latency, results, and logs)"""
        self.clear_couchdb_workflow_latency()
        self.clear_couchdb_results()
        self.clear_couchdb_logs()
        print("All databases cleared and recreated")

    def save_scalability_config(self, dir_path):
        self.couchdb['results']['scalability_config'] = {'dir': dir_path}

    def save_kafka_config(self, KAFKA_CHUNK_SIZE):
        self.couchdb['results']['kafka_config'] = {'KAFKA_CHUNK_SIZE': KAFKA_CHUNK_SIZE}

    def get_kafka_config(self):
        try:
            return self.couchdb['results']['kafka_config']
        except Exception:
            return None

    def save_latency(self, log):
        # 🔍 DEBUG: Log every latency save
        print(f"💾 SAVING LATENCY: {log.get('request_id', 'unknown')} - {log.get('template_name', 'unknown')}.{log.get('block_name', 'unknown')}")
        print(f"   Phase: {log.get('phase', 'unknown')}, Time: {log.get('time', 0):.3f}s")
        
        # if '_id' not in log:
        #     log['_id'] = log['request_id']
        self.couchdb['workflow_latency'].save(log)

    def save_redis_log(self, request_id, size, time):
        self.waiting_logs.append({'phase': 'redis', 'request_id': request_id, 'size': size, 'time': time})
        # self.couchdb['workflow_latency'].save({'phase': 'redis', 'request_id': request_id, 'size': size, 'time': time})

    def save_log(self, log):
        # 🔍 DEBUG: Log every general log save
        print(f"📝 SAVING LOG: {log.get('request_id', 'unknown')} - {log.get('log_type', 'unknown')}")
        self.couchdb['logs'].save(log)

    def get_latencies(self, phase):
        requests_logs = {}
        for k in self.couchdb['workflow_latency']:
            doc = self.couchdb['workflow_latency'][k]
            if doc['phase'] == phase:
                request_id = doc['request_id']
                if request_id not in requests_logs:
                    requests_logs[request_id] = []
                requests_logs[request_id].append(doc)
        return requests_logs

    def upload_waiting_logs(self):
        tmp_db = self.couchdb['workflow_latency']
        for log in self.waiting_logs:
            tmp_db.save(log)
