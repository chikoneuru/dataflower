import time
from collections import defaultdict

class Metrics:
    def __init__(self):
        self.invocation_count = defaultdict(int)
        self.success_count = defaultdict(int)
        self.failure_count = defaultdict(int)
        self.total_latency = defaultdict(float)
        self.cold_start_count = defaultdict(int)
        self.errors = defaultdict(lambda: defaultdict(int))
        self.last_invocation_time = defaultdict(float)

    def log_invocation(self, fn, container_id, start, end, cold_start=False, success=True, error_msg=None, response_size=None):
        latency = end - start
        self.invocation_count[fn] += 1
        self.total_latency[fn] += latency
        self.last_invocation_time[fn] = time.time()

        if success:
            self.success_count[fn] += 1
        else:
            self.failure_count[fn] += 1
            if error_msg:
                self.errors[fn][error_msg] += 1

        if cold_start:
            self.cold_start_count[fn] += 1

        # Optional: print/log here if you want

    def get_average_latency(self, fn):
        if self.invocation_count[fn] == 0:
            return 0
        return self.total_latency[fn] / self.invocation_count[fn]

    def get_summary(self):
        summary = {}
        for fn in self.invocation_count:
            summary[fn] = {
                "invocations": self.invocation_count[fn],
                "successes": self.success_count[fn],
                "failures": self.failure_count[fn],
                "cold_starts": self.cold_start_count[fn],
                "avg_latency": round(self.get_average_latency(fn), 4),
                "errors": dict(self.errors[fn]),
                "last_invoked": time.ctime(self.last_invocation_time[fn]),
            }
        return summary
