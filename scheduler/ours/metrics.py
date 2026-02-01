from typing import Dict, List

import numpy as np


class Metrics:
    """
    Metrics collection and reporting for workflow executions.
    Stores latency and resource usage for all task invocations.
    """

    def __init__(self):
        self.latencies: List[float] = []
        self.resources: List[float] = []

    def collect(self, exec_results: List[Dict[str, float]]):
        """
        Collect metrics from a batch of execution results.
        Each result should include keys: "latency" and "resource".
        """
        for r in exec_results:
            if "latency" in r:
                self.latencies.append(r["latency"])
            if "resource" in r:
                self.resources.append(r["resource"])

    def report(self) -> Dict[str, float]:
        """
        Generate a summary report of collected metrics.
        Returns:
            - avg_latency: average execution latency
            - tail_latency: 99th percentile latency
            - resource_usage: total resource consumption
        """
        if not self.latencies:
            return {"avg_latency": 0.0, "tail_latency": 0.0, "resource_usage": 0.0}

        avg = float(np.mean(self.latencies))
        tail = float(np.percentile(self.latencies, 99))
        res = float(np.sum(self.resources)) if self.resources else 0.0

        return {
            "avg_latency": avg,
            "tail_latency": tail,
            "resource_usage": res
        }

    def clear(self):
        """
        Reset all collected metrics (start fresh).
        """
        self.latencies.clear()
        self.resources.clear()
