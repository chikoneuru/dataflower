import time
from typing import Dict, List

import numpy as np


class Metrics:
    def __init__(self):
        self.records = []

    def collect(self, results: List[Dict]):
        """Collect raw per-task execution records"""
        self.records.extend(results)

    def _stats(self, arr: List[float]) -> Dict[str, float]:
        if not arr:
            return {"avg": 0.0, "tail": 0.0}
        return {
            "avg": float(np.mean(arr)),
            "tail": float(np.percentile(arr, 99))
        }

    def report(self) -> Dict:
        if not self.records:
            return {}

        # Extract arrays
        compute = [r["compute"] for r in self.records]
        cold = [r["cold_start"] for r in self.records]
        transfer = [r["transfer"] for r in self.records]
        interference = [r["interference"] for r in self.records]
        total = [r["total"] for r in self.records]
        overhead = [r.get("scheduler_overhead", 0) for r in self.records]
        queueing = [r.get("queue_delay", 0) for r in self.records]

        # Cold start count
        cold_count = sum(r.get("cold_start_count", 0) for r in self.records)

        # Locality / data transfer
        remote_bytes = sum(r.get("remote_bytes", 0) for r in self.records)  # MB
        local_bytes = sum(r.get("local_bytes", 0) for r in self.records)    # MB
        total_bytes = remote_bytes + local_bytes
        locality_fraction = local_bytes / total_bytes if total_bytes > 0 else 1.0

        # Resource usage
        resource_usage = sum(r.get("resource", 0) for r in self.records)
        cpu_usage = np.mean([r.get("cpu_util", 0) for r in self.records]) if any("cpu_util" in r for r in self.records) else 0
        mem_usage = np.mean([r.get("mem_util", 0) for r in self.records]) if any("mem_util" in r for r in self.records) else 0

        # Throughput: workflows/sec
        workflow_ids = set(r.get("workflow_id") for r in self.records)
        workflow_start = min(r.get("workflow_start", time.time()) for r in self.records)
        workflow_end = max(r.get("workflow_end", time.time()) for r in self.records)
        elapsed = max(workflow_end - workflow_start, 1e-9)
        throughput = len(workflow_ids) / elapsed

        return {
            "latency": {
                "compute_ms": self._stats(compute),
                "cold_start_ms": self._stats(cold),
                "transfer_ms": self._stats(transfer),
                "interference_ms": self._stats(interference),
                "scheduler_overhead_ms": self._stats(overhead),
                "queueing_delay_ms": self._stats(queueing),
                "end_to_end_ms": self._stats(total),
                "cold_start_count": cold_count
            },
            "throughput_wf_per_s": throughput,
            "remote_bytes_MB": remote_bytes,
            "locality_fraction": locality_fraction,
            "resource": {
                "usage": resource_usage,
                "cpu_avg_percent": cpu_usage * 100.0,   # fraction → %
                "mem_avg_GB": mem_usage
            }
        }
