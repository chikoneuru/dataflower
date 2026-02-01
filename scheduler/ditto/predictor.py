"""
Execution Time Predictor

Provides simple interfaces to build and query a per-stage execution time model
under different degrees of parallelism (DoP) and placements.

This is a lightweight scaffolding with pluggable hooks for profiling and
model fitting. Replace the placeholder logic with real implementations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Tuple


DoP = int
Placement = Any  # Replace with a structured placement type when available


@dataclass
class TimeModel:
    """Holds observed runtimes and provides estimation fallback."""

    observed_times_ms: Dict[Tuple[DoP, Placement], float]

    def predict(self, dop: DoP, placement: Placement) -> float:
        key = (dop, placement)
        if key in self.observed_times_ms:
            return float(self.observed_times_ms[key])
        # Simple fallback: nearest DoP with same placement, else global average
        same_placement = [t for (d, p), t in self.observed_times_ms.items() if p == placement]
        if same_placement:
            return float(sum(same_placement) / max(1, len(same_placement)))
        if self.observed_times_ms:
            values = list(self.observed_times_ms.values())
            return float(sum(values) / max(1, len(values)))
        return 0.0


class ExecutionTimePredictor:
    """
    Builds a simple per-stage time model from profiling data and exposes
    predict(stage, dop, placement) calls.
    """

    def __init__(self) -> None:
        self._stage_models: Dict[str, TimeModel] = {}

    def build_time_model(
        self,
        stage_id: str,
        dop_candidates: Iterable[DoP],
        placement_candidates: Iterable[Placement],
        profiler: "StageProfiler",
    ) -> TimeModel:
        """
        Profile runtime across (DoP, placement) and fit a simple lookup model.
        """
        observations: Dict[Tuple[DoP, Placement], float] = {}
        for dop in dop_candidates:
            for placement in placement_candidates:
                runtime_ms = profiler.profile(stage_id, dop, placement)
                observations[(dop, placement)] = float(runtime_ms)

        model = TimeModel(observed_times_ms=observations)
        self._stage_models[stage_id] = model
        return model

    def get_model(self, stage_id: str) -> TimeModel | None:
        return self._stage_models.get(stage_id)

    def predict_time(self, stage_id: str, dop: DoP, placement: Placement) -> float:
        model = self._stage_models.get(stage_id)
        if model is None:
            # Unknown stage; conservative fallback
            return 0.0
        
        prediction = model.predict(dop, placement)
        return prediction


class StageProfiler:
    """
    Abstract stage profiler.

    Implement profile(stage_id, dop, placement) to run a short job and return
    runtime in milliseconds. The default implementation is a placeholder.
    """

    def profile(self, stage_id: str, dop: DoP, placement: Placement) -> float:
        # TODO: Replace with real short-run profiling. For now, return a
        # realistic function that shows diminishing returns and overhead at high DoP
        base_ms = 1000.0
        
        # Diminishing returns: speedup = sqrt(dop) instead of linear
        speedup = max(1.0, float(dop) ** 0.5)
        # Overhead penalty for high DoP (coordination, memory contention, etc.)
        overhead_penalty = (float(dop) - 1) * 2.0  # 2ms overhead per additional worker
        # Placement penalty (small variation based on server)
        placement_penalty = (abs(hash(str(placement))) % 20) * 0.1
        return base_ms / speedup + overhead_penalty + placement_penalty


