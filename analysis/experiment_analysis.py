"""
Experiment Results Loader and Summarizer.

This script discovers and reads experiment outputs for various scenarios:
- results/<experiment_id>_results.json
- results/timelines/<experiment_id>_timelines.json
- results/timeseries/<experiment_id>_timeseries.csv

Supported scenarios:
- baseline-cleannet
- vary-input (concurrent)
- vary-input-seq (sequential)
- vary-concurrency
- vary-firingrate
- burst-spike
- small-msgs

Usage:
  python -m analysis.experiment_analysis
  python -m analysis.experiment_analysis --scenario vary-input-seq
  python -m analysis.experiment_analysis --results-dir results --out-dir analysis

No external dependencies required for basic functionality.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# Optional plotting dependencies
try:
    import matplotlib.pyplot as plt  # type: ignore
    import seaborn as sns  # type: ignore
    _PLOTTING_AVAILABLE = True
except Exception:
    _PLOTTING_AVAILABLE = False


BASELINE_PREFIX = "baseline-cleannet_"


@dataclass
class BaselineFileSet:
    experiment_id: str
    scheduler_type: str
    results_path: str
    timelines_path: Optional[str]
    timeseries_path: Optional[str]


def _safe_load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _safe_read_head(path: str, max_rows: int = 5) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                rows.append(row)
                if i + 1 >= max_rows:
                    break
    except FileNotFoundError:
        pass
    return rows


def discover_baseline_files(results_dir: str = "results") -> List[BaselineFileSet]:
    """Find available Baseline-CleanNet outputs for all schedulers present."""
    timelines_dir = os.path.join(results_dir, "timelines")
    timeseries_dir = os.path.join(results_dir, "timeseries")

    candidates: List[str] = []
    try:
        for name in os.listdir(results_dir):
            if name.startswith(BASELINE_PREFIX) and name.endswith("_results.json"):
                candidates.append(os.path.join(results_dir, name))
    except FileNotFoundError:
        return []

    filesets: List[BaselineFileSet] = []
    for results_path in sorted(candidates):
        filename = os.path.basename(results_path)
        experiment_id = filename.replace("_results.json", "")
        # experiment_id = baseline-cleannet_<scheduler>
        if not experiment_id.startswith(BASELINE_PREFIX):
            continue
        scheduler_type = experiment_id[len(BASELINE_PREFIX) :]

        timelines_path = os.path.join(timelines_dir, f"{experiment_id}_timelines.json")
        if not os.path.exists(timelines_path):
            timelines_path = None

        timeseries_path = os.path.join(timeseries_dir, f"{experiment_id}_timeseries.csv")
        if not os.path.exists(timeseries_path):
            timeseries_path = None

        filesets.append(
            BaselineFileSet(
                experiment_id=experiment_id,
                scheduler_type=scheduler_type,
                results_path=results_path,
                timelines_path=timelines_path,
                timeseries_path=timeseries_path,
            )
        )

    return filesets


def _scenario_prefix_map(name: str) -> Optional[str]:
    key = name.strip().lower().replace(" ", "-")
    mapping = {
        "baseline-cleannet": "baseline-cleannet_",
        "vary-concurrency": "vary-concurrency_",
        "vary-firingrate": "vary-firingrate_",
        "vary-input": "vary-input_",
        "vary-input-seq": "vary-input-seq_",
        "burst-spike": "burst-spike_",
        "small-msgs": "small-msgs_",
    }
    return mapping.get(key)


def _scenario_variable_key(name: str) -> Optional[str]:
    key = name.strip().lower().replace(" ", "-")
    mapping = {
        "vary-concurrency": "concurrency_level",
        "vary-firingrate": "request_rate_per_sec",
        "vary-input": "input_size_mb",
        "vary-input-seq": "input_size_mb",
        "small-msgs": "input_size_mb",
        "baseline-cleannet": None,
        "burst-spike": None,  # multi-varied; skip series for now
    }
    return mapping.get(key)


def discover_scenario_files(results_dir: str, scenario: str) -> List[BaselineFileSet]:
    prefix = _scenario_prefix_map(scenario)
    if not prefix:
        return []
    timelines_dir = os.path.join(results_dir, "timelines")
    timeseries_dir = os.path.join(results_dir, "timeseries")

    filesets: List[BaselineFileSet] = []
    try:
        for name in os.listdir(results_dir):
            if name.startswith(prefix) and name.endswith("_results.json"):
                results_path = os.path.join(results_dir, name)
                experiment_id = name.replace("_results.json", "")
                # Extract scheduler as the token after prefix
                remainder = experiment_id[len(prefix):]
                scheduler_type = remainder.split("_", 1)[0] if remainder else "unknown"
                timelines_path = os.path.join(timelines_dir, f"{experiment_id}_timelines.json")
                if not os.path.exists(timelines_path):
                    timelines_path = None
                timeseries_path = os.path.join(timeseries_dir, f"{experiment_id}_timeseries.csv")
                if not os.path.exists(timeseries_path):
                    timeseries_path = None
                filesets.append(BaselineFileSet(
                    experiment_id=experiment_id,
                    scheduler_type=scheduler_type,
                    results_path=results_path,
                    timelines_path=timelines_path,
                    timeseries_path=timeseries_path,
                ))
    except FileNotFoundError:
        return []
    return sorted(filesets, key=lambda fs: fs.experiment_id)


def _mean_ignore_none(values: List[Optional[float]]) -> Optional[float]:
    nums = [float(v) for v in values if isinstance(v, (int, float))]
    return (sum(nums) / float(len(nums))) if nums else None


def _aggregate_per_scheduler(
    res_summaries_by_sched: Dict[str, List[Dict[str, Any]]],
    tl_summaries_by_sched: Dict[str, List[Dict[str, Any]]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    agg_results: Dict[str, Dict[str, Any]] = {}
    agg_timelines: Dict[str, Dict[str, Any]] = {}
    for sched, lst in res_summaries_by_sched.items():
        agg_results[sched] = {
            "func_exec_avg_ms": _mean_ignore_none([x.get("func_exec_avg_ms") for x in lst]),
            "network_delay_avg_ms": _mean_ignore_none([x.get("network_delay_avg_ms") for x in lst]),
            "queuing_delay_avg_ms": _mean_ignore_none([x.get("queuing_delay_avg_ms") for x in lst]),
            "e2e_latency_avg_ms": _mean_ignore_none([x.get("e2e_latency_avg_ms") for x in lst]),
            "sched_overhead_avg_ms": _mean_ignore_none([x.get("sched_overhead_avg_ms") for x in lst]),
            "init_overhead_duration_ms": _mean_ignore_none([x.get("init_overhead_duration_ms") for x in lst]),
        }
    for sched, lst in tl_summaries_by_sched.items():
        agg_timelines[sched] = {
            "avg_scheduling_overhead_ms": _mean_ignore_none([x.get("avg_scheduling_overhead_ms") for x in lst]),
            "avg_workflow_execution_ms": _mean_ignore_none([x.get("avg_workflow_execution_ms") for x in lst]),
        }
    return agg_results, agg_timelines


def summarize_results(results_obj: Any) -> Dict[str, Any]:
    """Extract a concise summary from the results JSON structure."""
    # Results JSON is an array with one experiment object
    if not isinstance(results_obj, list) or not results_obj:
        return {"error": "results.json has unexpected structure"}

    exp = results_obj[0]
    cfg = exp.get("config", {})
    function_results = exp.get("function_results", [])

    summary: Dict[str, Any] = {
        "experiment_id": cfg.get("experiment_id"),
        "scheduler_type": cfg.get("scheduler_type"),
        "input_size_mb": cfg.get("input_size_mb"),
        "concurrency_level": cfg.get("concurrency_level"),
        "request_rate_per_sec": cfg.get("request_rate_per_sec"),
        "duration_sec": cfg.get("duration_sec"),
        "total_requests": cfg.get("total_requests"),
        "success_rate": exp.get("success_rate"),
        "avg_execution_time_ms": exp.get("avg_execution_time_ms"),
        "throughput_req_per_sec": exp.get("throughput_req_per_sec"),
        # Optional aggregate components if present (set by runner)
        "network_delay_avg_ms": exp.get("network_delay_avg_ms"),
        "queuing_delay_avg_ms": exp.get("queuing_delay_avg_ms"),
        "func_exec_avg_ms": exp.get("func_exec_avg_ms"),
        "e2e_latency_avg_ms": exp.get("e2e_latency_avg_ms"),
        "sched_overhead_avg_ms": exp.get("sched_overhead_avg_ms"),
        "init_overhead_duration_ms": exp.get("init_overhead_duration_ms"),
        "num_function_results": len(function_results),
    }

    # Per-function quick mean from stored execution_time_ms
    per_function: Dict[str, List[float]] = {}
    for fr in function_results:
        name = fr.get("function_name")
        t = fr.get("execution_time_ms")
        if name is None or t is None:
            continue
        per_function.setdefault(name, []).append(float(t))

    per_function_avg: Dict[str, float] = {}
    for name, vals in per_function.items():
        if vals:
            per_function_avg[name] = sum(vals) / float(len(vals))
    summary["per_function_avg_ms"] = per_function_avg

    return summary


def summarize_timelines(timelines_obj: Any) -> Dict[str, Any]:
    """Extract orchestration-level summary from timelines JSON if present."""
    if not isinstance(timelines_obj, dict):
        return {"error": "timelines.json has unexpected structure"}

    orch = timelines_obj.get("orchestrator_timelines", {})
    num_workflows = len(orch)
    sched_overheads: List[float] = []
    workflow_durations: List[float] = []

    for wf in orch.values():
        ts = wf.get("timestamps", {})
        wf_dur = ts.get("workflow_execution_duration_ms")
        if isinstance(wf_dur, (int, float)):
            workflow_durations.append(float(wf_dur))
        so = ts.get("scheduling_overhead_ms")
        if isinstance(so, (int, float)):
            sched_overheads.append(float(so))

    def _avg(xs: List[float]) -> Optional[float]:
        return (sum(xs) / float(len(xs))) if xs else None

    return {
        "num_workflows": num_workflows,
        "avg_workflow_execution_ms": _avg(workflow_durations),
        "avg_scheduling_overhead_ms": _avg(sched_overheads),
    }


def summarize_timeseries_head(rows: List[Dict[str, str]]) -> Dict[str, Any]:
    """Return a minimal preview of timeseries content (first few rows)."""
    return {
        "preview_rows": rows,
        "num_preview_rows": len(rows),
        "columns": list(rows[0].keys()) if rows else [],
    }


def _aggregate_timeseries_utilization(csv_path: str) -> Optional[Dict[str, float]]:
    """Aggregate average CPU percent and memory RSS MB from a timeseries CSV file.

    Returns { 'cpu_percent_avg': float, 'memory_rss_mb_avg': float } or None if unreadable/empty.
    """
    try:
        import csv as _csv
        cpu_vals: List[float] = []
        mem_vals: List[float] = []
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                try:
                    cpu = float(row.get('cpu_percent') or row.get(' cpu_percent') or 0.0)
                    mem = float(row.get('memory_rss_mb') or row.get(' memory_rss_mb') or 0.0)
                    cpu_vals.append(cpu)
                    mem_vals.append(mem)
                except Exception:
                    continue
        if not cpu_vals and not mem_vals:
            return None
        cpu_avg = (sum(cpu_vals) / float(len(cpu_vals))) if cpu_vals else 0.0
        mem_avg = (sum(mem_vals) / float(len(mem_vals))) if mem_vals else 0.0
        return {"cpu_percent_avg": cpu_avg, "memory_rss_mb_avg": mem_avg}
    except Exception:
        return None


def _ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


def _get_scheduler_colors(schedulers: List[str]) -> Dict[str, str]:
    """Get consistent color mapping for schedulers with 'ours' in red."""
    # Define color palette - red for 'ours', others in consistent order
    color_palette = ["#E15759", "#4C78A8", "#59A14F", "#F28E2B", "#B6992D", "#76B7B2"]
    
    # Sort schedulers with 'ours' first
    sorted_schedulers = sorted(schedulers, key=lambda s: (0 if str(s).lower() == "ours" else 1, str(s).lower()))
    
    # Map colors
    colors = {}
    for i, scheduler in enumerate(sorted_schedulers):
        colors[scheduler] = color_palette[i % len(color_palette)]
    
    return colors


def _derive_function_component_means(timelines_obj: Any) -> Dict[str, Dict[str, float]]:
    """Aggregate per-function derived timing components across all workflows.

    Returns a dict: { function_name: { 'exec_ms': avg, 'network_delay_ms': avg, 'network_return_ms': avg } }
    """
    if not isinstance(timelines_obj, dict):
        return {}
    orch = timelines_obj.get("orchestrator_timelines", {})
    accum: Dict[str, Dict[str, List[float]]] = {}
    for wf in orch.values():
        funcs = wf.get("functions", [])
        for entry in funcs:
            name = entry.get("function")
            derived = entry.get("derived", {})
            if not name or not isinstance(derived, dict):
                continue
            bucket = accum.setdefault(name, {"exec_ms": [], "network_delay_ms": [], "network_return_ms": []})
            for key in ("exec_ms", "network_delay_ms", "network_return_ms"):
                val = derived.get(key)
                if isinstance(val, (int, float)):
                    bucket[key].append(float(val))

    out: Dict[str, Dict[str, float]] = {}
    for name, comps in accum.items():
        out[name] = {}
        for key, values in comps.items():
            out[name][key] = (sum(values) / float(len(values))) if values else 0.0
    return out


def _plot_baseline_components(components_by_scheduler: Dict[str, Dict[str, Dict[str, float]]], out_dir: str) -> None:
    if not _PLOTTING_AVAILABLE:
        print("⚠️  Plotting libraries not available; skipping plots.")
        return
    _ensure_dir(out_dir)

    # Create a stacked bar per function for each scheduler
    for scheduler, comp in components_by_scheduler.items():
        if not comp:
            continue
        functions = sorted(comp.keys())
        exec_vals = [comp[f].get("exec_ms", 0.0) for f in functions]
        net_vals = [comp[f].get("network_delay_ms", 0.0) for f in functions]
        ret_vals = [comp[f].get("network_return_ms", 0.0) for f in functions]

        plt.figure(figsize=(10, 5))
        sns.set_style("whitegrid")
        x = range(len(functions))
        plt.bar(x, exec_vals, label="exec_ms")
        plt.bar(x, net_vals, bottom=exec_vals, label="network_delay_ms")
        bottom2 = [e + n for e, n in zip(exec_vals, net_vals)]
        plt.bar(x, ret_vals, bottom=bottom2, label="network_return_ms")
        plt.xticks(list(x), functions, rotation=30, ha="right")
        plt.ylabel("Average ms")
        plt.title(f"Baseline-CleanNet Latency Components ({scheduler})")
        plt.legend()
        out_path = os.path.join(out_dir, f"baseline_components_{scheduler}.png")
        plt.tight_layout()
        plt.savefig(out_path, dpi=200)
        plt.close()
        print(f"💾 Saved plot: {out_path}")


def _plot_orchestration_overhead(timeline_summaries: Dict[str, Dict[str, Any]], out_dir: str) -> None:
    if not _PLOTTING_AVAILABLE:
        return
    _ensure_dir(out_dir)
    # Exclude 'ditto'
    filtered = {k: v for k, v in timeline_summaries.items() if str(k).lower() != 'ditto'}
    schedulers = []
    values = []
    for scheduler, summ in filtered.items():
        val = summ.get("avg_scheduling_overhead_ms")
        if isinstance(val, (int, float)):
            schedulers.append(scheduler)
            values.append(float(val))
    if not values:
        return
    plt.figure(figsize=(6, 4))
    sns.set_style("whitegrid")
    colors = _get_scheduler_colors(schedulers)
    plt.bar(schedulers, values, color=[colors[s] for s in schedulers])
    plt.ylabel("Avg scheduling overhead (ms)")
    plt.title("Baseline-CleanNet: Orchestration Overhead")
    out_path = os.path.join(out_dir, "orchestration_overhead.png")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()
    print(f"💾 Saved plot: {out_path}")


def _plot_baseline_resource_utilization(util_by_scheduler: Dict[str, Dict[str, float]], out_dir: str) -> None:
    if not _PLOTTING_AVAILABLE:
        return
    _ensure_dir(out_dir)

    schedulers = sorted(util_by_scheduler.keys(), key=lambda s: (0 if s.lower() == "ours" else 1, s.lower()))
    cpu_vals = [float(util_by_scheduler[s].get('cpu_percent_avg') or 0.0) for s in schedulers]
    mem_vals = [float(util_by_scheduler[s].get('memory_rss_mb_avg') or 0.0) for s in schedulers]

    x = list(range(len(schedulers)))
    width = 0.35
    plt.figure(figsize=(7, 4))
    sns.set_style("whitegrid")
    colors = _get_scheduler_colors(schedulers)
    plt.bar([i - width/2 for i in x], cpu_vals, width=width, label="CPU % (avg)", color=[colors[s] for s in schedulers])
    plt.bar([i + width/2 for i in x], mem_vals, width=width, label="Memory RSS MB (avg)", color=[colors[s] for s in schedulers])
    plt.xticks(x, schedulers)
    plt.ylabel("Utilization")
    plt.title("Baseline-CleanNet: Avg Resource Utilization")
    plt.legend()
    out_path = os.path.join(out_dir, "resource_utilization.png")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()
    print(f"💾 Saved plot: {out_path}")


def _plot_scenario_resource_utilization_series(points: List[Dict[str, Any]], var_key: str, out_dir: str) -> None:
    if not _PLOTTING_AVAILABLE:
        return
    _ensure_dir(out_dir)
    import pandas as pd

    df = pd.DataFrame(points)
    if df.empty or var_key not in df.columns:
        return

    # Exclude 'ditto'
    # df = df[df['scheduler_type'].str.lower() != 'ditto']

    # Clean types
    df[var_key] = pd.to_numeric(df[var_key], errors='coerce')
    df['cpu_percent_avg'] = pd.to_numeric(df['cpu_percent_avg'], errors='coerce')
    df['memory_rss_mb_avg'] = pd.to_numeric(df['memory_rss_mb_avg'], errors='coerce')

    schedulers = sorted(df['scheduler_type'].dropna().unique(), key=lambda s: (0 if str(s).lower()=="ours" else 1, str(s).lower()))
    colors = _get_scheduler_colors(schedulers)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
    sns.set_style("whitegrid")

    # CPU plot
    for s in schedulers:
        d = df[df['scheduler_type'] == s].dropna(subset=[var_key, 'cpu_percent_avg']).sort_values(var_key)
        if not d.empty:
            ax1.plot(d[var_key], d['cpu_percent_avg'], marker='o', label=s, color=colors.get(s))
    ax1.set_title("CPU Utilization")
    ax1.set_xlabel(var_key.replace('_', ' '))
    ax1.set_ylabel("CPU % (avg)")
    ax1.legend()

    # Memory plot
    for s in schedulers:
        d = df[df['scheduler_type'] == s].dropna(subset=[var_key, 'memory_rss_mb_avg']).sort_values(var_key)
        if not d.empty:
            ax2.plot(d[var_key], d['memory_rss_mb_avg'], marker='o', label=s, color=colors.get(s))
    ax2.set_title("Memory Utilization")
    ax2.set_xlabel(var_key.replace('_', ' '))
    ax2.set_ylabel("Memory RSS MB (avg)")
    ax2.legend()

    fig.tight_layout()
    out_path = os.path.join(out_dir, f"resource_utilization_series_{var_key}.png")
    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"💾 Saved plot: {out_path}")


def _plot_scheduler_comparison_faceted(
    results_summaries: Dict[str, Dict[str, Any]],
    timeline_summaries: Dict[str, Dict[str, Any]],
    components_by_scheduler: Optional[Dict[str, Dict[str, Dict[str, float]]]],
    out_dir: str,
    util_by_scheduler: Optional[Dict[str, Dict[str, float]]] = None,
    scenario: str = "baseline",
) -> None:
    if not _PLOTTING_AVAILABLE:
        return
    _ensure_dir(out_dir)

    schedulers = sorted(
        set(list(results_summaries.keys()) + list(timeline_summaries.keys())),
        key=lambda s: (0 if s.lower() == "ours" else 1, s.lower())
    )
    # Exclude 'ditto'
    # schedulers = [s for s in schedulers if s.lower() != 'ditto']
    if not schedulers:
        return

    metrics = [
        ("Initialization overhead", "init_overhead_duration_ms", "init_fallback"),
        ("Function execution", "func_exec_avg_ms", None),
        ("Network delay", "network_delay_avg_ms", "net_fallback"),
        ("Scheduling overhead", "sched_overhead_avg_ms", "sched_fallback"),
        ("Workflow execution", "e2e_latency_avg_ms", "e2e_fallback"),
        ("Queuing delay", "queuing_delay_avg_ms", None),
    ]
    # Optionally add resource facets
    if util_by_scheduler:
        metrics.extend([
            ("CPU Utilization", "__cpu__", None),
            ("Memory Utilization", "__mem__", None),
        ])

    # Prepare values per metric per scheduler with fallbacks
    values: List[List[float]] = []
    for label, key, fallback in metrics:
        cur: List[float] = []
        for s in schedulers:
            v: Optional[float]
            if key == "__cpu__":
                v = (util_by_scheduler or {}).get(s, {}).get("cpu_percent_avg")  # type: ignore
            elif key == "__mem__":
                v = (util_by_scheduler or {}).get(s, {}).get("memory_rss_mb_avg")  # type: ignore
            else:
                v = results_summaries.get(s, {}).get(key)  # type: ignore
                if not isinstance(v, (int, float)):
                    if fallback == "sched_fallback":
                        v = timeline_summaries.get(s, {}).get("avg_scheduling_overhead_ms")
                    elif fallback == "e2e_fallback":
                        v = timeline_summaries.get(s, {}).get("avg_workflow_execution_ms")
                    elif fallback == "net_fallback" and components_by_scheduler:
                        comps = components_by_scheduler.get(s, {})
                        if comps:
                            vals = [vv.get("network_delay_ms", 0.0) for vv in comps.values()]
                            if vals:
                                v = sum(vals) / float(len(vals))
                    elif fallback == "init_fallback":
                        tl = timeline_summaries.get(s, {})
                        io = tl.get("initialization_overhead") if isinstance(tl, dict) else None
                        if isinstance(io, dict):
                            v = io.get("duration_ms")
            cur.append(float(v) if isinstance(v, (int, float)) else 0.0)
        values.append(cur)

    # Plot as 2 rows for better readability
    import math
    n_metrics = len(metrics)
    n_cols = 3
    n_rows = math.ceil(n_metrics / n_cols)
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(3.2 * n_cols, 3.6 * n_rows), sharex=False, sharey=False)
    axes = axes.flatten() if isinstance(axes, (list, tuple)) else axes.ravel()

    colors = _get_scheduler_colors(schedulers)
    for idx, ax in enumerate(axes[:n_metrics]):
        label = metrics[idx][0]
        vals = values[idx]
        bars = ax.bar(schedulers, vals, color=[colors[s] for s in schedulers])
        ax.set_title(label)
        ax.set_ylabel("ms" if "Utilization" not in label else ("CPU % (avg)" if label.startswith("CPU") else "Memory RSS MB (avg)"))
        ax.tick_params(axis='x')
        # Annotate values on bars for visibility when small
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2.0, b.get_height(), f"{v:.3f}", ha='center', va='bottom', fontsize=7)

    # Generate scenario-specific title
    scenario_titles = {
        "baseline": "Baseline Clean Network: Scheduler Comparison",
        "baseline-cleannet": "Baseline Clean Network: Scheduler Comparison",
        "vary-concurrency": "Varying Concurrency: Scheduler Comparison",
        "vary-firingrate": "Varying Firing Rate: Scheduler Comparison",
        "vary-input": "Varying Input Size: Scheduler Comparison",
        "vary-input-seq": "Sequential Varying Input Size: Scheduler Comparison",
        "burst-spike": "Burst Spike: Scheduler Comparison",
        "small-msgs": "Small Messages: Scheduler Comparison",
    }
    title = scenario_titles.get(scenario.lower(), f"{scenario.title()}: Scheduler Comparison (per metric)")
    fig.suptitle(title)
    fig.tight_layout(rect=[0, 0.02, 1, 0.95])
    out_path = os.path.join(out_dir, "scheduler_comparison_facets.png")
    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"💾 Saved plot: {out_path}")


def _plot_scenario_series(points: List[Dict[str, Any]], var_key: str, out_dir: str, scenario: str = "scenario") -> None:
    if not _PLOTTING_AVAILABLE:
        return
    _ensure_dir(out_dir)
    import pandas as pd  # local import to avoid hard dep at top if not used

    df = pd.DataFrame(points)
    if df.empty or var_key not in df.columns:
        return

    # Exclude 'ditto'
    # df = df[df['scheduler_type'].str.lower() != 'ditto']

    # Clean types
    df[var_key] = pd.to_numeric(df[var_key], errors='coerce')
    metric_cols = [
        ("Initialization overhead", "init_overhead_duration_ms"),
        ("Function execution", "func_exec_avg_ms"),
        ("Network delay", "network_delay_avg_ms"),
        ("Scheduling overhead", "sched_overhead_avg_ms"),
        ("Workflow execution", "e2e_latency_avg_ms"),
        ("Queuing delay", "queuing_delay_avg_ms"),
    ]

    # Add resource metrics if present
    if 'cpu_percent_avg' in df.columns or 'memory_rss_mb_avg' in df.columns:
        metric_cols.append(("CPU Utilization", "cpu_percent_avg"))
        metric_cols.append(("Memory Utilization", "memory_rss_mb_avg"))
        # Ensure numeric
        if 'cpu_percent_avg' in df.columns:
            df['cpu_percent_avg'] = pd.to_numeric(df['cpu_percent_avg'], errors='coerce')
        if 'memory_rss_mb_avg' in df.columns:
            df['memory_rss_mb_avg'] = pd.to_numeric(df['memory_rss_mb_avg'], errors='coerce')

    schedulers = sorted(df['scheduler_type'].dropna().unique(), key=lambda s: (0 if str(s).lower()=="ours" else 1, str(s).lower()))
    colors = _get_scheduler_colors(schedulers)

    import math
    n_cols = 3
    n_rows = math.ceil(len(metric_cols) / n_cols)
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(3.6 * n_cols, 3.2 * n_rows), sharex=False, sharey=False)
    axes = axes.flatten() if hasattr(axes, 'flatten') else [axes]

    for idx, (title, col) in enumerate(metric_cols):
        ax = axes[idx]
        sub = df[[var_key, 'scheduler_type', col]].dropna(subset=[var_key])
        # line per scheduler
        for s in schedulers:
            d = sub[sub['scheduler_type'] == s]
            if d.empty:
                continue
            d = d.sort_values(var_key)
            ax.plot(d[var_key], d[col], marker='o', label=s, color=colors.get(s))
        ax.set_title(title)
        ax.set_xlabel(var_key.replace('_', ' '))
        ylabel = 'ms'
        if title == 'CPU Utilization':
            ylabel = 'CPU % (avg)'
        elif title == 'Memory Utilization':
            ylabel = 'Memory RSS MB (avg)'
        ax.set_ylabel(ylabel)
    # put legend below
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='lower center', ncol=min(3, len(schedulers)))
    
    # Generate scenario-specific title
    scenario_titles = {
        "baseline": "Baseline Clean Network: Performance vs Parameter",
        "baseline-cleannet": "Baseline Clean Network: Performance vs Parameter",
        "vary-concurrency": "Varying Concurrency: Performance vs Concurrency Level",
        "vary-firingrate": "Varying Firing Rate: Performance vs Request Rate",
        "vary-input": "Varying Input Size: Performance vs Input Size",
        "vary-input-seq": "Sequential Varying Input Size: Performance vs Input Size",
        "burst-spike": "Burst Spike: Performance vs Parameter",
        "small-msgs": "Small Messages: Performance vs Input Size",
    }
    title = scenario_titles.get(scenario.lower(), f"{scenario.title()}: Performance vs {var_key.replace('_', ' ').title()}")
    fig.suptitle(title)
    
    fig.tight_layout(rect=[0, 0.08, 1, 0.95])
    out_path = os.path.join(out_dir, f"scenario_series_{var_key}.png")
    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"💾 Saved plot: {out_path}")

def run_baseline_load(results_dir: str = "results", out_dir: Optional[str] = None, no_plots: bool = False) -> int:
    filesets = discover_baseline_files(results_dir)
    if not filesets:
        print("❌ No Baseline-CleanNet results found.")
        return 1

    print(f"🔎 Found {len(filesets)} Baseline-CleanNet experiments:")
    for fs in filesets:
        print(f"  - {fs.experiment_id} ({fs.scheduler_type})")

    print("\n📥 Loading files and summarizing:\n")
    baseline_fig_dir = None
    if out_dir:
        baseline_fig_dir = os.path.join(out_dir, "figures", "baseline")
        _ensure_dir(baseline_fig_dir)

    # Collect for cross-scheduler plots
    components_by_scheduler: Dict[str, Dict[str, Dict[str, float]]] = {}
    timeline_summaries: Dict[str, Dict[str, Any]] = {}
    results_summaries: Dict[str, Dict[str, Any]] = {}
    util_by_scheduler: Dict[str, Dict[str, float]] = {}
    results_summaries: Dict[str, Dict[str, Any]] = {}
    for fs in filesets:
        print(f"▶ {fs.experiment_id}")
        # results
        try:
            results_obj = _safe_load_json(fs.results_path)
            res_summary = summarize_results(results_obj)
            print("  📄 results.json:")
            print(f"     success_rate={res_summary.get('success_rate')} avg_exec_ms={res_summary.get('avg_execution_time_ms')} throughput={res_summary.get('throughput_req_per_sec')}")
            pfa = res_summary.get("per_function_avg_ms", {})
            if isinstance(pfa, dict) and pfa:
                top = sorted(pfa.items(), key=lambda kv: kv[1], reverse=True)
                for name, avg_ms in top[:5]:
                    print(f"     func_avg {name}={avg_ms:.3f} ms")
            # cache per-scheduler results summary for comparison plots
            results_summaries[fs.scheduler_type] = res_summary
            results_summaries[fs.scheduler_type] = res_summary
        except Exception as e:
            print(f"  ❌ Failed to load results: {e}")

        # timelines
        if fs.timelines_path:
            try:
                timelines_obj = _safe_load_json(fs.timelines_path)
                tl_summary = summarize_timelines(timelines_obj)
                print("  📊 timelines.json:")
                print(f"     workflows={tl_summary.get('num_workflows')} avg_wf_ms={tl_summary.get('avg_workflow_execution_ms')} avg_sched_ms={tl_summary.get('avg_scheduling_overhead_ms')}")
                timeline_summaries[fs.scheduler_type] = tl_summary
                # Per-function components for plots
                comps = _derive_function_component_means(timelines_obj)
                components_by_scheduler[fs.scheduler_type] = comps
            except Exception as e:
                print(f"  ❌ Failed to load timelines: {e}")
        else:
            print("  ⚠️  timelines.json: not found")

        # timeseries
        if fs.timeseries_path:
            head_rows = _safe_read_head(fs.timeseries_path, max_rows=5)
            ts_preview = summarize_timeseries_head(head_rows)
            print("  📈 timeseries.csv:")
            print(f"     columns={ts_preview.get('columns')}")
            print(f"     preview_rows={ts_preview.get('num_preview_rows')}")
            agg = _aggregate_timeseries_utilization(fs.timeseries_path)
            if agg:
                # Accumulate per scheduler (average across available files)
                existing = util_by_scheduler.get(fs.scheduler_type)
                if existing:
                    # simple average of averages; acceptable for quick summary
                    existing['cpu_percent_avg'] = (existing.get('cpu_percent_avg', 0.0) + agg.get('cpu_percent_avg', 0.0)) / 2.0
                    existing['memory_rss_mb_avg'] = (existing.get('memory_rss_mb_avg', 0.0) + agg.get('memory_rss_mb_avg', 0.0)) / 2.0
                else:
                    util_by_scheduler[fs.scheduler_type] = agg
        else:
            print("  ⚠️  timeseries.csv: not found")

        print("")

    if not no_plots and baseline_fig_dir:
        # _plot_baseline_components(components_by_scheduler, baseline_fig_dir)
        # _plot_orchestration_overhead(timeline_summaries, baseline_fig_dir)
        _plot_scheduler_comparison_faceted(results_summaries, timeline_summaries, components_by_scheduler, baseline_fig_dir, util_by_scheduler, "baseline-cleannet")
        if util_by_scheduler:
            _plot_baseline_resource_utilization(util_by_scheduler, baseline_fig_dir)

    return 0


def run_scenario_load(scenario: str, results_dir: str = "results", out_dir: Optional[str] = None, no_plots: bool = False) -> int:
    filesets = discover_scenario_files(results_dir, scenario)
    if not filesets:
        print(f"❌ No results found for scenario '{scenario}'.")
        return 1

    # Filter out ditto experiments
    # filtered_filesets = [fs for fs in filesets if fs.scheduler_type.lower() != 'ditto']
    filtered_filesets = filesets
    print(f"🔎 Found {len(filtered_filesets)} experiments for scenario '{scenario}' (excluding ditto):")
    for fs in filtered_filesets:
        print(f"  - {fs.experiment_id} ({fs.scheduler_type})")

    fig_dir = None
    if out_dir:
        scen = scenario.strip().lower().replace(" ", "-")
        fig_dir = os.path.join(out_dir, "figures", scen)
        _ensure_dir(fig_dir)

    res_by_sched: Dict[str, List[Dict[str, Any]]] = {}
    tl_by_sched: Dict[str, List[Dict[str, Any]]] = {}
    components_by_scheduler: Dict[str, Dict[str, Dict[str, float]]] = {}
    series_points: List[Dict[str, Any]] = []
    util_by_scheduler: Dict[str, Dict[str, float]] = {}

    print("\n📥 Loading files and summarizing:\n")
    print(f"Found experiment result files: {len(filtered_filesets)}")
    for fs in filtered_filesets: 
        # results
        try:
            results_obj = _safe_load_json(fs.results_path)
            res_summary = summarize_results(results_obj)
            res_by_sched.setdefault(fs.scheduler_type, []).append(res_summary)
            # collect for series plotting
            series_points.append({
                "scheduler_type": res_summary.get("scheduler_type"),
                "concurrency_level": res_summary.get("concurrency_level"),
                "request_rate_per_sec": res_summary.get("request_rate_per_sec"),
                "input_size_mb": res_summary.get("input_size_mb"),
                "func_exec_avg_ms": res_summary.get("func_exec_avg_ms"),
                "network_delay_avg_ms": res_summary.get("network_delay_avg_ms"),
                "queuing_delay_avg_ms": res_summary.get("queuing_delay_avg_ms"),
                "sched_overhead_avg_ms": res_summary.get("sched_overhead_avg_ms"),
                "e2e_latency_avg_ms": res_summary.get("e2e_latency_avg_ms"),
                "init_overhead_duration_ms": res_summary.get("init_overhead_duration_ms"),
                "cpu_percent_avg": None,  # Will be filled from timeseries
                "memory_rss_mb_avg": None,  # Will be filled from timeseries
            })
        except Exception as e:
            print(f"  ❌ Failed to load results for {fs.experiment_id}: {e}")

        # timelines
        if fs.timelines_path:
            try:
                timelines_obj = _safe_load_json(fs.timelines_path)
                tl_summary = summarize_timelines(timelines_obj)
                tl_by_sched.setdefault(fs.scheduler_type, []).append(tl_summary)
                comps = _derive_function_component_means(timelines_obj)
                # keep last per scheduler for fallback use; fine since we aggregate across
                components_by_scheduler[fs.scheduler_type] = comps
            except Exception as e:
                print(f"  ❌ Failed to load timelines for {fs.experiment_id}: {e}")

        # timeseries
        if fs.timeseries_path:
            agg = _aggregate_timeseries_utilization(fs.timeseries_path)
            if agg:
                # Add to series points for this specific experiment
                for point in series_points:
                    if (point.get("scheduler_type") == fs.scheduler_type and 
                        point.get("concurrency_level") == res_summary.get("concurrency_level") and
                        point.get("request_rate_per_sec") == res_summary.get("request_rate_per_sec") and
                        point.get("input_size_mb") == res_summary.get("input_size_mb")):
                        point["cpu_percent_avg"] = agg.get("cpu_percent_avg")
                        point["memory_rss_mb_avg"] = agg.get("memory_rss_mb_avg")
                        break

    agg_results, agg_timelines = _aggregate_per_scheduler(res_by_sched, tl_by_sched)

    # Order schedulers with 'ours' first
    schedulers = sorted(
        set(agg_results.keys()) | set(agg_timelines.keys()),
        key=lambda s: (0 if str(s).lower() == "ours" else 1, str(s).lower())
    )
    # Filter dicts to only those schedulers (preserve order)
    agg_results = {s: agg_results.get(s, {}) for s in schedulers}
    agg_timelines = {s: agg_timelines.get(s, {}) for s in schedulers}

    # Build simple utilization avg by scheduler from series points
    util_sched: Dict[str, Dict[str, float]] = {}
    try:
        import statistics as _stats
        for s in schedulers:
            cpu_vals = [p.get('cpu_percent_avg') for p in series_points if p.get('scheduler_type') == s and isinstance(p.get('cpu_percent_avg'), (int, float))]
            mem_vals = [p.get('memory_rss_mb_avg') for p in series_points if p.get('scheduler_type') == s and isinstance(p.get('memory_rss_mb_avg'), (int, float))]
            if cpu_vals or mem_vals:
                util_sched[s] = {
                    'cpu_percent_avg': _stats.mean(cpu_vals) if cpu_vals else 0.0,
                    'memory_rss_mb_avg': _stats.mean(mem_vals) if mem_vals else 0.0,
                }
    except Exception:
        pass

    if not no_plots and fig_dir:
        # Only generate scheduler comparison facets for baseline-cleannet and burst-spike
        if scenario.lower() in ["baseline-cleannet", "burst-spike"]:
            _plot_scheduler_comparison_faceted(agg_results, agg_timelines, components_by_scheduler, fig_dir, util_sched if util_sched else None, scenario)

        # Series plot over the varied variable
        var_key = _scenario_variable_key(scenario)
        if var_key:
            _plot_scenario_series(series_points, var_key, fig_dir, scenario)
            # Only generate resource utilization series plot for baseline-cleannet and burst-spike
            if scenario.lower() in ["baseline-cleannet"]:
                _plot_scenario_resource_utilization_series(series_points, var_key, fig_dir)

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Load and summarize experiment outputs")
    parser.add_argument("--results-dir", default="results", help="Directory containing experiment outputs")
    parser.add_argument("--out-dir", default="analysis", help="Base directory to save plots and summaries")
    parser.add_argument("--no-plots", action="store_true", help="Do not generate plots")
    parser.add_argument(
        "--scenario",
        default="all",
        help=(
            "Scenario to load (baseline-cleannet, vary-input, vary-input-seq, vary-concurrency, "
            "vary-firingrate, burst-spike, small-msgs, all)"
        ),
    )
    args = parser.parse_args()

    # Define all available scenarios
    all_scenarios = [
        "baseline-cleannet",
        "vary-input",
        "vary-input-seq",
        "vary-concurrency",
        "vary-firingrate",
        "burst-spike",
        "small-msgs",
    ]
    if args.scenario.lower() == "all":
        # Run all scenarios
        print("🔄 Running all scenarios...")
        for scenario in all_scenarios:
            print(f"\n📊 Processing scenario: {scenario}")
            scen = scenario.strip().lower().replace(" ", "-")
            if scen == "baseline-cleannet":
                result = run_baseline_load(results_dir=args.results_dir, out_dir=args.out_dir, no_plots=args.no_plots)
            else:
                result = run_scenario_load(scenario=scenario, results_dir=args.results_dir, out_dir=args.out_dir, no_plots=args.no_plots)
            if result != 0:
                print(f"❌ Failed to process scenario: {scenario}")
                return result
        print("\n✅ All scenarios completed successfully!")
        return 0
    else:
        # Run single scenario
        scen = args.scenario.strip().lower().replace(" ", "-")
        if scen == "baseline-cleannet":
            return run_baseline_load(results_dir=args.results_dir, out_dir=args.out_dir, no_plots=args.no_plots)
        else:
            return run_scenario_load(scenario=args.scenario, results_dir=args.results_dir, out_dir=args.out_dir, no_plots=args.no_plots)


if __name__ == "__main__":
    import sys
    sys.exit(main())


