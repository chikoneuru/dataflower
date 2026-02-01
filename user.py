from scheduler.ours.orchestrator import Orchestrator
from scheduler.ours.config import ConfigPresets
from scheduler.ours.cost_models import TaskProfile, NodeProfile
from functions.dag_loader import load_workflow

# 1) Use development preset → all mock algorithms
orc = Orchestrator(ConfigPresets.get_development_config())

# 2) Load a workflow (recognizer)
wf = orc.load_workflow_from_file("functions/recognizer/recognizer_dag.yaml")

# 3) Convert workflow tasks to TaskProfile (lightweight mock)
tasks = []
for tid, node in wf["tasks"].items():
    if node["type"] != "task":  # skip virtual nodes (switch/foreach/end)
        continue
    tasks.append(TaskProfile(
        task_id=tid,
        function_name=tid,
        input_size=1_000_000,           # mock
        estimated_compute_time=1.0,     # mock
        memory_requirement=512,         # mock
        cpu_intensity=0.5               # mock
    ))

# 4) Build simple dependency map from edges (parents → child)
dag_structure = {}
parents = {t.task_id: [] for t in tasks}
task_ids = set(parents.keys())
for e in wf["edges"]:
    src, dst = e["from"], e["to"]
    if src in task_ids and dst in task_ids:
        parents[dst].append(src)
for t in tasks:
    dag_structure[t.task_id] = {"dependencies": [{"task_id": p, "data_size": 0}] for p in parents[t.task_id]}

# 5) Mock cluster nodes
nodes = [
    NodeProfile(node_id="node_0", cpu_cores=4, memory_total=8192, memory_available=4096, cpu_utilization=0.2, network_bandwidth=1000, container_cache={}),
    NodeProfile(node_id="node_1", cpu_cores=4, memory_total=8192, memory_available=4096, cpu_utilization=0.3, network_bandwidth=1000, container_cache={}),
]

# 6) Schedule (uses all 4 components in mock mode)
assignments = orc.orchestrate_workflow(tasks, nodes, dag_structure)
print("Assignments:", assignments)

# 7) Optional: metrics snapshot
print("Summary:", orc.get_performance_summary())
