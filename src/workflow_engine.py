"""
Core Workflow Engine for DataFlower
Implements DAG-based workflow execution with serverless function orchestration
"""

import asyncio
import json
import time
import uuid
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any, Set, Callable
from dataclasses import dataclass, field
import networkx as nx
from pydantic import BaseModel, Field
import logging
from concurrent.futures import ThreadPoolExecutor
import redis
import pickle

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task execution states"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class WorkflowStatus(Enum):
    """Workflow execution states"""
    CREATED = "created"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TaskResult:
    """Result of a task execution"""
    task_id: str
    status: TaskStatus
    output: Any = None
    error: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    execution_time: Optional[float] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class Task(BaseModel):
    """Represents a single task in the workflow"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    function: str  # Function identifier for serverless execution
    inputs: Dict[str, Any] = Field(default_factory=dict)
    dependencies: List[str] = Field(default_factory=list)
    timeout: int = 300  # seconds
    retries: int = 3
    resource_requirements: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class Workflow(BaseModel):
    """Represents a complete workflow DAG"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: str = ""
    tasks: List[Task]
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.now)
    
    def to_graph(self) -> nx.DiGraph:
        """Convert workflow to NetworkX directed graph"""
        G = nx.DiGraph()
        task_map = {task.id: task for task in self.tasks}
        
        for task in self.tasks:
            G.add_node(task.id, task=task)
            for dep_id in task.dependencies:
                if dep_id in task_map:
                    G.add_edge(dep_id, task.id)
        
        return G
    
    def validate_dag(self) -> bool:
        """Validate that workflow forms a valid DAG"""
        G = self.to_graph()
        return nx.is_directed_acyclic_graph(G)


class StateManager:
    """Manages workflow and task state persistence"""
    
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
        
    def save_workflow_state(self, workflow_id: str, state: Dict[str, Any]):
        """Save workflow state to Redis"""
        key = f"workflow:{workflow_id}"
        self.redis_client.set(key, pickle.dumps(state))
        self.redis_client.expire(key, 86400)  # 24 hour TTL
        
    def get_workflow_state(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve workflow state from Redis"""
        key = f"workflow:{workflow_id}"
        data = self.redis_client.get(key)
        return pickle.loads(data) if data else None
    
    def save_task_result(self, workflow_id: str, task_id: str, result: TaskResult):
        """Save task execution result"""
        key = f"task:{workflow_id}:{task_id}"
        self.redis_client.set(key, pickle.dumps(result))
        self.redis_client.expire(key, 86400)
        
    def get_task_result(self, workflow_id: str, task_id: str) -> Optional[TaskResult]:
        """Retrieve task execution result"""
        key = f"task:{workflow_id}:{task_id}"
        data = self.redis_client.get(key)
        return pickle.loads(data) if data else None


class ServerlessExecutor:
    """Executes tasks on serverless platforms"""
    
    def __init__(self, platform: str = "local"):
        self.platform = platform
        self.executor = ThreadPoolExecutor(max_workers=10)
        
    async def execute_task(self, task: Task, inputs: Dict[str, Any]) -> TaskResult:
        """Execute a task on the serverless platform"""
        start_time = datetime.now()
        result = TaskResult(
            task_id=task.id,
            status=TaskStatus.RUNNING,
            start_time=start_time
        )
        
        try:
            if self.platform == "local":
                # Local execution for testing
                output = await self._execute_local(task, inputs)
            elif self.platform == "aws_lambda":
                output = await self._execute_lambda(task, inputs)
            elif self.platform == "kubernetes":
                output = await self._execute_k8s(task, inputs)
            else:
                raise ValueError(f"Unsupported platform: {self.platform}")
            
            end_time = datetime.now()
            result.status = TaskStatus.COMPLETED
            result.output = output
            result.end_time = end_time
            result.execution_time = (end_time - start_time).total_seconds()
            
        except Exception as e:
            end_time = datetime.now()
            result.status = TaskStatus.FAILED
            result.error = str(e)
            result.end_time = end_time
            result.execution_time = (end_time - start_time).total_seconds()
            logger.error(f"Task {task.id} failed: {e}")
            
        return result
    
    async def _execute_local(self, task: Task, inputs: Dict[str, Any]) -> Any:
        """Execute task locally for testing"""
        # Simulate task execution
        await asyncio.sleep(0.1)  # Simulate processing time
        
        # Example task functions
        if task.function == "data_ingestion":
            return {"records_processed": 1000, "status": "success"}
        elif task.function == "data_transformation":
            return {"transformed_records": 950, "status": "success"}
        elif task.function == "data_validation":
            return {"valid_records": 940, "invalid_records": 10}
        elif task.function == "data_aggregation":
            return {"aggregated_results": {"total": 940, "avg": 47}}
        else:
            return {"result": f"Executed {task.function}"}
    
    async def _execute_lambda(self, task: Task, inputs: Dict[str, Any]) -> Any:
        """Execute task on AWS Lambda"""
        import boto3
        
        lambda_client = boto3.client('lambda')
        
        response = lambda_client.invoke(
            FunctionName=task.function,
            InvocationType='RequestResponse',
            Payload=json.dumps(inputs)
        )
        
        result = json.loads(response['Payload'].read())
        return result
    
    async def _execute_k8s(self, task: Task, inputs: Dict[str, Any]) -> Any:
        """Execute task on Kubernetes as a Job"""
        from kubernetes import client, config
        
        config.load_incluster_config()  # Use in-cluster config
        batch_v1 = client.BatchV1Api()
        
        job = client.V1Job(
            metadata=client.V1ObjectMeta(name=f"task-{task.id}"),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        containers=[
                            client.V1Container(
                                name="task",
                                image=f"dataflower/{task.function}:latest",
                                env=[
                                    client.V1EnvVar(name="INPUTS", value=json.dumps(inputs))
                                ]
                            )
                        ],
                        restart_policy="Never"
                    )
                )
            )
        )
        
        batch_v1.create_namespaced_job(namespace="default", body=job)
        
        # Wait for job completion (simplified)
        await asyncio.sleep(5)
        
        return {"status": "completed"}


class WorkflowEngine:
    """Main workflow orchestration engine"""
    
    def __init__(self, 
                 executor: Optional[ServerlessExecutor] = None,
                 state_manager: Optional[StateManager] = None):
        self.executor = executor or ServerlessExecutor(platform="local")
        self.state_manager = state_manager or StateManager()
        self.running_workflows: Dict[str, asyncio.Task] = {}
        
    async def submit_workflow(self, workflow: Workflow) -> str:
        """Submit a workflow for execution"""
        if not workflow.validate_dag():
            raise ValueError("Workflow contains cycles - not a valid DAG")
        
        workflow_state = {
            "id": workflow.id,
            "status": WorkflowStatus.SCHEDULED.value,
            "submitted_at": datetime.now().isoformat(),
            "tasks_status": {task.id: TaskStatus.PENDING.value for task in workflow.tasks}
        }
        
        self.state_manager.save_workflow_state(workflow.id, workflow_state)
        
        # Start workflow execution
        task = asyncio.create_task(self._execute_workflow(workflow))
        self.running_workflows[workflow.id] = task
        
        logger.info(f"Workflow {workflow.id} submitted for execution")
        return workflow.id
    
    async def _execute_workflow(self, workflow: Workflow):
        """Execute the workflow DAG"""
        logger.info(f"Starting execution of workflow {workflow.id}")
        
        # Update workflow status
        workflow_state = self.state_manager.get_workflow_state(workflow.id)
        workflow_state["status"] = WorkflowStatus.RUNNING.value
        workflow_state["started_at"] = datetime.now().isoformat()
        self.state_manager.save_workflow_state(workflow.id, workflow_state)
        
        # Create task graph
        graph = workflow.to_graph()
        task_map = {task.id: task for task in workflow.tasks}
        
        # Track completed tasks
        completed_tasks: Set[str] = set()
        task_results: Dict[str, TaskResult] = {}
        
        # Topological sort for execution order
        try:
            topo_order = list(nx.topological_sort(graph))
        except nx.NetworkXError:
            logger.error(f"Workflow {workflow.id} is not a DAG")
            workflow_state["status"] = WorkflowStatus.FAILED.value
            self.state_manager.save_workflow_state(workflow.id, workflow_state)
            return
        
        # Execute tasks in parallel when possible
        while len(completed_tasks) < len(workflow.tasks):
            # Find tasks ready to execute
            ready_tasks = []
            for task_id in topo_order:
                if task_id not in completed_tasks:
                    task = task_map[task_id]
                    # Check if all dependencies are completed
                    if all(dep in completed_tasks for dep in task.dependencies):
                        ready_tasks.append(task)
            
            if not ready_tasks:
                break
            
            # Execute ready tasks in parallel
            execution_tasks = []
            for task in ready_tasks:
                # Prepare inputs from dependent task outputs
                inputs = task.inputs.copy()
                for dep_id in task.dependencies:
                    if dep_id in task_results:
                        inputs[f"dep_{dep_id}"] = task_results[dep_id].output
                
                execution_tasks.append(self._execute_task_with_retry(task, inputs))
            
            # Wait for all parallel tasks to complete
            results = await asyncio.gather(*execution_tasks)
            
            # Update completed tasks and results
            for task, result in zip(ready_tasks, results):
                completed_tasks.add(task.id)
                task_results[task.id] = result
                self.state_manager.save_task_result(workflow.id, task.id, result)
                
                # Update workflow state
                workflow_state["tasks_status"][task.id] = result.status.value
                self.state_manager.save_workflow_state(workflow.id, workflow_state)
        
        # Update final workflow status
        all_successful = all(
            result.status == TaskStatus.COMPLETED 
            for result in task_results.values()
        )
        
        workflow_state["status"] = (
            WorkflowStatus.COMPLETED.value if all_successful 
            else WorkflowStatus.FAILED.value
        )
        workflow_state["completed_at"] = datetime.now().isoformat()
        self.state_manager.save_workflow_state(workflow.id, workflow_state)
        
        logger.info(f"Workflow {workflow.id} completed with status: {workflow_state['status']}")
    
    async def _execute_task_with_retry(self, task: Task, inputs: Dict[str, Any]) -> TaskResult:
        """Execute a task with retry logic"""
        retry_count = 0
        last_error = None
        
        while retry_count <= task.retries:
            try:
                result = await asyncio.wait_for(
                    self.executor.execute_task(task, inputs),
                    timeout=task.timeout
                )
                
                if result.status == TaskStatus.COMPLETED:
                    result.retry_count = retry_count
                    return result
                
                last_error = result.error
                
            except asyncio.TimeoutError:
                last_error = f"Task timed out after {task.timeout} seconds"
                logger.warning(f"Task {task.id} timed out, retry {retry_count}/{task.retries}")
            
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Task {task.id} failed: {e}, retry {retry_count}/{task.retries}")
            
            retry_count += 1
            if retry_count <= task.retries:
                await asyncio.sleep(2 ** retry_count)  # Exponential backoff
        
        # All retries exhausted
        return TaskResult(
            task_id=task.id,
            status=TaskStatus.FAILED,
            error=last_error,
            retry_count=retry_count - 1
        )
    
    def get_workflow_status(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of a workflow"""
        return self.state_manager.get_workflow_state(workflow_id)
    
    async def cancel_workflow(self, workflow_id: str):
        """Cancel a running workflow"""
        if workflow_id in self.running_workflows:
            self.running_workflows[workflow_id].cancel()
            
            workflow_state = self.state_manager.get_workflow_state(workflow_id)
            if workflow_state:
                workflow_state["status"] = WorkflowStatus.CANCELLED.value
                workflow_state["cancelled_at"] = datetime.now().isoformat()
                self.state_manager.save_workflow_state(workflow_id, workflow_state)
            
            logger.info(f"Workflow {workflow_id} cancelled")
