from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


Workflow = Dict[str, Any]
PlacementPlan = Dict[str, Any]
RoutingPlan = Dict[str, Any]


@dataclass
class WorkflowContext:
    workflow_name: str
    request_id: str
    placement_plan: PlacementPlan = field(default_factory=dict)
    routing_plan: RoutingPlan = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class InvocationResult:
    status: str
    function: str
    request_id: str
    result: Optional[Any] = None
    message: Optional[str] = None
    error_message: Optional[str] = None
    node: Optional[str] = None
    container_id: Optional[str] = None
    timestamps: Dict[str, Any] = field(default_factory=dict)
    server_timestamps: Dict[str, Any] = field(default_factory=dict)
    derived: Dict[str, Any] = field(default_factory=dict)
    routing: Optional[Dict[str, Any]] = None


def build_invocation_result_from_dict(function_name: str, request_id: str, payload: Dict[str, Any]) -> InvocationResult:
    return InvocationResult(
        status=str(payload.get('status', 'ok')),
        function=function_name,
        request_id=request_id,
        result=payload.get('result'),
        message=payload.get('message'),
        error_message=payload.get('error') or payload.get('error_message'),
        node=payload.get('node'),
        container_id=payload.get('container_id'),
        timestamps=payload.get('timestamps') or payload.get('timeline', {}).get('timestamps', {}),
        server_timestamps=payload.get('server_timestamps') or payload.get('timeline', {}).get('server_timestamps', {}),
        derived=payload.get('derived') or payload.get('timeline', {}).get('derived', {}),
        routing=payload.get('routing'),
    )


