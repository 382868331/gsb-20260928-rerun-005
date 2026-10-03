from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


NodeType = Literal["move", "pick", "place", "wait", "log", "manual"]
EdgeKind = Literal["success", "failure", "exhausted"]


class FlowNode(BaseModel):
    id: str
    type: NodeType
    label: str = ""
    params: dict[str, Any] = Field(default_factory=dict)


class FlowEdge(BaseModel):
    id: str
    source: str
    target: str
    kind: EdgeKind = "success"


class FlowDefinition(BaseModel):
    nodes: list[FlowNode]
    edges: list[FlowEdge] = Field(default_factory=list)


class FlowDoc(BaseModel):
    id: str
    name: str
    definition: FlowDefinition


class FlowIn(BaseModel):
    name: str
    definition: FlowDefinition


class ObstacleIn(BaseModel):
    present: bool


class LateFinishIn(BaseModel):
    enabled: bool = False


class ManualResolveIn(BaseModel):
    abort: bool = False


class RecoveryResolveIn(BaseModel):
    finish: bool = False


class LateActionIn(BaseModel):
    outcome: Literal["success", "failure"] = "success"
    detail: str = "设备迟到回应"


class EvidenceIn(BaseModel):
    category: Literal["position", "holding", "action_result"]
    source: Literal["onsite", "device_late"] = "onsite"
    value: str
    detail: str | None = None


class RetractIn(BaseModel):
    reason: str
