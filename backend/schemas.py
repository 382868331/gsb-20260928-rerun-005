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


EvidenceSubject = Literal["position", "holding", "action_result"]
EvidenceSource = Literal["manual_observation", "device_response", "checkpoint", "other"]


class EvidenceIn(BaseModel):
    subject: EvidenceSubject
    source: EvidenceSource = "manual_observation"
    observed_at: str = Field(description="观察时刻（ISO8601 或可比较的时间串）")
    value: str = Field(description="证据内容，如 (4,3) / 包裹A:holding / empty / success / failure")
    detail: str = ""
    supersedes_id: int | None = Field(
        default=None, description="被本条修订的旧证据序号；不传则为新增登记")


class EvidenceWithdrawIn(BaseModel):
    evidence_id: int
    detail: str = ""
