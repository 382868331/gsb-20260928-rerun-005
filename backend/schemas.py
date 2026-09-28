from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


NodeType = Literal["move", "pick", "place", "wait", "log"]
EdgeKind = Literal["success", "failure"]


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
