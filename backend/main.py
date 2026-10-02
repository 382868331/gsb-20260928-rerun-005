from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect

from .engine import Engine
from .schemas import (EvidenceIn, EvidenceWithdrawIn, FlowIn, LateActionIn,
                      LateFinishIn, ManualResolveIn, ObstacleIn, RecoveryResolveIn)
from .evidence import resolve_ledger
from .simulator import VirtualRobot
from .store import Store

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "orchestrator.db"

SEED_FLOW = {
    "nodes": [
        {"id": "n1", "type": "log", "label": "出发播报", "params": {"message": "开始执行取物任务"}},
        {"id": "n2", "type": "move", "label": "移动到货架", "params": {"x": 4, "y": 3}},
        {"id": "n3", "type": "wait", "label": "等待货架绿灯", "params": {"condition": "shelf_ready"}},
        {"id": "n4", "type": "pick", "label": "取物", "params": {"item": "包裹A", "max_retries": 2}},
        {"id": "n5", "type": "move", "label": "送到工作台", "params": {"x": 1, "y": 0}},
        {"id": "n6", "type": "place", "label": "放下包裹", "params": {}},
        {"id": "n7", "type": "log", "label": "绕行播报", "params": {"message": "取物失败，改走备用路径重试"}},
        {"id": "n8", "type": "move", "label": "回到移动节点重试", "params": {"x": 4, "y": 3}},
        {"id": "n9", "type": "manual", "label": "等待人工排障",
         "params": {"instruction": "取物连续失败 3 次，请现场排障：清除障碍后放行，或确认终止任务"}},
    ],
    "edges": [
        {"id": "e1", "source": "n1", "target": "n2", "kind": "success"},
        {"id": "e2", "source": "n2", "target": "n3", "kind": "success"},
        {"id": "e3", "source": "n3", "target": "n4", "kind": "success"},
        {"id": "e4", "source": "n4", "target": "n5", "kind": "success"},
        {"id": "e5", "source": "n5", "target": "n6", "kind": "success"},
        {"id": "e6", "source": "n4", "target": "n7", "kind": "failure"},
        {"id": "e7", "source": "n7", "target": "n8", "kind": "success"},
        {"id": "e8", "source": "n8", "target": "n2", "kind": "success"},
        {"id": "e9", "source": "n4", "target": "n9", "kind": "exhausted"},
    ],
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    store = Store(DB_PATH)
    robot = VirtualRobot()
    engine = Engine(store, robot)
    store.save_flow("fetch-demo", "取物任务（失败回路/有限重试/人工处置）", SEED_FLOW)
    app.state.store = store
    app.state.robot = robot
    app.state.engine = engine
    yield


app = FastAPI(title="机器人任务编排平台", lifespan=lifespan)


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/flows")
async def list_flows():
    return app.state.store.list_flows()


@app.post("/api/flows")
async def create_flow(flow: FlowIn):
    flow_id = uuid.uuid4().hex[:10]
    app.state.store.save_flow(flow_id, flow.name, flow.definition.model_dump())
    return {"id": flow_id}


@app.get("/api/flows/{flow_id}")
async def get_flow(flow_id: str):
    flow = app.state.store.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "流程不存在")
    return flow


@app.post("/api/flows/{flow_id}/runs")
async def start_run(flow_id: str):
    flow = app.state.store.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "流程不存在")
    app.state.robot.set_obstacle(False)
    execution_id = app.state.engine.start(flow_id, flow["definition"])
    return {"execution_id": execution_id}


@app.post("/api/executions/{execution_id}/pause")
async def pause_run(execution_id: str):
    if not app.state.engine.pause(execution_id):
        raise HTTPException(409, "当前无法暂停")
    return {"status": "paused"}


@app.post("/api/executions/{execution_id}/resume")
async def resume_run(execution_id: str):
    if not app.state.engine.resume_execution(execution_id):
        raise HTTPException(409, "当前未处于暂停状态")
    return {"status": "running"}


@app.post("/api/executions/{execution_id}/cancel")
async def cancel_run(execution_id: str):
    if not app.state.engine.cancel(execution_id):
        raise HTTPException(409, "任务已结束，无法取消")
    return {"status": "cancelling"}


@app.post("/api/executions/{execution_id}/signal")
async def signal_condition(execution_id: str, condition: str = "signal"):
    if not app.state.engine.signal(execution_id, condition):
        raise HTTPException(409, "当前没有等待该条件的任务")
    return {"status": "signalled"}


@app.post("/api/executions/{execution_id}/manual")
async def resolve_manual(execution_id: str, payload: ManualResolveIn):
    if not app.state.engine.resolve_manual(execution_id, abort=payload.abort):
        raise HTTPException(409, "当前没有等待人工处置的任务")
    return {"status": "aborted" if payload.abort else "resolved"}


@app.post("/api/executions/{execution_id}/recovery")
async def resolve_recovery(execution_id: str, payload: RecoveryResolveIn):
    if not app.state.engine.resolve_recovery(execution_id, finish=payload.finish):
        raise HTTPException(409, "当前没有待核实的恢复处置")
    return {"status": "finishing" if payload.finish else "resolved"}


@app.post("/api/executions/{execution_id}/late-action")
async def record_late_action(execution_id: str, payload: LateActionIn):
    if not app.state.engine.record_late_action(
        execution_id, payload.outcome, payload.detail
    ):
        raise HTTPException(409, "当前无法记录该迟到回应")
    return {"status": payload.outcome}


@app.get("/api/executions/{execution_id}/evidence")
async def list_evidence(execution_id: str):
    if app.state.store.get_execution(execution_id) is None:
        raise HTTPException(404, "执行记录不存在")
    ledger = app.state.store.get_recovery_evidence(execution_id)
    return {"ledger": ledger, "resolution": resolve_ledger(ledger),
            "state": app.state.store.get_recovery_state(execution_id)}


@app.post("/api/executions/{execution_id}/evidence")
async def add_evidence(execution_id: str, payload: EvidenceIn):
    engine = app.state.engine
    execution = engine.get(execution_id)
    record = app.state.store.get_execution(execution_id)
    if execution is None or record is None or record["status"] in (
        "succeeded", "failed", "resolved", "cancelled"
    ):
        raise HTTPException(409, "当前无法登记核实证据")
    if app.state.store.get_recovery_state(execution_id) is None:
        raise HTTPException(409, "当前没有待核实的恢复")
    evidence_id = engine.add_evidence(
        execution_id, payload.subject, payload.source, payload.observed_at,
        payload.value, payload.detail, payload.supersedes_id)
    return {"evidence_id": evidence_id}


@app.post("/api/executions/{execution_id}/evidence/withdraw")
async def withdraw_evidence(execution_id: str, payload: EvidenceWithdrawIn):
    engine = app.state.engine
    execution = engine.get(execution_id)
    record = app.state.store.get_execution(execution_id)
    if execution is None or record is None or record["status"] in (
        "succeeded", "failed", "resolved", "cancelled"
    ):
        raise HTTPException(409, "当前无法撤回核实证据")
    if app.state.store.get_recovery_state(execution_id) is None:
        raise HTTPException(409, "当前没有待核实的恢复")
    if not engine.withdraw_evidence(execution_id, payload.evidence_id, payload.detail):
        raise HTTPException(404, "该证据不存在或已失效")
    return {"evidence_id": payload.evidence_id, "status": "withdrawn"}


@app.get("/api/executions")
async def list_executions():
    return app.state.store.list_executions()


@app.get("/api/executions/{execution_id}")
async def get_execution(execution_id: str):
    record = app.state.store.get_execution(execution_id)
    if record is None:
        raise HTTPException(404, "执行记录不存在")
    record["robot"] = app.state.robot.status()
    execution = app.state.engine.get(execution_id)
    state = app.state.store.get_recovery_state(execution_id) or {}
    record["waiting_for"] = execution.wait_condition if execution else state.get("wait_condition")
    record["manual_node"] = execution.manual_node if execution else state.get("manual_node")
    record["current_node"] = execution.current_node if execution else state.get("current_node")
    record["current_attempt"] = execution.current_attempt if execution else state.get("current_attempt", 0)
    record["recovery"] = state
    if record["status"] == "awaiting_recovery":
        record["robot"] = state.get("last_robot", record["robot"])
        record["unconfirmed_action"] = {
            "node_id": state.get("current_node"),
            "attempt": state.get("current_attempt"),
            "phase": state.get("phase"),
            "failure_detail": state.get("failure_detail"),
            "last_progress": state.get("last_progress"),
        }
        ledger = app.state.store.get_recovery_evidence(execution_id)
        record["evidence"] = ledger
        record["evidence_resolution"] = resolve_ledger(ledger)
        record["blocked_followups"] = state.get("blocked_subjects")
        record["reawait_reason"] = state.get("reawait_reason")
    return record


@app.get("/api/executions/{execution_id}/events")
async def get_events(execution_id: str, after: int = 0):
    if app.state.store.get_execution(execution_id) is None:
        raise HTTPException(404, "执行记录不存在")
    return app.state.store.get_events(execution_id, after)


@app.get("/api/robot")
async def robot_status():
    return app.state.robot.status()


@app.post("/api/robot/obstacle")
async def set_obstacle(payload: ObstacleIn):
    app.state.robot.set_obstacle(payload.present)
    return {"obstacle": payload.present}


@app.post("/api/robot/late-finish")
async def set_late_finish(payload: LateFinishIn):
    """模拟真实设备：取消后尚未返回的动作稍后才报告成功（迟到结果场景）。"""
    app.state.robot.set_late_finish(payload.enabled)
    return {"late_finish": payload.enabled}


@app.websocket("/ws/events")
async def ws_events(websocket: WebSocket):
    await websocket.accept()
    queue = await app.state.engine.subscribe()
    try:
        while True:
            payload = await queue.get()
            await websocket.send_json(payload)
    except WebSocketDisconnect:
        pass
    except (RuntimeError, asyncio.CancelledError):
        pass
    finally:
        app.state.engine.unsubscribe(queue)
