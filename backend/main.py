from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect

from .engine import Engine
from .schemas import FlowIn, ObstacleIn
from .simulator import VirtualRobot
from .store import Store

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "orchestrator.db"

SEED_FLOW = {
    "nodes": [
        {"id": "n1", "type": "log", "label": "出发播报", "params": {"message": "开始执行取物任务"}},
        {"id": "n2", "type": "move", "label": "移动到货架", "params": {"x": 4, "y": 3}},
        {"id": "n3", "type": "wait", "label": "等待货架绿灯", "params": {"condition": "shelf_ready"}},
        {"id": "n4", "type": "pick", "label": "取物", "params": {"item": "包裹A"}},
        {"id": "n5", "type": "move", "label": "送到工作台", "params": {"x": 1, "y": 0}},
        {"id": "n6", "type": "place", "label": "放下包裹", "params": {}},
        {"id": "n7", "type": "log", "label": "绕行重试", "params": {"message": "取物失败，改走备用路径"}},
        {"id": "n8", "type": "wait", "label": "等待人工排障", "params": {"condition": "manual_clear"}},
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
    ],
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    store = Store(DB_PATH)
    robot = VirtualRobot()
    engine = Engine(store, robot)
    if not store.list_flows():
        store.save_flow("fetch-demo", "取物任务（含等待/失败分支）", SEED_FLOW)
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
    record["waiting_for"] = execution.wait_condition if execution else None
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
