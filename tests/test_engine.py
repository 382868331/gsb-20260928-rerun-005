from __future__ import annotations

import asyncio

import pytest

import pytest

pytestmark = pytest.mark.anyio

from backend.engine import Engine
from backend.simulator import VirtualRobot
from backend.store import Store


FLOW = {
    "nodes": [
        {"id": "a", "type": "move", "label": "出发", "params": {"x": 3, "y": 0}},
        {"id": "b", "type": "wait", "label": "等条件", "params": {"condition": "ready"}},
        {"id": "c", "type": "pick", "label": "取物", "params": {"item": "包裹"}},
        {"id": "d", "type": "log", "label": "失败分支", "params": {"message": "障碍物，绕行"}},
    ],
    "edges": [
        {"id": "1", "source": "a", "target": "b", "kind": "success"},
        {"id": "2", "source": "b", "target": "c", "kind": "success"},
        {"id": "3", "source": "c", "target": "d", "kind": "failure"},
    ],
}


async def wait_until(predicate, timeout=3.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("等待超时")


@pytest.fixture
def harness(tmp_path):
    store = Store(tmp_path / "test.db")
    robot = VirtualRobot()
    engine = Engine(store, robot)
    yield store, robot, engine
    for execution in list(engine._executions.values()):
        execution.task.cancel()



async def test_wait_then_branch_on_obstacle(harness):
    store, robot, engine = harness
    eid = engine.start("flow1", FLOW)
    await wait_until(lambda: store.get_execution(eid)["status"] == "waiting")
    assert engine.get(eid).wait_condition == "ready"

    robot.set_obstacle(True)
    assert engine.signal(eid, "ready")
    await wait_until(lambda: store.get_execution(eid)["status"] == "succeeded")

    events = [(e["type"], e["node_id"]) for e in store.get_events(eid)]
    assert ("node_waiting", "b") in events
    assert ("node_failed", "c") in events
    assert ("branch_taken", "c") in events
    branch = [e for e in store.get_events(eid) if e["type"] == "branch_taken"][0]
    assert branch["data"]["target"] == "d"
    assert robot.status()["holding"] is None
    assert robot.status()["idle"] is True



async def test_cancel_during_pick_after_obstacle(harness):
    """取物途中遇障碍，撤销任务，执行记录与实际状态一致。"""
    store, robot, engine = harness
    flow = {
        "nodes": [
            {"id": "m", "type": "move", "label": "移动", "params": {"x": 2, "y": 0}},
            {"id": "p", "type": "pick", "label": "取物", "params": {"item": "包裹A"}},
            {"id": "n", "type": "place", "label": "放置", "params": {}},
        ],
        "edges": [
            {"id": "1", "source": "m", "target": "p", "kind": "success"},
            {"id": "2", "source": "p", "target": "n", "kind": "success"},
        ],
    }
    eid = engine.start("flow1", flow)
    await wait_until(
        lambda: any(nr["node_id"] == "p" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    robot.set_obstacle(True)
    assert engine.cancel(eid)
    await wait_until(lambda: store.get_execution(eid)["status"] == "cancelled")

    record = store.get_execution(eid)
    runs = {nr["node_id"]: nr["status"] for nr in record["node_runs"]}
    assert runs["p"] == "cancelled"
    assert "n" not in runs
    status = robot.status()
    assert status["idle"] is True
    assert status["holding"] is None
    types = [e["type"] for e in store.get_events(eid)]
    assert "execution_cancelling" in types
    assert types.count("node_started") == 2



async def test_pause_resume_keeps_state(harness):
    store, robot, engine = harness
    flow = {"nodes": [
        {"id": "m", "type": "move", "label": "长距离移动", "params": {"x": 6, "y": 0}},
        {"id": "ok", "type": "log", "label": "到达", "params": {"message": "done"}},
    ], "edges": [{"id": "1", "source": "m", "target": "ok", "kind": "success"}]}
    eid = engine.start("flow1", flow)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    assert engine.pause(eid)
    await asyncio.sleep(0.8)
    frozen = robot.status()
    await asyncio.sleep(0.4)
    assert robot.status()["x"] == frozen["x"]
    assert robot.status()["y"] == frozen["y"]
    assert store.get_execution(eid)["status"] == "paused"
    assert engine.resume_execution(eid)
    await wait_until(lambda: store.get_execution(eid)["status"] == "succeeded")
    assert (robot.status()["x"], robot.status()["y"]) == (6.0, 0.0)
