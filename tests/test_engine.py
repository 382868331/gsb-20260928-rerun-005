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


RETRY_FLOW = {
    "nodes": [
        {"id": "s", "type": "log", "label": "出发", "params": {"message": "go"}},
        {"id": "m", "type": "move", "label": "移动到货架", "params": {"x": 2, "y": 0}},
        {"id": "p", "type": "pick", "label": "取物",
         "params": {"item": "包裹", "max_retries": 2}},
        {"id": "r", "type": "log", "label": "绕行播报", "params": {"message": "重试中"}},
        {"id": "back", "type": "move", "label": "回到移动节点", "params": {"x": 2, "y": 0}},
        {"id": "h", "type": "manual", "label": "人工排障",
         "params": {"instruction": "请现场处理"}},
        {"id": "ok", "type": "log", "label": "送达", "params": {"message": "done"}},
    ],
    "edges": [
        {"id": "0", "source": "s", "target": "m", "kind": "success"},
        {"id": "1", "source": "m", "target": "p", "kind": "success"},
        {"id": "2", "source": "p", "target": "ok", "kind": "success"},
        {"id": "3", "source": "p", "target": "r", "kind": "failure"},
        {"id": "4", "source": "r", "target": "back", "kind": "success"},
        {"id": "5", "source": "back", "target": "m", "kind": "success"},
        {"id": "6", "source": "p", "target": "h", "kind": "exhausted"},
        {"id": "7", "source": "h", "target": "ok", "kind": "success"},
    ],
}


async def test_consecutive_failures_stop_at_manual(harness):
    """连续失败达到上限：停在人工处置节点，node_runs 保留每一轮记录。"""
    store, robot, engine = harness
    robot.set_obstacle(True)
    eid = engine.start("flow1", RETRY_FLOW)
    await wait_until(lambda: store.get_execution(eid)["status"] == "awaiting_manual", timeout=12)

    assert engine.get(eid).manual_node == "h"
    runs = store.get_execution(eid)["node_runs"]
    pick_runs = [nr for nr in runs if nr["node_id"] == "p"]
    assert [(nr["attempt"], nr["status"]) for nr in pick_runs] == [
        (1, "failed"), (2, "failed"), (3, "failed")]
    assert sum(1 for e in store.get_events(eid) if e["type"] == "branch_taken") == 2
    exhausted = [e for e in store.get_events(eid) if e["type"] == "retries_exhausted"]
    assert len(exhausted) == 1 and exhausted[0]["data"]["target"] == "h"
    manual_runs = [nr for nr in runs if nr["node_id"] == "h"]
    assert manual_runs[0]["status"] == "manual"

    assert engine.resolve_manual(eid, abort=True)
    await wait_until(lambda: store.get_execution(eid)["status"] == "resolved")
    final = store.get_execution(eid)
    assert not any(nr["node_id"] == "ok" for nr in final["node_runs"])


async def test_second_attempt_succeeds(harness):
    """第二次成功：旧失败轮次保留为历史，本次状态为成功，流程继续。"""
    store, robot, engine = harness
    robot.set_obstacle(True)
    eid = engine.start("flow1", RETRY_FLOW)
    await wait_until(
        lambda: any(nr["node_id"] == "p" and nr["attempt"] == 1 and nr["status"] == "failed"
                    for nr in store.get_execution(eid)["node_runs"]))
    # 第一次失败走回路，第二轮取物前清障
    robot.set_obstacle(False)
    await wait_until(lambda: store.get_execution(eid)["status"] == "succeeded")

    runs = store.get_execution(eid)["node_runs"]
    pick_runs = [nr for nr in runs if nr["node_id"] == "p"]
    assert [(nr["attempt"], nr["status"]) for nr in pick_runs] == [
        (1, "failed"), (2, "succeeded")]
    events = store.get_events(eid)
    assert any(e["type"] == "branch_taken" and e["data"]["attempt"] == 1
               for e in events)
    success = [e for e in events if e["type"] == "node_succeeded" and e["node_id"] == "p"]
    assert success[0]["data"]["attempt"] == 2
    assert not any(e["type"] == "retries_exhausted" for e in events)


async def test_cancel_then_late_result_not_counted(harness):
    """回路执行中取消：迟到的步骤结果不写入新一轮，执行保持 cancelled。"""
    store, robot, engine = harness
    robot.set_obstacle(True)
    eid = engine.start("flow1", RETRY_FLOW)
    await wait_until(
        lambda: any(nr["node_id"] == "p" and nr["attempt"] == 1 and nr["status"] == "failed"
                    for nr in store.get_execution(eid)["node_runs"]))
    await wait_until(
        lambda: any(nr["node_id"] == "back" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    assert engine.cancel(eid)
    # 即使“迟到结果”此时到达（障碍清除），也不能让旧回路继续/完成新一轮
    robot.set_obstacle(False)
    await wait_until(lambda: store.get_execution(eid)["status"] == "cancelled")
    await asyncio.sleep(0.5)

    runs = store.get_execution(eid)["node_runs"]
    pick_runs = [nr for nr in runs if nr["node_id"] == "p"]
    assert [(nr["attempt"], nr["status"]) for nr in pick_runs] == [(1, "failed")]
    move_runs = [nr for nr in runs if nr["node_id"] == "m"]
    assert len(move_runs) == 1 and move_runs[0]["status"] == "succeeded"
    assert all(nr["status"] != "succeeded" for nr in runs if nr["node_id"] == "back")
    assert not any(nr["node_id"] == "ok" for nr in runs)
    assert store.get_execution(eid)["status"] == "cancelled"


async def test_late_success_after_cancel_keeps_cancelled(harness, monkeypatch):
    """移动动作在取消裁决后报告成功：记录保持 cancelled，成功只作迟到后续信息。"""
    store, robot, engine = harness
    flow = {
        "nodes": [
            {"id": "m", "type": "move", "label": "移动",
             "params": {"x": 2, "y": 0, "safe_point": [0, 0]}},
            {"id": "next", "type": "log", "label": "后续",
             "params": {"message": "不应执行"}},
        ],
        "edges": [{"id": "1", "source": "m", "target": "next", "kind": "success"}],
    }
    from backend import simulator as sim_mod

    real_steps = sim_mod.VirtualRobot._steps
    state = {"cancelled": False}

    async def racing_steps(self, n, delay, stop, on_step=None, gate=None):
        for i in range(1, n + 1):
            if gate is not None:
                await gate()
            if stop.is_set():
                state["cancelled"] = True
            await asyncio.sleep(delay)
            # 模拟硬件层未收到取消、最后一步仍报成功
            if on_step is not None and not (i == n and state["cancelled"]):
                result = on_step(i, n)
                if asyncio.iscoroutine(result):
                    await result

    monkeypatch.setattr(sim_mod.VirtualRobot, "_steps", racing_steps)
    eid = engine.start("flow1", flow)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    assert engine.cancel(eid)
    await wait_until(lambda: store.get_execution(eid)["status"] == "cancelled")
    await asyncio.sleep(0.3)
    monkeypatch.undo()

    record = store.get_execution(eid)
    run = next(nr for nr in record["node_runs"] if nr["node_id"] == "m")
    assert run["status"] == "cancelled"
    assert run["late_outcome"] == "success"
    assert not any(nr["node_id"] == "next" for nr in record["node_runs"])
    events = store.get_events(eid)
    assert [e["type"] for e in events].count("node_succeeded") == 0
    late = [e for e in events if e["type"] == "node_late_action_result"]
    assert len(late) == 1 and late[0]["data"]["outcome"] == "success"


FAIL_CLEANUP_FLOW = {
    "nodes": [
        {"id": "p", "type": "pick", "label": "取物",
         "params": {"item": "包裹", "max_retries": 0, "safe_point": [2, 0]}},
        {"id": "m", "type": "move", "label": "回安全点",
         "params": {"x": 2, "y": 0}},
        {"id": "h", "type": "manual", "label": "人工排障",
         "params": {"instruction": "请处理"}},
        {"id": "ok", "type": "log", "label": "送达", "params": {"message": "done"}},
    ],
    "edges": [
        {"id": "1", "source": "p", "target": "m", "kind": "failure"},
        {"id": "2", "source": "m", "target": "ok", "kind": "success"},
        {"id": "3", "source": "p", "target": "h", "kind": "exhausted"},
        {"id": "4", "source": "h", "target": "ok", "kind": "success"},
    ],
}


async def test_failure_cleanup_returns_to_safe_point(harness):
    """失败后先走返回安全位置的收尾，再走重试/分支；原失败原因保留。"""
    store, robot, engine = harness
    robot.set_obstacle(True)
    eid = engine.start("flow1", FAIL_CLEANUP_FLOW)
    await wait_until(lambda: store.get_execution(eid)["status"] == "awaiting_manual")

    run = next(nr for nr in store.get_execution(eid)["node_runs"] if nr["node_id"] == "p")
    assert run["status"] == "failed"
    assert run["cleanup_status"] == "succeeded"
    assert run["detail"] == "检测到障碍物，无法取物"
    assert (robot.status()["x"], robot.status()["y"]) == (2.0, 0.0)
    events = store.get_events(eid)
    assert any(e["type"] == "node_cleanup_started" and e["data"]["reason"] for e in events)
    assert any(e["type"] == "node_cleanup_finished" for e in events)


async def test_cleanup_failure_keeps_original_reason(harness):
    """收尾自身失败：不覆盖原失败原因，流程仍按失败分支继续。"""
    store, robot, engine = harness
    robot.set_obstacle(True)
    robot.set_cleanup_blocked(True)
    eid = engine.start("flow1", FAIL_CLEANUP_FLOW)
    await wait_until(lambda: store.get_execution(eid)["status"] == "awaiting_manual")

    run = next(nr for nr in store.get_execution(eid)["node_runs"] if nr["node_id"] == "p")
    assert run["status"] == "failed"
    assert run["detail"] == "检测到障碍物，无法取物"
    assert run["cleanup_status"] == "failed"
    assert "收尾失败" in run["cleanup_detail"]
    events = store.get_events(eid)
    assert any(e["type"] == "node_cleanup_failed" and e["data"]["reason"] for e in events)


async def test_cancel_during_cleanup(harness):
    """收尾过程中取消：原失败原因不被覆盖，终止说明保留原原因，不再推进分支。"""
    store, robot, engine = harness
    robot.set_obstacle(True)
    eid = engine.start("flow1", FAIL_CLEANUP_FLOW)
    await wait_until(
        lambda: any(e["type"] == "node_cleanup_started"
                    for e in store.get_events(eid)))
    assert engine.cancel(eid)
    await wait_until(lambda: store.get_execution(eid)["status"] == "cancelled")

    record = store.get_execution(eid)
    run = next(nr for nr in record["node_runs"] if nr["node_id"] == "p")
    assert run["status"] == "failed"
    assert run["cleanup_status"] == "cancelled"
    assert "障碍物" in record["termination_reason"]
    assert not any(nr["node_id"] in ("m", "h", "ok") for nr in record["node_runs"])


async def test_restart_preserves_phase_and_termination_reason(harness, tmp_path):
    """保存中间状态后重启：终止说明与中断时所处阶段一致。"""
    store, robot, engine = harness
    flow = {
        "nodes": [
            {"id": "m", "type": "move", "label": "长距离移动",
             "params": {"x": 8, "y": 0}},
            {"id": "ok", "type": "log", "label": "到达",
             "params": {"message": "done"}},
        ],
        "edges": [{"id": "1", "source": "m", "target": "ok", "kind": "success"}],
    }
    eid = engine.start("flow1", flow)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    for execution in list(engine._executions.values()):
        execution.task.cancel()
    await asyncio.sleep(0.1)

    store2 = Store(tmp_path / "test.db")
    Engine(store2, VirtualRobot())
    record = store2.get_execution(eid)
    assert record["status"] == "interrupted"
    assert "动作执行中断" in record["termination_reason"]
    run = record["node_runs"][0]
    assert run["status"] == "interrupted"
    assert any(e["type"] == "execution_interrupted" for e in store2.get_events(eid))
