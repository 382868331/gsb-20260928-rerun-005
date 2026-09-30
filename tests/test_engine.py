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


LOOP_FLOW = {
    "nodes": [
        {"id": "m", "type": "move", "label": "移动到货架", "params": {"x": 1, "y": 0}},
        {"id": "p", "type": "pick", "label": "取物", "params": {"item": "包裹"}},
        {"id": "h", "type": "manual", "label": "人工排障",
         "params": {"prompt": "取物失败，请处置", "retry_target": "m", "max_retries": 2}},
        {"id": "ok", "type": "log", "label": "完成播报", "params": {"message": "done"}},
    ],
    "edges": [
        {"id": "1", "source": "m", "target": "p", "kind": "success"},
        {"id": "2", "source": "p", "target": "ok", "kind": "success"},
        {"id": "3", "source": "p", "target": "h", "kind": "failure"},
    ],
}


async def test_continuous_failure_retries_capped_then_manual_abort(harness):
    """连续失败：有限次重试（2 次）用完后停在人工处置节点，只能终止。"""
    store, robot, engine = harness
    robot.set_obstacle(True)
    eid = engine.start("flow1", LOOP_FLOW)

    async def wait_manual(expected_attempt: int):
        await wait_until(lambda: (
            store.get_execution(eid)["status"] == "manual"
            and [r for r in store.get_execution(eid)["node_runs"] if r["node_id"] == "h"][-1]["attempt"] == expected_attempt
        ))
        return engine.get(eid).manual

    manual = await wait_manual(1)
    assert manual["remaining"] == 2
    assert engine.manual_decide(eid, "retry")[0]
    manual = await wait_manual(2)
    assert manual["remaining"] == 1
    assert engine.manual_decide(eid, "retry")[0]
    manual = await wait_manual(3)
    # 达到上限：明确停在人工处置节点，重试被拒绝
    assert manual["remaining"] == 0
    ok, reason = engine.manual_decide(eid, "retry")
    assert ok is False and "上限" in reason
    assert store.get_execution(eid)["status"] == "manual"
    assert engine.manual_decide(eid, "abort")[0]
    await wait_until(lambda: store.get_execution(eid)["status"] == "aborted")

    runs = store.get_execution(eid)["node_runs"]
    p_attempts = [r for r in runs if r["node_id"] == "p"]
    m_attempts = [r for r in runs if r["node_id"] == "m"]
    # 初次 + 2 次重试 = 3 轮，节点历史按 attempt 分清，没有被旧回路覆盖
    assert [r["attempt"] for r in p_attempts] == [1, 2, 3]
    assert [r["attempt"] for r in m_attempts] == [1, 2, 3]
    assert all(r["status"] == "failed" for r in p_attempts)
    assert "ok" not in {r["node_id"] for r in runs}


async def test_second_attempt_succeeds(harness):
    """第一次失败走回路，人工处置后重试；第二次成功，旧失败不被当成完成。"""
    store, robot, engine = harness
    robot.set_obstacle(True)
    eid = engine.start("flow1", LOOP_FLOW)
    await wait_until(lambda: store.get_execution(eid)["status"] == "manual")
    assert engine.manual_decide(eid, "retry")[0]

    # 第二次经过取物节点前清除障碍物
    await wait_until(
        lambda: [r for r in store.get_execution(eid)["node_runs"]
                 if r["node_id"] == "p"] and
        [r for r in store.get_execution(eid)["node_runs"] if r["node_id"] == "p"][-1]["attempt"] == 2
        and [r for r in store.get_execution(eid)["node_runs"] if r["node_id"] == "p"][-1]["status"] == "running")
    robot.set_obstacle(False)
    await wait_until(lambda: store.get_execution(eid)["status"] == "succeeded")

    runs = store.get_execution(eid)["node_runs"]
    p_runs = [r for r in runs if r["node_id"] == "p"]
    assert [(r["attempt"], r["status"]) for r in p_runs] == [
        (1, "failed"), (2, "succeeded")]
    ok_runs = [r for r in runs if r["node_id"] == "ok"]
    assert len(ok_runs) == 1
    assert robot.status()["holding"] == "包裹"


async def test_cancel_then_late_result_is_ignored(harness):
    """取消后动作迟到的成功结果不能当作新一轮完成，后续节点不得启动。"""
    store, robot, engine = harness

    class FlakyRobot(VirtualRobot):
        """模拟：move 的最后一步在收到取消后仍“迟到地”完成坐标推进。"""
        async def move(self, x, y, progress, stop, gate=None):  # type: ignore[override]
            import asyncio as _aio
            async with self._lock:
                self.state.idle = False
                await _aio.sleep(0.3)
                if stop.is_set():
                    # 现实中驱动可能依然回报了到位结果
                    self.state.x, self.state.y = float(x), float(y)
                    self.state.idle = True
                    return
                self.state.x, self.state.y = float(x), float(y)
                self.state.idle = True

    robot2 = FlakyRobot()
    engine2 = Engine(store, robot2)
    flow = {
        "nodes": [
            {"id": "m", "type": "move", "label": "移动", "params": {"x": 2, "y": 0}},
            {"id": "n", "type": "log", "label": "下一步", "params": {"message": "next"}},
        ],
        "edges": [{"id": "1", "source": "m", "target": "n", "kind": "success"}],
    }
    eid = engine2.start("flow1", flow)
    await wait_until(
        lambda: any(r["node_id"] == "m" and r["status"] == "running"
                    for r in store.get_execution(eid)["node_runs"]))
    assert engine2.cancel(eid)
    await wait_until(lambda: store.get_execution(eid)["status"] == "cancelled")

    runs = store.get_execution(eid)["node_runs"]
    assert "n" not in {r["node_id"] for r in runs}
    m_run = [r for r in runs if r["node_id"] == "m"][-1]
    assert m_run["status"] == "cancelled"
    types = [e["type"] for e in store.get_events(eid)]
    assert "node_late_result_ignored" in types
    assert "node_succeeded" not in types
