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


async def test_late_move_success_after_cancel_keeps_cancelled(harness):
    """取消后尚未返回的移动稍后报告成功：节点保持 cancelled，迟到结果只作追溯。"""
    store, robot, engine = harness
    robot.set_late_finish(True)
    flow = {"nodes": [
        {"id": "m", "type": "move", "label": "长距离移动", "params": {"x": 8, "y": 0}},
        {"id": "n", "type": "log", "label": "下一步", "params": {"message": "go"}},
    ], "edges": [
        {"id": "1", "source": "m", "target": "n", "kind": "success"},
    ]}
    eid = engine.start("flow1", flow)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    assert engine.cancel(eid)
    await wait_until(lambda: store.get_execution(eid)["status"] == "cancelled", timeout=6)
    # 移动动作稍后才真正跑完并报告成功
    await asyncio.sleep(1.0)

    record = store.get_execution(eid)
    assert record["status"] == "cancelled"
    move_runs = [nr for nr in record["node_runs"] if nr["node_id"] == "m"]
    assert [nr["status"] for nr in move_runs] == ["cancelled"]
    assert not any(nr["node_id"] == "n" for nr in record["node_runs"])

    events = store.get_events(eid)
    assert sum(1 for e in events if e["type"] == "node_succeeded" and e["node_id"] == "m") == 0
    assert sum(1 for e in events if e["type"] == "execution_finished") == 1
    late = [e for e in events if e["type"] == "late_result_recorded"]
    assert len(late) == 1 and late[0]["data"]["outcome"] == "success"

    followups = record["followups"]
    assert any(f["kind"] == "late_result" and f["outcome"] == "success"
               and f["node_id"] == "m" for f in followups)
    reason = record["termination_reason"]
    assert reason and "取消" in reason


CLEANUP_FLOW = {
    "nodes": [
        {"id": "m", "type": "move", "label": "移动", "params": {"x": 3, "y": 4}},
        {"id": "p", "type": "pick", "label": "取物",
         "params": {"item": "包裹", "max_retries": 0}},
        {"id": "ok", "type": "log", "label": "成功", "params": {"message": "done"}},
        {"id": "r", "type": "log", "label": "失败分支", "params": {"message": "fail"}},
    ],
    "edges": [
        {"id": "1", "source": "m", "target": "p", "kind": "success"},
        {"id": "2", "source": "p", "target": "ok", "kind": "success"},
        {"id": "3", "source": "p", "target": "r", "kind": "failure"},
        {"id": "4", "source": "p", "target": "r", "kind": "exhausted"},
    ],
}


async def test_failure_runs_cleanup_and_keeps_failure_reason(harness):
    """失败后收尾：放下/返回安全位置执行，节点仍为 failed，原因不被覆盖。"""
    store, robot, engine = harness
    robot.set_obstacle(True)
    eid = engine.start("flow1", CLEANUP_FLOW)
    await wait_until(lambda: store.get_execution(eid)["status"] == "succeeded", timeout=12)

    record = store.get_execution(eid)
    pick = [nr for nr in record["node_runs"] if nr["node_id"] == "p"]
    assert pick[0]["status"] == "failed"
    assert "障碍" in pick[0]["detail"]
    # 收尾结束回到安全位置 (0,0)
    assert (robot.status()["x"], robot.status()["y"]) == (0.0, 0.0)
    assert robot.status()["idle"] is True

    events = store.get_events(eid)
    types = [e["type"] for e in events]
    assert "cleanup_started" in types
    assert "cleanup_succeeded" in types
    followups = record["followups"]
    cleanup = [f for f in followups if f["kind"] == "cleanup"]
    assert any(f["outcome"] == "succeeded" for f in cleanup)
    # 失败分支照常推进
    assert any(e["type"] in ("branch_taken", "retries_exhausted") and e["node_id"] == "p"
               for e in events)
    node_runs = {nr["node_id"]: nr["status"] for nr in record["node_runs"]}
    assert node_runs["r"] == "succeeded"
    assert "ok" not in node_runs


async def test_cancel_during_wait_skips_cleanup(harness):
    """取消发生在等待条件期间：不启动任何收尾动作，记录为 cancelled。"""
    store, robot, engine = harness
    flow = {"nodes": [
        {"id": "m", "type": "move", "label": "移动", "params": {"x": 2, "y": 2}},
        {"id": "w", "type": "wait", "label": "等待", "params": {"condition": "ready"}},
        {"id": "p", "type": "pick", "label": "取物", "params": {"item": "x"}},
    ], "edges": [
        {"id": "1", "source": "m", "target": "w", "kind": "success"},
        {"id": "2", "source": "w", "target": "p", "kind": "success"},
    ]}
    eid = engine.start("flow1", flow)
    await wait_until(lambda: store.get_execution(eid)["status"] == "waiting")
    assert engine.cancel(eid)
    await wait_until(lambda: store.get_execution(eid)["status"] == "cancelled")
    events = store.get_events(eid)
    assert not any(e["type"].startswith("cleanup") for e in events)
    reason = store.get_execution(eid)["termination_reason"]
    assert "等待" in reason


async def test_cancel_during_cleanup_keeps_original_failure(harness):
    """收尾进行中再次取消：当前收尾动作允许做完，原失败原因保留。"""
    store, robot, engine = harness
    robot.set_obstacle(True)
    eid = engine.start("flow1", CLEANUP_FLOW)
    await wait_until(
        lambda: any(e["type"] == "cleanup_started"
                    for e in store.get_events(eid)), timeout=12)
    assert engine.cancel(eid)
    await wait_until(lambda: store.get_execution(eid)["status"] == "cancelled", timeout=6)

    record = store.get_execution(eid)
    pick = [nr for nr in record["node_runs"] if nr["node_id"] == "p"]
    assert pick[0]["status"] == "failed"
    assert "障碍" in pick[0]["detail"]
    reason = record["termination_reason"]
    assert "失败" in reason and "障碍" in reason
    followups = record["followups"]
    cleanup = [f for f in followups if f["kind"] == "cleanup"]
    assert any(f["outcome"] == "cancelled" for f in cleanup)


async def test_persisted_termination_and_recovery(tmp_path):
    """落库的终止原因/后续信息在重启后保持一致；遗留运行态停在待核实阶段。"""
    db = tmp_path / "test.db"
    store = Store(db)
    robot = VirtualRobot()
    engine = Engine(store, robot)
    robot.set_late_finish(True)
    flow = {"nodes": [
        {"id": "m", "type": "move", "label": "移动", "params": {"x": 6, "y": 0}},
        {"id": "n", "type": "log", "label": "下一步", "params": {"message": "go"}},
    ], "edges": [
        {"id": "1", "source": "m", "target": "n", "kind": "success"},
    ]}
    eid = engine.start("flow1", flow)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    engine.cancel(eid)
    await wait_until(lambda: store.get_execution(eid)["status"] == "cancelled", timeout=6)
    await asyncio.sleep(0.8)
    reason_before = store.get_termination_reason(eid)
    assert reason_before

    # 模拟后端重启：旧的运行态标记 interrupted；已终止记录与其原因/后续信息保持不变
    Store(db)  # 重启时仅打开数据库，不影响已终止记录
    record = store.get_execution(eid)
    assert record["status"] == "cancelled"
    assert record["termination_reason"] == reason_before
    assert any(f["kind"] == "late_result" for f in record["followups"])
    move_runs = [nr for nr in record["node_runs"] if nr["node_id"] == "m"]
    assert [nr["status"] for nr in move_runs] == ["cancelled"]

    # 另一个处于 running 的执行在重启（旧进程消失、新引擎构造）后变为 interrupted
    store2 = Store(db)
    robot2 = VirtualRobot()
    engine2 = Engine(store2, robot2)
    eid2 = engine2.start("flow1", flow)
    await wait_until(lambda: store2.get_execution(eid2)["status"] == "running")
    for execution in list(engine2._executions.values()):
        execution.task.cancel()
    await asyncio.sleep(0.1)
    # 新进程启动新引擎：遗留运行态停在「待核实」，已终止记录不受影响
    store3 = Store(db)
    Engine(store3, VirtualRobot())
    assert store3.get_execution(eid2)["status"] == "awaiting_verification"
    assert store3.get_execution(eid)["status"] == "cancelled"
    for execution in list(engine._executions.values()):
        execution.task.cancel()


async def test_recovery_continue_mid_action_no_replay(tmp_path):
    """重启待核实：选择继续收尾后，已发出动作不重放，从后续顺序接续。"""
    db = tmp_path / "test.db"
    store = Store(db)
    robot = VirtualRobot()
    engine = Engine(store, robot)
    engine.schedule_pending_recoveries()
    flow = {"nodes": [
        {"id": "m", "type": "move", "label": "长距离移动", "params": {"x": 6, "y": 0}},
        {"id": "n", "type": "log", "label": "下一步", "params": {"message": "go"}},
    ], "edges": [{"id": "1", "source": "m", "target": "n", "kind": "success"}]}
    eid = engine.start("flow1", flow)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    for execution in list(engine._executions.values()):
        execution.task.cancel()
    await asyncio.sleep(0.1)

    # 模拟重启：执行停在待核实，展示持有物/最后位置/未确认动作
    store2 = Store(db)
    robot2 = VirtualRobot()
    engine2 = Engine(store2, robot2)
    engine2.schedule_pending_recoveries()
    record = store2.get_execution(eid)
    assert record["status"] == "awaiting_verification"
    pending = record["pending_action"]
    assert pending["node_id"] == "m" and pending["action_type"] == "move"
    snapshot = record["device_snapshot"]
    assert snapshot is not None

    # 核实期间的迟到回应只补充到旧执行记录，不发出新动作
    assert engine2.report_late_response(eid, "success", "移动实际已完成")
    await asyncio.sleep(0.2)
    assert store2.get_execution(eid)["status"] == "awaiting_verification"
    followups = store2.get_execution(eid)["followups"]
    assert any(f["kind"] == "late_result" and f["node_id"] == "m" for f in followups)

    assert engine2.verify_recovery(eid, True, "现场确认设备安全")
    await wait_until(lambda: store2.get_execution(eid)["status"] == "succeeded", timeout=8)
    final = store2.get_execution(eid)
    move_runs = [nr for nr in final["node_runs"] if nr["node_id"] == "m"]
    # 移动没有被第二次执行：旧记录定格为 interrupted（结果未知）
    assert [nr["status"] for nr in move_runs] == ["interrupted"]
    assert any(nr["node_id"] == "n" for nr in final["node_runs"])
    assert final["termination_reason"] == "全部节点执行成功"


async def test_recovery_end_keeps_original_failure(tmp_path):
    """待核实时选择结束：原故障原因与人工处置原因分别保留，不发出新动作。"""
    db = tmp_path / "test.db"
    store = Store(db)
    robot = VirtualRobot()
    engine = Engine(store, robot)
    engine.schedule_pending_recoveries()
    robot.set_obstacle(True)
    eid = engine.start("flow1", CLEANUP_FLOW)
    await wait_until(
        lambda: any(nr["node_id"] == "p" and nr["status"] == "failed"
                    for nr in store.get_execution(eid)["node_runs"]), timeout=12)
    await asyncio.sleep(0.05)
    for execution in list(engine._executions.values()):
        execution.task.cancel()
    await asyncio.sleep(0.1)

    store2 = Store(db)
    engine2 = Engine(store2, VirtualRobot())
    engine2.schedule_pending_recoveries()
    assert store2.get_execution(eid)["status"] == "awaiting_verification"
    assert engine2.verify_recovery(eid, False, "现场无法确认，结束流程")
    record = store2.get_execution(eid)
    assert record["status"] == "resolved"
    reason = record["termination_reason"]
    assert "障碍" in reason  # 原故障保留
    assert "现场无法确认，结束流程" in reason  # 人工处置原因保留

    # 连续重开：决定已保存，不重复要求，也不改变终态
    store3 = Store(db)
    engine3 = Engine(store3, VirtualRobot())
    engine3.schedule_pending_recoveries()
    assert store3.get_execution(eid)["status"] == "resolved"
    assert engine3.verify_recovery(eid, True) is False


async def test_recovery_cancel_again_and_idempotent_decision(tmp_path):
    """待核实期间再次取消：结束流程、不移动；重复决定被拒绝。"""
    db = tmp_path / "test.db"
    store = Store(db)
    robot = VirtualRobot()
    engine = Engine(store, robot)
    engine.schedule_pending_recoveries()
    flow = {"nodes": [
        {"id": "m", "type": "move", "label": "移动", "params": {"x": 5, "y": 0}},
    ], "edges": []}
    eid = engine.start("flow1", flow)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    for execution in list(engine._executions.values()):
        execution.task.cancel()
    await asyncio.sleep(0.1)

    store2 = Store(db)
    robot2 = VirtualRobot()
    engine2 = Engine(store2, robot2)
    engine2.schedule_pending_recoveries()
    before = robot2.status()
    assert engine2.cancel(eid)
    await asyncio.sleep(0.2)
    assert store2.get_execution(eid)["status"] == "resolved"
    assert robot2.status() == before  # 没有发出任何新动作
    assert engine2.cancel(eid) is False


async def test_recovery_resume_cleanup_skips_done_stage(tmp_path):
    """在失败收尾中断重启：恢复后只做未完成的收尾阶段，已放下不重复。"""
    db = tmp_path / "test.db"
    store = Store(db)
    robot = VirtualRobot()
    engine = Engine(store, robot)
    engine.schedule_pending_recoveries()
    robot.set_obstacle(True)
    eid = engine.start("flow1", CLEANUP_FLOW)
    # 等到收尾的放下阶段已完成（此处机械爪为空，记 noop）、尚未返回安全位置
    await wait_until(
        lambda: any(f["kind"] == "cleanup" and f["outcome"] in ("succeeded", "noop")
                    for f in store.get_execution(eid)["followups"]), timeout=12)
    for execution in list(engine._executions.values()):
        execution.task.cancel()
    await asyncio.sleep(0.1)

    store2 = Store(db)
    robot2 = VirtualRobot()
    engine2 = Engine(store2, robot2)
    engine2.schedule_pending_recoveries()
    assert store2.get_execution(eid)["status"] == "awaiting_verification"
    pending = store2.get_execution(eid)["pending_action"]
    assert pending["phase"] == "cleanup"

    release_calls = {"n": 0}
    orig_release = robot2.release_if_holding

    async def counted_release(*args, **kwargs):
        release_calls["n"] += 1
        return await orig_release(*args, **kwargs)

    robot2.release_if_holding = counted_release  # type: ignore[assignment]
    assert engine2.verify_recovery(eid, True)
    await wait_until(lambda: store2.get_execution(eid)["status"] == "succeeded", timeout=10)
    # 已完成的放下阶段没有再次执行
    assert release_calls["n"] == 0
    assert (robot2.status()["x"], robot2.status()["y"]) == (0.0, 0.0)
    # 原失败与失败分支照常推进
    record = store2.get_execution(eid)
    pick = [nr for nr in record["node_runs"] if nr["node_id"] == "p"][0]
    assert pick["status"] == "failed" and "障碍" in pick["detail"]
    assert any(nr["node_id"] == "r" for nr in record["node_runs"])


async def test_recovery_resume_wait_keeps_waiting(tmp_path):
    """等待条件期间重启：恢复后继续等待原条件，满足后正常接续。"""
    db = tmp_path / "test.db"
    store = Store(db)
    robot = VirtualRobot()
    engine = Engine(store, robot)
    engine.schedule_pending_recoveries()
    eid = engine.start("flow1", FLOW)
    await wait_until(lambda: store.get_execution(eid)["status"] == "waiting")
    for execution in list(engine._executions.values()):
        execution.task.cancel()
    await asyncio.sleep(0.1)

    store2 = Store(db)
    robot2 = VirtualRobot()
    engine2 = Engine(store2, robot2)
    engine2.schedule_pending_recoveries()
    assert store2.get_execution(eid)["status"] == "awaiting_verification"
    assert engine2.verify_recovery(eid, True)
    await wait_until(lambda: store2.get_execution(eid)["status"] == "waiting")
    assert engine2.get(eid).wait_condition == "ready"
    assert engine2.signal(eid, "ready")
    await wait_until(lambda: store2.get_execution(eid)["status"] == "succeeded", timeout=10)


async def test_saved_decision_auto_resumes_after_restart(tmp_path):
    """已作出继续决定但恢复协程未跑完又重启：不重复询问，自动接续。"""
    db = tmp_path / "test.db"
    store = Store(db)
    robot = VirtualRobot()
    engine = Engine(store, robot)
    engine.schedule_pending_recoveries()
    flow = {"nodes": [
        {"id": "m", "type": "move", "label": "移动", "params": {"x": 3, "y": 0}},
        {"id": "n", "type": "log", "label": "下一步", "params": {"message": "go"}},
    ], "edges": [{"id": "1", "source": "m", "target": "n", "kind": "success"}]}
    eid = engine.start("flow1", flow)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    for execution in list(engine._executions.values()):
        execution.task.cancel()
    await asyncio.sleep(0.1)

    store2 = Store(db)
    engine2 = Engine(store2, VirtualRobot())
    # 保存继续决定（模拟核实完成后尚未跑完又重启）
    assert store2.save_recovery_decision(eid, "continue", "已核实")

    store3 = Store(db)
    engine3 = Engine(store3, VirtualRobot())
    engine3.schedule_pending_recoveries()
    await wait_until(lambda: store3.get_execution(eid)["status"] == "succeeded", timeout=8)
    # 不会重复要求决定
    assert engine3.verify_recovery(eid, True) is False
