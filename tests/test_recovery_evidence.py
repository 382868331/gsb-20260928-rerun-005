from __future__ import annotations

import asyncio

import pytest

pytestmark = pytest.mark.anyio

from backend.engine import Engine
from backend.simulator import VirtualRobot
from backend.store import Store


MOVE_FLOW = {
    "nodes": [
        {"id": "m", "type": "move", "label": "移动", "params": {"x": 6, "y": 0}},
        {"id": "n", "type": "log", "label": "下一步", "params": {"message": "go"}},
    ],
    "edges": [{"id": "1", "source": "m", "target": "n", "kind": "success"}],
}


async def wait_until(predicate, timeout=4.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("等待超时")


async def crash_while_moving(db_path):
    """启动移动动作后模拟服务中断，返回执行 id。"""
    store = Store(db_path)
    engine = Engine(store, VirtualRobot())
    eid = engine.start("flow1", MOVE_FLOW)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    for execution in list(engine._executions.values()):
        execution.task.cancel()
    await asyncio.sleep(0.1)
    return eid


async def test_conflicting_evidence_blocks_and_retract_allows(tmp_path):
    """矛盾证据阻止继续；撤回其一后可继续，且原动作只执行一次。"""
    db = tmp_path / "evidence.db"
    eid = await crash_while_moving(db)

    store = Store(db)
    engine = Engine(store, VirtualRobot())
    assert store.get_execution(eid)["status"] == "awaiting_recovery"

    # 现场观察：动作未完成；设备迟到回应：动作已完成 → 互相矛盾
    ev1 = engine.record_evidence(
        eid, "action_result", "onsite",
        {"outcome": "failure", "detail": "现场看到机器人停在半途"},
        observed_at="2026-10-04 10:00:00")
    assert ev1 is not None
    assert engine.record_late_action(eid, "success", "设备迟到报告移动完成")
    conflicts = engine.evidence_conflicts(eid, "m", 1)
    assert "action_result" in conflicts

    # 矛盾未解除：不允许继续执行，且旧记录不被覆盖
    assert not engine.resolve_recovery(eid, finish=True)
    assert store.get_execution(eid)["status"] == "awaiting_recovery"
    evidence = store.list_evidence(eid)
    assert len(evidence) == 2 and all(not e["retracted"] for e in evidence)
    assert {e["source"] for e in evidence} == {"onsite", "device"}
    assert all(e["observed_at"] for e in evidence)
    assert any(e["type"] == "recovery_blocked_conflict"
               for e in store.get_events(eid))

    # 撤回错误观察（说明原因）后继续：基于剩余有效证据作出决定
    wrong_id = ev1
    assert engine.retract_evidence(eid, wrong_id, "观察位置记错，机器人实际已到位")
    assert engine.evidence_conflicts(eid, "m", 1) == {}
    assert engine.resolve_recovery(eid, finish=True)
    await wait_until(lambda: store.get_execution(eid)["status"] == "succeeded")

    record = store.get_execution(eid)
    move_runs = [nr for nr in record["node_runs"] if nr["node_id"] == "m"]
    assert len(move_runs) == 1 and move_runs[0]["status"] == "succeeded"
    started = [e for e in store.get_events(eid)
               if e["type"] == "node_started" and e["node_id"] == "m"]
    assert len(started) == 1  # 原动作只执行了一次

    # 决定记录了实际依赖的证据；被撤回的观察不在依据中
    decisions = store.list_recovery_decisions(eid)
    assert len(decisions) == 1 and decisions[0]["decision"] == "finish"
    kept = [e for e in store.list_evidence(eid)
            if not e["retracted"] and e["category"] == "action_result"]
    assert decisions[0]["evidence_ids"] == [e["id"] for e in kept]
    assert wrong_id not in decisions[0]["evidence_ids"]
    # 撤回的记录仍保留（含原因），历史可独立查看
    retracted = [e for e in store.list_evidence(eid) if e["retracted"]]
    assert retracted[0]["retract_reason"] == "观察位置记错，机器人实际已到位"


async def test_missing_action_evidence_stays_pending(tmp_path):
    """动作结果未确认且缺少证据时：保持待核实，不发出任何新动作。"""
    db = tmp_path / "missing.db"
    eid = await crash_while_moving(db)
    store = Store(db)
    engine = Engine(store, VirtualRobot())
    assert store.get_execution(eid)["status"] == "awaiting_recovery"
    # 只有位置/持物证据，没有动作结果证据 → 继续被拒绝
    assert engine.record_evidence(eid, "position", "onsite", {"x": 6, "y": 0})
    assert engine.record_evidence(eid, "holding", "onsite", {"holding": None})
    assert not engine.resolve_recovery(eid, finish=True)
    assert store.get_execution(eid)["status"] == "awaiting_recovery"
    assert any(e["type"] == "recovery_blocked_missing_evidence"
               for e in store.get_events(eid))
    move_runs = [nr for nr in store.get_execution(eid)["node_runs"]
                 if nr["node_id"] == "m"]
    assert len(move_runs) == 1  # 没有重复发出移动


async def test_evidence_and_pending_state_survive_restart(tmp_path):
    """关闭再重开：待核实执行、证据与未决定项都能找回；重复确认被拒绝。"""
    db = tmp_path / "restart.db"
    eid = await crash_while_moving(db)
    store = Store(db)
    engine = Engine(store, VirtualRobot())
    assert engine.record_evidence(
        eid, "action_result", "device",
        {"outcome": "success", "detail": "设备迟到确认到位"})

    # 模拟页面关闭/服务重启：证据与待核实状态仍在
    store2 = Store(db)
    engine2 = Engine(store2, VirtualRobot())
    assert store2.get_execution(eid)["status"] == "awaiting_recovery"
    evidence = store2.list_evidence(eid)
    assert len(evidence) == 1 and evidence[0]["category"] == "action_result"
    assert store2.list_recovery_decisions(eid) == []

    # 决定落库后：重复确认被拒绝；再次重启不会重放决定、不重复发出动作
    assert engine2.resolve_recovery(eid, finish=True)
    assert not engine2.resolve_recovery(eid, finish=True)
    assert not engine2.resolve_recovery(eid, finish=False)
    await wait_until(lambda: store2.get_execution(eid)["status"] == "succeeded")

    engine3 = Engine(Store(db), VirtualRobot())
    await asyncio.sleep(0.2)
    final = engine3.store.get_execution(eid)
    assert final["status"] == "succeeded"
    move_runs = [nr for nr in final["node_runs"] if nr["node_id"] == "m"]
    assert len(move_runs) == 1
    decisions = engine3.store.list_recovery_decisions(eid)
    assert len(decisions) == 1
    assert engine3.store.list_evidence(eid)  # 证据历史仍可独立查看


WAIT_AFTER_MOVE_FLOW = {
    "nodes": [
        {"id": "m", "type": "move", "label": "移动", "params": {"x": 6, "y": 0}},
        {"id": "w", "type": "wait", "label": "等待",
         "params": {"condition": "shelf_ready"}},
        {"id": "n", "type": "log", "label": "下一步", "params": {"message": "go"}},
    ],
    "edges": [
        {"id": "1", "source": "m", "target": "w", "kind": "success"},
        {"id": "2", "source": "w", "target": "n", "kind": "success"},
    ],
}


async def _kill_all(engine):
    for execution in list(engine._executions.values()):
        execution.task.cancel()
    await asyncio.sleep(0.1)


async def test_new_crash_after_resolved_decision_allows_fresh_recovery(tmp_path):
    """处置决定应用后，恢复出的尾部执行再次崩溃：新一轮仍可登记证据并决定。"""
    db = tmp_path / "twice.db"
    store = Store(db)
    engine = Engine(store, VirtualRobot())
    eid = engine.start("flow1", WAIT_AFTER_MOVE_FLOW)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    await _kill_all(engine)

    s2 = Store(db)
    e2 = Engine(s2, VirtualRobot())
    assert s2.get_execution(eid)["status"] == "awaiting_recovery"
    assert e2.record_late_action(eid, "success", "设备迟到确认到位")
    assert e2.resolve_recovery(eid, finish=True)
    await wait_until(lambda: s2.get_execution(eid)["status"] == "waiting")
    await asyncio.sleep(0.1)
    await _kill_all(e2)

    s3 = Store(db)
    e3 = Engine(s3, VirtualRobot())
    assert s3.get_execution(eid)["status"] == "awaiting_recovery"
    evidence_id = e3.record_evidence(
        eid, "position", "onsite", {"x": 6, "y": 0})
    assert evidence_id is not None
    e3.record_evidence(eid, "holding", "onsite", {"holding": None})
    assert e3.resolve_recovery(eid, finish=True)
    await wait_until(lambda: s3.get_execution(eid)["status"] == "waiting")
    await asyncio.sleep(0.1)
    assert e3.signal(eid, "shelf_ready")
    await wait_until(lambda: s3.get_execution(eid)["status"] == "succeeded")

    final = s3.get_execution(eid)
    move_runs = [nr for nr in final["node_runs"] if nr["node_id"] == "m"]
    assert len(move_runs) == 1 and move_runs[0]["status"] == "succeeded"
    decisions = s3.list_recovery_decisions(eid)
    assert [d["decision"] for d in decisions] == ["finish", "finish"]
    # 第二次决定只引用第二次崩溃周期登记的证据
    assert decisions[1]["evidence_ids"] == [evidence_id, evidence_id + 1]


async def test_pending_decision_replayed_after_restart_mid_apply(tmp_path):
    """决定已提交但应用过程中崩溃：重启自动续作，操作者无需重复确认。"""
    db = tmp_path / "pending.db"
    store = Store(db)
    engine = Engine(store, VirtualRobot())
    eid = engine.start("flow1", MOVE_FLOW)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    await _kill_all(engine)

    s2 = Store(db)
    e2 = Engine(s2, VirtualRobot())
    assert e2.record_late_action(eid, "success", "设备迟到确认到位")
    assert e2.resolve_recovery(eid, finish=True)
    assert s2.get_recovery_state(eid).get("pending_decision") == "finish"
    await _kill_all(e2)

    s3 = Store(db)
    Engine(s3, VirtualRobot())  # 构造即应自动重放待应用决定
    await wait_until(lambda: s3.get_execution(eid)["status"] == "succeeded")
    decisions = s3.list_recovery_decisions(eid)
    assert len(decisions) == 1  # 重放不产生重复决定
