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
        if execution.task is not None:
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


async def test_two_conflicting_positions_block_progress(tmp_path):
    """两条现场位置证据互相矛盾：分别留存并阻止推进；撤回错误一条后可继续。"""
    db = tmp_path / "pos.db"
    eid = await crash_while_moving(db)
    store = Store(db)
    engine = Engine(store, VirtualRobot())

    ev_a = engine.record_evidence(
        eid, "position", "onsite", {"x": 2, "y": 0},
        observed_at="2026-10-04 10:00:00")
    ev_b = engine.record_evidence(
        eid, "position", "onsite", {"x": 5, "y": 0},
        observed_at="2026-10-04 10:01:00")
    # 动作结果证据（迟到成功）补齐后，仅因位置矛盾仍必须阻止
    assert engine.record_late_action(eid, "success", "设备迟到报告移动完成")
    conflicts = engine.evidence_conflicts(eid, "m", 1)
    assert conflicts["position"] == [ev_a, ev_b]
    assert not engine.resolve_recovery(eid, finish=True)
    assert store.get_execution(eid)["status"] == "awaiting_recovery"
    # 旧证据原样保留、互不覆盖
    positions = [e["value"] for e in store.list_evidence(eid)
                 if e["category"] == "position"]
    assert positions == [{"x": 2, "y": 0}, {"x": 5, "y": 0}]

    assert engine.retract_evidence(eid, ev_a, "看错地标，实际在 x=5")
    assert engine.evidence_conflicts(eid, "m", 1) == {}
    assert engine.resolve_recovery(eid, finish=True)
    await wait_until(lambda: store.get_execution(eid)["status"] == "succeeded")
    record = store.get_execution(eid)
    move_runs = [nr for nr in record["node_runs"] if nr["node_id"] == "m"]
    assert len(move_runs) == 1 and move_runs[0]["status"] == "succeeded"
    # 恢复快照采用剩余有效位置证据
    assert engine.robot.state.x == 5
    retracted = [e for e in store.list_evidence(eid) if e["retracted"]]
    assert retracted[0]["retract_reason"] == "看错地标，实际在 x=5"


async def test_holding_conflict_blocks_and_history_reopen(tmp_path):
    """持物冲突分别留存、阻止推进；结束决定引用证据，历史可独立重开查看。"""
    db = tmp_path / "holding.db"
    eid = await crash_while_moving(db)
    store = Store(db)
    engine = Engine(store, VirtualRobot())
    assert engine.record_evidence(
        eid, "action_result", "device",
        {"outcome": "success", "detail": "设备迟到确认到位"})
    assert engine.record_evidence(
        eid, "holding", "onsite", {"holding": "包裹A"})
    assert engine.record_evidence(
        eid, "holding", "device", {"holding": None})
    assert "holding" in engine.evidence_conflicts(eid, "m", 1)
    assert not engine.resolve_recovery(eid, finish=True)
    holdings = [e["value"]["holding"] for e in store.list_evidence(eid)
                if e["category"] == "holding"]
    assert holdings == ["包裹A", None]

    # 撤回错误观察（保留原因）后矛盾解除，结束流程
    wrong = [e for e in store.list_evidence(eid)
             if e["category"] == "holding" and e["value"]["holding"] is None][0]["id"]
    assert engine.retract_evidence(eid, wrong, "设备上报为空爪，现场复核确有持物")
    assert engine.resolve_recovery(eid, finish=False)
    await wait_until(lambda: store.get_execution(eid)["status"] == "resolved")

    decisions = store.list_recovery_decisions(eid)
    assert len(decisions) == 1 and decisions[0]["decision"] == "abort"
    kept = [e["id"] for e in store.list_evidence(eid)
            if not e["retracted"]]
    # 决定只引用当时有效的证据（被撤回的不在内）
    assert set(decisions[0]["evidence_ids"]) == set(kept)
    assert wrong not in decisions[0]["evidence_ids"]
    # 已结束执行的证据与决定可独立重开查看
    assert {e["category"] for e in store.list_evidence(eid)} == {
        "action_result", "holding"}


async def test_late_success_keeps_single_move_and_no_resend(tmp_path):
    """迟到成功：不重发原移动；恢复后从成功分支继续，旧回应不落到后续节点。"""
    db = tmp_path / "late.db"
    eid = await crash_while_moving(db)
    store = Store(db)
    engine = Engine(store, VirtualRobot())
    assert engine.record_late_action(eid, "success", "设备迟到：已到 (6,0)")
    assert engine.resolve_recovery(eid, finish=True)
    await wait_until(lambda: store.get_execution(eid)["status"] == "succeeded")
    record = store.get_execution(eid)
    move_runs = [nr for nr in record["node_runs"] if nr["node_id"] == "m"]
    assert len(move_runs) == 1 and move_runs[0]["status"] == "succeeded"
    started = [e for e in store.get_events(eid) if e["type"] == "node_started"]
    assert [e["node_id"] for e in started if e["node_id"] == "m"] == ["m"]
    # 迟到结果只挂在旧（节点,轮次）的 followups，不出现在后续节点
    followups = record["followups"]
    assert all(f["node_id"] == "m" and f["attempt"] == 1 for f in followups)
    # 执行已结束：迟到回应不能再写入该执行
    assert not engine.record_late_action(eid, "failure", "更晚的回应不得归属旧执行")


async def test_decision_then_crash_again_allows_new_episode(tmp_path):
    """处置后恢复途中再次重启：旧决定不重放，可重新登记证据与决定。"""
    db = tmp_path / "replay.db"
    flow = {
        "nodes": [
            {"id": "m", "type": "move", "params": {"x": 6, "y": 0}},
            {"id": "n", "type": "move", "params": {"x": -3, "y": 0}},
            {"id": "z", "type": "log", "params": {"message": "done"}},
        ],
        "edges": [
            {"id": "1", "source": "m", "target": "n", "kind": "success"},
            {"id": "2", "source": "n", "target": "z", "kind": "success"},
        ],
    }
    store = Store(db)
    engine = Engine(store, VirtualRobot())
    eid = engine.start("flow1", flow)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    for execution in list(engine._executions.values()):
        execution.task.cancel()
    await asyncio.sleep(0.1)

    # 第一次重启：迟到成功 + 决定继续
    store2 = Store(db)
    engine2 = Engine(store2, VirtualRobot())
    assert store2.get_execution(eid)["status"] == "awaiting_recovery"
    assert engine2.record_late_action(eid, "success", "第一次迟到成功")
    assert engine2.resolve_recovery(eid, finish=True)
    await wait_until(
        lambda: any(nr["node_id"] == "n" and nr["status"] == "running"
                    for nr in store2.get_execution(eid)["node_runs"]))
    del engine2, store2

    # 再次重启：进入新一轮待核实，旧 applied 决定不自动重放
    store3 = Store(db)
    engine3 = Engine(store3, VirtualRobot())
    await asyncio.sleep(0.15)
    assert store3.get_execution(eid)["status"] == "awaiting_recovery"
    assert (store3.get_recovery_state(eid) or {}).get("recovery_decision") is None
    # 可以为新执行登记位置/持物/动作结果
    assert engine3.record_evidence(eid, "position", "onsite", {"x": 6, "y": 0})
    assert engine3.record_evidence(eid, "holding", "onsite", {"holding": None})
    ev2 = engine3.record_evidence(
        eid, "action_result", "device",
        {"outcome": "success", "detail": "第二次迟到成功"})
    assert engine3.resolve_recovery(eid, finish=True)
    await wait_until(lambda: store3.get_execution(eid)["status"] == "succeeded")

    decisions = store3.list_recovery_decisions(eid)
    assert [d["episode"] for d in decisions] == [1, 2]
    assert decisions[1]["evidence_ids"] == [ev2 - 2, ev2 - 1, ev2]
    # 两轮核实都只发出了各自应有的动作：m 与 n 各一次，无重复移动
    runs = store3.get_execution(eid)["node_runs"]
    counts = {nr["node_id"]: 0 for nr in runs}
    for nr in runs:
        counts[nr["node_id"]] += 1
    assert counts == {"m": 1, "n": 1, "z": 1}


async def test_retract_scoped_to_execution_and_state(tmp_path):
    """撤回：不能作用于其他执行或已离开待核实状态的证据，原因必须保留。"""
    db = tmp_path / "cross.db"
    eid_a = await crash_while_moving(db)
    eid_b = await crash_while_moving(db)
    store_a, store_b = Store(db), Store(db)
    engine_a = Engine(store_a, VirtualRobot())
    engine_b = Engine(store_b, VirtualRobot())
    ev_a = engine_a.record_evidence(
        eid_a, "position", "onsite", {"x": 1, "y": 0})
    ev_b = engine_b.record_evidence(
        eid_b, "position", "onsite", {"x": 2, "y": 0})
    # 不能撤回别的执行的证据 id
    assert not engine_b.retract_evidence(eid_b, ev_a, "不应生效")
    # 待核实期间可撤回，原因保留
    assert engine_a.retract_evidence(eid_a, ev_a, "读数错误")
    assert store_a.list_evidence(eid_a)[0]["retract_reason"] == "读数错误"
    # 执行离开待核实后不得再撤回
    assert engine_b.record_late_action(eid_b, "success", "迟到成功")
    assert engine_b.resolve_recovery(eid_b, finish=True)
    await wait_until(lambda: store_b.get_execution(eid_b)["status"] == "succeeded")
    assert not engine_b.retract_evidence(eid_b, ev_b, "结束后撤回不允许")
    assert not store_b.list_evidence(eid_b)[0]["retracted"]


async def test_crash_right_after_decision_opens_new_episode(tmp_path):
    """决定刚应用、新动作尚未开始就再重启：开启新一轮核实，可再登记证据与决定。"""
    db = tmp_path / "edge.db"
    flow = {
        "nodes": [
            {"id": "m", "type": "move", "params": {"x": 6, "y": 0}},
            {"id": "n", "type": "move", "params": {"x": -3, "y": 0}},
            {"id": "z", "type": "log", "params": {"message": "done"}},
        ],
        "edges": [
            {"id": "1", "source": "m", "target": "n", "kind": "success"},
            {"id": "2", "source": "n", "target": "z", "kind": "success"},
        ],
    }
    store = Store(db)
    engine = Engine(store, VirtualRobot())
    eid = engine.start("flow1", flow)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    for execution in list(engine._executions.values()):
        if execution.task is not None:
            execution.task.cancel()
    await asyncio.sleep(0.1)

    store2 = Store(db)
    engine2 = Engine(store2, VirtualRobot())
    assert engine2.record_late_action(eid, "success", "第一次确认")
    assert engine2.resolve_recovery(eid, finish=True)
    # 恢复任务继续跑到后续节点 n（动作进行中）时再次“杀进程”
    await wait_until(
        lambda: any(nr["node_id"] == "n" and nr["status"] == "running"
                    for nr in store2.get_execution(eid)["node_runs"]))
    del engine2, store2

    store3 = Store(db)
    engine3 = Engine(store3, VirtualRobot())
    await asyncio.sleep(0.1)
    assert store3.get_execution(eid)["status"] == "awaiting_recovery"
    state = store3.get_recovery_state(eid)
    assert state.get("recovery_decision") is None
    assert state.get("recovery_episode") == 2
    # 原动作没有被重发：m 已成功（由证据核实，未重发），n 在进行中
    runs = store3.get_execution(eid)["node_runs"]
    assert [(nr["node_id"], nr["status"]) for nr in runs] == [
        ("m", "succeeded"), ("n", "running")]
    # 新一轮可登记证据、再作决定
    assert engine3.record_evidence(
        eid, "action_result", "onsite",
        {"outcome": "success", "detail": "现场再次确认到位"})
    assert engine3.resolve_recovery(eid, finish=True)
    await wait_until(lambda: store3.get_execution(eid)["status"] == "succeeded")
    episodes = [d["episode"] for d in store3.list_recovery_decisions(eid)]
    assert episodes == [1, 2]
