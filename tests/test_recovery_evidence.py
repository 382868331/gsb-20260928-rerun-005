"""恢复核实证据：三类证据分别登记、来源与时刻保留、矛盾阻止推进、
撤回后再决定、动作不重复发出、重启后待核实项与历史依据可找回。"""
from __future__ import annotations

import asyncio

import pytest

pytestmark = pytest.mark.anyio

from backend.engine import Engine
from backend.simulator import VirtualRobot
from backend.store import Store


MOVE_FLOW = {
    "nodes": [
        {"id": "m", "type": "move", "label": "移动到货架", "params": {"x": 6, "y": 0}},
        {"id": "n", "type": "log", "label": "到达播报", "params": {"message": "done"}},
    ],
    "edges": [
        {"id": "1", "source": "m", "target": "n", "kind": "success"},
    ],
}

TWO_MOVE_FLOW = {
    "nodes": [
        {"id": "m", "type": "move", "label": "移动一", "params": {"x": 6, "y": 0}},
        {"id": "n", "type": "move", "label": "移动二", "params": {"x": 2, "y": 0}},
    ],
    "edges": [
        {"id": "1", "source": "m", "target": "n", "kind": "success"},
    ],
}


async def wait_until(predicate, timeout=6.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("等待超时")


def crash(engine: Engine) -> None:
    for execution in list(engine._executions.values()):
        if execution.task:
            execution.task.cancel()


class CountingRobot(VirtualRobot):
    def __init__(self):
        super().__init__()
        self.move_calls = 0

    async def move(self, *args, **kwargs):
        self.move_calls += 1
        return await super().move(*args, **kwargs)


async def start_and_crash_during_move(db, flow=MOVE_FLOW):
    store = Store(db)
    robot = VirtualRobot()
    engine = Engine(store, robot)
    eid = engine.start("flow1", flow)
    await wait_until(
        lambda: any(nr["node_id"] == "m" and nr["status"] == "running"
                    for nr in store.get_execution(eid)["node_runs"]))
    crash(engine)
    await asyncio.sleep(0.1)
    return eid


async def test_conflicting_evidence_blocks_recovery_until_retracted(tmp_path):
    """矛盾证据不覆盖旧记录且阻止推进；撤回一条后依据剩余证据继续，动作只执行一次。"""
    db = tmp_path / "evidence.db"
    eid = await start_and_crash_during_move(db)

    store2 = Store(db)
    robot2 = CountingRobot()
    engine2 = Engine(store2, robot2)
    assert store2.get_execution(eid)["status"] == "awaiting_recovery"

    # 三类证据分别登记，来源与观察时刻保留
    pos1 = engine2.record_evidence(eid, "position", "onsite", "(3, 0)", "现场目测在途中")
    assert pos1 is not None
    engine2.record_evidence(eid, "holding", "onsite", "空", "机械爪为空")
    # 设备迟到回应（动作结果）与一条矛盾的位置观察
    assert engine2.record_late_action(eid, "success", "设备稍后确认移动已完成")
    pos2 = engine2.record_evidence(eid, "position", "device_late", "(9, 9)", "设备上报坐标异常")
    assert pos2 is not None

    evidence = store2.get_evidence(eid)
    assert {e["category"] for e in evidence} == {"position", "holding", "action_result"}
    assert all(e["observed_at"] for e in evidence)
    assert {e["source"] for e in evidence} >= {"onsite", "device_late"}

    # 矛盾：两条位置证据都保留，且阻止恢复决定
    conflicts = store2.evidence_conflicts(eid)
    assert set(conflicts) == {"position"}
    assert len(conflicts["position"]) == 2
    assert engine2.resolve_recovery(eid, finish=True) is False
    assert store2.get_execution(eid)["status"] == "awaiting_recovery"
    events = store2.get_events(eid)
    assert any(e["type"] == "recovery_blocked_by_conflict" for e in events)
    assert any(e["type"] == "evidence_conflict" for e in events)

    # 撤回错误的设备上报观察并说明原因，旧记录仍保留
    assert engine2.retract_evidence(eid, pos2, "设备坐标上报异常，现场复核为准")
    assert store2.evidence_conflicts(eid) == {}
    retracted = [e for e in store2.get_evidence(eid) if e["id"] == pos2][0]
    assert retracted["retracted"] == 1
    assert retracted["retract_reason"] == "设备坐标上报异常，现场复核为准"

    # 基于剩余有效证据继续：原移动动作不重复发出
    assert engine2.resolve_recovery(eid, finish=True)
    await wait_until(lambda: store2.get_execution(eid)["status"] == "succeeded")
    assert robot2.move_calls == 0
    runs = store2.get_execution(eid)["node_runs"]
    assert [nr["status"] for nr in runs if nr["node_id"] == "m"] == ["succeeded"]
    assert [nr["status"] for nr in runs if nr["node_id"] == "n"] == ["succeeded"]

    # 决定指出了实际依赖的证据
    decision = [e for e in store2.get_events(eid)
                if e["type"] == "recovery_decision_saved"
                and e["data"] and e["data"].get("evidence_ids") is not None]
    assert decision, "恢复决定应记录所依据的证据"
    relied = decision[-1]["data"]["evidence_ids"]
    assert pos1 in relied and pos2 not in relied

    # 已终结执行不接受新证据/迟到回应，历史不被重写
    assert engine2.record_evidence(eid, "position", "onsite", "(0, 0)") is None
    assert engine2.record_late_action(eid, "failure", "无关迟到") is False
    assert [nr["status"] for nr in store2.get_execution(eid)["node_runs"]
            if nr["node_id"] == "m"] == ["succeeded"]

    # 关闭重开（新进程）：完成的历史依据（证据、撤回、决定、后续信息）可独立查看
    store3 = Store(db)
    Engine(store3, VirtualRobot())
    record = store3.get_execution(eid)
    assert record["status"] == "succeeded"
    assert len(record["evidence"]) == 4
    assert any(e["retracted"] for e in record["evidence"])
    assert any(f["kind"] == "late_result" for f in record["followups"])
    crash(engine2)


async def test_restart_recovers_pending_evidence_and_no_duplicate_action(tmp_path):
    """服务再重启：待核实执行、证据与未决定项原样找回；已保存决定不重复发动作。"""
    db = tmp_path / "restart.db"
    eid = await start_and_crash_during_move(db, flow=TWO_MOVE_FLOW)

    store2 = Store(db)
    engine2 = Engine(store2, VirtualRobot())
    assert store2.get_execution(eid)["status"] == "awaiting_recovery"
    engine2.record_evidence(eid, "position", "onsite", "(4, 0)")
    assert engine2.record_late_action(eid, "success", "设备确认移动一完成")
    assert engine2.resolve_recovery(eid, finish=True)
    # 恢复推进到第二个移动动作时再次“断电”
    await wait_until(
        lambda: any(nr["node_id"] == "n" and nr["status"] == "running"
                    for nr in store2.get_execution(eid)["node_runs"]))
    crash(engine2)
    await asyncio.sleep(0.1)

    # 第三次启动：m 已确认完成不得重发，n 结果未知保持待核实
    store3 = Store(db)
    robot3 = CountingRobot()
    engine3 = Engine(store3, robot3)
    await wait_until(
        lambda: store3.get_execution(eid)["status"] == "awaiting_recovery")
    await asyncio.sleep(0.3)
    assert robot3.move_calls == 0
    runs = store3.get_execution(eid)["node_runs"]
    assert [nr["status"] for nr in runs if nr["node_id"] == "m"] == ["succeeded"]
    assert [nr["status"] for nr in runs if nr["node_id"] == "n"] == ["running"]
    assert any(e["type"] == "recovery_required" for e in store3.get_events(eid))

    # 待核实项与既有证据都能找回；旧决定已因状态缺失被清除，可重新核实
    recovery = store3.get_recovery_state(eid)
    assert recovery["current_node"] == "n"
    assert recovery.get("recovery_decision") is None
    evidence = store3.get_evidence(eid)
    assert any(e["category"] == "position" for e in evidence)
    assert any(e["category"] == "action_result" for e in evidence)

    # 重复确认/并发决定只会生效一次，n 仍只执行一次（此处零次重发）
    assert engine3.record_late_action(eid, "success", "设备确认移动二完成")
    assert engine3.resolve_recovery(eid, finish=True)
    assert engine3.resolve_recovery(eid, finish=True) is False
    await wait_until(lambda: store3.get_execution(eid)["status"] == "succeeded")
    assert robot3.move_calls == 0
    runs = store3.get_execution(eid)["node_runs"]
    assert len([nr for nr in runs if nr["node_id"] == "n"]) == 1
    assert [nr["status"] for nr in runs if nr["node_id"] == "n"] == ["succeeded"]

    # 结束后的历史依据独立可查
    record = Store(db).get_execution(eid)
    assert record["status"] == "succeeded"
    assert len(record["evidence"]) >= 3
    crash(engine3)


async def test_evidence_failure_leads_to_cleanup_not_success(tmp_path):
    """有效证据确认动作失败：按失败收尾并结束，不当作成功继续。"""
    db = tmp_path / "failure.db"
    eid = await start_and_crash_during_move(db)

    store2 = Store(db)
    robot2 = CountingRobot()
    engine2 = Engine(store2, robot2)
    engine2.record_evidence(eid, "action_result", "onsite", "failure", "现场确认未到位")
    assert engine2.resolve_recovery(eid, finish=True)
    await wait_until(lambda: store2.get_execution(eid)["status"] == "resolved")
    record = store2.get_execution(eid)
    assert [nr["status"] for nr in record["node_runs"] if nr["node_id"] == "m"] == ["failed"]
    assert not any(nr["node_id"] == "n" for nr in record["node_runs"])
    assert robot2.move_calls == 0  # 收尾移动是 return_to_safe，不是业务 move
    assert (robot2.status()["x"], robot2.status()["y"]) == (0.0, 0.0)
    crash(engine2)

