from __future__ import annotations
import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from .simulator import ActionCancelled, ActionFailed, VirtualRobot
from .store import Store


TERMINAL = {"succeeded", "failed", "cancelled", "interrupted", "resolved"}
# 重启后等待人工核实设备状态的阶段
AWAITING_VERIFICATION = "awaiting_verification"


@dataclass
class Execution:
    id: str
    flow_id: str
    definition: dict
    task: asyncio.Task | None = None
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    paused: bool = False
    resume: asyncio.Event = field(default_factory=asyncio.Event)
    wait_condition: str | None = None
    condition_met: asyncio.Event = field(default_factory=asyncio.Event)
    current_node: str | None = None
    current_attempt: int = 0
    manual_node: str | None = None
    manual_resolution: asyncio.Event = field(default_factory=asyncio.Event)
    manual_abort: bool = False
    attempts: dict[str, int] = field(default_factory=dict)
    cancel_phase: str | None = None  # 取消发生时所处阶段：等待/重试回路/失败收尾/动作收尾
    final_status: str | None = None
    termination_reason: str | None = None
    cleanup_active: bool = False  # 是否正在执行失败收尾流程
    in_retry_loop: bool = False  # 是否已进入过失败回路
    recovery_verify_reason: str | None = None  # 人工结束流程时填写的核实说明

    def next_node(self, node_id: str, kind: str) -> str | None:
        for edge in self.definition.get("edges", []):
            if edge["source"] == node_id and edge.get("kind", "success") == kind:
                return edge["target"]
        return None


class Engine:
    def __init__(self, store: Store, robot: VirtualRobot) -> None:
        self.store = store
        self.robot = robot
        self._executions: dict[str, Execution] = {}
        self._subscribers: set[asyncio.Queue] = set()
        store.mark_stale_running()
        self._schedule_saved_decisions()

    # ------------------------------------------------------------------
    # 重启恢复
    # ------------------------------------------------------------------

    def _schedule_saved_decisions(self) -> None:
        """重启时：已保存“继续收尾”决定的执行自动接续，不再重复询问操作者。"""
        self._pending_recoveries: list[str] = []
        for eid in self.store.list_status([AWAITING_VERIFICATION]):
            decision = self.store.get_recovery_decision(eid)
            if decision and decision["decision"] == "continue":
                self._pending_recoveries.append(eid)

    def schedule_pending_recoveries(self) -> None:
        """事件循环可用后（应用启动/测试）调度已决定继续的恢复任务。"""
        for eid in self._pending_recoveries:
            record = self.store.get_execution(eid)
            if record and record["status"] == AWAITING_VERIFICATION:
                self._start_recovery(eid)
        self._pending_recoveries.clear()

    async def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=256)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)

    def _emit(self, execution_id: str, event_type: str, node_id: str | None = None,
              message: str | None = None, data: dict | None = None) -> dict:
        seq = self.store.add_event(execution_id, event_type, node_id, message, data)
        payload = {"seq": seq, "execution_id": execution_id, "type": event_type,
                   "node_id": node_id, "message": message, "data": data}
        for q in list(self._subscribers):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                pass
        return payload

    def _set_status(self, execution_id: str, status: str) -> None:
        self.store.set_execution_status(execution_id, status)

    def start(self, flow_id: str, definition: dict) -> str:
        execution_id = uuid.uuid4().hex[:12]
        self.store.create_execution(execution_id, flow_id)
        # 恢复时需要按 flow_id 取回流程定义（测试中流程可能未单独入库）
        if self.store.get_flow(flow_id) is None:
            self.store.save_flow(flow_id, flow_id, definition)
        execution = Execution(id=execution_id, flow_id=flow_id, definition=definition)
        self._executions[execution_id] = execution
        execution.task = asyncio.create_task(self._run(execution))
        return execution_id

    def get(self, execution_id: str) -> Execution | None:
        return self._executions.get(execution_id)

    def pause(self, execution_id: str) -> bool:
        execution = self._executions.get(execution_id)
        if not execution or execution.stop.is_set():
            return False
        execution.paused = True
        execution.resume.clear()
        self._set_status(execution_id, "paused")
        self._emit(execution_id, "execution_paused", message="任务已暂停")
        return True

    def resume_execution(self, execution_id: str) -> bool:
        execution = self._executions.get(execution_id)
        if not execution or not execution.paused:
            return False
        execution.paused = False
        execution.resume.set()
        restored = "waiting" if execution.wait_condition else "running"
        self._set_status(execution_id, restored)
        self._emit(execution_id, "execution_resumed",
                   message=f"任务已恢复（原状态：暂停于{'等待' if execution.wait_condition else '动作'}）",
                   data={"status": restored})
        return True

    def cancel(self, execution_id: str) -> bool:
        execution = self._executions.get(execution_id)
        record = self.store.get_execution(execution_id)
        if not record or record["status"] in TERMINAL:
            return False
        if record["status"] == AWAITING_VERIFICATION:
            # 待核实阶段再次取消：操作者已选择结束，设备状态尚未确认，
            # 不替操作者发出任何新的移动/放下动作。
            decision = self.store.save_recovery_decision(
                execution_id, "end", "核实期间再次取消，结束流程，不发出新动作")
            if not decision:
                return False
            reason = self._recovery_end_reason(execution_id,
                                               "核实期间取消，结束流程")
            self.store.set_execution_status(execution_id, "resolved", reason)
            self._emit(execution_id, "recovery_verification",
                       message="核实期间取消：结束本次流程，未发出任何新动作",
                       data={"decision": "end"})
            self._emit(execution_id, "execution_finished",
                       message="任务结束：resolved",
                       data={"status": "resolved", "reason": reason,
                             "robot": self.robot.status()})
            return True
        if execution is None:
            return False
        execution.stop.set()
        execution.resume.set()  # 唤醒暂停中的任务，让它响应取消
        execution.condition_met.set()
        self._set_status(execution_id, "cancelling")
        self._emit(execution_id, "execution_cancelling",
                   message="取消请求已送达，等待当前动作收尾")
        return True

    def verify_recovery(self, execution_id: str, continue_cleanup: bool,
                        note: str | None = None) -> bool:
        """恢复前的人工处置：操作者核实设备状态后选择继续收尾或结束流程。

        决定持久化：连续重开不会重复要求已处理的决定。
        """
        record = self.store.get_execution(execution_id)
        if not record or record["status"] != AWAITING_VERIFICATION:
            return False
        decision = "continue" if continue_cleanup else "end"
        if not self.store.save_recovery_decision(execution_id, decision, note):
            return False
        self._emit(execution_id, "recovery_verification",
                   message="继续收尾并接续原阶段" if continue_cleanup
                   else "操作者结束本次流程",
                   data={"decision": decision, "note": note})
        if continue_cleanup:
            self._start_recovery(execution_id)
        else:
            reason = self._recovery_end_reason(execution_id, note)
            self.store.set_execution_status(execution_id, "resolved", reason)
            self._emit(execution_id, "execution_finished",
                       message="任务结束：resolved（人工核实后结束）",
                       data={"status": "resolved", "reason": reason,
                             "robot": self.robot.status()})
        return True

    def report_late_response(self, execution_id: str, outcome: str,
                             detail: str | None = None) -> bool:
        """核实期间收到的迟到回应：只补充到对应旧执行记录，不触发新动作。

        设备状态无法确认、等待核实的执行尤其适用：回应仅作追溯信息，
        不能替操作者发出新的移动，也不改变待核实/终止状态。
        """
        record = self.store.get_execution(execution_id)
        if not record:
            return False
        pending = record.get("pending_action")
        if pending is None:
            return False
        self.store.add_node_followup(
            execution_id, pending["node_id"], pending["attempt"],
            "late_result", outcome, detail)
        self._emit(execution_id, "late_result_recorded", pending["node_id"],
                   f"重启后的迟到回应（{outcome}）仅补充到旧执行记录：{detail}",
                   data={"attempt": pending["attempt"], "outcome": outcome,
                         "detail": detail, "phase": pending["phase"]})
        return True

    def _recovery_end_reason(self, execution_id: str,
                             note: str | None) -> str:
        """人工结束：原故障原因与人工处置原因分别保留。"""
        parts = ["重启后人工核实，结束本次流程"]
        last_failure = None
        record = self.store.get_execution(execution_id)
        for nr in reversed(record["node_runs"]):
            if nr["status"] == "failed":
                last_failure = (f"原故障：节点 {nr['node_id']} 第 {nr['attempt']} 次失败："
                                f"{nr['detail']}")
                break
        if last_failure is None:
            pending = record.get("pending_action")
            failure_detail = ((pending or {}).get("params") or {}).get("failure_detail")
            if failure_detail:
                last_failure = (f"原故障：节点 {pending['node_id']} 第 {pending['attempt']} 次失败："
                                f"{failure_detail}")
        if last_failure:
            parts.append(last_failure)
        if note:
            parts.append(f"人工处置原因：{note}")
        return "；".join(parts)

    def _start_recovery(self, execution_id: str) -> None:
        record = self.store.get_execution(execution_id)
        flow = self.store.get_flow(record["flow_id"])
        execution = Execution(id=execution_id, flow_id=record["flow_id"],
                              definition=flow["definition"])
        # 已有的各节点尝试次数从 node_runs 还原，重试上限判定不重置
        attempts: dict[str, int] = {}
        for nr in record["node_runs"]:
            attempts[nr["node_id"]] = max(attempts.get(nr["node_id"], 0),
                                           nr["attempt"])
        execution.attempts = attempts
        self._executions[execution_id] = execution
        self._set_status(execution_id, "running")
        self._emit(execution_id, "execution_recovering",
                   message="按人工核实决定恢复：已完成动作不重复执行，从原阶段接续",
                   data={"snapshot": record.get("device_snapshot"),
                         "pending": record.get("pending_action")})
        execution.task = asyncio.create_task(self._recover(execution, record))

    def signal(self, execution_id: str, condition: str = "signal") -> bool:
        execution = self._executions.get(execution_id)
        if not execution or execution.stop.is_set():
            return False
        if execution.wait_condition in (None, condition):
            execution.condition_met.set()
            return True
        return False

    def resolve_manual(self, execution_id: str, abort: bool = False) -> bool:
        execution = self._executions.get(execution_id)
        if not execution or execution.manual_node is None:
            return False
        execution.manual_abort = abort
        execution.manual_resolution.set()
        return True

    async def _gate(self, execution: Execution) -> None:
        while execution.paused and not execution.stop.is_set():
            try:
                await asyncio.wait_for(execution.resume.wait(), timeout=0.1)
            except TimeoutError:
                pass

    async def _run(self, execution: Execution) -> None:
        eid = execution.id
        self._emit(eid, "execution_started", message="任务开始",
                   data={"flow_id": execution.flow_id})
        # 初始已确认状态：设备在安全位置、空手、空闲
        self.store.save_device_snapshot(eid, self.robot.state.x,
                                        self.robot.state.y,
                                        self.robot.state.holding, True)
        nodes = {n["id"]: n for n in execution.definition["nodes"]}
        incoming = {e["target"] for e in execution.definition.get("edges", [])}
        starters = [n["id"] for n in execution.definition["nodes"]
                    if n["id"] not in incoming]
        current = starters[0] if starters else None
        try:
            final_status, current = await self._drive(
                execution, current, nodes, emit_started=True)
            execution.current_node = None
            execution.current_attempt = 0
            reason = self._termination_reason(execution, final_status)
            self.store.set_execution_status(eid, final_status, reason)
            execution.final_status = final_status
            execution.termination_reason = reason
            self._emit(eid, "execution_finished",
                       message=f"任务结束：{final_status}",
                       data={"status": final_status, "reason": reason,
                             "robot": self.robot.status()})
        except Exception as exc:  # 兜底：引擎异常也要落库，不留幽灵运行态
            reason = f"引擎异常：{exc}"
            self.store.set_execution_status(eid, "failed", reason)
            self._emit(eid, "execution_finished", message=f"引擎异常：{exc}",
                       data={"status": "failed", "reason": reason})
        finally:
            self._executions.pop(eid, None)

    async def _drive(self, execution: Execution, current: str | None,
                     nodes: dict[str, dict], emit_started: bool = False,
                     ) -> tuple[str, str | None]:
        """按既定顺序推进节点。正常运行与重启恢复共用，保证顺序与终态一致。"""
        eid = execution.id
        while current is not None:
            await self._gate(execution)
            if execution.stop.is_set():
                return "cancelled", None
            node = nodes[current]
            if node.get("type") == "manual":
                current = await self._run_manual_node(execution, node)
                if current is None:
                    return "resolved", None
                if current == "__cancel__":
                    return "cancelled", None
                continue

            attempt = execution.attempts.get(current, 0) + 1
            execution.attempts[current] = attempt
            execution.current_node = current
            execution.current_attempt = attempt
            self.store.start_node_run(eid, current, attempt)
            self._emit(eid, "node_started", current,
                       f"第 {attempt} 次开始：{node.get('label') or current}",
                       data={"attempt": attempt})
            kind, detail = await self._execute_node(execution, node, attempt)
            next_current, status = await self._handle_outcome(
                execution, nodes, node, current, attempt, kind, detail)
            if status is not None:
                return status, None
            current = next_current
        return "succeeded", None

    async def _handle_outcome(
        self, execution: Execution, nodes: dict[str, dict], node: dict,
        current: str, attempt: int, kind: str, detail: str | None,
    ) -> tuple[str | None, str | None]:
        eid = execution.id
        # 仅“失败且准备进入收尾”时保留 pending，供重启恢复收尾；
        # 其余结果的 pending 已在节点执行结束/收尾结束处清除。
        # 动作本身跑完了，但取消决定已在结果到达前生效：
        # 节点记录必须定格在 cancelled，迟到结果只作为可追溯的后续信息。
        if kind in ("success", "failure") and execution.stop.is_set():
            if execution.cancel_phase is None:
                execution.cancel_phase = "重试回路" if execution.in_retry_loop else "动作收尾"
            stored = self.store.node_run_status(eid, current, attempt)
            self.store.finish_node_run_guarded(
                eid, current, attempt, "cancelled", "取消决定已生效，等待动作收尾")
            self.store.add_node_followup(
                eid, current, attempt, "late_result", kind, detail)
            if stored != "cancelled":
                self._emit(eid, "node_cancelled", current,
                           f"第 {attempt} 次动作收尾后取消：{detail}",
                           data={"attempt": attempt})
            self._emit(eid, "late_result_recorded", current,
                       f"迟到结果（{kind}）仅作追溯，不改变取消状态：{detail}",
                       data={"attempt": attempt, "outcome": kind,
                             "detail": detail})
            self.store.clear_pending_action(eid)
            return None, "cancelled"
        if kind == "cancelled":
            if execution.cancel_phase is None:
                execution.cancel_phase = "重试回路" if execution.in_retry_loop else "动作收尾"
            self.store.finish_node_run_guarded(
                eid, current, attempt, "cancelled", detail)
            self._emit(eid, "node_cancelled", current,
                       f"第 {attempt} 次动作收尾后取消：{detail}",
                       data={"attempt": attempt})
            self.store.clear_pending_action(eid)
            return None, "cancelled"
        if kind == "failure":
            max_retries = int(node.get("params", {}).get("max_retries", 3))
            failure_detail = detail
            self.store.finish_node_run(eid, current, attempt, "failed", failure_detail)
            self.store.set_pending_action(
                eid, current, attempt, "cleanup", "cleanup",
                {"failure_detail": failure_detail})
            self._emit(eid, "node_failed", current,
                       f"第 {attempt}/{max_retries + 1} 次失败：{failure_detail}",
                       data={"detail": failure_detail, "attempt": attempt,
                             "max_retries": max_retries})
            # 失败后先走收尾流程（放下持有物 → 返回安全位置）。
            # 收尾自身的成功/失败/取消只作为可追溯信息，
            # 绝不覆盖本次失败原因，也不改变重试/分支决定。
            cancelled_in_cleanup = await self._run_failure_cleanup(
                execution, node, attempt, failure_detail)
            if execution.stop.is_set() or cancelled_in_cleanup:
                execution.cancel_phase = "失败收尾"
                self._emit(
                    eid, "node_cancelled", current,
                    f"失败收尾期间取消；原失败原因保留：{failure_detail}",
                    data={"attempt": attempt, "failure_detail": failure_detail})
                return None, "cancelled"
            self.store.clear_pending_action(eid)
            if attempt > max_retries:
                target = execution.next_node(current, "exhausted")
                if target is None:
                    target = execution.next_node(current, "failure")
                if target is None:
                    return None, "failed"
                self._emit(eid, "retries_exhausted", current,
                           f"已达 {max_retries + 1} 次上限，转入："
                           f"{nodes[target].get('label') or target}",
                           data={"attempt": attempt, "target": target})
                return target, None
            target = execution.next_node(current, "failure")
            if target is None:
                return None, "failed"
            self._emit(eid, "branch_taken", current,
                       f"第 {attempt} 次失败后走向："
                       f"{nodes[target].get('label') or target}",
                       data={"edge": "failure", "target": target,
                             "attempt": attempt})
            execution.in_retry_loop = True
            return target, None
        # 动作完成点即新的已确认设备状态
        self.store.save_device_snapshot(
            eid, self.robot.state.x, self.robot.state.y,
            self.robot.state.holding, True)
        self.store.clear_pending_action(eid)
        self.store.finish_node_run(eid, current, attempt, "succeeded", detail)
        if attempt > 1:
            self._emit(eid, "node_succeeded", current,
                       f"第 {attempt} 次尝试成功：{node.get('label') or current}",
                       data={"attempt": attempt})
        else:
            self._emit(eid, "node_succeeded", current,
                       f"完成：{node.get('label') or current}",
                       data={"attempt": attempt})
        return execution.next_node(current, "success"), None

    # ------------------------------------------------------------------
    # 重启后的恢复协程
    # ------------------------------------------------------------------

    async def _recover(self, execution: Execution, record: dict) -> None:
        """按持久化的崩溃点恢复：快照还原设备，已完成动作不重复执行。"""
        eid = execution.id
        nodes = {n["id"]: n for n in execution.definition["nodes"]}
        final_status = "succeeded"
        try:
            snapshot = record.get("device_snapshot") or {
                "x": 0.0, "y": 0.0, "holding": None, "idle": True}
            self.robot.restore_state(snapshot["x"], snapshot["y"],
                                     snapshot["holding"])
            pending = record.get("pending_action")
            current: str | None = None
            if pending is None:
                incoming = {e["target"] for e in execution.definition.get("edges", [])}
                starters = [n["id"] for n in execution.definition["nodes"]
                            if n["id"] not in incoming]
                current = starters[0] if starters else None
                # 极端情况：执行已开始但崩溃点未落库，从头开始且不会重复释放
            elif pending["phase"] == "wait":
                # 等待条件期间崩溃：恢复后继续等待，不发出任何机器人动作
                node = nodes[pending["node_id"]]
                execution.current_node = node["id"]
                execution.current_attempt = pending["attempt"]
                kind, detail = await self._execute_node(
                    execution, node, pending["attempt"],
                    existing_run=True)
                self.store.clear_pending_action(eid)
                if kind == "cancelled" or execution.stop.is_set():
                    final_status = "cancelled"
                    current = None
                else:
                    next_current, status = await self._handle_outcome(
                        execution, nodes, node, node["id"],
                        pending["attempt"], kind, detail)
                    final_status = status or "continue"
                    current = next_current
            elif pending["phase"] == "manual":
                node = nodes[pending["node_id"]]
                execution.current_node = node["id"]
                execution.current_attempt = pending["attempt"]
                current = await self._run_manual_node(execution, node,
                                                      existing_run=True)
                if current is None:
                    final_status = "resolved"
                elif current == "__cancel__":
                    final_status = "cancelled"
                    current = None
            elif pending["phase"] == "cleanup":
                node = nodes[pending["node_id"]]
                execution.current_node = node["id"]
                execution.current_attempt = pending["attempt"]
                execution.in_retry_loop = True
                cancelled_in_cleanup = await self._run_failure_cleanup(
                    execution, node, pending["attempt"],
                    pending.get("params", {}).get("failure_detail"),
                    resuming=True)
                self.store.clear_pending_action(eid)
                if execution.stop.is_set() or cancelled_in_cleanup:
                    execution.cancel_phase = "失败收尾"
                    final_status = "cancelled"
                else:
                    current, status = self._route_after_failure(
                        execution, nodes, node, pending["attempt"])
                    final_status = status or "continue"
            else:
                # 动作（移动/取物/放下）执行中断电：结果未知，该动作不重复执行。
                node = nodes[pending["node_id"]]
                execution.current_node = node["id"]
                execution.current_attempt = pending["attempt"]
                self._emit(eid, "recovery_action_skipped", node["id"],
                           f"{pending['action_type']} 动作在重启前已发出且结果未知，"
                           f"不重复执行；先按已确认状态收尾，再从后续顺序接续",
                           data={"attempt": pending["attempt"],
                                 "action_type": pending["action_type"]})
                self.store.finish_node_run_guarded(
                    eid, node["id"], pending["attempt"], "interrupted",
                    f"重启前动作 {pending['action_type']} 结果未知，已按核实决定收尾后续接")
                # 按最后已确认持有物/位置完成一次安全收尾；release 天然幂等
                cancelled_in_cleanup = await self._run_failure_cleanup(
                    execution, node, pending["attempt"],
                    "重启时动作结果未知（待核实后收尾）", resuming=True,
                    verification=True)
                self.store.clear_pending_action(eid)
                if execution.stop.is_set() or cancelled_in_cleanup:
                    execution.cancel_phase = "恢复收尾"
                    final_status = "cancelled"
                else:
                    # 动作本体视为中断，沿 success 顺序继续后续节点，不重放该动作
                    self.store.save_device_snapshot(
                        eid, self.robot.state.x, self.robot.state.y,
                        self.robot.state.holding, True)
                    current = execution.next_node(node["id"], "success")

            if final_status != "cancelled" and final_status != "resolved" and current is not None:
                status, _ = await self._drive(execution, current, nodes,
                                             emit_started=False)
                final_status = status
            execution.current_node = None
            execution.current_attempt = 0
            reason = self._termination_reason(execution, final_status)
            if final_status == "continue":
                final_status = "succeeded"
            self.store.set_execution_status(eid, final_status, reason)
            execution.final_status = final_status
            execution.termination_reason = reason
            self._emit(eid, "execution_finished",
                       message=f"任务结束：{final_status}",
                       data={"status": final_status, "reason": reason,
                             "recovered": True,
                             "robot": self.robot.status()})
        except Exception as exc:  # 兜底：恢复异常也落库
            reason = f"恢复异常：{exc}"
            self.store.set_execution_status(eid, "failed", reason)
            self._emit(eid, "execution_finished", message=f"恢复异常：{exc}",
                       data={"status": "failed", "reason": reason})
        finally:
            self._executions.pop(eid, None)

    def _route_after_failure(self, execution: Execution, nodes: dict[str, dict],
                             node: dict, attempt: int) -> tuple[str | None, str | None]:
        """收尾完成后的失败路由（重试 / 超限分支 / 终止），与不中断运行一致。"""
        current = node["id"]
        max_retries = int(node.get("params", {}).get("max_retries", 3))
        if attempt > max_retries:
            target = execution.next_node(current, "exhausted")
            if target is None:
                target = execution.next_node(current, "failure")
            if target is None:
                return None, "failed"
            self._emit(execution.id, "retries_exhausted", current,
                       f"恢复后收尾完成，已达 {max_retries + 1} 次上限，转入："
                       f"{nodes[target].get('label') or target}",
                       data={"attempt": attempt, "target": target})
            return target, None
        target = execution.next_node(current, "failure")
        if target is None:
            return None, "failed"
        self._emit(execution.id, "branch_taken", current,
                   f"恢复后收尾完成，第 {attempt} 次失败后走向："
                   f"{nodes[target].get('label') or target}",
                   data={"edge": "failure", "target": target,
                         "attempt": attempt})
        execution.in_retry_loop = True
        return target, None

    def _termination_reason(self, execution: Execution, status: str) -> str | None:
        eid = execution.id
        if status == "succeeded":
            return "全部节点执行成功"
        if status == "cancelled":
            # 取消若发生在失败收尾阶段，原失败原因必须保留在终止说明中
            last_failure = None
            for nr in reversed(self.store.get_execution(eid)["node_runs"]):
                if nr["status"] == "failed":
                    last_failure = f"节点 {nr['node_id']} 第 {nr['attempt']} 次失败：{nr['detail']}"
                    break
            phase = execution.cancel_phase or "动作"
            recovery = self.store.get_recovery_decision(eid)
            recovery_note = "；重启恢复后" if recovery else ""
            if last_failure:
                return f"{recovery_note}取消于{phase}；{last_failure}"
            return f"{recovery_note}取消于{phase}：取消时已确定的节点状态保持不变，迟到结果仅作追溯"
        if status == "failed":
            return "节点失败且无后续分支"
        if status == "resolved":
            recovery = self.store.get_recovery_decision(eid)
            if recovery:
                return self._recovery_end_reason(eid, recovery.get("reason"))
            return "人工确认终止"
        return None

    async def _run_failure_cleanup(
        self, execution: Execution, failed_node: dict, attempt: int,
               failure_detail: str | None,
       resuming: bool = False, verification: bool = False,
    ) -> bool:
        """失败后的收尾流程：就地放下持有物 → 返回安全位置。

        取消语义：
        - 取消发生在重试/等待过程中（进入收尾前 stop 已置位）：不启动任何收尾动作；
        - 取消发生在收尾过程中：允许当前正在执行的那一个收尾动作做完，
          不再启动后续收尾动作；再次到达的取消（stop 已存在）立即终止下一个动作；
        - 收尾自身失败或被取消，只记为该失败节点的可追溯后续信息，
          原失败原因与分支决定不变。

        返回 True 表示收尾过程被取消（流程应停止，不再进入重试分支）。
        """
        eid = execution.id
        node_id = failed_node["id"]

        async def cleanup_progress(data: dict) -> None:
            self._emit(eid, "cleanup_progress", node_id, data={**data, "attempt": attempt})

        async def gate_steps() -> None:
            await self._gate(execution)

        # 恢复时：已完成的收尾阶段按持久化的后续信息判定，绝不重复放下/返回。
        prior = self.store.get_node_followups(eid)
        release_done = any(
            f["node_id"] == node_id and f["attempt"] == attempt
            and f["kind"] == "cleanup" and f["outcome"] in ("succeeded", "noop")
            and (f["detail"] or "").startswith("已就地放下")
            for f in prior)
        release_noop = any(
            f["node_id"] == node_id and f["attempt"] == attempt
            and f["kind"] == "cleanup" and f["outcome"] == "noop"
            for f in prior)
        return_done = any(
            f["node_id"] == node_id and f["attempt"] == attempt
            and f["kind"] == "cleanup" and f["outcome"] == "succeeded"
            and (f["detail"] or "").startswith("已返回安全位置")
            for f in prior)
        done_stages = set()
        if release_done or release_noop:
            done_stages.add("release")
        if return_done:
            done_stages.add("return")

        # 收尾流程定义：放下持有物 → 返回安全位置。暂停仍然生效。
        stop = execution.stop
        stage_builders: list[tuple[str, Callable[[], Awaitable[object]], str]] = [
            ("release", lambda: self.robot.release_if_holding(
                cleanup_progress, stop, gate_steps), "放下持有物"),
            ("return", lambda: self.robot.return_to_safe(
                cleanup_progress, stop, gate_steps), "返回安全位置"),
        ]
        if not resuming:
            self.store.set_pending_action(
                eid, node_id, attempt, "cleanup", "cleanup",
                {"failure_detail": failure_detail, "verification": verification})
            self._emit(eid, "cleanup_started", node_id,
                       f"第 {attempt} 次失败后开始收尾：放下持有物 → 返回安全位置；"
                       f"原失败原因：{failure_detail}",
                       data={"attempt": attempt, "failure_detail": failure_detail})
        else:
            self.store.set_pending_action(
                eid, node_id, attempt, "cleanup", "cleanup",
                {"failure_detail": failure_detail, "verification": verification})
            skipped_labels = [label for stage, _, label in stage_builders
                              if stage in done_stages]
            self._emit(eid, "cleanup_started", node_id,
                       f"恢复后继续未完成的收尾（已完成阶段不重复执行："
                       f"{'、'.join(skipped_labels) if skipped_labels else '无'}）；"
                       f"原失败原因保留：{failure_detail}",
                       data={"attempt": attempt, "failure_detail": failure_detail,
                             "resuming": True, "done_stages": sorted(done_stages)})
        cancelled = False
        for idx, (stage, build_coro, label) in enumerate(stage_builders):
            if stage in done_stages:
                continue
            coro = build_coro()
            self.store.set_pending_action(
                eid, node_id, attempt, "cleanup", "cleanup",
                {"failure_detail": failure_detail, "verification": verification,
                 "stage": stage})
            await self._gate(execution)
            # idx==0 且 stop 未置位：正常启动收尾。
            # idx==0 且 stop 已置位：取消发生在重试/等待阶段（收尾从未开始），不启动收尾。
            # idx>0 且 stop 已置位：取消发生在收尾过程中；当前动作由 simulator 允许做完，
            #   后续收尾动作在此处不再启动。
            if stop.is_set():
                if idx == 0:
                    skip_detail = f"收尾未启动（取消于重试/等待阶段），跳过：{label}"
                    skip_message = (f"取消已在收尾前生效，收尾动作 {label} 不再启动；"
                                    f"原失败原因保留：{failure_detail}")
                else:
                    skip_detail = f"收尾中途取消，后续动作不再启动：{label}"
                    skip_message = (f"收尾中途取消，{label} 不再启动；"
                                    f"原失败原因保留：{failure_detail}")
                self.store.add_node_followup(
                    eid, node_id, attempt, "cleanup", "cancelled", skip_detail)
                self._emit(eid, "cleanup_skipped", node_id, skip_message,
                           data={"attempt": attempt, "stage": stage})
                cancelled = True
                break
            execution.cleanup_active = True
            try:
                result = await coro
            except ActionCancelled as exc:
                self.store.add_node_followup(
                    eid, node_id, attempt, "cleanup", "cancelled", f"{label}：{exc}")
                self._emit(eid, "cleanup_cancelled", node_id,
                           f"收尾动作 {label} 被取消；原失败原因保留：{failure_detail}",
                           data={"attempt": attempt, "stage": stage})
                cancelled = True
                break
            except ActionFailed as exc:
                self.store.add_node_followup(
                    eid, node_id, attempt, "cleanup", "failed", f"{label}：{exc}")
                self._emit(eid, "cleanup_failed", node_id,
                           f"收尾动作 {label} 失败：{exc}；原失败原因保留：{failure_detail}",
                           data={"attempt": attempt, "stage": stage, "detail": str(exc)})
                continue
            if stage == "release" and not result:
                self.store.add_node_followup(
                    eid, node_id, attempt, "cleanup", "noop", "机械爪为空，无需放下")
                if stop.is_set():
                    self.store.add_node_followup(
                        eid, node_id, attempt, "cleanup", "cancelled",
                        "收尾中途取消，后续动作不再启动：返回安全位置")
                    self._emit(eid, "cleanup_skipped", node_id,
                               f"收尾中途取消，返回安全位置不再启动；"
                               f"原失败原因保留：{failure_detail}",
                               data={"attempt": attempt, "stage": "return"})
                    cancelled = True
                    break
                continue
            if stage == "release" and result:
                self.store.add_node_followup(
                    eid, node_id, attempt, "cleanup", "succeeded",
                    "已就地放下（机械爪已空）")
                self.store.save_device_snapshot(
                    eid, self.robot.state.x, self.robot.state.y, None, True)
            if stage == "return":
                sx, sy = result  # type: ignore[misc]
                self.store.add_node_followup(
                    eid, node_id, attempt, "cleanup", "succeeded",
                    f"已返回安全位置 ({sx:g}, {sy:g})")
                self.store.save_device_snapshot(eid, sx, sy, None, True)
                self._emit(eid, "cleanup_succeeded", node_id,
                           f"收尾完成，已返回安全位置 ({sx:g}, {sy:g})；"
                           f"原失败原因保留：{failure_detail}",
                           data={"attempt": attempt, "safe": [sx, sy]})
        execution.cleanup_active = False
        if not cancelled:
            self.store.clear_pending_action(eid)
        return cancelled

    async def _run_manual_node(self, execution: Execution, node: dict,
                               existing_run: bool = False) -> str | None:
        """人工处置节点：停在明确位置，等待人工确认继续或终止。"""
        eid = execution.id
        node_id = node["id"]
        execution.manual_node = node_id
        execution.manual_abort = False
        execution.manual_resolution.clear()
        instruction = node.get("params", {}).get("instruction", "等待人工处置")
        attempt = 1
        if not existing_run:
            self.store.start_node_run(eid, node_id, attempt)
        self.store.update_node_run(eid, node_id, attempt, "manual", instruction)
        self.store.set_pending_action(eid, node_id, attempt, "manual", "manual")
        self._set_status(eid, "awaiting_manual")
        self._emit(eid, "node_manual_required", node_id,
                   f"停在人工处置节点：{node.get('label') or node_id}（{instruction}）",
                   data={"instruction": instruction})
        while not execution.manual_resolution.is_set() and not execution.stop.is_set():
            try:
                await asyncio.wait_for(execution.manual_resolution.wait(), timeout=0.2)
            except asyncio.TimeoutError:
                pass
        execution.manual_node = None
        self.store.clear_pending_action(eid)
        if execution.stop.is_set():
            self.store.finish_node_run(eid, node_id, attempt, "cancelled", "等待处置期间被取消")
            self._emit(eid, "node_cancelled", node_id, "人工处置节点被取消")
            return "__cancel__"
        if execution.manual_abort:
            self.store.finish_node_run(eid, node_id, attempt, "aborted", "人工终止流程")
            self._emit(eid, "node_manual_aborted", node_id, "人工确认终止，流程停在此处")
            return None
        self.store.finish_node_run(eid, node_id, attempt, "succeeded", "人工处置完成，继续执行")
        self._emit(eid, "node_manual_resolved", node_id, "人工处置完成，流程继续")
        return execution.next_node(node_id, "success")

    async def _execute_node(self, execution: Execution, node: dict, attempt: int = 1,
                            existing_run: bool = False) -> tuple[str, str | None]:
        eid = execution.id
        ntype = node["type"]
        params = node.get("params", {})

        async def progress(data: dict) -> None:
            self._emit(eid, "node_progress", node["id"],
                       data={**data, "attempt": attempt, "robot": self.robot.status()})

        if ntype == "log":
            message = str(params.get("message", ""))
            self._emit(eid, "node_log", node["id"], message, data={"attempt": attempt})
            return "success", message

        if ntype == "wait":
            condition = params.get("condition", "signal")
            timeout = params.get("timeout")
            execution.wait_condition = condition
            execution.condition_met.clear()
            if not existing_run:
                self.store.mark_node_run_waiting(eid, node["id"], attempt,
                                                 f"等待条件：{condition}")
                self._emit(eid, "node_waiting", node["id"],
                           f"第 {attempt} 次等待条件：{condition}",
                           data={"attempt": attempt})
            self.store.set_pending_action(eid, node["id"], attempt, "wait", "wait",
                                          {"condition": condition})
            self._set_status(eid, "waiting")
            try:
                deadline = asyncio.get_running_loop().time() + float(timeout) if timeout else None
                while True:
                    await self._gate(execution)
                    if execution.stop.is_set():
                        execution.cancel_phase = "等待条件"
                        return "cancelled", "等待期间被取消"
                    if execution.paused:
                        continue
                    remaining = None if deadline is None else max(0.0, deadline - asyncio.get_running_loop().time())
                    try:
                        await asyncio.wait_for(execution.condition_met.wait(),
                                               timeout=remaining if remaining is not None else 0.2)
                    except asyncio.TimeoutError:
                        if deadline is not None and asyncio.get_running_loop().time() >= deadline:
                            return "failure", f"等待 {condition} 超时"
                        continue
                    if execution.stop.is_set():
                        execution.cancel_phase = "等待条件"
                        return "cancelled", "等待期间被取消"
                    self._set_status(eid, "running")
                    return "success", f"条件 {condition} 已满足"
            finally:
                execution.wait_condition = None
                self.store.clear_pending_action(eid)

        async def gate_steps() -> None:
            await self._gate(execution)

        stop = execution.stop
        try:
            if ntype == "move":
                self.store.set_pending_action(
                    eid, node["id"], attempt, "move", "action", dict(params))
                await self.robot.move(float(params.get("x", 0)), float(params.get("y", 0)),
                                     progress, stop, gate_steps)
                return "success", "移动完成"
            if ntype == "pick":
                self.store.set_pending_action(
                    eid, node["id"], attempt, "pick", "action", dict(params))
                await self.robot.pick(str(params.get("item", "物品")), progress, stop, gate_steps)
                return "success", "取物完成"
            if ntype == "place":
                self.store.set_pending_action(
                    eid, node["id"], attempt, "place", "action", dict(params))
                await self.robot.place(progress, stop, gate_steps)
                return "success", "放置完成"
            return "failure", f"未知节点类型：{ntype}"
        except ActionCancelled as exc:
            return "cancelled", str(exc)
        except ActionFailed as exc:
            # 失败原因在收尾开始前落进崩溃点，重启后人工结束时仍可保留原故障
            self.store.set_pending_action(
                eid, node["id"], attempt, ntype, "action",
                {**params, "failure_detail": str(exc)})
            return "failure", str(exc)
