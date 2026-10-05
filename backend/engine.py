from __future__ import annotations
import asyncio
import json
import uuid
from collections.abc import Awaitable
from dataclasses import dataclass, field

from .simulator import ActionCancelled, ActionFailed, VirtualRobot
from .store import Store


TERMINAL = {"succeeded", "failed", "cancelled", "interrupted", "resolved"}
RECOVERY_STATUS = "awaiting_recovery"


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
    recovery_state: dict | None = None
    active_definition: dict | None = None

    def next_node(self, node_id: str, kind: str) -> str | None:
        definition = self.active_definition or self.definition
        for edge in definition.get("edges", []):
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
        # 重启恢复以持久化执行状态为唯一来源：只要 executions 行处于待核实/
        # 待处置，就必须恢复出活动执行对象；recovery_states 缺失时按 node_runs
        # 补建检查点，杜绝“页面待核实、入口却判定不在待核实状态”的矛盾。
        for record in store.list_pending_recovery_executions():
            execution_id = record["id"]
            state = store.get_recovery_state(execution_id)
            if state is None:
                state = self._rebuild_recovery_state(record)
                store.save_recovery_state(execution_id, state)
                self._emit(execution_id, "recovery_checkpoint_rebuilt",
                           state.get("current_node"),
                           "重启后未找到检查点，已按持久化节点记录补建待核实上下文；不会重发动作",
                           data={"attempt": state.get("current_attempt")})
            execution = self._hydrate_execution(state)
            self._executions[execution_id] = execution
            decision: str | None = None
            if record["status"] == "awaiting_manual" and state.get("manual_decision") is not None:
                decision = "manual_" + str(state["manual_decision"])
            elif state.get("pending_decision") is not None:
                # 仅重放“已提交但尚未应用完”的决定；上一决定已应用后，
                # 恢复出的尾部执行再次崩溃属于新的待核实周期，可重新登记决定。
                decision = str(state["pending_decision"])
            if decision is not None:
                execution.task = asyncio.create_task(self._recover(execution, decision))

    def _rebuild_recovery_state(self, record: dict) -> dict:
        """检查点缺失时，依据 flow 定义与 node_runs 补建待核实上下文。"""
        execution_id = record["id"]
        flow_id = record["flow_id"]
        flow = self.store.get_flow(flow_id)
        definition = (flow or {}).get("definition") or {"nodes": [], "edges": []}
        state: dict = {
            "execution_id": execution_id,
            "flow_id": flow_id,
            "definition": definition,
            "phase": "manual" if record["status"] == "awaiting_manual" else "action",
            "attempts": {},
        }
        last = self.store.last_unconfirmed_node_run(execution_id)
        if last is not None:
            node_id, attempt, _status, detail = last
            state["current_node"] = node_id
            state["current_attempt"] = attempt
            state["attempts"] = {node_id: attempt}
            if record["status"] == "awaiting_manual":
                state["manual_node"] = node_id
            if detail:
                state["last_progress"] = detail
        state["last_robot"] = self.robot.status()
        return state

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

    def _save_checkpoint(self, execution: Execution, **changes: object) -> None:
        state = getattr(execution, "recovery_state", {}) or {}
        for key, value in changes.items():
            if value is None:
                state.pop(key, None)
            else:
                state[key] = value
        state.update({
            "flow_id": execution.flow_id,
            "definition": execution.definition,
            "current_node": execution.current_node,
            "current_attempt": execution.current_attempt,
            "attempts": execution.attempts,
            "in_retry_loop": execution.in_retry_loop,
            "cancel_requested": execution.stop.is_set(),
            "cancel_phase": execution.cancel_phase,
            "wait_condition": execution.wait_condition,
        })
        state.setdefault("last_robot", self.robot.status())
        execution.recovery_state = state
        self.store.save_recovery_state(execution.id, state)

    def _hydrate_execution(self, state: dict) -> Execution:
        execution = Execution(
            id=state["execution_id"], flow_id=state["flow_id"],
            definition=state["definition"])
        execution.current_node = state.get("current_node")
        execution.current_attempt = int(state.get("current_attempt") or 0)
        execution.attempts = {k: int(v) for k, v in (state.get("attempts") or {}).items()}
        execution.in_retry_loop = bool(state.get("in_retry_loop"))
        execution.cancel_phase = state.get("cancel_phase")
        execution.wait_condition = state.get("wait_condition")
        execution.manual_node = state.get("manual_node")
        execution.recovery_state = state
        execution.resume.set()
        if state.get("cancel_requested"):
            execution.stop.set()
        return execution

    def start(self, flow_id: str, definition: dict) -> str:
        execution_id = uuid.uuid4().hex[:12]
        self.store.create_execution(execution_id, flow_id)
        execution = Execution(id=execution_id, flow_id=flow_id, definition=definition)
        self._executions[execution_id] = execution
        execution.recovery_state = {"execution_id": execution_id, "phase": "starting"}
        self._save_checkpoint(execution, phase="starting")
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
        if not execution or not record or record["status"] in TERMINAL or record["status"] == RECOVERY_STATUS:
            return False
        execution.stop.set()
        execution.resume.set()  # 唤醒暂停中的任务，让它响应取消
        execution.condition_met.set()
        self._save_checkpoint(execution, cancel_requested=True)
        self._set_status(execution_id, "cancelling")
        self._emit(execution_id, "execution_cancelling",
                   message="取消请求已送达，等待当前动作收尾")
        return True

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
        record = self.store.get_execution(execution_id)
        if not execution or not record:
            return False
        if execution.manual_node is None and record["status"] != "awaiting_manual":
            return False
        if (execution.recovery_state or {}).get("manual_decision") is not None:
            return False
        execution.manual_abort = abort
        execution.manual_resolution.set()
        self._save_checkpoint(execution, phase="manual",
                              current_node=execution.manual_node, manual_node=execution.manual_node,
                              manual_decision="abort" if abort else "continue")
        if record["status"] == "awaiting_manual" and execution.task is None:
            execution.task = asyncio.create_task(
                self._recover(execution, "manual_abort" if abort else "manual_continue"))
        return True

    def resolve_recovery(self, execution_id: str, finish: bool) -> bool:
        target = self._recovery_target(execution_id)
        if target is None:
            return False
        execution, record = target
        decision = "finish" if finish else "abort"
        state = execution.recovery_state or {}
        if state.get("pending_decision") is not None:
            return False
        node_id = execution.current_node
        attempt = execution.current_attempt
        # 同一（节点,轮次）已有决定：重复确认直接拒绝，绝不再次发出同一动作
        if any(d["node_id"] == node_id and d["attempt"] == attempt
               for d in self.store.list_recovery_decisions(execution_id)):
            return False
        phase = state.get("phase")
        active = self._active_evidence(execution_id, node_id, attempt)
        # 矛盾证据对任何处置都阻断。
        conflicts = self.evidence_conflicts(execution_id, node_id, attempt)
        if conflicts:
            self._emit(execution_id, "recovery_blocked_conflict", node_id,
                       "有效证据互相矛盾，处置被阻止；请撤回错误观察后再决定",
                       data={"attempt": attempt, "decision": decision,
                             "conflicts": conflicts})
            return False
        required = self._required_evidence_categories(execution, phase, finish)
        missing = [cat for cat in required
                   if not any(e["category"] == cat for e in active)]
        # 只有存在“未确认动作”（action 阶段）时，继续/结束才必须引用有效证据；
        # 等待/失败收尾/人工节点的处置依据是已落库的持久化事实，不强制证据。
        need_any = phase == "action"
        if missing or (need_any and not active):
            self._emit(execution_id, "recovery_blocked_missing_evidence", node_id,
                       "处置决定必须引用有效证据；证据不足，保持待核实",
                       data={"attempt": attempt, "decision": decision,
                             "missing": missing or (["any"] if need_any else [])})
            return False
        evidence_ids = [e["id"] for e in active]
        summary = self._decision_summary(decision, active)
        self.store.add_recovery_decision(
            execution_id, node_id, attempt, decision, evidence_ids, summary)
        # pending_decision 表示“已提交、待应用”：应用过程中崩溃可在重启后重放；
        # 应用开始后该标记清除，恢复出的尾部执行再次崩溃时可重新核实与决定。
        self._save_checkpoint(execution, pending_decision=decision,
                              decision_evidence_ids=evidence_ids)
        execution.task = asyncio.create_task(self._recover(execution, decision))
        return True

    @staticmethod
    def _decision_summary(decision: str, active: list[dict]) -> str:
        label = "继续" if decision == "finish" else "结束"
        parts = [f"#{e['id']} {e['category']}/{e['source']}={e['value']}"
                 for e in active]
        return f"人工决定{label}，依据证据：" + "；".join(parts)

    def _recovery_target(
        self, execution_id: str,
    ) -> tuple[Execution, dict] | None:
        """统一解析待核实处置目标，避免持久化状态与活动对象判断不一致。

        以 executions 持久化状态为准：处于 awaiting_recovery 时，即使内存对象
        缺失也按恢复检查点重新水合，保证证据/迟到回应/处置都登记到原来的
        未确认动作，而不是被 409 拒绝或落到新任务上。
        """
        record = self.store.get_execution(execution_id)
        if record is None or record["status"] != RECOVERY_STATUS:
            return None
        execution = self._executions.get(execution_id)
        if execution is None:
            state = self.store.get_recovery_state(execution_id)
            if state is None:
                state = self._rebuild_recovery_state(record)
                self.store.save_recovery_state(execution_id, state)
                self._emit(execution_id, "recovery_checkpoint_rebuilt",
                           state.get("current_node"),
                           "待核实入口重新补建检查点；证据登记到原未确认动作，不重发动作",
                           data={"attempt": state.get("current_attempt")})
            execution = self._hydrate_execution(state)
            self._executions[execution_id] = execution
        elif execution.recovery_state is None:
            state = self.store.get_recovery_state(execution_id)
            if state is not None:
                execution.recovery_state = state
                execution.current_node = state.get("current_node")
                execution.current_attempt = int(state.get("current_attempt") or 0)
        return execution, record

    @staticmethod
    def _required_evidence_categories(
        execution: Execution, phase: str | None, finish: bool,
    ) -> list[str]:
        """处置决定必须依赖的证据类别。

        仅针对未确认动作（action 阶段）：继续必须有动作结果证据；结束也必须
        引用至少一条有效证据（由调用方统一校验非空）。
        位置/持物无证据时恢复逻辑回退到最后已知快照，因此不强制。
        """
        if phase != "action":
            return []
        if finish and not execution.stop.is_set():
            return ["action_result"]
        return []  # 结束至少引用一条有效证据，由统一非空校验保证

    def _has_newer_live_execution(self, execution_id: str) -> bool:
        """是否存在比当前旧执行更晚启动、仍在活动的新任务。

        防止恢复旧执行时用旧设备快照覆盖新任务；证据与迟到回应按各自的
        execution_id 分别落库，天然不会串任务。
        """
        current = self._executions.get(execution_id)
        current_started = None
        record = self.store.get_execution(execution_id)
        if record is not None:
            current_started = record["started_at"]
        for other_id, other in self._executions.items():
            if other_id == execution_id or other.task is None:
                continue
            other_record = self.store.get_execution(other_id)
            if other_record is None:
                continue
            if other_record["status"] in ("running", "waiting", "paused", "cancelling"):
                if current_started is None or other_record["started_at"] >= current_started:
                    return True
        return False

    async def _gate(self, execution: Execution) -> None:
        while execution.paused and not execution.stop.is_set():
            try:
                await asyncio.wait_for(execution.resume.wait(), timeout=0.1)
            except TimeoutError:
                pass

    def record_late_action(self, execution_id: str, outcome: str, detail: str) -> bool:
        target = self._recovery_target(execution_id)
        if target is None:
            return False
        execution, _record = target
        node_id = execution.current_node
        attempt = execution.current_attempt
        if not node_id or not attempt:
            return False
        # 设备迟到回应同时作为“动作结果”证据登记（来源=device），只插入不覆盖
        self.store.add_evidence(
            execution_id, node_id, attempt, "action_result", "device",
            {"outcome": outcome, "detail": detail})
        self.store.add_node_followup(
            execution_id, node_id, attempt, "late_result", outcome, detail)
        self._emit(execution_id, "late_result_recorded", node_id,
                   f"重启核实期间收到的迟到结果（{outcome}）仅补充到旧执行：{detail}",
                   data={"attempt": attempt, "outcome": outcome, "detail": detail,
                         "recovery": True})
        self._save_checkpoint(execution, late_action_outcome=outcome,
                              late_action_detail=detail)
        return True

    @staticmethod
    def _evidence_key(category: str, value: dict) -> object:
        """同类证据的归一化取值，用于冲突判定（细节描述不参与比较）。"""
        if category == "action_result":
            return value.get("outcome")
        if category == "position":
            return (value.get("x"), value.get("y"))
        if category == "holding":
            return value.get("holding")
        return json.dumps(value, sort_keys=True, ensure_ascii=False)

    def _active_evidence(self, execution_id: str,
                         node_id: str | None = None,
                         attempt: int | None = None) -> list[dict]:
        return [e for e in self.store.list_evidence(execution_id)
                if not e["retracted"]
                and (node_id is None or e["node_id"] == node_id)
                and (attempt is None or e["attempt"] == attempt)]

    def evidence_conflicts(self, execution_id: str,
                           node_id: str | None = None,
                           attempt: int | None = None) -> dict[str, list[int]]:
        """同一（节点,轮次,类别）下取值不一致的有效证据 → 冲突，返回各类证据 id。"""
        by_category: dict[str, dict[object, list[int]]] = {}
        for e in self._active_evidence(execution_id, node_id, attempt):
            key = self._evidence_key(e["category"], e["value"])
            # 归一化键可能不可哈希（如 dict），统一转成字符串比较
            by_category.setdefault(e["category"], {}).setdefault(
                repr(key), []).append(e["id"])
        return {cat: [eid for ids in groups.values() for eid in ids]
                for cat, groups in by_category.items() if len(groups) > 1}

    def record_evidence(self, execution_id: str, category: str, source: str,
                        value: dict, observed_at: str | None = None) -> int | None:
        """登记现场观察/设备回应证据；与既有证据矛盾时并保留，不覆盖旧记录。"""
        target = self._recovery_target(execution_id)
        if target is None:
            return None
        execution, _record = target
        if category not in ("position", "holding", "action_result"):
            return None
        node_id = execution.current_node
        attempt = execution.current_attempt
        evidence_id = self.store.add_evidence(
            execution_id, node_id, attempt, category, source, value, observed_at)
        conflicts = self.evidence_conflicts(execution_id, node_id, attempt)
        self._emit(execution_id, "evidence_recorded", node_id,
                   f"证据已登记（{category}/{source}）：{value}"
                   + ("；与既有证据矛盾，继续执行被阻止" if conflicts else ""),
                   data={"evidence_id": evidence_id, "category": category,
                         "source": source, "value": value, "attempt": attempt,
                         "conflicts": conflicts})
        return evidence_id

    def retract_evidence(self, execution_id: str, evidence_id: int, reason: str) -> bool:
        record = self.store.get_execution(execution_id)
        if not record:
            return False
        # 只能撤回本执行的证据；已结束执行仅可查看历史，不再改动证据
        if record["status"] not in (RECOVERY_STATUS, "awaiting_manual"):
            return False
        if not any(e["id"] == evidence_id
                   for e in self.store.list_evidence(execution_id)):
            return False
        if not self.store.retract_evidence(evidence_id, reason):
            return False
        self._emit(execution_id, "evidence_retracted", None,
                   f"证据 #{evidence_id} 已撤回：{reason}",
                   data={"evidence_id": evidence_id, "reason": reason})
        return True

    async def _recover(self, execution: Execution, decision: str) -> None:
        eid = execution.id
        state = execution.recovery_state or {}
        node_id = state.get("current_node")
        attempt = int(state.get("current_attempt", 1))
        evidence_ids = state.get("decision_evidence_ids") or []
        # 决定开始应用即清除“待应用”标记：重启不会重放同一决定、重复发出同一动作；
        # 恢复出的尾部执行若再次崩溃，属于新的待核实周期，可重新登记证据与决定。
        self._save_checkpoint(execution, pending_decision=None,
                              recovery_decision=f"applied_{decision}")
        self._emit(eid, "recovery_decision_saved",
                   message=f"恢复处置已保存：{decision}",
                   data={"decision": decision, "evidence_ids": evidence_ids})
        active = self._active_evidence(eid, node_id, attempt)
        last_robot = dict(state.get("last_robot") or self.robot.status())
        # 位置/持物以有效证据为准（无证据时回退到最后已知快照）
        for e in active:
            if e["category"] == "position":
                last_robot["x"] = e["value"].get("x", last_robot.get("x", 0))
                last_robot["y"] = e["value"].get("y", last_robot.get("y", 0))
            elif e["category"] == "holding":
                last_robot["holding"] = e["value"].get("holding")

        async def apply_robot_snapshot() -> None:
            # 恢复旧执行时，若已有后来启动的活动任务（如用户先开了新任务），
            # 不能用旧快照覆盖共享设备状态；旧执行的核实结论只用于其自身的
            # 收尾/分支判定，迟到回应也只登记到旧执行身份。
            if self._has_newer_live_execution(eid):
                self._emit(eid, "recovery_robot_snapshot_skipped", node_id,
                           "检测到后来启动的新任务，恢复不覆盖当前设备状态",
                           data={"attempt": attempt})
                return
            self.robot.state.x = float(last_robot.get("x", 0))
            self.robot.state.y = float(last_robot.get("y", 0))
            self.robot.state.holding = last_robot.get("holding")
            self.robot.state.idle = True

        phase = state.get("phase")
        failure_detail = state.get("failure_detail")
        reason = f"人工恢复处置：{decision}"
        if failure_detail:
            reason += f"；原故障：{failure_detail}"

        if decision == "abort":
            # 结束同样执行安全收尾，但原未确认动作绝不重发；未确认动作身份保留。
            if node_id:
                uncertain = next((n for n in execution.definition["nodes"]
                                  if n["id"] == node_id), None)
                if uncertain is not None:
                    self.store.add_node_followup(
                        eid, node_id, attempt, "recovery", "aborted",
                        "人工核实后选择结束；原动作结果保留为后续信息，不重复执行该动作")
                    self._save_checkpoint(execution, phase="recovery_abort_cleanup",
                                          abort_reason=reason)
                    await apply_robot_snapshot()
                    await self._run_failure_cleanup(
                        execution, uncertain, attempt,
                        failure_detail or "重启前动作结果无法确认",
                        resuming=True)
            self.store.set_execution_status(eid, "resolved", reason)
            self._emit(eid, "execution_finished", message="人工核实后结束流程",
                       data={"status": "resolved", "reason": reason})
            self.store.delete_recovery_state(eid)
            return

        # 所有“继续”路径在重新驱动设备前统一应用证据快照；
        # abort 分支已在其收尾前单独应用。
        await apply_robot_snapshot()

        if phase == "manual":
            node_id = state.get("manual_node") or state.get("current_node")
            if decision == "manual_abort":
                self.store.finish_node_run(eid, node_id, 1, "aborted", "重启后人工确认终止")
                self.store.set_execution_status(eid, "resolved", "重启后人工确认终止")
                self._emit(eid, "execution_finished", message="人工确认终止，流程停在此处",
                           data={"status": "resolved", "reason": "重启后人工确认终止"})
                self.store.delete_recovery_state(eid)
                return
            self.store.finish_node_run(eid, node_id, 1, "succeeded", "重启后人工确认继续")
            self._set_status(eid, "running")
            resume_definition = self._tail_definition(execution, node_id, "success")
            await self._run(execution, resume_definition=resume_definition,
                            emit_started=False)
            return

        if phase == "waiting":
            # 恢复出的是全新事件对象，必须重新关联等待条件并清掉置位，
            # 否则恢复后页面再次“满足条件”无法唤醒该等待节点。
            execution.wait_condition = state.get("wait_condition")
            execution.condition_met = asyncio.Event()
            self._set_status(eid, "waiting")
            self._emit(eid, "recovery_waiting", message="恢复到原等待条件；未发出新的移动")
            execution.task = asyncio.current_task()
            node_id = state.get("current_node")
            node = next(n for n in execution.definition["nodes"] if n["id"] == node_id)
            kind, detail = await self._execute_node(
                execution, node, int(state.get("current_attempt", 1)))
            if kind != "success":
                final_status = "cancelled" if kind == "cancelled" else "resolved"
                wait_reason = f"人工核实后结束流程；等待结果：{detail}"
                self.store.set_execution_status(eid, final_status, wait_reason)
                self._emit(eid, "execution_finished", message="人工核实后结束流程",
                           data={"status": final_status, "reason": wait_reason})
                self.store.delete_recovery_state(eid)
                return
            self.store.finish_node_run(
                eid, node_id, int(state.get("current_attempt", 1)), "succeeded", detail)
            resume_definition = self._tail_definition(execution, node_id, "success")
            await self._run(execution, resume_definition=resume_definition,
                            emit_started=False)
            return

        if phase == "failure_cleanup":
            self._set_status(eid, "running")
            failed_node = next(n for n in execution.definition["nodes"]
                               if n["id"] == state.get("failed_node"))
            await self._run_failure_cleanup(
                execution, failed_node, int(state.get("failure_attempt", 1)),
                failure_detail, resuming=True)
            if execution.stop.is_set():
                final_reason = f"人工确认继续完成收尾后结束；原故障：{failure_detail}"
                self.store.set_execution_status(eid, "cancelled", final_reason)
                self._emit(eid, "execution_finished",
                           message="收尾完成，既有取消决定继续生效",
                           data={"status": "cancelled", "reason": final_reason})
                self.store.delete_recovery_state(eid)
                return
            resume_definition = self._failure_tail_definition(
                execution, failed_node["id"],
                int(state.get("failure_attempt", 1)), failed_node)
            await self._run(execution, resume_definition=resume_definition,
                            emit_started=False)
            return

        # 动作结果以有效证据为准；无证据时回退到兼容字段
        outcome_evidence = [e for e in active if e["category"] == "action_result"]
        late = (outcome_evidence[-1]["value"].get("outcome") if outcome_evidence
                else state.get("late_action_outcome"))
        if late == "success":
            self._set_status(eid, "running")
            written = self.store.finish_node_run_guarded(
                eid, node_id, attempt, "succeeded",
                state.get("late_action_detail", "重启后由设备迟到回应确认完成"))
            if written:
                self._emit(eid, "node_succeeded", node_id,
                           "未确认动作经证据核实为成功，不重复执行",
                           data={"attempt": attempt, "evidence_ids": evidence_ids})
            resume_definition = self._tail_definition(execution, node_id, "success")
            await self._run(execution, resume_definition=resume_definition,
                            emit_started=False)
            return

        if execution.stop.is_set():
            if node_id:
                uncertain = next(n for n in execution.definition["nodes"]
                                 if n["id"] == node_id)
                await self._run_failure_cleanup(
                    execution, uncertain, attempt,
                    state.get("late_action_detail") or "重启前取消中的动作结果无法确认",
                    resuming=True)
            cancel_reason = "人工确认继续收尾；取消决定继续生效"
            if failure_detail:
                prefix = "原故障：" if phase == "failure_cleanup" else "未确认动作："
                cancel_reason += f"；{prefix}{failure_detail}"
            self.store.set_execution_status(eid, "cancelled", cancel_reason)
            self._emit(eid, "execution_finished", message="收尾完成，取消决定继续生效",
                       data={"status": "cancelled", "reason": cancel_reason})
            self.store.delete_recovery_state(eid)
            return

        if node_id:
            failed_node = next(n for n in execution.definition["nodes"]
                               if n["id"] == node_id)
            self.store.add_node_followup(
                eid, node_id, attempt, "recovery", "aborted",
                "设备动作无法确认，人工选择结束；不重复执行该动作")
            await self._run_failure_cleanup(
                execution, failed_node, attempt,
                state.get("late_action_detail") or "重启前动作结果无法确认",
                resuming=True)
        self.store.set_execution_status(eid, "resolved", reason)
        self._emit(eid, "execution_finished", message="人工核实后结束流程",
                   data={"status": "resolved", "reason": reason})
        self.store.delete_recovery_state(eid)

    def _tail_definition(self, execution: Execution, node_id: str, kind: str) -> dict:
        target = execution.next_node(node_id, kind)
        reachable = set()
        if target:
            stack = [target]
            while stack:
                current = stack.pop()
                if current in reachable:
                    continue
                reachable.add(current)
                for edge in execution.definition.get("edges", []):
                    if edge["source"] == current:
                        stack.append(edge["target"])
        nodes = [n for n in execution.definition["nodes"] if n["id"] in reachable]
        edges = [e for e in execution.definition.get("edges", [])
                 if e["source"] in reachable and e["target"] in reachable]
        return {"nodes": nodes, "edges": edges}

    def _failure_tail_definition(self, execution: Execution, node_id: str,
                                 attempt: int, node: dict) -> dict:
        max_retries = int(node.get("params", {}).get("max_retries", 3))
        kind = "failure" if attempt <= max_retries else "exhausted"
        return self._tail_definition(execution, node_id, kind)

    async def _run(self, execution: Execution,
                   resume_definition: dict | None = None,
                   emit_started: bool = True) -> None:
        eid = execution.id
        definition = resume_definition or execution.definition
        execution.active_definition = definition
        if emit_started:
            self._emit(eid, "execution_started", message="任务开始",
                       data={"flow_id": execution.flow_id})
        nodes = {n["id"]: n for n in definition["nodes"]}
        incoming = {e["target"] for e in definition.get("edges", [])}
        starters = [n["id"] for n in definition["nodes"]
                    if n["id"] not in incoming]
        current = starters[0] if starters else None
        final_status = "succeeded"
        try:
            while current is not None:
                await self._gate(execution)
                if execution.stop.is_set():
                    final_status = "cancelled"
                    break
                node = nodes[current]
                if node.get("type") == "manual":
                    current = await self._run_manual_node(execution, node)
                    if current is None:
                        final_status = "resolved"
                        break
                    if current == "__cancel__":
                        final_status = "cancelled"
                        break
                    continue

                attempt = execution.attempts.get(current, 0) + 1
                execution.attempts[current] = attempt
                execution.current_node = current
                execution.current_attempt = attempt
                self.store.start_node_run(eid, current, attempt)
                self._save_checkpoint(
                    execution, phase="action",
                    completed_cleanup_stages=[], cleanup_next_stage="release",
                    wait_condition=None)
                self._emit(eid, "node_started", current,
                           f"第 {attempt} 次开始：{node.get('label') or current}",
                           data={"attempt": attempt})
                outcome = await self._execute_node(execution, node, attempt)
                kind, detail = outcome
                # 动作本身跑完了，但取消决定已在结果到达前生效：
                # 节点记录必须定格在 cancelled，迟到结果只作为可追溯的后续信息。
                if kind in ("success", "failure") and execution.stop.is_set():
                    if execution.cancel_phase is None:
                        execution.cancel_phase = "重试回路" if execution.in_retry_loop else "动作收尾"
                    self._save_checkpoint(
                        execution, failure_detail=f"取消时动作结果尚未确认：{detail}")
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
                    final_status = "cancelled"
                    break
                if kind == "cancelled":
                    if execution.cancel_phase is None:
                        execution.cancel_phase = "重试回路" if execution.in_retry_loop else "动作收尾"
                    self._save_checkpoint(
                        execution, failure_detail=f"取消时动作结果尚未确认：{detail}")
                    self.store.finish_node_run_guarded(
                        eid, current, attempt, "cancelled", detail)
                    self._emit(eid, "node_cancelled", current,
                               f"第 {attempt} 次动作收尾后取消：{detail}",
                               data={"attempt": attempt})
                    final_status = "cancelled"
                    break
                if kind == "failure":
                    max_retries = int(node.get("params", {}).get("max_retries", 3))
                    failure_detail = detail
                    self.store.finish_node_run(eid, current, attempt, "failed", failure_detail)
                    self._emit(eid, "node_failed", current,
                               f"第 {attempt}/{max_retries + 1} 次失败：{failure_detail}",
                               data={"detail": failure_detail, "attempt": attempt,
                                     "max_retries": max_retries})
                    # 失败后先走收尾流程（放下持有物 → 返回安全位置）。
                    # 收尾自身的成功/失败/取消只作为可追溯信息，
                    # 绝不覆盖本次失败原因，也不改变重试/分支决定。
                    self._save_checkpoint(
                        execution, phase="failure_cleanup", failed_node=current,
                        failure_attempt=attempt, failure_detail=failure_detail,
                        completed_cleanup_stages=[], cleanup_next_stage="release")
                    cancelled_in_cleanup = await self._run_failure_cleanup(
                        execution, node, attempt, failure_detail, resuming=False)
                    if execution.stop.is_set() or cancelled_in_cleanup:
                        execution.cancel_phase = "失败收尾"
                        self._emit(
                            eid, "node_cancelled", current,
                            f"失败收尾期间取消；原失败原因保留：{failure_detail}",
                            data={"attempt": attempt, "failure_detail": failure_detail})
                        final_status = "cancelled"
                        break
                    if attempt > max_retries:
                        target = execution.next_node(current, "exhausted")
                        if target is None:
                            target = execution.next_node(current, "failure")
                        if target is None:
                            final_status = "failed"
                            break
                        self._emit(eid, "retries_exhausted", current,
                                   f"已达 {max_retries + 1} 次上限，转入："
                                   f"{nodes[target].get('label') or target}",
                                   data={"attempt": attempt, "target": target})
                        current = target
                        continue
                    target = execution.next_node(current, "failure")
                    if target is None:
                        final_status = "failed"
                        break
                    self._emit(eid, "branch_taken", current,
                               f"第 {attempt} 次失败后走向："
                               f"{nodes[target].get('label') or target}",
                               data={"edge": "failure", "target": target,
                                     "attempt": attempt})
                    execution.in_retry_loop = True
                    current = target
                    continue
                self.store.finish_node_run(eid, current, attempt, "succeeded", detail)
                self._save_checkpoint(execution, phase="node_succeeded",
                                      last_completed_node=current,
                                      completed_cleanup_stages=[])
                if attempt > 1:
                    self._emit(eid, "node_succeeded", current,
                               f"第 {attempt} 次尝试成功：{node.get('label') or current}",
                               data={"attempt": attempt})
                else:
                    self._emit(eid, "node_succeeded", current,
                               f"完成：{node.get('label') or current}",
                               data={"attempt": attempt})
                target = execution.next_node(current, "success")
                current = target
            execution.current_node = None
            execution.current_attempt = 0
            reason = self._termination_reason(execution, final_status)
            self.store.set_execution_status(eid, final_status, reason)
            self.store.delete_recovery_state(eid)
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
            if last_failure:
                return f"取消于{phase}；{last_failure}"
            return f"取消于{phase}：取消时已确定的节点状态保持不变，迟到结果仅作追溯"
        if status == "failed":
            return "节点失败且无后续分支"
        if status == "resolved":
            return "人工确认终止"
        return None

    async def _run_failure_cleanup(
        self, execution: Execution, failed_node: dict, attempt: int,
        failure_detail: str | None,
        resuming: bool = False,
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

        stop = execution.stop
        completed = set((execution.recovery_state or {}).get("completed_cleanup_stages") or [])
        all_stages = [("release", "放下持有物"), ("return", "返回安全位置")]
        if resuming:
            stages = [(stage, label) for stage, label in all_stages if stage not in completed]
            if stages:
                self._emit(eid, "cleanup_resuming", node_id,
                           f"人工确认继续收尾；已完成阶段：{', '.join(sorted(completed)) or '无'}；"
                           f"待确认阶段：{', '.join(label for _, label in stages)}",
                           data={"attempt": attempt, "completed": sorted(completed)})
            else:
                self._emit(eid, "cleanup_resumed_already_complete", node_id,
                           "收尾阶段均已确认完成，不重复放下或移动",
                           data={"attempt": attempt})
                return False
        else:
            stages = all_stages
        self._emit(eid, "cleanup_started", node_id,
                   f"第 {attempt} 次失败后开始收尾：放下持有物 → 返回安全位置；"
                   f"原失败原因：{failure_detail}",
                   data={"attempt": attempt, "failure_detail": failure_detail})
        cancelled = False
        for idx, (stage, label) in enumerate(stages):
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
                if stage == "release":
                    result = await self.robot.release_if_holding(
                        cleanup_progress, stop, gate_steps)
                else:
                    result = await self.robot.return_to_safe(
                        cleanup_progress, stop, gate_steps)
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
            completed.add(stage)
            self._save_checkpoint(
                execution, completed_cleanup_stages=sorted(completed),
                cleanup_next_stage="return" if stage == "release" else None,
                last_robot=self.robot.status())
            if stage == "return":
                sx, sy = result  # type: ignore[misc]
                self.store.add_node_followup(
                    eid, node_id, attempt, "cleanup", "succeeded",
                    f"已返回安全位置 ({sx:g}, {sy:g})")
                self._emit(eid, "cleanup_succeeded", node_id,
                           f"收尾完成，已返回安全位置 ({sx:g}, {sy:g})；"
                           f"原失败原因保留：{failure_detail}",
                           data={"attempt": attempt, "safe": [sx, sy]})
        execution.cleanup_active = False
        return cancelled

    async def _run_manual_node(self, execution: Execution, node: dict) -> str | None:
        """人工处置节点：停在明确位置，等待人工确认继续或终止。"""
        eid = execution.id
        node_id = node["id"]
        execution.manual_node = node_id
        execution.manual_abort = False
        execution.manual_resolution.clear()
        instruction = node.get("params", {}).get("instruction", "等待人工处置")
        self.store.start_node_run(eid, node_id, 1)
        self.store.update_node_run(eid, node_id, 1, "manual", instruction)
        execution.manual_node = node_id
        execution.manual_abort = False
        self._save_checkpoint(execution, phase="manual", current_node=node_id,
                              current_attempt=1, manual_node=node_id,
                              manual_decision=None)
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
        if execution.stop.is_set():
            self.store.finish_node_run(eid, node_id, 1, "cancelled", "等待处置期间被取消")
            self._emit(eid, "node_cancelled", node_id, "人工处置节点被取消")
            return "__cancel__"
        if execution.manual_abort:
            self.store.finish_node_run(eid, node_id, 1, "aborted", "人工终止流程")
            self._emit(eid, "node_manual_aborted", node_id, "人工确认终止，流程停在此处")
            return None
        self.store.finish_node_run(eid, node_id, 1, "succeeded", "人工处置完成，继续执行")
        self._save_checkpoint(execution, current_node=node_id,
                              completed_cleanup_stages=[])
        self._emit(eid, "node_manual_resolved", node_id, "人工处置完成，流程继续")
        return execution.next_node(node_id, "success")

    async def _execute_node(self, execution: Execution, node: dict, attempt: int = 1) -> tuple[str, str | None]:
        eid = execution.id
        ntype = node["type"]
        params = node.get("params", {})

        async def progress(data: dict) -> None:
            self._save_checkpoint(execution, last_progress=data,
                                  last_robot={**data, "obstacle": self.robot.obstacle}
                                  if "x" in data else None)
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
            self.store.mark_node_run_waiting(eid, node["id"], attempt,
                                             f"等待条件：{condition}")
            self._save_checkpoint(
                execution, phase="waiting", current_node=node["id"],
                current_attempt=attempt, wait_condition=condition)
            self._emit(eid, "node_waiting", node["id"],
                       f"第 {attempt} 次等待条件：{condition}",
                       data={"attempt": attempt})
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
                    self._save_checkpoint(execution, phase="condition_met",
                                          wait_condition=None,
                                          last_robot=self.robot.status())
                    return "success", f"条件 {condition} 已满足"
            finally:
                execution.wait_condition = None

        async def gate_steps() -> None:
            await self._gate(execution)

        stop = execution.stop
        try:
            if ntype == "move":
                await self.robot.move(float(params.get("x", 0)), float(params.get("y", 0)),
                                     progress, stop, gate_steps)
                return "success", "移动完成"
            if ntype == "pick":
                await self.robot.pick(str(params.get("item", "物品")), progress, stop, gate_steps)
                return "success", "取物完成"
            if ntype == "place":
                await self.robot.place(progress, stop, gate_steps)
                return "success", "放置完成"
            return "failure", f"未知节点类型：{ntype}"
        except ActionCancelled as exc:
            return "cancelled", str(exc)
        except ActionFailed as exc:
            return "failure", str(exc)
