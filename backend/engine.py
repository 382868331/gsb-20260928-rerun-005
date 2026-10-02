from __future__ import annotations
import asyncio
import uuid
from collections.abc import Awaitable
from dataclasses import dataclass, field

from .evidence import SUBJECT_LABELS, blocked_subjects, resolve_ledger
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
    recovery_token: int | None = None  # 恢复决定令牌：证据修订后旧决定失效

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
        # 测试钩子：决定闸门在核对证据前等待（模拟“决定已保存、动作尚未发出”窗口）
        self.decision_gate_delay: float = 0.0
        self._executions: dict[str, Execution] = {}
        self._subscribers: set[asyncio.Queue] = set()
        store.mark_stale_running()
        for state in store.list_recovery_states():
            if store.get_execution(state["execution_id"])["status"] in (
                RECOVERY_STATUS, "awaiting_manual"
            ):
                self._seed_checkpoint_evidence(state)
                execution = self._hydrate_execution(state)
                self._executions[execution.id] = execution
                record = store.get_execution(execution.id)
                decision = ("manual_" + state.get("manual_decision")
                            if record["status"] == "awaiting_manual"
                            and state.get("manual_decision") is not None
                            else state.get("recovery_decision"))
                if decision is not None:
                    execution.recovery_token = state.get("recovery_token")
                    execution.task = asyncio.create_task(
                        self._recover(execution, decision, execution.recovery_token))

    def _seed_checkpoint_evidence(self, state: dict) -> None:
        """重启水合时把检查点快照作为最早一批核实证据逐项登记。

        位置、持物分别记录来源 checkpoint 与观察时刻；未确认动作的结果不臆造。
        操作者后续的现场观察/设备回应与之矛盾时保持待定，而不是互相覆盖。
        """
        eid = state["execution_id"]
        if self.store.get_recovery_evidence(eid):
            return
        snapshot = state.get("last_robot")
        observed_at = state.get("checkpoint_at") or ""
        if not observed_at:
            from datetime import datetime, timezone
            observed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if snapshot:
            if snapshot.get("x") is not None and snapshot.get("y") is not None:
                self.store.add_recovery_evidence(
                    eid, "position", "checkpoint", observed_at,
                    f"({snapshot.get('x')}, {snapshot.get('y')})",
                    "重启前最后检查点位置")
            holding = snapshot.get("holding")
            self.store.add_recovery_evidence(
                eid, "holding", "checkpoint", observed_at,
                holding if holding else "empty",
                "重启前最后检查点持物状态")

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
        from datetime import datetime, timezone
        state.setdefault("evidence_revision", 0)
        state.update({key: value for key, value in changes.items() if value is not None})
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
        state["checkpoint_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
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
        execution.recovery_token = state.get("recovery_token")
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
        if not execution or not record or record["status"] in TERMINAL:
            return False
        execution.stop.set()
        execution.resume.set()  # 唤醒暂停中的任务，让它响应取消
        execution.condition_met.set()
        if record["status"] == RECOVERY_STATUS:
            # 核实时再次取消：只落库决定，尚未发出的动作不启动；
            # 已经完成的移动/放下记录保持实际发生的事实。
            execution.cancel_phase = execution.cancel_phase or "核实等待"
            self._save_checkpoint(execution, cancel_requested=True,
                                  cancel_during_recovery=True,
                                  cancel_phase=execution.cancel_phase)
            self._emit(execution_id, "recovery_cancel_recorded",
                       message="核实期间收到取消：尚未发出的动作不再启动，等待重新确认")
        else:
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
        execution = self._executions.get(execution_id)
        record = self.store.get_execution(execution_id)
        if not execution or not record or record["status"] != RECOVERY_STATUS:
            return False
        decision = "finish" if finish else "abort"
        state = self.store.get_recovery_state(execution_id) or execution.recovery_state or {}
        if state.get("recovery_decision") is not None:
            return False
        ledger = self.store.get_recovery_evidence(execution_id)
        resolution = resolve_ledger(ledger)
        # 继续/收尾都必须确定设备是否持物；动作未确认时继续还需要动作结果。
        needed = []
        if finish:
            needed.append("holding")
            phase = state.get("phase")
            if (not state.get("cancel_requested")
                    and phase in ("action", "condition_met", "waiting")
                    and state.get("current_node")):
                needed.append("action_result")
            if state.get("last_robot") is None:
                needed.append("position")
        blocked = blocked_subjects(resolution, needed)
        if blocked:
            # 决定依据不足或存在矛盾：保持待决定，不由迟到资料自动覆盖现场。
            self._emit(execution_id, "recovery_decision_blocked",
                       message="核实证据不足或互相矛盾，决定保持待决定："
                               + "、".join(SUBJECT_LABELS[b["subject"]] for b in blocked),
                       data={"blocked": blocked, "resolution": resolution,
                             "decision": decision})
            return False
        token = int(state.get("evidence_revision", 0))
        basis = {s: resolution[s] for s in needed}
        # 决定保存后、动作发出前若证据再被修订，凭该令牌识别并回到等待确认。
        self._save_checkpoint(
            execution, recovery_decision=decision, recovery_token=token,
            decision_basis=basis, decision_revision=token)
        execution.recovery_token = token
        self._emit(execution_id, "recovery_decision_saved",
                   message=f"恢复处置已保存：{decision}",
                   data={"decision": decision, "basis": basis, "token": token,
                         "evidence": ledger})
        execution.task = asyncio.create_task(
            self._recover(execution, decision, token))
        return True

    async def _gate(self, execution: Execution) -> None:
        while execution.paused and not execution.stop.is_set():
            try:
                await asyncio.wait_for(execution.resume.wait(), timeout=0.1)
            except TimeoutError:
                pass

    def record_late_action(self, execution_id: str, outcome: str, detail: str) -> bool:
        execution = self._executions.get(execution_id)
        record = self.store.get_execution(execution_id)
        if not execution or not record or record["status"] != RECOVERY_STATUS:
            return False
        node_id = execution.current_node
        attempt = execution.current_attempt
        if not node_id or not attempt:
            return False
        observed_at = self._now()
        evidence_id = self.add_evidence(
            execution_id, "action_result", "device_response", observed_at,
            "success" if outcome == "success" else "failure",
            detail or "设备迟到回应", emit=False)
        self.store.add_node_followup(
            execution_id, node_id, attempt, "late_result", outcome, detail)
        self._emit(execution_id, "late_result_recorded", node_id,
                   f"重启核实期间收到的迟到结果（{outcome}）登记为动作结果证据：{detail}",
                   data={"attempt": attempt, "outcome": outcome, "detail": detail,
                         "recovery": True, "evidence_id": evidence_id})
        self._invalidate_decision(
            execution_id, reason=f"设备迟到动作回应到达（{outcome}）",
            changed_subjects=["action_result"])
        return True

    def _now(self) -> str:
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def add_evidence(
        self, execution_id: str, subject: str, source: str,
        observed_at: str, value: str, detail: str = "",
        supersedes_id: int | None = None, emit: bool = True,
    ) -> int:
        """逐项登记核实证据：位置/持物/动作结果分别记录来源与观察时刻。"""
        evidence_id = self.store.add_recovery_evidence(
            execution_id, subject, source, observed_at, value,
            detail or None, supersedes_id)
        if emit:
            self._emit(
                execution_id, "recovery_evidence_registered",
                message=(f"核实证据登记（{SUBJECT_LABELS[subject]}）：{value}"
                         f"；来源：{source}；观察时刻：{observed_at}"),
                data={"evidence_id": evidence_id, "subject": subject,
                      "source": source, "observed_at": observed_at,
                      "value": value, "detail": detail,
                      "supersedes_id": supersedes_id,
                      "ledger": self.store.get_recovery_evidence(execution_id)})
            self._invalidate_decision(
                execution_id, reason=f"新增{SUBJECT_LABELS[subject]}证据",
                changed_subjects=[subject])
        return evidence_id

    def withdraw_evidence(self, execution_id: str, evidence_id: int,
                          detail: str = "") -> bool:
        """撤回一条现场观察证据（撤回不删除台账，保留修订历史）。"""
        entries = {e["id"]: e
                   for e in self.store.get_recovery_evidence(execution_id)}
        entry = entries.get(evidence_id)
        if entry is None:
            return False
        if not self.store.withdraw_recovery_evidence(execution_id, evidence_id):
            return False
        self._emit(
            execution_id, "recovery_evidence_withdrawn",
            message=(f"核实证据撤回（{SUBJECT_LABELS[entry['subject']]}）："
                     f"{entry['value']}{('；' + detail) if detail else ''}"),
            data={"evidence_id": evidence_id, "subject": entry["subject"],
                  "detail": detail,
                  "ledger": self.store.get_recovery_evidence(execution_id)})
        self._invalidate_decision(
            execution_id, reason=f"撤回{SUBJECT_LABELS[entry['subject']]}证据",
            changed_subjects=[entry["subject"]])
        return True

    def _invalidate_decision(self, execution_id: str, reason: str,
                             changed_subjects: list[str]) -> None:
        """证据修订后让尚未执行完的恢复决定失效，回到等待重新确认。

        已经完成的移动/放下事实不受影响（node_runs 终态与收尾记录保留），
        但尚未发出的后续动作必须重新等待确认。
        """
        execution = self._executions.get(execution_id)
        if execution is None:
            return
        state = self.store.get_recovery_state(execution_id) or execution.recovery_state or {}
        revision = int(state.get("evidence_revision", 0)) + 1
        if state.get("recovery_decision") is not None:
            state.update({"recovery_decision": None, "decision_token": None,
                          "decision_basis": None, "reawait_reason": reason,
                          "reawait_subjects": changed_subjects,
                          "evidence_revision": revision})
            execution.recovery_token = None
        else:
            state["evidence_revision"] = revision
        execution.recovery_state = state
        self.store.save_recovery_state(execution_id, state)

    async def _decision_gate(self, execution: Execution, token: int | None,
                             needed: list[str]) -> tuple[list[dict], dict] | None:
        """动作发出前的决定闸门。

        每次决定只推进一次：证据在决定保存后被修订（revision 变化），
        或所需主题存在矛盾/无证据，则尚未发出的动作回到等待确认，
        不自动继续。返回 None 表示可以继续；否则返回 (受阻主题, 归并结果)。
        """
        if self.decision_gate_delay:
            await asyncio.sleep(self.decision_gate_delay)
        await self._gate(execution)
        state = execution.recovery_state or {}
        revision = int(state.get("evidence_revision", 0))
        ledger = self.store.get_recovery_evidence(execution.id)
        resolution = resolve_ledger(ledger)
        if token is None or revision != token:
            blocked = blocked_subjects(resolution, needed)
            self._reawait(
                execution, blocked, resolution,
                reason=("核实证据在决定保存后被更正，尚未发出的动作重新等待确认"
                        if token is not None else "恢复决定已失效，需要重新确认"))
            return blocked, resolution
        blocked = blocked_subjects(resolution, needed)
        if blocked:
            self._reawait(execution, blocked, resolution,
                          reason="核实证据互相矛盾或缺失，后续动作受阻")
            return blocked, resolution
        return None

    def _reawait(self, execution: Execution, blocked: list[dict],
                 resolution: dict, reason: str) -> None:
        labels = "、".join(SUBJECT_LABELS[b["subject"]] for b in blocked) or "相关"
        execution.recovery_token = None
        self.store.set_execution_status(execution.id, RECOVERY_STATUS, None)
        state = self.store.get_recovery_state(execution.id) or execution.recovery_state or {}
        state.update({
            "recovery_decision": None, "decision_token": None,
            "decision_basis": None,
            "blocked_subjects": [b["subject"] for b in blocked],
            "reawait_reason": reason,
        })
        execution.recovery_state = state
        self.store.save_recovery_state(execution.id, state)
        self._emit(execution.id, "recovery_reawait_confirmation",
                   message=f"{reason}；受阻主题：{labels}",
                   data={"blocked": blocked, "resolution": resolution})

    async def _run_node_gated(self, execution: Execution, node: dict, attempt: int,
                              token: int | None, needed: list[str]) -> bool:
        """恢复后启动一个节点前的闸门；返回 True 表示受阻（已回到待确认）。"""
        gate = await self._decision_gate(execution, token, needed)
        if gate is None:
            return False
        blocked, resolution = gate
        blocked_actions = []
        ntype = node.get("type")
        for subject in (b["subject"] for b in blocked):
            if subject == "holding":
                blocked_actions.append("再次取物(pick)" if ntype == "pick"
                                       else "就地放下(release)")
            elif subject == "position":
                blocked_actions.append(f"节点移动({node.get('label') or node['id']})")
            elif subject == "action_result":
                blocked_actions.append("按原动作结果接续分支")
        self._emit(
            execution.id, "recovery_followups_blocked", node["id"],
            message="以下后续动作因核实证据待定而受阻：" + ("、".join(blocked_actions) or "相关动作"),
            data={"attempt": attempt, "blocked": blocked,
                  "blocked_actions": blocked_actions, "resolution": resolution})
        return True

    async def _recover(self, execution: Execution, decision: str,
                       token: int | None = None) -> None:
        eid = execution.id
        state = execution.recovery_state or {}
        phase = state.get("phase")
        failure_detail = state.get("failure_detail")
        state = execution.recovery_state or {}

        def restore_snapshot() -> None:
            # 设备快照以已确认的证据为准；位置/持物存在矛盾时保持待定，不臆造现场。
            ledger = self.store.get_recovery_evidence(eid)
            resolution = resolve_ledger(ledger)
            last_robot = state.get("last_robot") or self.robot.status()
            pos = resolution["position"]
            hold = resolution["holding"]
            if pos["status"] == "confirmed":
                x, y = pos["value"]
            else:
                x, y = float(last_robot.get("x", 0)), float(last_robot.get("y", 0))
            self.robot.state.x, self.robot.state.y = x, y
            self.robot.state.holding = hold["value"] if hold["status"] == "confirmed" else None
            self.robot.state.idle = True

        restore_snapshot()

        def terminal_reason(status: str) -> str:
            parts = []
            if failure_detail:
                parts.append(f"原故障：{failure_detail}")
            basis = state.get("decision_basis") or {}
            used = [SUBJECT_LABELS[s] for s, item in basis.items()
                    if item.get("status") == "confirmed"]
            parts.append("依据证据：" + ("、".join(used) if used else "无未决主题"))
            parts.append(f"后续处置：{'继续' if status == 'continue' else '结束/取消'}")
            return "；".join(parts)

        async def cleanup_resume(node, attempt: int, detail: str | None) -> str:
            """收尾阶段的决定闸门；返回 done/blocked/cancelled。"""
            # 放下需要先确认持物；返回安全位置需要确认当前位置。
            gate = await self._decision_gate(execution, token, ["holding", "position"])
            if gate is not None:
                blocked, resolution = gate
                self._emit(
                    eid, "recovery_followups_blocked", node.get("id"),
                    message="安全收尾因核实证据待定受阻：持物有争议时不得重复放下，位置未定时不得返回移动",
                    data={"attempt": attempt, "blocked": blocked,
                          "blocked_actions": ["就地放下(release)", "返回安全位置(return)"],
                          "resolution": resolution})
                return "blocked"
            cancelled = await self._run_failure_cleanup(
                execution, node, attempt, detail, resuming=True,
                gate_token=token)
            if cancelled == "blocked":
                return "blocked"
            restore_snapshot()
            return "cancelled" if cancelled else "done"

        if decision == "abort":
            node_id = state.get("current_node")
            if node_id:
                node = next((n for n in execution.definition["nodes"]
                             if n["id"] == node_id), None)
                if node is not None:
                    self.store.add_node_followup(
                        eid, node_id, int(state.get("current_attempt", 1)),
                        "recovery", "aborted",
                        "人工核实后结束；不重复执行未确认动作")
                    outcome = await cleanup_resume(
                        node, int(state.get("current_attempt", 1)),
                        failure_detail or "重启前动作结果无法确认")
                    if outcome == "blocked":
                        return
            self.store.set_execution_status(
                eid, "resolved", "人工核实后结束流程；" + terminal_reason("abort"))
            self._emit(eid, "execution_finished", message="人工核实后结束流程",
                       data={"status": "resolved",
                             "reason": terminal_reason("abort")})
            self.store.delete_recovery_state(eid)
            return

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
            ledger = self.store.get_recovery_evidence(eid)
            if resolve_ledger(ledger)["holding"]["status"] == "disputed":
                self._reawait(
                    execution,
                    [{"subject": "holding", "status": "disputed"}],
                    resolve_ledger(ledger),
                    reason="持物归属仍有争议，不得再次取物：放行继续受阻")
                return
            self._set_status(eid, "running")
            resume_definition = self._tail_definition(execution, node_id, "success")
            await self._run(execution, resume_definition=resume_definition,
                            emit_started=False, recovery_token=token)
            return

        if phase == "waiting":
            gate = await self._decision_gate(execution, token, ["holding"])
            if gate is not None:
                return
            self._set_status(eid, "waiting")
            execution.wait_condition = state.get("wait_condition")
            self._emit(eid, "recovery_waiting", message="恢复到原等待条件；未发出新的移动")
            execution.task = asyncio.current_task()
            node_id = state.get("current_node")
            node = next(n for n in execution.definition["nodes"] if n["id"] == node_id)
            if await self._run_node_gated(execution, node,
                                          int(state.get("current_attempt", 1)),
                                          token, ["position", "holding"]):
                return
            kind, detail = await self._execute_node(
                execution, node, int(state.get("current_attempt", 1)))
            if kind != "success":
                final_status = "cancelled" if kind == "cancelled" else "resolved"
                wait_reason = (f"人工核实后结束流程；等待结果：{detail}；"
                               + terminal_reason(final_status))
                self.store.set_execution_status(eid, final_status, wait_reason)
                self._emit(eid, "execution_finished", message="人工核实后结束流程",
                           data={"status": final_status, "reason": wait_reason})
                self.store.delete_recovery_state(eid)
                return
            self.store.finish_node_run(
                eid, node_id, int(state.get("current_attempt", 1)), "succeeded", detail)
            resume_definition = self._tail_definition(execution, node_id, "success")
            await self._run(execution, resume_definition=resume_definition,
                            emit_started=False, recovery_token=token)
            return

        if phase == "failure_cleanup":
            failed_node = next(n for n in execution.definition["nodes"]
                               if n["id"] == state.get("failed_node"))
            outcome = await cleanup_resume(
                failed_node, int(state.get("failure_attempt", 1)), failure_detail)
            if outcome == "blocked":
                return
            self._set_status(eid, "running")
            if execution.stop.is_set():
                final_reason = "人工确认继续完成必要收尾后仍记录为取消；" + terminal_reason("cancelled")
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
                                emit_started=False, recovery_token=token)
            return

        node_id = state.get("current_node")
        attempt = int(state.get("current_attempt", 1))
        ledger = self.store.get_recovery_evidence(eid)
        resolution = resolve_ledger(ledger)
        result_item = resolution["action_result"]
        node = next((n for n in execution.definition["nodes"]
                     if n["id"] == node_id), None)

        if result_item["status"] == "confirmed":
            result = result_item["value"]
            if result == "success":
                # 动作结果由证据逐项确认后才接续 success：迟到完成不能单独自动覆盖现场。
                gate = await self._decision_gate(execution, token,
                                                 ["position", "holding", "action_result"])
                if gate is not None:
                    return
                self._set_status(eid, "running")
                self.store.finish_node_run_guarded(
                    eid, node_id, attempt, "succeeded",
                    "重启后由核实证据确认动作完成")
                resume_definition = self._tail_definition(execution, node_id, "success")
                await self._run(execution, resume_definition=resume_definition,
                                emit_started=False, recovery_token=token)
                return
            failure_detail = failure_detail or "设备迟到回应确认动作失败"
            self.store.finish_node_run_guarded(
                eid, node_id, attempt, "failed", failure_detail)
            self.store.add_node_followup(
                eid, node_id, attempt, "late_result", "failure", failure_detail)

        if execution.stop.is_set():
            if node is not None:
                outcome = await cleanup_resume(
                    node, attempt,
                    failure_detail or "重启前取消中的动作结果无法确认")
                if outcome == "blocked":
                    return
            cancel_reason = "核实期间再次取消，完成必要收尾后仍为取消；" + terminal_reason("cancelled")
            if failure_detail:
                prefix = "原故障：" if phase == "failure_cleanup" else "未确认动作："
                cancel_reason += f"；{prefix}{failure_detail}"
            self.store.set_execution_status(eid, "cancelled", cancel_reason)
            self._emit(eid, "execution_finished", message="收尾完成，取消决定继续生效",
                       data={"status": "cancelled", "reason": cancel_reason})
            self.store.delete_recovery_state(eid)
            return

        if node is not None:
            self.store.add_node_followup(
                eid, node_id, attempt, "recovery", "aborted",
                "设备动作无法确认，人工选择结束；不重复执行该动作")
            outcome = await cleanup_resume(
                node, attempt,
                failure_detail or "重启前动作结果无法确认")
            if outcome == "blocked":
                return
        self.store.set_execution_status(eid, "resolved", "人工核实后结束流程；" + terminal_reason("abort"))
        self._emit(eid, "execution_finished", message="人工核实后结束流程",
                   data={"status": "resolved", "reason": terminal_reason("abort")})
        self.store.delete_recovery_state(eid)

    def _tail_definition(self, execution: Execution, node_id: str, kind: str) -> dict:
        # 尾定义始终从完整流程定义计算：恢复受阻后再次确认时，
        # active_definition 可能是上一次接续遗留的尾定义（只含部分节点）。
        target = next(
            (edge["target"] for edge in execution.definition.get("edges", [])
             if edge["source"] == node_id and edge.get("kind", "success") == kind),
            None)
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
                   emit_started: bool = True,
                   recovery_token: int | None = "live") -> None:
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
                if recovery_token != "live":
                    needed = {"move": ["position", "holding"],
                              "pick": ["holding"],
                              "place": ["holding"]}.get(node.get("type"), [])
                    if await self._run_node_gated(
                        execution, node,
                        execution.attempts.get(current, 0) + 1,
                        recovery_token, needed):
                        # 证据更正后尚未发出的动作重新等待确认：本次决定只推进到这里。
                        # 保留已确认完成的节点现场；尚未启动的后继不写入当前节点。
                        execution.task = None
                        return
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
                    if execution.stop.is_set() or cancelled_in_cleanup == "cancelled":
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
            # 恢复后证据被更正、后续动作受阻时执行回到 awaiting_recovery：
            # 必须保留执行对象，供操作者补充证据后重新确认。
            if self.store.get_execution(eid)["status"] != RECOVERY_STATUS:
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
        gate_token: int | None = "live",
    ) -> str:
        """失败后的收尾流程：就地放下持有物 → 返回安全位置。

        取消语义：
        - 取消发生在重试/等待过程中（进入收尾前 stop 已置位）：不启动任何收尾动作；
        - 取消发生在收尾过程中：允许当前正在执行的那一个收尾动作做完，
          不再启动后续收尾动作；再次到达的取消（stop 已存在）立即终止下一个动作；
        - 收尾自身失败或被取消，只记为该失败节点的可追溯后续信息，
          原失败原因与分支决定不变。

        返回值：""（未受阻也未取消）/ "cancelled" / "blocked"。
        blocked 只可能出现在人工核实恢复（gate_token 为整数令牌）时：
        持物归属有争议时不得再次放下，位置待定时不得移动返回。
        """
        eid = execution.id
        node_id = failed_node["id"]

        async def cleanup_progress(data: dict) -> None:
            self._emit(eid, "cleanup_progress", node_id, data={**data, "attempt": attempt})

        async def gate_steps() -> None:
            await self._gate(execution)

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
                return ""
        else:
            stages = all_stages
        self._emit(eid, "cleanup_started", node_id,
                   f"第 {attempt} 次失败后开始收尾：放下持有物 → 返回安全位置；"
                   f"原失败原因：{failure_detail}",
                   data={"attempt": attempt, "failure_detail": failure_detail})
        result = ""
        for idx, (stage, label) in enumerate(stages):
            await self._gate(execution)
            stop = execution.stop
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
                result = "cancelled"
                break
            if resuming and gate_token != "live":
                needed = ["holding"] if stage == "release" else ["position"]
                gate = await self._decision_gate(execution, gate_token, needed)
                if gate is not None:
                    blocked, resolution = gate
                    actions = {"release": "就地放下(release)",
                               "return": "返回安全位置(return)"}
                    self.store.add_node_followup(
                        eid, node_id, attempt, "cleanup", "blocked",
                        f"{label} 因核实证据待定未启动")
                    self._emit(eid, "recovery_followups_blocked", node_id,
                               f"收尾动作 {actions[stage]} 因核实证据待定受阻，不重复取物/放下",
                               data={"attempt": attempt, "stage": stage,
                                     "blocked": blocked, "resolution": resolution})
                    result = "blocked"
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
                result = "cancelled"
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
                    result = "cancelled"
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
        return result

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
