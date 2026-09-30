from __future__ import annotations
import asyncio
import uuid
from dataclasses import dataclass, field

from .simulator import ActionCancelled, ActionFailed, VirtualRobot
from .store import Store


TERMINAL = {"succeeded", "failed", "cancelled", "interrupted", "aborted"}


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
    # 当前等待人工处置的节点信息：{"node_id","remaining","detail"}
    manual: dict | None = None
    manual_action: str | None = None  # "retry" / "abort"
    manual_event: asyncio.Event = field(default_factory=asyncio.Event)

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
        if execution.manual is not None:
            restored = "manual"
        else:
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
        execution.stop.set()
        execution.resume.set()  # 唤醒暂停中的任务，让它响应取消
        execution.condition_met.set()
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

    def manual_decide(self, execution_id: str, action: str) -> tuple[bool, str | None]:
        """人工处置：retry 回到指定节点重试；abort 终止在当前处置节点。"""
        execution = self._executions.get(execution_id)
        if not execution or execution.stop.is_set() or execution.manual is None:
            return False, "当前没有等待人工处置的节点"
        if action not in ("retry", "abort"):
            return False, f"未知人工处置动作：{action}"
        if action == "retry" and execution.manual["remaining"] <= 0:
            return False, "重试次数已达上限，请终止任务"
        execution.manual_action = action
        execution.manual_event.set()
        return True, None

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
        nodes = {n["id"]: n for n in execution.definition["nodes"]}
        incoming = {e["target"] for e in execution.definition.get("edges", [])}
        starters = [n["id"] for n in execution.definition["nodes"]
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
                attempt = self.store.start_node_run(eid, current)
                self._emit(eid, "node_started", current,
                           f"开始（第 {attempt} 次）：{node.get('label') or current}",
                           data={"attempt": attempt})
                outcome = await self._execute_node(execution, node)
                kind, detail, next_kind = outcome
                # 取消之后到达的任何结果都是“迟到结果”：不能当成新一轮完成，
                # 后续节点也绝不允许再启动。
                if execution.stop.is_set() and kind != "cancelled":
                    self.store.update_node_run(
                        eid, current, "cancelled",
                        f"取消后收到迟到结果（{detail}），不计入", True)
                    self._emit(eid, "node_late_result_ignored", current,
                               f"取消后的迟到结果已忽略：{detail}",
                               data={"attempt": attempt, "late_outcome": kind})
                    final_status = "cancelled"
                    break
                if kind == "cancelled":
                    self.store.update_node_run(eid, current, "cancelled", detail, True)
                    self._emit(eid, "node_cancelled", current,
                               f"动作收尾后取消：{detail}", data={"attempt": attempt})
                    final_status = "cancelled"
                    break
                if kind == "failure":
                    self.store.update_node_run(eid, current, "failed", detail, True)
                    self._emit(eid, "node_failed", current,
                               f"第 {attempt} 次失败：{detail}",
                               data={"attempt": attempt, "detail": detail})
                    target = execution.next_node(current, "failure")
                    if target is None:
                        final_status = "failed"
                        break
                    self._emit(eid, "branch_taken", current,
                               f"失败后走向：{nodes[target].get('label') or target}",
                               data={"edge": "failure", "target": target,
                                     "attempt": attempt})
                    current = target
                    continue
                if kind == "manual":
                    if detail == "abort":
                        self.store.update_node_run(eid, current, "cancelled",
                                                   "人工终止，流程停在此处", True)
                        self._emit(eid, "node_manual_aborted", current,
                                   "人工终止：流程停在人工处置节点",
                                   data={"attempt": attempt})
                        final_status = "aborted"
                        break
                    target = detail.split(":", 1)[1]
                    self.store.update_node_run(eid, current, "succeeded",
                                               f"人工处置后回到 {target} 重试", True)
                    self._emit(eid, "node_manual_retry", current,
                               f"人工处置：回到 {nodes[target].get('label') or target} 重试",
                               data={"attempt": attempt, "target": target})
                    current = target
                    continue
                self.store.update_node_run(eid, current, "succeeded", detail, True)
                self._emit(eid, "node_succeeded", current,
                           f"完成（第 {attempt} 次）：{node.get('label') or current}",
                           data={"attempt": attempt})
                target = execution.next_node(current, "success")
                current = target
            self._set_status(eid, final_status)
            self._emit(eid, "execution_finished",
                       message=f"任务结束：{final_status}",
                       data={"status": final_status, "robot": self.robot.status()})
        except Exception as exc:  # 兜底：引擎异常也要落库，不留幽灵运行态
            self._set_status(eid, "failed")
            self._emit(eid, "execution_finished", message=f"引擎异常：{exc}",
                       data={"status": "failed"})
        finally:
            self._executions.pop(eid, None)

    async def _execute_node(self, execution: Execution, node: dict) -> tuple[str, str | None, str]:
        eid = execution.id
        ntype = node["type"]
        params = node.get("params", {})

        async def progress(data: dict) -> None:
            self._emit(eid, "node_progress", node["id"],
                       data={**data, "robot": self.robot.status()})

        if ntype == "manual":
            # 人工处置节点：失败回路到此后，由人工选择“重试（回到指定节点）”或“终止”。
            # params.retry_target 指定重试回到的节点；params.max_retries 为该节点允许的
            # 重试总次数（从 0 开始计数），达到上限后只能终止，执行明确停在此节点。
            max_retries = int(params.get("max_retries", 2))
            retry_target = params.get("retry_target")
            prior_retries = sum(
                1 for nr in self.store.get_execution(eid)["node_runs"]
                if nr["node_id"] == node["id"]
                and (nr["detail"] or "").startswith("人工处置后回到")
            )
            remaining = max_retries - prior_retries
            prompt = str(params.get("prompt", "等待人工处置：重试或终止"))
            execution.manual = {"node_id": node["id"], "remaining": remaining,
                                "detail": prompt, "retry_target": retry_target}
            execution.manual_action = None
            execution.manual_event.clear()
            self.store.update_node_run(eid, node["id"], "manual", prompt)
            self._set_status(eid, "manual")
            if remaining > 0 and retry_target:
                hint = f"{prompt}（剩余重试 {remaining}/{max_retries}）"
            else:
                hint = f"{prompt}（重试已达上限 {max_retries} 次，请终止）"
            self._emit(eid, "node_manual_waiting", node["id"], hint,
                       data={"remaining": remaining, "max_retries": max_retries,
                             "retry_target": retry_target})
            try:
                while True:
                    await self._gate(execution)
                    if execution.stop.is_set():
                        return "cancelled", "人工处置等待被取消", ""
                    try:
                        await asyncio.wait_for(execution.manual_event.wait(), timeout=0.2)
                    except asyncio.TimeoutError:
                        continue
                    if execution.stop.is_set():
                        return "cancelled", "人工处置等待被取消", ""
                    action = execution.manual_action
                    self._emit(eid, "node_manual_decision", node["id"],
                               "人工选择：重试" if action == "retry" else "人工选择：终止",
                               data={"action": action, "remaining": remaining})
                    if action == "abort":
                        return "manual", "abort", ""
                    return "manual", f"retry:{retry_target}", "success"
            finally:
                execution.manual = None
                execution.manual_action = None

        if ntype == "log":
            message = str(params.get("message", ""))
            self._emit(eid, "node_log", node["id"], message)
            return "success", message, "success"

        if ntype == "wait":
            condition = params.get("condition", "signal")
            timeout = params.get("timeout")
            execution.wait_condition = condition
            execution.condition_met.clear()
            self.store.update_node_run(eid, node["id"], "waiting",
                                       f"等待条件：{condition}")
            self._emit(eid, "node_waiting", node["id"],
                       f"等待条件：{condition}")
            self._set_status(eid, "waiting")
            try:
                deadline = asyncio.get_running_loop().time() + float(timeout) if timeout else None
                while True:
                    await self._gate(execution)
                    if execution.stop.is_set():
                        return "cancelled", "等待被取消", ""
                    if execution.paused:
                        continue
                    remaining = None if deadline is None else max(0.0, deadline - asyncio.get_running_loop().time())
                    try:
                        await asyncio.wait_for(execution.condition_met.wait(),
                                               timeout=remaining if remaining is not None else 0.2)
                    except asyncio.TimeoutError:
                        if deadline is not None and asyncio.get_running_loop().time() >= deadline:
                            return "failure", f"等待 {condition} 超时", "failure"
                        continue
                    if execution.stop.is_set():
                        return "cancelled", "等待被取消", ""
                    self._set_status(eid, "running")
                    return "success", f"条件 {condition} 已满足", "success"
            finally:
                execution.wait_condition = None

        async def gate_steps() -> None:
            await self._gate(execution)

        stop = execution.stop
        try:
            if ntype == "move":
                await self.robot.move(float(params.get("x", 0)), float(params.get("y", 0)),
                                     progress, stop, gate_steps)
                return "success", "移动完成", "success"
            if ntype == "pick":
                await self.robot.pick(str(params.get("item", "物品")), progress, stop, gate_steps)
                return "success", "取物完成", "success"
            if ntype == "place":
                await self.robot.place(progress, stop, gate_steps)
                return "success", "放置完成", "success"
            return "failure", f"未知节点类型：{ntype}", "failure"
        except ActionCancelled as exc:
            return "cancelled", str(exc), ""
        except ActionFailed as exc:
            return "failure", str(exc), "failure"
