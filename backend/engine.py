from __future__ import annotations
import asyncio
import uuid
from dataclasses import dataclass, field

from .simulator import ActionCancelled, ActionFailed, VirtualRobot
from .store import Store


TERMINAL = {"succeeded", "failed", "cancelled", "interrupted", "resolved"}


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
                self._emit(eid, "node_started", current,
                           f"第 {attempt} 次开始：{node.get('label') or current}",
                           data={"attempt": attempt})
                outcome = await self._execute_node(execution, node, attempt)
                kind, detail = outcome
                if kind == "cancelled":
                    self.store.finish_node_run(eid, current, attempt, "cancelled", detail)
                    self._emit(eid, "node_cancelled", current,
                               f"第 {attempt} 次动作收尾后取消：{detail}",
                               data={"attempt": attempt})
                    final_status = "cancelled"
                    break
                if kind == "failure":
                    max_retries = int(node.get("params", {}).get("max_retries", 3))
                    self.store.finish_node_run(eid, current, attempt, "failed", detail)
                    self._emit(eid, "node_failed", current,
                               f"第 {attempt}/{max_retries + 1} 次失败：{detail}",
                               data={"detail": detail, "attempt": attempt,
                                     "max_retries": max_retries})
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
                    current = target
                    continue
                self.store.finish_node_run(eid, current, attempt, "succeeded", detail)
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
        self._emit(eid, "node_manual_resolved", node_id, "人工处置完成，流程继续")
        return execution.next_node(node_id, "success")

    async def _execute_node(self, execution: Execution, node: dict, attempt: int = 1) -> tuple[str, str | None]:
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
            self.store.mark_node_run_waiting(eid, node["id"], attempt,
                                             f"等待条件：{condition}")
            self._emit(eid, "node_waiting", node["id"],
                       f"第 {attempt} 次等待条件：{condition}",
                       data={"attempt": attempt})
            self._set_status(eid, "waiting")
            try:
                deadline = asyncio.get_running_loop().time() + float(timeout) if timeout else None
                while True:
                    await self._gate(execution)
                    if execution.stop.is_set():
                        return "cancelled", "等待被取消"
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
                        return "cancelled", "等待被取消"
                    self._set_status(eid, "running")
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
