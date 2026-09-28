import threading
import time
import uuid
from collections import deque

from .robot import SimRobot


NODE_TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED", "SKIPPED"}
RUN_ACTIVE = {"RUNNING", "PAUSED", "CANCELLING"}


class ConditionError(Exception):
    pass


def _get(path, ctx):
    cur = ctx
    for part in str(path).split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def eval_condition(expr, ctx):
    """Minimal safe condition evaluator: a.b == 'x', !=, >, <, exists."""
    for op in ("==", "!=", ">=", "<=", ">", "<"):
        if op in expr:
            lhs, rhs = expr.split(op, 1)
            lv, rv = _get(lhs.strip(), ctx), rhs.strip().strip("'\"")
            try:
                lv_n, rv_n = float(lv), float(rv)
                lv, rv = lv_n, rv_n
            except (TypeError, ValueError):
                pass
            try:
                result = {"==": lv == rv, "!=": lv != rv, ">=": lv >= rv,
                          "<=": lv <= rv, ">": lv > rv, "<": lv < rv}[op]
            except TypeError:
                result = False
            return result
    if expr.strip().endswith(" exists"):
        return _get(expr.strip()[:-7], ctx) is not None
    return bool(_get(expr.strip(), ctx))


class RunExecution:
    def __init__(self, engine, run_id, definition):
        self.engine = engine
        self.db = engine.db
        self.run_id = run_id
        self.defn = definition
        self.nodes = {n["id"]: n for n in definition["nodes"]}
        self.edges = definition.get("edges", [])
        self.signal = threading.Event()
        self.cancel_flag = False
        self.paused = False
        self.wait_signal: threading.Event | None = None
        self.wait_node_id = None
        self._wake = threading.Event()
        self.thread = None
        self.seq_lock = threading.Lock()

    # ---- event log (source of truth shared with SSE) ----
    def emit(self, kind, node_id=None, payload=None):
        with self.seq_lock:
            seq = self.db.next_seq(self.run_id)
            self.db.append_event(self.run_id, seq, kind, node_id, payload)
        self.engine.broadcast(self.run_id)

    def set_node(self, node_id, status, detail=None, attempt=None):
        self.db.upsert_node(self.run_id, node_id, status, detail, attempt)
        self.emit("node", node_id, {"status": status, "detail": detail})

    def set_run(self, **kw):
        self.db.update_run(self.run_id, **kw)
        self.emit("run", None, kw)

    # ---- control ----
    def pause(self):
        if self.cancel_flag:
            return False
        self.paused = True
        self.set_run(status="PAUSED", detail="paused by user")
        return True

    def resume(self):
        self.paused = False
        self._wake.set()
        self.set_run(status="RUNNING", detail="resumed")
        if self.wait_signal is not None:
            self.wait_signal.set()
        return True

    def cancel(self):
        self.cancel_flag = True
        self._wake.set()
        if self.wait_signal is not None:
            self.wait_signal.set()
        self.set_run(status="CANCELLING",
                    detail="cancellation requested; finishing current step")
        return True

    def send_signal(self, key, value):
        if self.wait_node_id is None:
            return False
        node = self.nodes[self.wait_node_id]
        if node.get("data", {}).get("signal") not in (None, "", key):
            return False
        if self.paused:
            return False
        self.engine.facts[key] = value
        self.emit("signal", self.wait_node_id, {key: value})
        if self.wait_signal is not None:
            self.wait_signal.set()
        return True

    def _gate(self):
        """Pause gate checked only between steps/nodes."""
        while self.paused and not self.cancel_flag:
            self._wake.wait(timeout=0.5)
            self._wake.clear()

    def _next_node(self, node_id, branch=None):
        for e in self.edges:
            if e["source"] != node_id:
                continue
            if branch is None or e.get("sourceHandle") in (None, branch, "out"):
                return e["target"]
        return None

    def _ctx(self):
        ctx = dict(self.engine.facts)
        ctx["robot"] = self.engine.robot.snapshot()
        return ctx

    # ---- main loop ----
    def run(self):
        entry = next((n["id"] for n in self.defn["nodes"]
                      if n.get("data", {}).get("kind") == "START"),
                     self.defn["nodes"][0]["id"])
        current = self._next_node(entry) or entry
        self.set_run(current_node_id=current, status="RUNNING")
        self.emit("robot", None, self.engine.robot.snapshot())
        try:
            while current:
                self._gate()
                if self.cancel_flag:
                    break
                node = self.nodes[current]
                data = node.get("data", {})
                kind = data.get("kind", "ACTION")
                self.db.update_run(self.run_id, current_node_id=current)

                if kind == "START":
                    current = self._next_node(current)
                    continue
                if kind in ("END", "SUCC_END", "FAIL_END"):
                    self.set_node(current, "SUCCEEDED", "end reached")
                    current = None
                    break

                if kind == "CONDITION":
                    self.set_node(current, "RUNNING", "evaluating condition")
                    try:
                        result = eval_condition(data.get("expr", ""),
                                                self._ctx())
                    except ConditionError as exc:
                        self.set_node(current, "FAILED", str(exc))
                        self.set_run(status="FAILED", detail=str(exc),
                                    ended_at=time.time())
                        return
                    branch = "true" if result else "false"
                    self.set_node(current, "SUCCEEDED",
                                  f"{data.get('expr')} -> {branch}")
                    self.emit("branch", current, {"branch": branch})
                    current = self._next_node(current, branch)
                    continue

                if kind == "WAIT":
                    current = self._run_wait(current, data)
                    continue

                outcome = self._run_action(current, data)
                if outcome == "CANCELLED":
                    break
                branch = "success" if outcome == "SUCCEEDED" else "failure"
                self.emit("branch", current, {"branch": branch})
                nxt = self._next_node(current, branch)
                if nxt is None:
                    if outcome == "FAILED":
                        self.set_run(status="FAILED",
                                    detail=f"node {current} failed with no "
                                           f"failure branch",
                                    ended_at=time.time())
                        return
                    current = None
                    break
                current = nxt

            if self.cancel_flag:
                self.set_run(status="CANCELLED",
                            detail="cancelled; no successor nodes were started",
                            ended_at=time.time())
            else:
                self.set_run(status="SUCCEEDED", detail="workflow complete",
                            ended_at=time.time())
        except Exception as exc:  # noqa: BLE001
            self.set_run(status="ERROR", detail=repr(exc), ended_at=time.time())
            raise
        finally:
            self.engine._run_finished(self.run_id)

    def _run_action(self, node_id, data):
        action = data.get("action", "WAIT")
        params = data.get("params", {}) or {}
        self.set_node(node_id, "RUNNING", f"{action} {params}", attempt=1)
        state = self.engine.robot.begin_action(node_id, action, params)
        while not state["done"]:
            self._gate()
            result = self.engine.robot.step(state)
            self.db.upsert_node(self.run_id, node_id, "RUNNING",
                                result["detail"])
            self.emit("node", node_id, {"status": "RUNNING",
                                        "detail": result["detail"],
                                        "progress": result["progress"]})
            self.emit("robot", None, result)
            if result["status"] in ("SUCCEEDED", "FAILED"):
                if self.cancel_flag:
                    # The running action was allowed to reach a clean
                    # stopping point; record its real outcome but do NOT
                    # advance to any successor.
                    self.set_node(node_id, "CANCELLED",
                                  f"action reached {result['status']}; "
                                  f"cancelled before next node")
                    return "CANCELLED"
                self.set_node(node_id, result["status"], result["detail"])
                return result["status"]
        return "SUCCEEDED"

    def _run_wait(self, node_id, data):
        signal_key = data.get("signal") or "signal"
        timeout = data.get("timeout")
        self.wait_signal = threading.Event()
        self.wait_node_id = node_id
        self.set_node(node_id, "WAITING",
                      f"waiting for signal '{signal_key}'")
        self.set_run(detail=f"waiting for {signal_key}")
        start = time.time()
        fired = False
        while not fired:
            self._gate()
            if self.cancel_flag:
                self.set_node(node_id, "CANCELLED", "wait aborted by cancel")
                nxt = None
                self.wait_node_id = None
                self.wait_signal = None
                return "__CANCEL__"
            if timeout and time.time() - start > float(timeout):
                self.set_node(node_id, "SUCCEEDED", "wait timed out")
                self.emit("branch", node_id, {"branch": "timeout"})
                nxt = self._next_node(node_id, "timeout") or \
                    self._next_node(node_id, "success")
                self.wait_node_id = None
                self.wait_signal = None
                return nxt
            fired = self.wait_signal.wait(timeout=0.5)
            self.wait_signal.clear()
            if fired and self.cancel_flag:
                fired = False
        self.set_node(node_id, "SUCCEEDED",
                      f"signal '{signal_key}' received")
        self.emit("branch", node_id, {"branch": "success"})
        nxt = self._next_node(node_id, "success")
        self.wait_node_id = None
        self.wait_signal = None
        return nxt


class Engine:
    def __init__(self, db):
        self.db = db
        self.robot = SimRobot()
        self.runs: dict[str, RunExecution] = {}
        self.facts = {}
        self._subscribers: dict[str, list[deque]] = {}
        self._sub_lock = threading.Lock()
        self._recover()

    def _recover(self):
        for row in self.db.active_runs():
            self.db.update_run(
                row["id"], status="PAUSED",
                detail="interrupted by server restart; resume to continue")

    # ---- SSE pub/sub ----
    def subscribe(self, run_id):
        q: deque = deque()
        with self._sub_lock:
            self._subscribers.setdefault(run_id, []).append(q)
        return q

    def unsubscribe(self, run_id, q):
        with self._sub_lock:
            subs = self._subscribers.get(run_id, [])
            if q in subs:
                subs.remove(q)

    def broadcast(self, run_id):
        with self._sub_lock:
            for q in self._subscribers.get(run_id, []):
                q.append(1)

    def _run_finished(self, run_id):
        self.runs.pop(run_id, None)

    # ---- API ----
    def start_run(self, workflow):
        run_id = uuid.uuid4().hex[:12]
        self.db.create_run(run_id, workflow["id"], workflow.get("name", ""),
                           workflow["definition"]["nodes"][0]["id"])
        execution = RunExecution(self, run_id, workflow["definition"])
        self.runs[run_id] = execution
        thread = threading.Thread(target=execution.run, daemon=True)
        execution.thread = thread
        thread.start()
        return run_id

    def pause_run(self, run_id):
        execution = self.runs.get(run_id)
        return execution.pause() if execution else False

    def resume_run(self, run_id):
        execution = self.runs.get(run_id)
        if not execution:
            return False
        return execution.resume()

    def cancel_run(self, run_id):
        execution = self.runs.get(run_id)
        return execution.cancel() if execution else False

    def send_signal(self, run_id, key, value):
        execution = self.runs.get(run_id)
        return execution.send_signal(key, value) if execution else False

    def resume_after_restart(self, run_id):
        """A run that was PAUSED by crash recovery cannot continue its old
        thread; mark it as resumable from its last active node by starting
        a fresh execution seeded at current_node_id."""
        row = self.db.get_run(run_id)
        if not row or row["status"] != "PAUSED" or run_id in self.runs:
            return False
        wf = self.db.get_workflow(row["workflow_id"])
        if not wf:
            return False
        execution = RunExecution(self, run_id, wf["definition"])
        node_id = row["current_node_id"] or wf["definition"]["nodes"][0]["id"]
        self.runs[run_id] = execution
        thread = threading.Thread(
            target=lambda: self._resume_at(execution, node_id), daemon=True)
        execution.thread = thread
        thread.start()
        return True

    def _resume_at(self, execution, node_id):
        execution.paused = False
        execution.set_run(status="RUNNING", detail="resumed after restart")
        execution.emit("robot", None, self.robot.snapshot())
        current = node_id
        try:
            while current:
                execution._gate()
                if execution.cancel_flag:
                    break
                node = execution.nodes[current]
                data = node.get("data", {})
                kind = data.get("kind", "ACTION")
                self.db.update_run(execution.run_id, current_node_id=current)
                if kind == "START":
                    current = execution._next_node(current)
                    continue
                if kind in ("END", "SUCC_END", "FAIL_END"):
                    execution.set_node(current, "SUCCEEDED", "end reached")
                    current = None
                    break
                if kind == "CONDITION":
                    execution.set_node(current, "RUNNING",
                                       "re-evaluating condition")
                    result = eval_condition(data.get("expr", ""),
                                            execution._ctx())
                    branch = "true" if result else "false"
                    execution.set_node(current, "SUCCEEDED",
                                       f"{data.get('expr')} -> {branch}")
                    execution.emit("branch", current, {"branch": branch})
                    current = execution._next_node(current, branch)
                    continue
                if kind == "WAIT":
                    current = execution._run_wait(current, data)
                    if current == "__CANCEL__":
                        current = None
                    continue
                outcome = execution._run_action(current, data)
                if outcome == "CANCELLED":
                    break
                branch = "success" if outcome == "SUCCEEDED" else "failure"
                execution.emit("branch", current, {"branch": branch})
                nxt = execution._next_node(current, branch)
                if nxt is None:
                    if outcome == "FAILED":
                        execution.set_run(status="FAILED",
                                          detail="failed with no failure branch",
                                          ended_at=time.time())
                        return
                    break
                current = nxt
            if execution.cancel_flag:
                execution.set_run(status="CANCELLED",
                                  detail="cancelled; no successors started",
                                  ended_at=time.time())
            else:
                execution.set_run(status="SUCCEEDED",
                                  detail="workflow complete",
                                  ended_at=time.time())
        except Exception as exc:  # noqa: BLE001
            execution.set_run(status="ERROR", detail=repr(exc),
                              ended_at=time.time())
        finally:
            self._run_finished(execution.run_id)
