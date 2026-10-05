from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any


class Store:
    """SQLite 持久化：流程定义、执行状态、节点运行记录、事件日志。"""

    def __init__(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._init()

    def _init(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS flows(
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    definition TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS executions(
                    id TEXT PRIMARY KEY,
                    flow_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    termination_reason TEXT,
                    started_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS node_runs(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL,
                    detail TEXT,
                    started_at TEXT NOT NULL DEFAULT (datetime('now')),
                    finished_at TEXT
                );
                CREATE TABLE IF NOT EXISTS events(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    type TEXT NOT NULL,
                    node_id TEXT,
                    message TEXT,
                    data TEXT
                );
                CREATE TABLE IF NOT EXISTS node_followups(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    detail TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS recovery_states(
                    execution_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS evidence(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    node_id TEXT,
                    attempt INTEGER,
                    category TEXT NOT NULL,
                    source TEXT NOT NULL,
                    value TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    retracted INTEGER NOT NULL DEFAULT 0,
                    retract_reason TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS recovery_decisions(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    node_id TEXT,
                    attempt INTEGER,
                    decision TEXT NOT NULL,
                    evidence_ids TEXT,
                    summary TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                """
            )
            # 旧库升级：补充终止原因列
            cols = {r[1] for r in self._conn.execute(
                "PRAGMA table_info(executions)")}
            if "termination_reason" not in cols:
                self._conn.execute(
                    "ALTER TABLE executions ADD COLUMN termination_reason TEXT")

    def save_flow(self, flow_id: str, name: str, definition: dict[str, Any]) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO flows(id,name,definition) VALUES(?,?,?)",
                (flow_id, name, json.dumps(definition, ensure_ascii=False)),
            )

    def list_flows(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id,name,definition FROM flows ORDER BY created_at"
            ).fetchall()
        return [
            {"id": r["id"], "name": r["name"],
             "definition": json.loads(r["definition"])} for r in rows
        ]

    def get_flow(self, flow_id: str) -> dict[str, Any] | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT id,name,definition FROM flows WHERE id=?", (flow_id,)
            ).fetchone()
        if not r:
            return None
        return {"id": r["id"], "name": r["name"],
                "definition": json.loads(r["definition"])}

    def delete_flow(self, flow_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM flows WHERE id=?", (flow_id,))

    def create_execution(self, execution_id: str, flow_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO executions(id,flow_id,status) VALUES(?,?, 'running')",
                (execution_id, flow_id),
            )

    def set_execution_status(
        self, execution_id: str, status: str, reason: str | None = None,
    ) -> None:
        with self._lock, self._conn:
            if reason is None:
                self._conn.execute(
                    "UPDATE executions SET status=?, updated_at=datetime('now') WHERE id=?",
                    (status, execution_id),
                )
            else:
                self._conn.execute(
                    "UPDATE executions SET status=?, termination_reason=?, "
                    "updated_at=datetime('now') WHERE id=?",
                    (status, reason, execution_id),
                )

    def get_termination_reason(self, execution_id: str) -> str | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT termination_reason FROM executions WHERE id=?",
                (execution_id,),
            ).fetchone()
        return r[0] if r else None

    NODE_TERMINAL_STATUSES = {
        "succeeded", "failed", "cancelled", "aborted",
    }

    def node_run_status(self, execution_id: str, node_id: str, attempt: int) -> str | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT status FROM node_runs WHERE execution_id=? AND node_id=? AND attempt=?",
                (execution_id, node_id, attempt),
            ).fetchone()
        return r[0] if r else None

    def finish_node_run_guarded(
        self, execution_id: str, node_id: str, attempt: int, status: str,
        detail: str | None = None,
    ) -> bool:
        """终态保护：节点已处于终态（如已取消）时，迟到结果不得覆盖。

        返回 True 表示本次写入生效；False 表示记录已定格，调用方应改记后续信息。
        """
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT status FROM node_runs WHERE execution_id=? AND node_id=? AND attempt=?",
                (execution_id, node_id, attempt),
            ).fetchone()
            if row is not None and row[0] in self.NODE_TERMINAL_STATUSES:
                return False
            self._conn.execute(
                "UPDATE node_runs SET status=?, detail=?, finished_at=datetime('now') "
                "WHERE execution_id=? AND node_id=? AND attempt=?",
                (status, detail, execution_id, node_id, attempt),
            )
            return True

    def add_node_followup(
        self, execution_id: str, node_id: str, attempt: int, kind: str,
        outcome: str, detail: str | None = None,
    ) -> None:
        """记录迟到结果/收尾情况等可追溯的后续信息，不改变节点终态。"""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO node_followups(execution_id,node_id,attempt,kind,outcome,detail) "
                "VALUES(?,?,?,?,?,?)",
                (execution_id, node_id, attempt, kind, outcome, detail),
            )

    def get_node_followups(self, execution_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT node_id,attempt,kind,outcome,detail,created_at "
                "FROM node_followups WHERE execution_id=? ORDER BY id",
                (execution_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def start_node_run(self, execution_id: str, node_id: str, attempt: int = 1) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO node_runs(execution_id,node_id,attempt,status) "
                "VALUES(?,?,?,?)",
                (execution_id, node_id, attempt, "running"),
            )

    def finish_node_run(
        self, execution_id: str, node_id: str, attempt: int, status: str,
        detail: str | None = None,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE node_runs SET status=?, detail=?, finished_at=datetime('now') "
                "WHERE execution_id=? AND node_id=? AND attempt=?",
                (status, detail, execution_id, node_id, attempt),
            )

    def mark_node_run_waiting(
        self, execution_id: str, node_id: str, attempt: int, detail: str | None = None,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE node_runs SET status=?, detail=? "
                "WHERE execution_id=? AND node_id=? AND attempt=?",
                ("waiting", detail, execution_id, node_id, attempt),
            )

    def update_node_run(
        self, execution_id: str, node_id: str, attempt: int, status: str,
        detail: str | None = None,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE node_runs SET status=?, detail=? "
                "WHERE execution_id=? AND node_id=? AND attempt=?",
                (status, detail, execution_id, node_id, attempt),
            )

    def add_event(
        self,
        execution_id: str,
        event_type: str,
        node_id: str | None = None,
        message: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> int:
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(seq),0)+1 AS seq FROM events WHERE execution_id=?",
                (execution_id,),
            ).fetchone()
            seq = row["seq"]
            self._conn.execute(
                "INSERT INTO events(execution_id,seq,type,node_id,message,data) "
                "VALUES(?,?,?,?,?,?)",
                (execution_id, seq, event_type, node_id, message,
                 json.dumps(data, ensure_ascii=False) if data is not None else None),
            )
        return seq

    def get_events(self, execution_id: str, after_seq: int = 0) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq,type,node_id,message,data FROM events "
                "WHERE execution_id=? AND seq>? ORDER BY seq",
                (execution_id, after_seq),
            ).fetchall()
        return [
            {"seq": r["seq"], "type": r["type"], "node_id": r["node_id"],
             "message": r["message"],
             "data": json.loads(r["data"]) if r["data"] else None}
            for r in rows
        ]

    def get_execution(self, execution_id: str) -> dict[str, Any] | None:
        with self._lock:
            er = self._conn.execute(
                "SELECT * FROM executions WHERE id=?", (execution_id,)
            ).fetchone()
            if not er:
                return None
            nr = self._conn.execute(
                "SELECT node_id, attempt, status, detail, started_at, finished_at "
                "FROM node_runs WHERE execution_id=? ORDER BY id",
                (execution_id,),
            ).fetchall()
        return {
            "id": er["id"], "flow_id": er["flow_id"], "status": er["status"],
            "termination_reason": er["termination_reason"],
            "started_at": er["started_at"], "updated_at": er["updated_at"],
            "node_runs": [dict(r) for r in nr],
            "followups": self.get_node_followups(execution_id),
        }

    def list_executions(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id,flow_id,status,started_at,updated_at FROM executions "
                "ORDER BY started_at DESC LIMIT 50"
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_stale_running(self) -> None:
        """进程重启后，活动执行先停在人工待核实状态，而不是直接判中断。"""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE executions SET status='awaiting_recovery', "
                "termination_reason=COALESCE(termination_reason, '进程重启，设备状态待人工核实'), "
                "updated_at=datetime('now') "
                "WHERE status IN ('running','paused','waiting','cancelling')"
            )

    def save_recovery_state(self, execution_id: str, state: dict[str, Any]) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO recovery_states(execution_id,state) VALUES(?,?) "
                "ON CONFLICT(execution_id) DO UPDATE SET state=excluded.state, "
                "updated_at=datetime('now')",
                (execution_id, json.dumps(state, ensure_ascii=False)),
            )

    def get_recovery_state(self, execution_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT state FROM recovery_states WHERE execution_id=?",
                (execution_id,),
            ).fetchone()
        return json.loads(row["state"]) if row else None

    def list_pending_recovery_executions(self) -> list[dict[str, Any]]:
        """列出持久化层面仍待人工核实/处置的执行。

        重启恢复以该结果为唯一来源：执行状态一旦落库为
        awaiting_recovery/awaiting_manual，就必须能找回处置入口，
        即使 recovery_states 行因异常缺失（孤儿待核实状态）也要补建，
        避免页面显示待核实、证据入口却判定“不在待核实状态”。
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, flow_id, status FROM executions "
                "WHERE status IN ('awaiting_recovery','awaiting_manual')"
            ).fetchall()
        return [dict(r) for r in rows]

    def last_unconfirmed_node_run(
        self, execution_id: str,
    ) -> tuple[str, int, str, str | None] | None:
        """从 node_runs 重建待核实时的未确认动作（节点, 轮次, 状态, 详情）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT node_id, attempt, status, detail FROM node_runs "
                "WHERE execution_id=? ORDER BY id DESC LIMIT 1",
                (execution_id,),
            ).fetchone()
        if row is None:
            return None
        return row["node_id"], int(row["attempt"]), row["status"], row["detail"]

    def list_recovery_states(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT execution_id,state FROM recovery_states"
            ).fetchall()
        return [{"execution_id": r["execution_id"], **json.loads(r["state"])} for r in rows]

    def delete_recovery_state(self, execution_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "DELETE FROM recovery_states WHERE execution_id=?",
                (execution_id,),
            )

    def add_evidence(
        self, execution_id: str, node_id: str | None, attempt: int | None,
        category: str, source: str, value: dict[str, Any],
        observed_at: str | None = None,
    ) -> int:
        """登记一条核实证据（位置/持物/动作结果），只插入不覆盖。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO evidence(execution_id,node_id,attempt,category,source,value,observed_at) "
                "VALUES(?,?,?,?,?,?,COALESCE(?, datetime('now')))",
                (execution_id, node_id, attempt, category, source,
                 json.dumps(value, ensure_ascii=False), observed_at),
            )
        return int(cur.lastrowid)

    def list_evidence(self, execution_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM evidence WHERE execution_id=? ORDER BY id",
                (execution_id,),
            ).fetchall()
        result = []
        for r in rows:
            item = dict(r)
            item["value"] = json.loads(item["value"])
            item["retracted"] = bool(item["retracted"])
            result.append(item)
        return result

    def retract_evidence(self, evidence_id: int, reason: str) -> bool:
        """撤回一条错误观察：保留原记录，仅标记失效并记录原因。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE evidence SET retracted=1, retract_reason=? "
                "WHERE id=? AND retracted=0",
                (reason, evidence_id),
            )
        return cur.rowcount > 0

    def add_recovery_decision(
        self, execution_id: str, node_id: str | None, attempt: int | None,
        decision: str, evidence_ids: list[int], summary: str,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO recovery_decisions(execution_id,node_id,attempt,decision,evidence_ids,summary) "
                "VALUES(?,?,?,?,?,?)",
                (execution_id, node_id, attempt, decision,
                 json.dumps(evidence_ids), summary),
            )

    def list_recovery_decisions(self, execution_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM recovery_decisions WHERE execution_id=? ORDER BY id",
                (execution_id,),
            ).fetchall()
        result = []
        for r in rows:
            item = dict(r)
            item["evidence_ids"] = json.loads(item["evidence_ids"] or "[]")
            result.append(item)
        return result
