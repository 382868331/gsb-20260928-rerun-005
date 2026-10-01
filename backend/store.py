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

                CREATE TABLE IF NOT EXISTS device_snapshots(
                    execution_id TEXT PRIMARY KEY,
                    x REAL NOT NULL,
                    y REAL NOT NULL,
                    holding TEXT,
                    idle INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS pending_actions(
                    execution_id TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    action_type TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    params TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS recovery_decisions(
                    execution_id TEXT PRIMARY KEY,
                    decision TEXT NOT NULL,
                    reason TEXT,
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

    # ---- 恢复相关：设备快照 / 未确认动作 / 人工核实决定 ----

    def save_device_snapshot(
        self, execution_id: str, x: float, y: float, holding: str | None,
        idle: bool = True,
    ) -> None:
        """记录最后已确认的设备状态（动作完成点），用于重启后恢复。"""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO device_snapshots(execution_id,x,y,holding,idle) "
                "VALUES(?,?,?,?,?) "
                "ON CONFLICT(execution_id) DO UPDATE SET "
                "x=excluded.x,y=excluded.y,holding=excluded.holding,idle=excluded.idle,"
                "updated_at=datetime('now')",
                (execution_id, x, y, holding, 1 if idle else 0),
            )

    def get_device_snapshot(self, execution_id: str) -> dict[str, Any] | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT x,y,holding,idle FROM device_snapshots WHERE execution_id=?",
                (execution_id,),
            ).fetchone()
        if not r:
            return None
        return {"x": r["x"], "y": r["y"], "holding": r["holding"],
                "idle": bool(r["idle"])}

    def set_pending_action(
        self, execution_id: str, node_id: str, attempt: int, action_type: str,
        phase: str, params: dict[str, Any] | None = None,
    ) -> None:
        """记录启动后尚未确认结果的动作（崩溃点）。"""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO pending_actions(execution_id,node_id,attempt,action_type,phase,params) "
                "VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(execution_id) DO UPDATE SET node_id=excluded.node_id,"
                "attempt=excluded.attempt,action_type=excluded.action_type,"
                "phase=excluded.phase,params=excluded.params,created_at=datetime('now')",
                (execution_id, node_id, attempt, action_type, phase,
                 json.dumps(params, ensure_ascii=False) if params is not None else None),
            )

    def clear_pending_action(self, execution_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "DELETE FROM pending_actions WHERE execution_id=?", (execution_id,))

    def get_pending_action(self, execution_id: str) -> dict[str, Any] | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT node_id,attempt,action_type,phase,params FROM pending_actions "
                "WHERE execution_id=?",
                (execution_id,),
            ).fetchone()
        if not r:
            return None
        return {"node_id": r["node_id"], "attempt": r["attempt"],
                "action_type": r["action_type"], "phase": r["phase"],
                "params": json.loads(r["params"]) if r["params"] else None}

    def save_recovery_decision(
        self, execution_id: str, decision: str, reason: str | None = None,
    ) -> bool:
        """保存恢复前的人工核实决定。已保存时返回 False（连续重开不重复要求）。"""
        with self._lock, self._conn:
            exists = self._conn.execute(
                "SELECT 1 FROM recovery_decisions WHERE execution_id=?",
                (execution_id,),
            ).fetchone()
            if exists:
                return False
            self._conn.execute(
                "INSERT INTO recovery_decisions(execution_id,decision,reason) VALUES(?,?,?)",
                (execution_id, decision, reason),
            )
            return True

    def get_recovery_decision(self, execution_id: str) -> dict[str, Any] | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT decision,reason FROM recovery_decisions WHERE execution_id=?",
                (execution_id,),
            ).fetchone()
        return dict(r) if r else None

    def list_status(self, statuses: list[str]) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT id FROM executions WHERE status IN ({','.join('?' * len(statuses))})",
                statuses,
            ).fetchall()
        return [r["id"] for r in rows]

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
            "device_snapshot": self.get_device_snapshot(execution_id),
            "pending_action": self.get_pending_action(execution_id),
            "recovery_decision": self.get_recovery_decision(execution_id),
        }

    def list_executions(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id,flow_id,status,started_at,updated_at FROM executions "
                "ORDER BY started_at DESC LIMIT 50"
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_stale_running(self) -> None:
        """进程重启后，遗留的未终止执行停在「待核实」阶段，等待人工处置。

        不再直接标记为 interrupted：执行顺序、已确定的终止决定都要保留，
        由操作者核实设备状态后选择继续收尾或结束流程。
        """
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE executions SET status='awaiting_verification' "
                "WHERE status IN ('running','paused','waiting','cancelling','awaiting_manual')"
            )
