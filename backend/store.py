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
                CREATE TABLE IF NOT EXISTS checkpoints(
                    execution_id TEXT PRIMARY KEY,
                    node_id TEXT,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    phase TEXT NOT NULL,
                    detail TEXT
                );
                """
            )
            self._migrate()

    def _migrate(self) -> None:
        """给旧库补齐首轮后新增的列。"""
        def columns(table: str) -> set[str]:
            return {r["name"] for r in self._conn.execute(
                f"PRAGMA table_info({table})").fetchall()}

        node_cols = columns("node_runs")
        for col, decl in (
            ("cleanup_status", "TEXT"),
            ("cleanup_detail", "TEXT"),
            ("late_outcome", "TEXT"),
            ("late_detail", "TEXT"),
        ):
            if col not in node_cols:
                self._conn.execute(
                    f"ALTER TABLE node_runs ADD COLUMN {col} {decl}")
        if "termination_reason" not in columns("executions"):
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

    def set_execution_status(self, execution_id: str, status: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE executions SET status=?, updated_at=datetime('now') WHERE id=?",
                (status, execution_id),
            )

    def set_execution_terminal(
        self, execution_id: str, status: str, reason: str | None = None,
    ) -> None:
        """写入终态与终止说明；终态一旦确定不再被迟到结果覆盖。"""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE executions SET status=?, termination_reason=?, "
                "updated_at=datetime('now') WHERE id=?",
                (status, reason, execution_id),
            )

    def save_checkpoint(
        self, execution_id: str, node_id: str | None, attempt: int,
        phase: str, detail: str | None = None,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO checkpoints(execution_id,node_id,attempt,phase,detail) "
                "VALUES(?,?,?,?,?)",
                (execution_id, node_id, attempt, phase, detail),
            )

    def clear_checkpoint(self, execution_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "DELETE FROM checkpoints WHERE execution_id=?", (execution_id,))

    def get_checkpoint(self, execution_id: str) -> dict[str, Any] | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT node_id,attempt,phase,detail FROM checkpoints "
                "WHERE execution_id=?", (execution_id,),
            ).fetchone()
        return dict(r) if r else None

    def open_node_runs(self, execution_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT node_id,attempt,status,detail FROM node_runs "
                "WHERE execution_id=? AND finished_at IS NULL ORDER BY id DESC",
                (execution_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def finish_open_node_runs(
        self, execution_id: str, status: str, detail: str | None = None,
    ) -> None:
        """重启恢复时，把没有收尾的节点运行记录统一终止。"""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE node_runs SET status=?, detail=COALESCE(detail,?), "
                "finished_at=datetime('now') WHERE execution_id=? AND finished_at IS NULL",
                (status, detail, execution_id),
            )

    def set_node_cleanup(
        self, execution_id: str, node_id: str, attempt: int,
        status: str, detail: str | None = None,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE node_runs SET cleanup_status=?, cleanup_detail=? "
                "WHERE execution_id=? AND node_id=? AND attempt=?",
                (status, detail, execution_id, node_id, attempt),
            )

    def record_late_action_result(
        self, execution_id: str, node_id: str, attempt: int,
        outcome: str, detail: str | None = None,
    ) -> None:
        """迟到的动作结果只作为可追溯的后续信息，不改变节点终态。"""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE node_runs SET late_outcome=?, late_detail=? "
                "WHERE execution_id=? AND node_id=? AND attempt=?",
                (outcome, detail, execution_id, node_id, attempt),
            )

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
                "SELECT node_id, attempt, status, detail, cleanup_status, "
                "cleanup_detail, late_outcome, late_detail, started_at, finished_at "
                "FROM node_runs WHERE execution_id=? ORDER BY id",
                (execution_id,),
            ).fetchall()
        return {
            "id": er["id"], "flow_id": er["flow_id"], "status": er["status"],
            "termination_reason": er["termination_reason"],
            "started_at": er["started_at"], "updated_at": er["updated_at"],
            "node_runs": [dict(r) for r in nr],
        }

    def list_executions(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id,flow_id,status,started_at,updated_at FROM executions "
                "ORDER BY started_at DESC LIMIT 50"
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_stale_running(self) -> list[str]:
        """进程重启后，把遗留的运行/等待中执行标记为 interrupted。"""
        with self._lock, self._conn:
            rows = self._conn.execute(
                "SELECT id FROM executions "
                "WHERE status IN ('running','paused','waiting','cancelling','awaiting_manual')"
            ).fetchall()
            ids = [r["id"] for r in rows]
            self._conn.execute(
                "UPDATE executions SET status='interrupted' "
                "WHERE status IN ('running','paused','waiting','cancelling','awaiting_manual')"
            )
        return ids
