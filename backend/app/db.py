import json
import os
import sqlite3
import threading
import time


def _now():
    return time.time()


SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    definition TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    workflow_name TEXT NOT NULL,
    status TEXT NOT NULL,
    current_node_id TEXT,
    detail TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    ended_at REAL
);
CREATE TABLE IF NOT EXISTS run_nodes (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    detail TEXT,
    started_at REAL,
    ended_at REAL,
    PRIMARY KEY (run_id, node_id)
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    node_id TEXT,
    payload TEXT,
    created_at REAL NOT NULL
);
"""


class DB:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def query(self, sql, args=()):
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, args).fetchall()]

    def one(self, sql, args=()):
        rows = self.query(sql, args)
        return rows[0] if rows else None

    def execute(self, sql, args=()):
        with self._lock:
            cur = self._conn.execute(sql, args)
            self._conn.commit()
            return cur.lastrowid

    # ---- workflows ----
    def upsert_workflow(self, wf_id, name, definition):
        d = json.dumps(definition, ensure_ascii=False)
        existing = self.one("SELECT id FROM workflows WHERE id=?", (wf_id,))
        if existing:
            self.execute(
                "UPDATE workflows SET name=?, definition=?, updated_at=? WHERE id=?",
                (name, d, _now(), wf_id),
            )
        else:
            self.execute(
                "INSERT INTO workflows(id,name,definition,created_at,updated_at) "
                "VALUES(?,?,?,?,?)",
                (wf_id, name, d, _now(), _now()),
            )

    def get_workflow(self, wf_id):
        row = self.one("SELECT * FROM workflows WHERE id=?", (wf_id,))
        if row:
            row["definition"] = json.loads(row["definition"])
        return row

    def list_workflows(self):
        rows = self.query("SELECT * FROM workflows ORDER BY updated_at DESC")
        for r in rows:
            r["definition"] = json.loads(r["definition"])
        return rows

    # ---- runs ----
    def create_run(self, run_id, wf_id, wf_name, start_node):
        now = _now()
        self.execute(
            "INSERT INTO runs(id,workflow_id,workflow_name,status,current_node_id,"
            "detail,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (run_id, wf_id, wf_name, "RUNNING", start_node, None, now, now),
        )

    def update_run(self, run_id, **fields):
        if not fields:
            return
        fields["updated_at"] = _now()
        cols = ",".join(f"{k}=?" for k in fields)
        args = list(fields.values()) + [run_id]
        self.execute(f"UPDATE runs SET {cols} WHERE id=?", args)

    def get_run(self, run_id):
        return self.one("SELECT * FROM runs WHERE id=?", (run_id,))

    def list_runs(self, limit=50):
        return self.query(
            "SELECT * FROM runs ORDER BY created_at DESC LIMIT ?", (limit,)
        )

    def active_runs(self):
        return self.query(
            "SELECT * FROM runs WHERE status IN "
            "('RUNNING','PAUSED','CANCELLING')"
        )

    # ---- node records ----
    def upsert_node(self, run_id, node_id, status, detail=None, attempt=None):
        row = self.one(
            "SELECT * FROM run_nodes WHERE run_id=? AND node_id=?",
            (run_id, node_id),
        )
        now = _now()
        if row is None:
            self.execute(
                "INSERT INTO run_nodes(run_id,node_id,status,detail,attempt,"
                "started_at,ended_at) VALUES(?,?,?,?,?,?,?)",
                (run_id, node_id, status, detail, attempt or 1, now,
                 now if status in ("SUCCEEDED", "FAILED", "CANCELLED", "SKIPPED")
                 else None),
            )
        else:
            new_attempt = attempt if attempt is not None else row["attempt"]
            terminal = status in ("SUCCEEDED", "FAILED", "CANCELLED", "SKIPPED")
            self.execute(
                "UPDATE run_nodes SET status=?, detail=?, attempt=?, "
                "started_at=COALESCE(started_at,?), ended_at=? "
                "WHERE run_id=? AND node_id=?",
                (status, detail, new_attempt, now,
                 now if terminal else row["ended_at"], run_id, node_id),
            )

    def get_nodes(self, run_id):
        return self.query(
            "SELECT * FROM run_nodes WHERE run_id=? ORDER BY started_at", (run_id,)
        )

    # ---- events ----
    def append_event(self, run_id, seq, kind, node_id, payload):
        self.execute(
            "INSERT INTO events(run_id,seq,kind,node_id,payload,created_at) "
            "VALUES(?,?,?,?,?,?)",
            (run_id, seq, kind, node_id,
             json.dumps(payload or {}, ensure_ascii=False), _now()),
        )

    def events_after(self, run_id, after_seq=0):
        rows = self.query(
            "SELECT * FROM events WHERE run_id=? AND seq>? ORDER BY seq",
            (run_id, after_seq),
        )
        for r in rows:
            r["payload"] = json.loads(r["payload"] or "{}")
        return rows

    def next_seq(self, run_id):
        row = self.one(
            "SELECT COALESCE(MAX(seq),0)+1 AS s FROM events WHERE run_id=?",
            (run_id,),
        )
        return row["s"]
