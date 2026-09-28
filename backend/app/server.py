import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .db import DB
from .engine import Engine
from .default_flow import DEFAULT_FLOW

HERE = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIST = os.path.normpath(os.path.join(HERE, "..", "..", "frontend",
                                               "dist"))


class App:
    def __init__(self, db_path):
        self.db = DB(db_path)
        self.engine = Engine(self.db)
        if not self.db.get_workflow(DEFAULT_FLOW["id"]):
            self.db.upsert_workflow(DEFAULT_FLOW["id"], DEFAULT_FLOW["name"],
                                    DEFAULT_FLOW["definition"])


def build_handler(app: App):
    engine, db = app.engine, app.db

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def _json(self, obj, status=200):
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self):
            length = int(self.headers.get("Content-Length", 0))
            if not length:
                return {}
            return json.loads(self.rfile.read(length).decode("utf-8"))

        def _run_payload(self, run_id):
            run = db.get_run(run_id)
            if not run:
                return None
            return {
                "run": run,
                "nodes": db.get_nodes(run_id),
                "events": db.events_after(run_id, 0),
                "robot": engine.robot.snapshot(),
                "live": run_id in engine.runs,
            }

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)
            try:
                if path == "/api/health":
                    return self._json({"ok": True})
                if path == "/api/workflows":
                    return self._json({"workflows": db.list_workflows()})
                if path.startswith("/api/workflows/"):
                    wf_id = path.rsplit("/", 1)[-1]
                    wf = db.get_workflow(wf_id)
                    return self._json(wf or {"error": "not found"},
                                      200 if wf else 404)
                if path == "/api/runs":
                    return self._json({"runs": db.list_runs()})
                if path.startswith("/api/runs/"):
                    parts = path.strip("/").split("/")
                    run_id = parts[2]
                    if len(parts) == 4 and parts[3] == "events":
                        return self._sse(run_id,
                                         int(qs.get("after", [0])[0]))
                    payload = self._run_payload(run_id)
                    return self._json(payload or {"error": "not found"},
                                      200 if payload else 404)
                if path == "/api/robot":
                    return self._json(engine.robot.snapshot())
                return self._static(path)
            except Exception as exc:  # noqa: BLE001
                return self._json({"error": repr(exc)}, 500)

        def do_POST(self):
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                body = self._read_json()
                if path == "/api/workflows":
                    wf_id = body.get("id") or "wf-"
                    db.upsert_workflow(wf_id, body.get("name", "未命名"),
                                       body["definition"])
                    return self._json({"id": wf_id, "saved": True})
                if path.startswith("/api/runs/"):
                    parts = path.strip("/").split("/")
                    run_id = parts[2]
                    action = parts[3] if len(parts) > 3 else ""
                    if action == "pause":
                        ok = engine.pause_run(run_id)
                    elif action == "resume":
                        ok = (engine.resume_run(run_id)
                              or engine.resume_after_restart(run_id))
                    elif action == "cancel":
                        ok = engine.cancel_run(run_id)
                    elif action == "signal":
                        ok = engine.send_signal(run_id,
                                                body.get("key", "signal"),
                                                body.get("value", True))
                    else:
                        return self._json({"error": "unknown action"}, 400)
                    return self._json({"ok": ok})
                if path == "/api/runs":
                    wf = db.get_workflow(body["workflow_id"])
                    if not wf:
                        return self._json({"error": "workflow not found"}, 404)
                    run_id = engine.start_run(wf)
                    return self._json({"run_id": run_id})
                if path == "/api/robot/obstacle":
                    engine.robot.add_obstacle(int(body["x"]), int(body["y"]))
                    return self._json(engine.robot.snapshot())
                if path == "/api/robot/clear-obstacle":
                    engine.robot.clear_obstacle(int(body["x"]), int(body["y"]))
                    return self._json(engine.robot.snapshot())
                if path == "/api/robot/reset":
                    engine.robot.reset()
                    return self._json(engine.robot.snapshot())
                return self._json({"error": "not found"}, 404)
            except Exception as exc:  # noqa: BLE001
                return self._json({"error": repr(exc)}, 500)

        def _sse(self, run_id, after_seq):
            if not db.get_run(run_id):
                return self._json({"error": "not found"}, 404)
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            q = engine.subscribe(run_id)
            last = after_seq
            try:
                while True:
                    events = db.events_after(run_id, last)
                    if events:
                        last = events[-1]["seq"]
                        for ev in events:
                            data = json.dumps(
                                {"seq": ev["seq"], "kind": ev["kind"],
                                 "node_id": ev["node_id"],
                                 "payload": ev["payload"]},
                                ensure_ascii=False)
                            self.wfile.write(f"data: {data}\n\n".encode())
                        self.wfile.flush()
                    else:
                        notified = q.__len__() > 0
                        q.clear()
                        if not notified:
                            self.wfile.write(b": ping\n\n")
                            self.wfile.flush()
                            threading.Event().wait(0.5)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                engine.unsubscribe(run_id, q)

        def _static(self, path):
            if not os.path.isdir(FRONTEND_DIST):
                return self._json({"error": "frontend not built"}, 404)
            rel = path.lstrip("/") or "index.html"
            full = os.path.normpath(os.path.join(FRONTEND_DIST, rel))
            if not full.startswith(FRONTEND_DIST) or not os.path.isfile(full):
                full = os.path.join(FRONTEND_DIST, "index.html")
            ctype = {".html": "text/html", ".js": "text/javascript",
                     ".css": "text/css", ".json": "application/json",
                     ".svg": "image/svg+xml"}.get(
                os.path.splitext(full)[1], "application/octet-stream")
            with open(full, "rb") as fh:
                body = fh.read()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def serve(db_path, port=8000):
    app = App(db_path)
    host = os.environ.get("ORCH_HOST", "127.0.0.1")
    server = ThreadingHTTPServer((host, port), build_handler(app))
    print(f"orchestrator on http://{host}:{port}")
    server.serve_forever()
