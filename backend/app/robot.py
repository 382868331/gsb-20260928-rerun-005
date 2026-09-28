import threading
import time


def manhattan(a, b):
    return abs(a[0] - b[0]) + abs(a[1] - b[1])


class SimRobot:
    """Stateful simulated robot. Action steps are advanced one tick at a
    time so the engine never has to interrupt a running action: on cancel
    the current tick finishes (a clean stopping point) and no successor
    node is ever started."""

    def __init__(self):
        self.lock = threading.RLock()
        self.pos = [0, 0]
        self.carrying = False
        self.battery = 100
        self.obstacles = set()
        self.step_seconds = 0.25

    def snapshot(self):
        with self.lock:
            return {
                "pos": list(self.pos),
                "carrying": self.carrying,
                "battery": self.battery,
                "obstacles": [list(o) for o in sorted(self.obstacles)],
            }

    def reset(self):
        with self.lock:
            self.pos = [0, 0]
            self.carrying = False
            self.battery = 100
            self.obstacles.clear()

    def add_obstacle(self, x, y):
        with self.lock:
            self.obstacles.add((int(x), int(y)))

    def clear_obstacle(self, x, y):
        with self.lock:
            self.obstacles.discard((int(x), int(y)))

    def begin_action(self, node_id, action, params):
        state = {"id": action, "node_id": node_id, "params": params,
                 "done": False, "detail": "", "progress": 0.0}
        if action == "MOVE":
            target = [int(params.get("x", 0)), int(params.get("y", 0))]
            state["from"] = list(self.pos)
            state["target"] = target
            state["total"] = max(1, manhattan(self.pos, target))
        elif action == "WAIT":
            state["remaining"] = float(params.get("seconds", 1))
        elif action == "PICK":
            state["remaining"] = float(params.get("seconds", 1))
        elif action == "PLACE":
            state["remaining"] = float(params.get("seconds", 1))
        elif action == "CHARGE":
            state["remaining"] = float(params.get("seconds", 1))
        else:
            state["done"] = True
            state["detail"] = f"unknown action {action}"
        return state

    def _finish_ok(self, state, detail):
        state["done"] = True
        state["progress"] = 1.0
        state["detail"] = detail
        return {"status": "SUCCEEDED", "detail": detail,
                "progress": 1.0, **self.snapshot()}

    def _fail(self, state, detail):
        state["done"] = True
        state["detail"] = detail
        return {"status": "FAILED", "detail": detail,
                "progress": state["progress"], **self.snapshot()}

    def _running(self, state, detail, progress):
        state["progress"] = progress
        return {"status": "RUNNING", "detail": detail,
                "progress": round(progress, 3), **self.snapshot()}

    def step(self, state):
        """Advance exactly one tick. Returns an outcome dict."""
        if state["done"]:
            return {"status": "SUCCEEDED", "detail": state["detail"],
                    "progress": state["progress"], **self.snapshot()}
        with self.lock:
            kind = state["id"]
            if kind == "MOVE":
                tx, ty = state["target"]
                if tuple(self.pos) in self.obstacles:
                    return self._fail(
                        state,
                        f"blocked by obstacle at ({self.pos[0]},{self.pos[1]})",
                    )
                if self.pos[0] != tx:
                    self.pos[0] += 1 if tx > self.pos[0] else -1
                elif self.pos[1] != ty:
                    self.pos[1] += 1 if ty > self.pos[1] else -1
                time.sleep(self.step_seconds)
                if tuple(self.pos) in self.obstacles:
                    return self._fail(
                        state,
                        f"hit obstacle at ({self.pos[0]},{self.pos[1]})",
                    )
                done_steps = manhattan(state["from"], self.pos)
                if self.pos == [tx, ty]:
                    return self._finish_ok(
                        state, f"arrived at ({tx},{ty}) in {done_steps} steps")
                return self._running(
                    state, f"moving via ({self.pos[0]},{self.pos[1]})",
                    done_steps / state["total"])

            if kind == "PICK":
                if self.carrying:
                    return self._fail(state, "gripper already holding an item")
                state["remaining"] -= self.step_seconds
                time.sleep(self.step_seconds)
                if state["remaining"] <= 0:
                    self.carrying = True
                    return self._finish_ok(state, "picked up item")
                return self._running(state, "gripping item",
                                     1 - state["remaining"] /
                                     max(0.001, float(state["params"]
                                                      .get("seconds", 1))))

            if kind == "PLACE":
                if not self.carrying:
                    return self._fail(state, "no item in gripper")
                state["remaining"] -= self.step_seconds
                time.sleep(self.step_seconds)
                if state["remaining"] <= 0:
                    self.carrying = False
                    return self._finish_ok(state, "placed item")
                return self._running(state, "placing item",
                                     1 - state["remaining"] /
                                     max(0.001, float(state["params"]
                                                      .get("seconds", 1))))

            if kind == "CHARGE":
                state["remaining"] -= self.step_seconds
                time.sleep(self.step_seconds)
                self.battery = min(100, self.battery + 20)
                if state["remaining"] <= 0:
                    return self._finish_ok(state, f"battery at {self.battery}%")
                return self._running(state, f"charging ({self.battery}%)",
                                     1 - state["remaining"] /
                                     max(0.001, float(state["params"]
                                                      .get("seconds", 1))))

            if kind == "WAIT":
                state["remaining"] -= self.step_seconds
                time.sleep(self.step_seconds)
                if state["remaining"] <= 0:
                    return self._finish_ok(state, "wait elapsed")
                return self._running(state, "holding position",
                                     1 - state["remaining"] /
                                     max(0.001, float(state["params"]
                                                      .get("seconds", 1))))

        return self._fail(state, f"unsupported action {kind}")
