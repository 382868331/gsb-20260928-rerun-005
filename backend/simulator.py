from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable


class ActionCancelled(Exception):
    """动作收到取消请求并已收尾。"""


class ActionFailed(Exception):
    pass


@dataclass
class RobotState:
    x: float = 0.0
    y: float = 0.0
    holding: str | None = None
    idle: bool = True

    def snapshot(self) -> dict:
        return {"x": round(self.x, 2), "y": round(self.y, 2),
                "holding": self.holding, "idle": self.idle}


ProgressCb = Callable[[dict], Awaitable[None]]


class VirtualRobot:
    """虚拟机器人：坐标 + 是否持物；动作分步推进，可被优雅取消。"""

    def __init__(self) -> None:
        self.state = RobotState()
        self.obstacle = False
        self._lock = asyncio.Lock()

    def set_obstacle(self, present: bool) -> None:
        self.obstacle = present

    def status(self) -> dict:
        return {**self.state.snapshot(), "obstacle": self.obstacle}

    async def _steps(self, n: int, delay: float, stop: asyncio.Event,
                     on_step: Callable[[int, int], object] | None = None,
                     gate: Callable[[], Awaitable[None]] | None = None) -> None:
        for i in range(1, n + 1):
            if gate is not None:
                await gate()
            if stop.is_set():
                raise ActionCancelled("收到取消请求，动作收尾")
            await asyncio.sleep(delay)
            if stop.is_set():
                raise ActionCancelled("收到取消请求，动作收尾")
            if on_step is not None:
                result = on_step(i, n)
                if asyncio.iscoroutine(result):
                    await result

    async def move(self, x: float, y: float, progress: ProgressCb,
                   stop: asyncio.Event,
                   gate: Callable[[], Awaitable[None]] | None = None) -> None:
        async with self._lock:
            self.state.idle = False
            tx, ty = float(x), float(y)
            sx, sy = self.state.x, self.state.y
            steps = max(2, int(abs(tx - sx) + abs(ty - sy)))

            def advance(i: int, total: int) -> None:
                self.state.x = sx + (tx - sx) * i / total
                self.state.y = sy + (ty - sy) * i / total

            async def report(i: int, total: int) -> None:
                advance(i, total)
                await progress({"step": i, "total": total, **self.state.snapshot()})

            try:
                await self._steps(steps, 0.25, stop, report, gate)
            except ActionCancelled:
                self.state.idle = True
                raise
            self.state.x, self.state.y = tx, ty
            self.state.idle = True

    async def pick(self, item: str, progress: ProgressCb,
                   stop: asyncio.Event,
                   gate: Callable[[], Awaitable[None]] | None = None) -> None:
        async with self._lock:
            self.state.idle = False
            try:
                async def report(i: int, total: int) -> None:
                    await progress({"step": i, "total": total,
                                    "phase": "夹取中" if i == 1 else "提起中"})

                await self._steps(2, 0.4, stop, report, gate)
                if stop.is_set():
                    raise ActionCancelled("收到取消请求，动作收尾")
                if self.obstacle:
                    raise ActionFailed("检测到障碍物，无法取物")
            except (ActionCancelled, ActionFailed):
                self.state.idle = True
                raise
            self.state.holding = item
            self.state.idle = True

    async def place(self, progress: ProgressCb, stop: asyncio.Event,
                    gate: Callable[[], Awaitable[None]] | None = None) -> None:
        async with self._lock:
            item = self.state.holding
            if item is None:
                self.state.idle = True
                raise ActionFailed("机械爪为空，无法放置")
            self.state.idle = False
            try:
                async def report(i: int, total: int) -> None:
                    await progress({"step": i, "total": total,
                                    "phase": "放下中" if i == 1 else "松开夹爪"})

                await self._steps(2, 0.4, stop, report, gate)
                if stop.is_set():
                    raise ActionCancelled("收到取消请求，动作收尾")
            except ActionCancelled:
                self.state.idle = True
                raise
            self.state.holding = None
            self.state.idle = True
