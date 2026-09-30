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

    def __init__(self, safe_x: float = 0.0, safe_y: float = 0.0) -> None:
        self.state = RobotState()
        self.obstacle = False
        self.safe_x = float(safe_x)
        self.safe_y = float(safe_y)
        self._lock = asyncio.Lock()
        # 测试开关：动作在收到取消后不立刻收尾，而是继续跑到完成后再回报，
        # 模拟真实设备“取消已发出、但尚未返回的移动稍后报告成功”的迟到结果场景。
        self.late_finish = False

    def set_obstacle(self, present: bool) -> None:
        self.obstacle = present

    def set_late_finish(self, enabled: bool) -> None:
        self.late_finish = enabled

    def status(self) -> dict:
        return {**self.state.snapshot(), "obstacle": self.obstacle}

    async def _steps(self, n: int, delay: float, stop: asyncio.Event,
                     on_step: Callable[[int, int], object] | None = None,
                     gate: Callable[[], Awaitable[None]] | None = None,
                     finish_on_stop: bool = False) -> None:
        """推进 n 个步骤。

        finish_on_stop=False（业务动作默认）：收到取消立即收尾，抛 ActionCancelled。
        finish_on_stop=True（收尾动作）：允许把当前正在执行的动作做完，
            但取消标志会阻止再启动后续步骤；取消再次生效时立即收尾。
        """
        stopped_before_action = stop.is_set()
        for i in range(1, n + 1):
            if gate is not None:
                await gate()
            if stop.is_set():
                if not finish_on_stop or (stopped_before_action and i > 1):
                    raise ActionCancelled("收到取消请求，动作收尾")
            await asyncio.sleep(delay)
            if stop.is_set() and not (finish_on_stop and self.late_finish):
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
                await self._steps(steps, 0.25, stop, report, gate,
                                  finish_on_stop=self.late_finish)
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
            except ActionCancelled:
                self.state.idle = True
                raise
            self.state.holding = None
            self.state.idle = True

    async def release_if_holding(self, progress: ProgressCb, stop: asyncio.Event,
                                 gate: Callable[[], Awaitable[None]] | None = None) -> bool:
        """收尾动作：若机械爪持有物品则就地放下。始终允许被调用。"""
        async with self._lock:
            item = self.state.holding
            if item is None:
                return False
            self.state.idle = False
            try:
                async def report(i: int, total: int) -> None:
                    await progress({"step": i, "total": total,
                                    "phase": f"收尾：就地放下 {item}"})

                await self._steps(1, 0.3, stop, report, gate,
                                  finish_on_stop=True)
            except ActionCancelled:
                self.state.idle = True
                raise
            self.state.holding = None
            self.state.idle = True
            return True

    async def return_to_safe(self, progress: ProgressCb, stop: asyncio.Event,
                             gate: Callable[[], Awaitable[None]] | None = None) -> tuple[float, float]:
        """收尾动作：返回安全位置。取消时允许把当前这一步走完，不再启动新步骤。"""
        async with self._lock:
            tx, ty = self.safe_x, self.safe_y
            sx, sy = self.state.x, self.state.y
            steps = max(2, int(abs(tx - sx) + abs(ty - sy)))
            self.state.idle = False

            def advance(i: int, total: int) -> None:
                self.state.x = sx + (tx - sx) * i / total
                self.state.y = sy + (ty - sy) * i / total

            async def report(i: int, total: int) -> None:
                advance(i, total)
                await progress({"step": i, "total": total,
                                "phase": "收尾：返回安全位置",
                                **self.state.snapshot()})

            try:
                await self._steps(steps, 0.25, stop, report, gate,
                                  finish_on_stop=True)
            except ActionCancelled:
                self.state.idle = True
                raise
            self.state.x, self.state.y = tx, ty
            self.state.idle = True
            return tx, ty
