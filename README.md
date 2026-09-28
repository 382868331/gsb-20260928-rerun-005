# 机器人任务编排平台（首轮：虚拟机器人）

把几个动作（移动 / 取物 / 放置 / 等待条件 / 播报）接成流程，实时查看每个节点的执行、等待、失败与失败后走向；支持暂停/恢复（接回原状态）和优雅取消（当前动作收尾后停止，不继续后续动作）。状态全部持久化在服务端 SQLite，刷新页面、重启后端都不丢。

## 架构

- `backend/main.py` — FastAPI：流程/执行 REST API + `/ws/events` 实时事件推送
- `backend/engine.py` — 异步编排引擎：节点状态机、success/failure 分支、等待条件、暂停恢复、优雅取消
- `backend/simulator.py` — 虚拟机器人：坐标/持物/空闲状态，动作分步推进，可注入障碍物、可收尾取消
- `backend/store.py` — SQLite 持久化：flows / executions / node_runs / events；重启后遗留运行态标记为 interrupted
- `frontend/` — React 19 + @xyflow/react 流程画布，节点颜色表示 运行/等待/失败/取消，失败边红色虚线并高亮实际走向
- `tests/test_engine.py` — 等待+失败分支、取物遇障碍后取消、暂停恢复三个场景

## 运行

```bash
# 后端
.venv/bin/python -m uvicorn backend.main:app --port 8000

# 前端（另开终端，已配置 /api、/ws 代理到 8000）
cd frontend && npx vite
```

打开 http://localhost:5173 ，内置「取物任务」演示流程：
移动到货架 → 等待绿灯 → 取物 → 送达放置；取物失败走红色虚线分支（绕行播报 → 等待人工排障 → 重试）。

## 验收操作路径

1. 点「运行」，流程走到「等待货架绿灯」时节点黄色脉动，顶部出现「满足条件」按钮。
2. 打开「障碍物：开」，再点满足条件：取物失败，节点变红，红色失败边高亮，进入「等待人工排障」。
3. 另开一次运行，取物途中打开障碍物并点「取消」：当前取物动作收尾，执行记录为 cancelled，后续「放置」从未启动；右侧机器人实际状态（空闲、未持物）与执行记录一致。
4. 运行中点「暂停」：机器人坐标冻结，恢复后从原地继续走到目标点。

## 测试

```bash
.venv/bin/python -m pytest tests/ -q
```

## 真实设备接入（后续）

执行器目前只有 `VirtualRobot`。后续新增实现 `move/pick/place`（同样支持分步进度、收尾取消）的驱动类，在 `main.py` 替换注入即可，编排引擎、持久化与前端无需改动。
