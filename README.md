# 机器人任务编排平台（首轮：虚拟机器人）

Python（仅标准库）负责任务执行与状态持久化，React + React Flow 负责流程编排
与实时进度。执行状态全部落在服务端 SQLite，刷新页面、重开历史执行都不丢。

## 能力

- **分支**：动作节点有 `success` / `failure` 两条出边；条件节点支持
  `== != > >= < <=` 与 `exists`，走 `true` / `false` 分支。
- **等待**：等待节点停在 `WAITING`，由命名信号（如 `obstacle_cleared`）
  放行，也支持超时分支。
- **暂停 / 恢复**：暂停门闩只在动作 tick 之间生效；恢复后从原节点、
  原机器人坐标继续。服务重启会把存活执行标为 `PAUSED`，可从当前节点恢复。
- **取消**：先进入 `CANCELLING`，正在执行的动作允许跑到下一个干净 tick
  收尾（不会被硬杀），但**绝不推进任何后继节点**；最终状态为 `CANCELLED`。
- **实时进度**：SSE 推送事件，前端节流回源 `/api/runs/:id` 拿权威状态；
  节点按 RUNNING / WAITING / FAILED / SUCCEEDED 着色，失败实际走的边高亮。

## 运行

```bash
# 后端（Python 3.10+，无第三方依赖）
cd backend
../.venv/bin/python run.py --port 8000

# 前端开发模式
cd frontend && npx vite dev   # 代理 /api -> localhost:8000

# 或构建后由后端直接托管
cd frontend && npx vite build
# 浏览器打开 http://127.0.0.1:8000/
```

## 验收场景：取物途中遇障碍后撤销

1. 打开页面，点“启动新执行”。
2. 机器人从基地沿 y=0 去货架 (4,0)。在场景网格点 (2,0) 放置障碍 ▲。
3. `MOVE (4,0)` 在 (2,0) 失败（红色节点），自动沿**红色 failure 边**走到
   “等待障碍清除”，该节点显示紫色 `WAITING`。
4. 点“取消”：等待立即终止为 `CANCELLED`；后续的“重新前往货架 / 取货”
   都不会启动；事件日志最后一条为 `run CANCELLED`。
5. 一致性：右侧机器人位置仍是 `(2,0)`，与失败节点详情和事件流完全一致。
   也可在 MOVE 执行中直接取消，动作完成当前 tick 后停住、无后继执行。

清除障碍（再点一次格子）会向等待节点发送信号放行，可观察
“等待 → 重试 → 取货 → 条件分支 → 交货”的完整成功路径。

## 测试

```bash
cd backend && ../.venv/bin/python -m unittest tests.test_orchestrator -v
```

覆盖：障碍→等待→取消的记录一致性、移动途中取消、暂停恢复原状态、
条件低电量分支、信号放行等待、SQLite 重开持久化（共 7 项）。

## 关键文件

- `backend/app/db.py`：SQLite 工作流 / 执行 / 节点记录 / 事件日志
- `backend/app/engine.py`：编排引擎（分支、等待、暂停、优雅取消、重启恢复）
- `backend/app/robot.py`：状态化虚拟机器人（tick 推进、障碍、取放货、充电）
- `backend/app/server.py`：HTTP REST + SSE
- `frontend/src/App.tsx`：画布、场景、执行控制与事件流

真实设备接入点：后续新增与 `SimRobot` 同接口的驱动实现
（`begin_action` / `step` / `snapshot`），在 `Engine` 中替换即可，
编排与持久化层无需改动。
