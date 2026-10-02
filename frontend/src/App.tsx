import { useCallback, useEffect, useRef, useState } from 'react';
import FlowCanvas from './FlowCanvas';
import { api } from './api';
import type { Execution, ExecEvent, Flow } from './types';

const STATUS_TEXT: Record<string, string> = {
  running: '运行中',
  waiting: '等待条件',
  paused: '已暂停',
  cancelling: '取消中（动作收尾）',
  awaiting_manual: '等待人工处置',
  awaiting_recovery: '待人工核实恢复',
  succeeded: '成功',
  failed: '失败',
  cancelled: '已取消',
  resolved: '已人工终止',
  interrupted: '中断',
};

const TERMINAL = new Set(['succeeded', 'failed', 'cancelled', 'interrupted', 'resolved']);

export default function App() {
  const [flows, setFlows] = useState<Flow[]>([]);
  const [flowId, setFlowId] = useState<string>('');
  const [execution, setExecution] = useState<Execution | null>(null);
  const [events, setEvents] = useState<ExecEvent[]>([]);
  const [obstacle, setObstacle] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const seqRef = useRef(0);
  const executionIdRef = useRef<string | null>(null);

  useEffect(() => {
    api.flows().then((list) => {
      setFlows(list);
      if (list[0]) setFlowId(list[0].id);
    });
  }, []);

  const refresh = useCallback(async (id: string) => {
    const [exec] = await Promise.all([api.execution(id), api.events(id, seqRef.current)]);
    setExecution(exec);
    return exec;
  }, []);

  useEffect(() => {
    const id = setInterval(async () => {
      const eid = executionIdRef.current;
      if (!eid) return;
      const exec = await refresh(eid);
      const newEvents = await api.events(eid, seqRef.current);
      if (newEvents.length) {
        seqRef.current = newEvents[newEvents.length - 1].seq;
        setEvents((prev) => {
          const known = new Set(prev.map((e) => e.seq));
          return [...prev, ...newEvents.filter((e) => !known.has(e.seq))].slice(-200);
        });
      }
      setObstacle(exec.robot.obstacle);
    }, 700);
    return () => clearInterval(id);
  }, [refresh]);

  useEffect(() => {
    const protocol = location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${protocol}://${location.host}/ws/events`);
    ws.onmessage = async (msg) => {
      const event: ExecEvent = JSON.parse(msg.data);
      const eid = executionIdRef.current;
      if (!eid || event.execution_id !== eid) return;
      seqRef.current = Math.max(seqRef.current, event.seq);
      setEvents((prev) => (prev.some((e) => e.seq === event.seq) ? prev : [...prev, event].slice(-200)));
      setExecution(await refresh(eid));
    };
    return () => ws.close();
  }, [refresh]);

  const flow = flows.find((f) => f.id === flowId) ?? null;
  const active = execution && !TERMINAL.has(execution.status);

  async function runAction(action: () => Promise<unknown>) {
    setError(null);
    try {
      await action();
      const eid = executionIdRef.current;
      if (eid) setExecution(await refresh(eid));
    } catch (err) {
      setError((err as Error).message);
    }
  }

  async function start() {
    if (!flowId) return;
    setError(null);
    setEvents([]);
    seqRef.current = 0;
    try {
      const { execution_id } = await api.startRun(flowId);
      executionIdRef.current = execution_id;
      setExecution(await refresh(execution_id));
    } catch (err) {
      setError((err as Error).message);
    }
  }

  async function toggleObstacle() {
    const next = !obstacle;
    await api.setObstacle(next);
    setObstacle(next);
  }

  const waitingCondition = execution?.waiting_for;
  const manualNodeId = execution?.manual_node;
  const manualNode = manualNodeId
    ? flow?.definition.nodes.find((n) => n.id === manualNodeId) ?? null
    : null;
  const recoveryNodeId = execution?.unconfirmed_action?.node_id;
  const recoveryNode = recoveryNodeId
    ? flow?.definition.nodes.find((n) => n.id === recoveryNodeId) ?? null
    : null;

  return (
    <div className="app">
      <header className="topbar">
        <h1>机器人任务编排平台</h1>
        <select value={flowId} onChange={(e) => setFlowId(e.target.value)} disabled={!!active}>
          {flows.map((f) => (
            <option key={f.id} value={f.id}>{f.name}</option>
          ))}
        </select>
        <button onClick={start} disabled={!flowId || !!active} className="btn-primary">▶ 运行</button>
        <button
          onClick={() => runAction(() => api.pause(execution!.id))}
          disabled={!active || (execution?.status !== 'running' && execution?.status !== 'waiting')}
        >⏸ 暂停</button>
        <button
          onClick={() => runAction(() => api.resume(execution!.id))}
          disabled={execution?.status !== 'paused'}
        >⏵ 恢复</button>
        <button
          onClick={() => runAction(() => api.cancel(execution!.id))}
          disabled={!active || execution?.status === 'cancelling' || execution?.status === 'awaiting_recovery'}
          className="btn-danger"
        >⊘ 取消（动作收尾）</button>
        <button onClick={toggleObstacle} className={obstacle ? 'btn-warn active' : 'btn-warn'}>
          {obstacle ? '🚧 障碍物：开' : '🚧 障碍物：关'}
        </button>
        {waitingCondition && execution?.status !== 'paused' && (
          <button
            className="btn-primary"
            onClick={() => runAction(() => api.signal(execution!.id, waitingCondition))}
          >✓ 满足条件：{waitingCondition}</button>
        )}
      </header>
      {error && <div className="error-bar">{error}</div>}
      <main className="main">
        <section className="canvas-wrap">
          <div className="status-line">
            <span className={`badge badge-${execution?.status ?? 'idle'}`}>
              {execution ? STATUS_TEXT[execution.status] ?? execution.status : '未运行'}
            </span>
            {execution && <span>执行 {execution.id}</span>}
            {execution?.termination_reason && TERMINAL.has(execution.status) && (
              <span className="termination-reason">{execution.termination_reason}</span>
            )}
          </div>
          {execution?.status === 'awaiting_manual' && manualNode && (
            <div className="manual-bar">
              <span className="manual-bar__title">✋ {manualNode.label}：{String(manualNode.params.instruction ?? '等待人工处置')}</span>
              <button
                className="btn-primary"
                onClick={() => runAction(() => api.resolveManual(execution.id, false))}
              >已排障，放行继续</button>
              <button
                className="btn-danger"
                onClick={() => runAction(() => api.resolveManual(execution.id, true))}
              >确认终止，流程停在此处</button>
            </div>
          )}
          {execution?.status === 'awaiting_recovery' && (
            <div className="manual-bar recovery-bar">
              <span className="manual-bar__title">
                🛠 设备状态待核实：持有物「{execution.robot.holding ?? '空'}」，
                最后位置 ({execution.robot.x}, {execution.robot.y})，
                未确认动作：{recoveryNode?.label ?? recoveryNodeId ?? '无'}
                {execution.unconfirmed_action?.attempt ? `（第${execution.unconfirmed_action.attempt}次）` : ''}
                {execution.unconfirmed_action?.failure_detail ? `；原故障：${execution.unconfirmed_action.failure_detail}` : ''}
              </span>
              <button
                className="btn-secondary"
                onClick={() => runAction(() => api.recordLateAction(execution.id, 'success', '现场确认原动作已完成'))}
              >登记迟到成功（不发新移动）</button>
              <button
                className="btn-primary"
                onClick={() => runAction(() => api.resolveRecovery(execution.id, true))}
              >继续收尾</button>
              <button
                className="btn-danger"
                onClick={() => runAction(() => api.resolveRecovery(execution.id, false))}
              >结束本次流程</button>
            </div>
          )}
          {flow && (
            <FlowCanvas
              nodes={flow.definition.nodes}
              edges={flow.definition.edges}
              execution={execution}
              events={events}
            />
          )}
        </section>
        <aside className="sidebar">
          <div className="panel">
            <h2>机器人实际状态</h2>
            <dl>
              <dt>坐标</dt><dd>({execution?.robot.x ?? 0}, {execution?.robot.y ?? 0})</dd>
              <dt>持有物</dt><dd>{execution?.robot.holding ?? '空'}</dd>
              <dt>动作机</dt><dd>{execution?.robot.idle === false ? '忙碌（动作收尾中…）' : '空闲'}</dd>
              <dt>障碍物</dt><dd>{obstacle ? '存在 🚧' : '无'}</dd>
            </dl>
          </div>
          <div className="panel panel-log">
            <h2>执行记录（服务端持久化）</h2>
            <ul>
              {events.length === 0 && <li className="muted">暂无事件</li>}
              {events.map((event) => (
                <li key={event.seq} className={`event event-${event.type}`}>
                  <span className="event-seq">#{event.seq}</span>
                  <span className="event-msg">{event.message ?? event.type}</span>
                  {event.data?.attempt ? (
                    <span className="event-attempt">第{String(event.data.attempt)}次</span>
                  ) : null}
                  {event.node_id && <span className="event-node">{event.node_id}</span>}
                </li>
              ))}
            </ul>
          </div>
        </aside>
      </main>
    </div>
  );
}
