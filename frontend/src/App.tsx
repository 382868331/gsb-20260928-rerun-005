import { useCallback, useEffect, useRef, useState } from 'react';
import FlowCanvas from './FlowCanvas';
import { api } from './api';
import type { Execution, ExecEvent, ExecutionSummary, Flow } from './types';
import type { EvidenceItem } from './types';

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
  const [evCategory, setEvCategory] = useState<EvidenceItem['category']>('position');
  const [evSource, setEvSource] = useState<EvidenceItem['source']>('onsite');
  const [evPosX, setEvPosX] = useState('0');
  const [evPosY, setEvPosY] = useState('0');
  const [evHolding, setEvHolding] = useState('');
  const [evOutcome, setEvOutcome] = useState<'success' | 'failure'>('success');
  const [evDetail, setEvDetail] = useState('');
  const seqRef = useRef(0);
  const executionIdRef = useRef<string | null>(null);
  const [pastExecutions, setPastExecutions] = useState<ExecutionSummary[]>([]);
  const [history, setHistory] = useState<{ evidence: import('./types').EvidenceItem[]; decisions: import('./types').RecoveryDecision[] } | null>(null);

  useEffect(() => {
    api.flows().then((list) => {
      setFlows(list);
      if (list[0]) setFlowId(list[0].id);
    });
    loadExecutions();
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
      loadExecutions();
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

  function loadExecutions() {
    api.listExecutions()
      .then(setPastExecutions)
      .catch((err) => setError((err as Error).message));
  }

  async function reopenExecution(id: string) {
    setError(null);
    setHistory(null);
    seqRef.current = 0;
    executionIdRef.current = id;
    try {
      setEvents(await api.events(id));
      setExecution(await refresh(id));
    } catch (err) {
      setError((err as Error).message);
    }
  }

  async function viewHistory(id: string) {
    setError(null);
    try {
      setHistory(await api.recoveryHistory(id));
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

  const evidence = execution?.evidence ?? [];
  const decisions = execution?.recovery_decisions ?? [];
  const conflicts = execution?.evidence_conflicts ?? {};
  const hasConflict = Object.keys(conflicts).length > 0;

  const CATEGORY_TEXT: Record<EvidenceItem['category'], string> = {
    position: '位置',
    holding: '持物',
    action_result: '动作结果',
  };

  function evidenceValueText(item: EvidenceItem): string {
    if (item.category === 'position') return `(${String(item.value.x)}, ${String(item.value.y)})`;
    if (item.category === 'holding') return String(item.value.holding ?? '空');
    return `${String(item.value.outcome ?? '')}${item.value.detail ? `：${String(item.value.detail)}` : ''}`;
  }

  async function submitEvidence() {
    if (!execution) return;
    let value: Record<string, unknown>;
    if (evCategory === 'position') value = { x: Number(evPosX), y: Number(evPosY) };
    else if (evCategory === 'holding') value = { holding: evHolding || null };
    else value = { outcome: evOutcome, detail: evDetail || '人工观察' };
    await runAction(() => api.recordEvidence(execution.id, evCategory, evSource, value));
  }

  async function retractEvidence(item: EvidenceItem) {
    if (!execution) return;
    const reason = window.prompt('撤回原因（必填）', '观察有误');
    if (!reason) return;
    await runAction(() => api.retractEvidence(execution.id, item.id, reason));
  }

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
          {(execution?.status === 'awaiting_recovery' || (evidence.length > 0)) && (
            <div className="evidence-panel">
              <h3>核实证据（位置 / 持物 / 动作结果）</h3>
              {hasConflict && (
                <div className="conflict-warning">
                  ⚠ 有效证据互相矛盾（{Object.keys(conflicts).map((c) => CATEGORY_TEXT[c as EvidenceItem['category']] ?? c).join('、')}），
                  已阻止继续执行；请撤回错误观察后再决定。
                </div>
              )}
              <div className="evidence-form">
                <select value={evCategory} onChange={(e) => setEvCategory(e.target.value as EvidenceItem['category'])}>
                  <option value="position">位置</option>
                  <option value="holding">持物</option>
                  <option value="action_result">动作结果</option>
                </select>
                <select value={evSource} onChange={(e) => setEvSource(e.target.value as EvidenceItem['source'])}>
                  <option value="onsite">现场观察</option>
                  <option value="device">设备迟到回应</option>
                </select>
                {evCategory === 'position' && (
                  <span>
                    x:<input className="evidence-num" value={evPosX} onChange={(e) => setEvPosX(e.target.value)} />
                    y:<input className="evidence-num" value={evPosY} onChange={(e) => setEvPosY(e.target.value)} />
                  </span>
                )}
                {evCategory === 'holding' && (
                  <input placeholder="持有物（留空表示空爪）" value={evHolding} onChange={(e) => setEvHolding(e.target.value)} />
                )}
                {evCategory === 'action_result' && (
                  <span>
                    <select value={evOutcome} onChange={(e) => setEvOutcome(e.target.value as 'success' | 'failure')}>
                      <option value="success">成功</option>
                      <option value="failure">失败</option>
                    </select>
                    <input placeholder="说明" value={evDetail} onChange={(e) => setEvDetail(e.target.value)} />
                  </span>
                )}
                <button className="btn-secondary" onClick={() => void submitEvidence()}>登记证据</button>
              </div>
              <ul className="evidence-list">
                {evidence.length === 0 && <li className="muted">暂无证据</li>}
                {evidence.map((item) => (
                  <li key={item.id} className={item.retracted ? 'evidence-item retracted' : 'evidence-item'}>
                    <span>
                      #{item.id} {CATEGORY_TEXT[item.category]}＝{evidenceValueText(item)}
                      （{item.source === 'onsite' ? '现场观察' : '设备迟到回应'}，观察时刻 {item.observed_at}）
                    </span>
                    {item.retracted ? (
                      <span className="retract-info">已撤回：{item.retract_reason}</span>
                    ) : execution?.status !== 'awaiting_recovery' ? null : (
                      <button className="btn-link" onClick={() => void retractEvidence(item)}>撤回</button>
                    )}
                  </li>
                ))}
              </ul>
            </div>
          )}
          {decisions.length > 0 && (
            <div className="decision-panel">
              <h3>恢复处置决定与依据</h3>
              <ul>
                {decisions.map((d) => (
                  <li key={d.id}>
                    [{d.created_at}] {d.decision === 'finish' ? '继续' : '结束'}
                    {d.node_id ? `（节点 ${d.node_id} 第${String(d.attempt ?? 1)}次${d.episode && d.episode > 1 ? `，第${d.episode}轮核实` : ''}）` : ''}：{d.summary}
                  </li>
                ))}
              </ul>
            </div>
          )}
          <div className="history-panel">
            <h3>历史执行（可重开查看证据与处置依据）</h3>
            <ul className="history-list">
              {pastExecutions.length === 0 && <li className="muted">暂无历史执行</li>}
              {pastExecutions.map((item) => (
                <li key={item.id} className={item.id === execution?.id ? 'history-item current' : 'history-item'}>
                  <button className="btn-link" onClick={() => void reopenExecution(item.id)}>
                    {item.id}｜{STATUS_TEXT[item.status] ?? item.status}｜{item.started_at}
                  </button>
                  <button className="btn-link" onClick={() => void viewHistory(item.id)}>证据/决定</button>
                </li>
              ))}
            </ul>
            {history && (
              <div className="history-detail">
                <h4>证据</h4>
                <ul>
                  {history.evidence.length === 0 && <li className="muted">无证据记录</li>}
                  {history.evidence.map((item) => (
                    <li key={item.id} className={item.retracted ? 'evidence-item retracted' : 'evidence-item'}>
                      #{item.id} {CATEGORY_TEXT[item.category]}＝{evidenceValueText(item)}
                      （{item.source === 'onsite' ? '现场观察' : '设备迟到回应'}，{item.observed_at}）
                      {item.retracted ? ` 已撤回：${item.retract_reason ?? ''}` : ''}
                    </li>
                  ))}
                </ul>
                <h4>处置决定</h4>
                <ul>
                  {history.decisions.length === 0 && <li className="muted">无决定记录</li>}
                  {history.decisions.map((d) => (
                    <li key={d.id}>[{d.created_at}] {d.decision === 'finish' ? '继续' : '结束'}：{d.summary}</li>
                  ))}
                </ul>
              </div>
            )}
          </div>
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
