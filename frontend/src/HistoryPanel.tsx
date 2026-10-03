import { useEffect, useState } from 'react';
import { api } from './api';
import type { Execution } from './types';
import { EvidenceList } from './EvidencePanel';

const STATUS_TEXT: Record<string, string> = {
  running: '运行中',
  waiting: '等待条件',
  paused: '已暂停',
  cancelling: '取消中',
  awaiting_manual: '等待人工处置',
  awaiting_recovery: '待人工核实',
  succeeded: '成功',
  failed: '失败',
  cancelled: '已取消',
  resolved: '已人工终止',
  interrupted: '中断',
};

export default function HistoryPanel() {
  const [items, setItems] = useState<{ id: string; status: string; started_at: string }[]>([]);
  const [selected, setSelected] = useState<Execution | null>(null);

  useEffect(() => {
    const load = () => api.listExecutions().then(setItems).catch(() => undefined);
    load();
    const timer = setInterval(load, 3000);
    return () => clearInterval(timer);
  }, []);

  return (
    <div className="panel">
      <h2>历史执行（完成 / 结束依据可独立查看）</h2>
      <ul className="history-list">
        {items.length === 0 && <li className="muted">暂无执行记录</li>}
        {items.map((item) => (
          <li key={item.id}>
            <button
              className="btn-link"
              onClick={() => api.execution(item.id).then(setSelected).catch(() => undefined)}
            >
              {item.id}
            </button>
            <span className={`badge badge-${item.status}`}>
              {STATUS_TEXT[item.status] ?? item.status}
            </span>
            <time>{item.started_at}</time>
          </li>
        ))}
      </ul>
      {selected && (
        <div className="history-detail">
          <h3>执行 {selected.id}（{STATUS_TEXT[selected.status] ?? selected.status}）</h3>
          {selected.termination_reason && <p>终止说明：{selected.termination_reason}</p>}
          <h4>证据</h4>
          <EvidenceList evidence={selected.evidence ?? []} canRetract={false} onRetract={() => undefined} />
          <h4>后续信息</h4>
          <ul className="followup-list">
            {(selected.followups ?? []).length === 0 && <li className="muted">无</li>}
            {(selected.followups ?? []).map((f, i) => (
              <li key={i}>[{f.kind}/{f.outcome}] {f.node_id} 第{f.attempt}次：{f.detail ?? ''}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
