import { useState } from 'react';
import { api } from './api';
import type { Evidence, Execution } from './types';

const CATEGORY_TEXT: Record<string, string> = {
  position: '位置',
  holding: '持物',
  action_result: '动作结果',
};

const SOURCE_TEXT: Record<string, string> = {
  onsite: '现场观察',
  device_late: '设备迟到回应',
};

export function EvidenceList({ evidence, canRetract, onRetract }: {
  evidence: Evidence[];
  canRetract: boolean;
  onRetract: (id: number) => void;
}) {
  if (evidence.length === 0) return <p className="muted">暂无证据</p>;
  return (
    <ul className="evidence-list">
      {evidence.map((item) => (
        <li key={item.id} className={item.retracted ? 'evidence evidence-retracted' : 'evidence'}>
          <span className="evidence-head">
            #{item.id} {CATEGORY_TEXT[item.category] ?? item.category}
            <em>{SOURCE_TEXT[item.source] ?? item.source}</em>
            <time>{item.observed_at}</time>
          </span>
          <span className="evidence-value">{item.value}</span>
          {item.detail && <span className="evidence-detail">{item.detail}</span>}
          {item.retracted ? (
            <span className="evidence-retract">已撤回：{item.retract_reason}</span>
          ) : canRetract ? (
            <button className="btn-link" onClick={() => onRetract(item.id)}>撤回</button>
          ) : null}
        </li>
      ))}
    </ul>
  );
}

export default function EvidencePanel({ execution, runAction }: {
  execution: Execution;
  runAction: (action: () => Promise<unknown>) => void;
}) {
  const [category, setCategory] = useState('position');
  const [source, setSource] = useState('onsite');
  const [value, setValue] = useState('');
  const [detail, setDetail] = useState('');
  const evidence = execution.evidence ?? [];
  const conflicts = execution.evidence_conflicts ?? {};
  const conflictCategories = Object.keys(conflicts);
  const awaiting = execution.status === 'awaiting_recovery';
  const decided = Boolean(execution.recovery?.recovery_decision);

  function retract(id: number) {
    const reason = window.prompt('请说明撤回原因');
    if (!reason) return;
    runAction(() => api.retractEvidence(execution.id, id, reason));
  }

  function submit() {
    if (!value.trim()) return;
    runAction(async () => {
      await api.recordEvidence(execution.id, {
        category, source, value: value.trim(), detail: detail.trim() || undefined,
      });
      setValue('');
      setDetail('');
    });
  }

  return (
    <div className="panel">
      <h2>核实证据（位置 / 持物 / 动作结果）</h2>
      {conflictCategories.length > 0 && (
        <div className="conflict-bar">
          ⚠ 证据互相矛盾（{conflictCategories.map((c) => CATEGORY_TEXT[c] ?? c).join('、')}），
          旧记录已保留，撤回错误观察前无法继续或结束
        </div>
      )}
      {decided && (
        <p className="muted">
          恢复决定：{String(execution.recovery?.recovery_decision)}；依据证据：
          {((execution.recovery?.recovery_evidence as number[] | undefined) ?? [])
            .map((i) => '#' + i).join('、') || '无'}
        </p>
      )}
      <EvidenceList evidence={evidence} canRetract={awaiting && !decided} onRetract={retract} />
      {awaiting && !decided && (
        <div className="evidence-form">
          <select value={category} onChange={(e) => setCategory(e.target.value)}>
            <option value="position">位置</option>
            <option value="holding">持物</option>
            <option value="action_result">动作结果</option>
          </select>
          <select value={source} onChange={(e) => setSource(e.target.value)}>
            <option value="onsite">现场观察</option>
            <option value="device_late">设备迟到回应</option>
          </select>
          <input
            value={value}
            onChange={(e) => setValue(e.target.value)}
            placeholder="观察值，如 (3, 0) / 空 / success"
          />
          <input
            value={detail}
            onChange={(e) => setDetail(e.target.value)}
            placeholder="备注（可选）"
          />
          <button className="btn-secondary" onClick={submit} disabled={!value.trim()}>
            登记证据
          </button>
        </div>
      )}
    </div>
  );
}

