import { useState } from 'react';
import { api } from './api';
import type { Execution } from './types';

const SUBJECT_META: Record<string, { label: string; hints: string[] }> = {
  position: { label: '位置', hints: ['(0, 0)', '(4, 3)', '(1, 0)'] },
  holding: { label: '持物', hints: ['empty', '包裹A'] },
  action_result: { label: '动作结果', hints: ['success', 'failure'] },
};

const STATUS_STYLE: Record<string, string> = {
  confirmed: '已确认',
  disputed: '矛盾·待决定',
  unknown: '无有效证据',
};

const ENTRY_STATUS_TEXT: Record<string, string> = {
  active: '有效',
  withdrawn: '已撤回',
  superseded: '已被修订',
};

export default function RecoveryPanel({
  execution,
  onChanged,
}: {
  execution: Execution;
  onChanged: () => Promise<void>;
}) {
  const [subject, setSubject] = useState<'position' | 'holding' | 'action_result'>('holding');
  const [source, setSource] = useState('manual_observation');
  const [value, setValue] = useState('');
  const [detail, setDetail] = useState('');
  const [busy, setBusy] = useState(false);

  const resolution = execution.evidence_resolution ?? {};
  const ledger = execution.evidence ?? [];

  async function withBusy(action: () => Promise<unknown>) {
    setBusy(true);
    try {
      await action();
      await onChanged();
    } finally {
      setBusy(false);
    }
  }

  function observedNow() {
    return new Date().toISOString().slice(0, 19);
  }

  async function register() {
    if (!value.trim()) return;
    await withBusy(() =>
      api.addEvidence(execution.id, {
        subject,
        source,
        observed_at: observedNow(),
        value: value.trim(),
        detail: detail.trim(),
      }),
    );
    setDetail('');
  }

  async function registerHint(hint: string) {
    await withBusy(() =>
      api.addEvidence(execution.id, {
        subject,
        source,
        observed_at: observedNow(),
        value: hint,
        detail: detail.trim(),
      }),
    );
    setDetail('');
  }

  return (
    <div className="evidence-panel">
      {execution.reawait_reason && (
        <div className="evidence-reawait">
          ⚠ {execution.reawait_reason}
          {execution.blocked_followups && execution.blocked_followups.length > 0 && (
            <span>（受阻：{execution.blocked_followups.map((s) => SUBJECT_META[s]?.label ?? s).join('、')}）</span>
          )}
        </div>
      )}
      <div className="evidence-grid">
        {(['position', 'holding', 'action_result'] as const).map((key) => {
          const item = resolution[key];
          const statusClass = item ? `evidence-${item.status}` : 'evidence-unknown';
          const valueText = item?.status === 'confirmed'
            ? (key === 'position' && Array.isArray(item.value)
              ? `(${String(item.value[0])}, ${String(item.value[1])})`
              : key === 'holding' && item.value === null
                ? '空手'
                : String(item.value))
            : STATUS_STYLE[item?.status ?? 'unknown'];
          return (
            <div key={key} className={`evidence-cell ${statusClass}`}>
              <div className="evidence-cell__title">{SUBJECT_META[key].label}</div>
              <div className="evidence-cell__value">{valueText}</div>
              <div className="evidence-cell__ids">
                依据 #{(item?.evidence_ids ?? []).join(', #') || '—'}
              </div>
            </div>
          );
        })}
      </div>
      <table className="evidence-table">
        <thead>
          <tr><th>#</th><th>主题</th><th>内容</th><th>来源</th><th>观察时刻</th><th>状态</th><th>操作</th></tr>
        </thead>
        <tbody>
          {ledger.length === 0 && (
            <tr><td colSpan={7} className="muted">暂无核实证据</td></tr>
          )}
          {ledger.map((entry) => (
            <tr key={entry.id} className={entry.status !== 'active' ? 'evidence-row-inactive' : ''}>
              <td>{entry.id}</td>
              <td>{SUBJECT_META[entry.subject]?.label ?? entry.subject}</td>
              <td title={entry.detail ?? ''}>{entry.value}</td>
              <td>{entry.source === 'manual_observation' ? '现场观察' : entry.source === 'device_response' ? '设备回应' : entry.source === 'checkpoint' ? '检查点' : entry.source}</td>
              <td>{entry.observed_at}</td>
              <td>{ENTRY_STATUS_TEXT[entry.status] ?? entry.status}</td>
              <td>
                {entry.status === 'active' && (
                  <button
                    disabled={busy}
                    onClick={() => withBusy(() => api.withdrawEvidence(execution.id, entry.id, '人工观察撤回'))}
                  >撤回</button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="evidence-form">
        <select value={subject} onChange={(e) => { setSubject(e.target.value as typeof subject); setValue(''); }}>
          {Object.entries(SUBJECT_META).map(([key, meta]) => (
            <option key={key} value={key}>{meta.label}</option>
          ))}
        </select>
        <select value={source} onChange={(e) => setSource(e.target.value)}>
          <option value="manual_observation">现场观察</option>
          <option value="device_response">设备迟到回应</option>
          <option value="other">其他来源</option>
        </select>
        <input
          value={value}
          placeholder="证据内容（矛盾时保持待决定，不覆盖旧观察）"
          onChange={(e) => setValue(e.target.value)}
        />
        <input value={detail} placeholder="备注（可选）" onChange={(e) => setDetail(e.target.value)} />
        <button disabled={busy || !value.trim()} onClick={register} className="btn-secondary">逐项登记</button>
        {SUBJECT_META[subject].hints.map((hint) => (
          <button key={hint} disabled={busy} onClick={() => registerHint(hint)}>
            登记「{hint}」
          </button>
        ))}
      </div>
      <div className="evidence-basis-hint">
        继续/收尾的决定依据：持物必须已确认；继续动作还需动作结果已确认。
        互相矛盾的证据保持待决定，已完成的移动/放下记录保留实际发生的事实。
      </div>
    </div>
  );
}
