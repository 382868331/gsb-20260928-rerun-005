import type { Execution, ExecEvent, Flow } from './types';

async function req<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init);
  if (!response.ok) throw new Error((await response.json()).detail || response.statusText);
  return response.json();
}

export const api = {
  flows: () => req<Flow[]>('/api/flows'),
  startRun: (flowId: string) =>
    req<{ execution_id: string }>(`/api/flows/${flowId}/runs`, { method: 'POST' }),
  execution: (id: string) => req<Execution>(`/api/executions/${id}`),
  events: (id: string, after = 0) =>
    req<ExecEvent[]>(`/api/executions/${id}/events?after=${after}`),
  pause: (id: string) => req(`/api/executions/${id}/pause`, { method: 'POST' }),
  resume: (id: string) => req(`/api/executions/${id}/resume`, { method: 'POST' }),
  cancel: (id: string) => req(`/api/executions/${id}/cancel`, { method: 'POST' }),
  signal: (id: string, condition: string) =>
    req(`/api/executions/${id}/signal?condition=${encodeURIComponent(condition)}`, { method: 'POST' }),
  resolveManual: (id: string, abort: boolean) =>
    req(`/api/executions/${id}/manual`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ abort }),
    }),
  resolveRecovery: (id: string, finish: boolean) =>
    req(`/api/executions/${id}/recovery`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ finish }),
    }),
  recordLateAction: (id: string, outcome: 'success' | 'failure', detail: string) =>
    req(`/api/executions/${id}/late-action`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ outcome, detail }),
    }),
  addEvidence: (
    id: string,
    payload: { subject: string; source: string; observed_at: string; value: string; detail?: string; supersedes_id?: number | null },
  ) =>
    req<{ evidence_id: number }>(`/api/executions/${id}/evidence`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    }),
  withdrawEvidence: (id: string, evidenceId: number, detail = '') =>
    req(`/api/executions/${id}/evidence/withdraw`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ evidence_id: evidenceId, detail }),
    }),
  setObstacle: (present: boolean) =>
    req('/api/robot/obstacle', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ present }),
    }),
};
