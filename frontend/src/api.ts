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
  manualDecide: (id: string, action: 'retry' | 'abort') =>
    req(`/api/executions/${id}/manual?action=${encodeURIComponent(action)}`, { method: 'POST' }),
  setObstacle: (present: boolean) =>
    req('/api/robot/obstacle', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ present }),
    }),
};
