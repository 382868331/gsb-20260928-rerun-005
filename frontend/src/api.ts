export type RunStatus =
  | "RUNNING" | "PAUSED" | "CANCELLING" | "SUCCEEDED"
  | "FAILED" | "CANCELLED" | "ERROR";

export interface NodeData {
  kind: "START" | "END" | "ACTION" | "CONDITION" | "WAIT";
  label: string;
  action?: string;
  params?: Record<string, number | string>;
  expr?: string;
  signal?: string;
  timeout?: number;
}

export interface FlowNode {
  id: string;
  position: { x: number; y: number };
  data: NodeData;
}

export interface FlowEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string;
}

export interface FlowDefinition {
  nodes: FlowNode[];
  edges: FlowEdge[];
}

async function req<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) throw new Error(`${url} -> ${res.status}`);
  return res.json();
}

export const api = {
  workflow: (id: string) =>
    req<{ id: string; name: string; definition: FlowDefinition }>(
      `/api/workflows/${id}`),
  saveWorkflow: (id: string, name: string, definition: FlowDefinition) =>
    req("/api/workflows", {
      method: "POST",
      body: JSON.stringify({ id, name, definition }),
    }),
  startRun: (workflowId: string) =>
    req<{ run_id: string }>("/api/runs", {
      method: "POST",
      body: JSON.stringify({ workflow_id: workflowId }),
    }),
  run: (id: string) =>
    req<RunDetail>(`/api/runs/${id}`),
  runs: () => req<{ runs: RunRow[] }>("/api/runs"),
  pause: (id: string) =>
    req(`/api/runs/${id}/pause`, { method: "POST", body: "{}" }),
  resume: (id: string) =>
    req(`/api/runs/${id}/resume`, { method: "POST", body: "{}" }),
  cancel: (id: string) =>
    req(`/api/runs/${id}/cancel`, { method: "POST", body: "{}" }),
  signal: (id: string, key: string, value: unknown) =>
    req(`/api/runs/${id}/signal`, {
      method: "POST",
      body: JSON.stringify({ key, value }),
    }),
  robot: () => req<RobotState>("/api/robot"),
  obstacle: (x: number, y: number) =>
    req<RobotState>("/api/robot/obstacle", {
      method: "POST",
      body: JSON.stringify({ x, y }),
    }),
  clearObstacle: (x: number, y: number) =>
    req<RobotState>("/api/robot/clear-obstacle", {
      method: "POST",
      body: JSON.stringify({ x, y }),
    }),
  resetRobot: () =>
    req<RobotState>("/api/robot/reset", { method: "POST", body: "{}" }),
};

export interface RunRow {
  id: string;
  workflow_name: string;
  status: RunStatus;
  detail: string | null;
  current_node_id: string | null;
  created_at: number;
  ended_at: number | null;
}

export interface NodeRecord {
  run_id: string;
  node_id: string;
  status: string;
  detail: string | null;
  attempt: number;
  started_at: number | null;
  ended_at: number | null;
}

export interface EventRow {
  seq: number;
  kind: string;
  node_id: string | null;
  payload: Record<string, unknown>;
  created_at?: number;
}

export interface RobotState {
  pos: [number, number];
  carrying: boolean;
  battery: number;
  obstacles: [number, number][];
}

export interface RunDetail {
  run: RunRow;
  nodes: NodeRecord[];
  events: EventRow[];
  robot: RobotState;
  live: boolean;
}
