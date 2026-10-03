export type NodeType = 'move' | 'pick' | 'place' | 'wait' | 'log' | 'manual';

export interface FlowNode {
  id: string;
  type: NodeType;
  label: string;
  params: Record<string, unknown>;
  x?: number;
  y?: number;
}

export interface FlowEdge {
  id: string;
  source: string;
  target: string;
  kind: 'success' | 'failure' | 'exhausted';
}

export interface Flow {
  id: string;
  name: string;
  definition: { nodes: FlowNode[]; edges: FlowEdge[] };
}

export interface NodeRun {
  node_id: string;
  status: string;
  detail: string | null;
  attempt: number;
  started_at?: string;
  finished_at?: string | null;
}
export interface Evidence {
  id: number;
  category: 'position' | 'holding' | 'action_result';
  source: 'onsite' | 'device_late';
  value: string;
  detail: string | null;
  observed_at: string;
  retracted: number;
  retract_reason: string | null;
  retracted_at: string | null;
  node_id?: string | null;
  attempt?: number | null;
}


export interface Execution {
  id: string;
  flow_id: string;
  status: string;
  termination_reason?: string | null;
  node_runs: NodeRun[];
  followups?: {
    node_id: string;
    attempt: number;
    kind: string;
    outcome: string;
    detail: string | null;
  }[];
  evidence?: Evidence[];
  evidence_conflicts?: Record<string, Evidence[]>;
  waiting_for: string | null;
  manual_node?: string | null;
  current_node?: string | null;
  current_attempt?: number;
  recovery?: Record<string, unknown> | null;
  unconfirmed_action?: {
    node_id?: string | null;
    attempt?: number | null;
    phase?: string | null;
    failure_detail?: string | null;
    last_progress?: Record<string, unknown> | null;
  } | null;
  robot: { x: number; y: number; holding: string | null; idle: boolean; obstacle: boolean };
}

export interface ExecEvent {
  seq: number;
  execution_id: string;
  type: string;
  node_id: string | null;
  message: string | null;
  data: Record<string, unknown> | null;
}
