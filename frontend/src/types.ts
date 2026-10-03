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
  waiting_for: string | null;
  manual_node?: string | null;
  current_node?: string | null;
  current_attempt?: number;
  recovery?: Record<string, unknown> | null;
  evidence?: EvidenceItem[];
  evidence_conflicts?: Record<string, number[]>;
  recovery_decisions?: RecoveryDecision[];
  unconfirmed_action?: {
    node_id?: string | null;
    attempt?: number | null;
    phase?: string | null;
    failure_detail?: string | null;
    last_progress?: Record<string, unknown> | null;
  } | null;
  robot: { x: number; y: number; holding: string | null; idle: boolean; obstacle: boolean };
}

export interface EvidenceItem {
  id: number;
  node_id: string | null;
  attempt: number | null;
  category: 'position' | 'holding' | 'action_result';
  source: 'onsite' | 'device';
  value: Record<string, unknown>;
  observed_at: string;
  retracted: boolean;
  retract_reason: string | null;
}

export interface RecoveryDecision {
  id: number;
  node_id: string | null;
  attempt: number | null;
  decision: string;
  evidence_ids: number[];
  summary: string;
  created_at: string;
}

export interface ExecEvent {
  seq: number;
  execution_id: string;
  type: string;
  node_id: string | null;
  message: string | null;
  data: Record<string, unknown> | null;
}
