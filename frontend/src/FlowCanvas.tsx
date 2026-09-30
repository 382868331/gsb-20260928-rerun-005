import { useMemo } from 'react';
import { ReactFlow,
  Background,
  type Edge,
  type Node,
  type NodeProps,
  Handle,
  MarkerType,
  Position,
} from '@xyflow/react';
import type { Execution, FlowEdge, FlowNode, NodeRun } from './types';
import { layoutNodes } from './layout';

const TYPE_LABEL: Record<string, string> = {
  move: '移动',
  pick: '取物',
  place: '放置',
  wait: '等待',
  log: '播报',
  manual: '人工处置',
};

interface NodeData extends Record<string, unknown> {
  node: FlowNode;
  runStatus?: string;
  detail?: string | null;
  waiting?: boolean;
  manual?: boolean;
  attempt?: number;
  history: NodeRun[];
}

const STATUS_TEXT: Record<string, string> = {
  running: '▶ 本次执行中',
  waiting: '⏳ 本次等待条件',
  manual: '✋ 等待人工处置',
  succeeded: '✓ 本次完成',
  failed: '✗ 本次失败',
  cancelled: '⊘ 本次取消',
};

function TaskNode({ data }: NodeProps<Node<NodeData>>) {
  const { node, runStatus, detail, waiting, manual, attempt, history } = data;
  const cls = `task-node ${runStatus ?? 'idle'} ${waiting || manual ? 'waiting-pulse' : ''}`;
  const params = node.type === 'move'
    ? `→(${node.params.x}, ${node.params.y})`
    : node.type === 'wait'
      ? `条件: ${node.params.condition}`
      : node.type === 'pick'
        ? `抓取 ${node.params.item ?? ''}`
        : node.type === 'manual'
          ? `重试上限 ${node.params.max_retries ?? 2} 次 → ${node.params.retry_target ?? '—'}`
          : node.type === 'log'
            ? String(node.params.message ?? '')
            : '放下物品';
  const finishedHistory = history.filter((h) => h.attempt !== attempt || (h.attempt === attempt && h.status !== runStatus));
  return (
    <div className={cls}>
      <Handle type="target" position={Position.Left} />
      <div className="task-node__type">{TYPE_LABEL[node.type] ?? node.type}</div>
      <div className="task-node__label">{node.label || node.id}</div>
      <div className="task-node__params" title={params}>{params}</div>
      <div className="task-node__status">
        {runStatus ? (STATUS_TEXT[runStatus] ?? runStatus) : '未执行'}
        {attempt != null && <span className="attempt-badge">第 {attempt} 次</span>}
        {runStatus === 'failed' && detail ? <div className="attempt-detail">{detail}</div> : null}
        {manual && detail ? <div className="attempt-detail">{detail}</div> : null}
      </div>
      {history.length > 0 && (
        <div className="task-node__history" title="该节点历史经过（旧回路）">
          <span className="history-title">历史经过 ×{history.length}</span>
          {history.map((h) => (
            <span key={`${h.attempt}-${h.status}`} className={`history-chip h-${h.status}`}>
              #{h.attempt} {STATUS_TEXT[h.status]?.replace('本次', '') ?? h.status}
            </span>
          ))}
        </div>
      )}
      <Handle type="source" position={Position.Right} id="success" />
    </div>
  );
}

const nodeTypes = { task: TaskNode };

interface Props {
  nodes: FlowNode[];
  edges: FlowEdge[];
  execution: Execution | null;
}

export default function FlowCanvas({ nodes, edges, execution }: Props) {
  const { positions, backEdges } = useMemo(() => layoutNodes(nodes, edges), [nodes, edges]);

  const runsByNode = useMemo(() => {
    const map = new Map<string, NodeRun[]>();
    execution?.node_runs.forEach((run) => {
      map.set(run.node_id, [...(map.get(run.node_id) ?? []), run]);
    });
    return map;
  }, [execution]);

  const rfNodes: Node<NodeData>[] = useMemo(
    () =>
      nodes.map((node) => {
        const all = runsByNode.get(node.id) ?? [];
        // 最新一条 = 本轮（当前回路）的状态；更早的是历史经过
        const current = all[all.length - 1];
        const history = all.slice(0, -1);
        const isManualWait = execution?.manual?.node_id === node.id && current?.status === 'manual';
        const isCondWait = execution?.waiting_for != null && node.type === 'wait'
          && current?.status === 'waiting';
        return {
          id: node.id,
          type: 'task',
          position: positions.get(node.id) ?? { x: 0, y: 0 },
          data: {
            node,
            runStatus: current?.status,
            detail: current?.detail,
            waiting: isCondWait,
            manual: isManualWait,
            attempt: current?.attempt,
            history,
          },
        };
      }),
    [nodes, positions, runsByNode, execution],
  );

  const rfEdges: Edge[] = useMemo(
    () =>
      edges.map((edge) => {
        const isBack = backEdges.has(edge.id);
        // 边的“本次走向”只看源节点最新一轮结果；历史经过不改变当前高亮
        const runs = runsByNode.get(edge.source) ?? [];
        const latest = runs[runs.length - 1];
        const taken =
          (edge.kind === 'success' && latest?.status === 'succeeded') ||
          (edge.kind === 'failure' && latest?.status === 'failed');
        const takenAttempt = taken ? latest?.attempt : undefined;
        return {
          id: edge.id,
          source: edge.source,
          target: edge.target,
          sourceHandle: 'success',
          type: isBack ? 'smoothstep' : 'default',
          label: isBack
            ? `失败回路（重试第 ${takenAttempt ?? ''} 次）`
            : edge.kind === 'failure'
              ? '失败走向'
              : undefined,
          animated: taken || isBack,
          className: isBack
            ? 'edge-back'
            : edge.kind === 'failure'
              ? 'edge-failure'
              : taken
                ? 'edge-taken'
                : '',
          style: isBack
            ? { stroke: '#9333ea', strokeDasharray: '8 5' }
            : edge.kind === 'failure'
              ? { stroke: '#e5484d', strokeDasharray: '6 4' }
              : taken
                ? { stroke: '#46a758' }
                : undefined,
          markerEnd: { type: MarkerType.ArrowClosed },
        };
      }),
    [edges, backEdges, runsByNode],
  );

  return (
    <div className="canvas">
      <ReactFlow nodes={rfNodes} edges={rfEdges} nodeTypes={nodeTypes} fitView proOptions={{ hideAttribution: true }}>
        <Background />
      </ReactFlow>
    </div>
  );
}
