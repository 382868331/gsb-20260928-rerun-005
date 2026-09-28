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
import type { Execution, FlowEdge, FlowNode } from './types';
import { layoutNodes } from './layout';

const TYPE_LABEL: Record<string, string> = {
  move: '移动',
  pick: '取物',
  place: '放置',
  wait: '等待',
  log: '播报',
};

interface NodeData extends Record<string, unknown> {
  node: FlowNode;
  runStatus?: string;
  detail?: string | null;
  waiting?: boolean;
}

function TaskNode({ data }: NodeProps<Node<NodeData>>) {
  const { node, runStatus, detail, waiting } = data;
  const cls = `task-node ${runStatus ?? 'idle'} ${waiting ? 'waiting-pulse' : ''}`;
  const params = node.type === 'move'
    ? `→(${node.params.x}, ${node.params.y})`
    : node.type === 'wait'
      ? `条件: ${node.params.condition}`
      : node.type === 'pick'
        ? `抓取 ${node.params.item ?? ''}`
        : node.type === 'log'
          ? String(node.params.message ?? '')
          : '放下物品';
  return (
    <div className={cls}>
      <Handle type="target" position={Position.Left} />
      <div className="task-node__type">{TYPE_LABEL[node.type] ?? node.type}</div>
      <div className="task-node__label">{node.label || node.id}</div>
      <div className="task-node__params" title={params}>{params}</div>
      <div className="task-node__status">
        {waiting && '⏳ 正在等待条件'}
        {runStatus === 'running' && !waiting && '▶ 执行中'}
        {runStatus === 'succeeded' && '✓ 完成'}
        {runStatus === 'failed' && `✗ 失败${detail ? `：${detail}` : ''}`}
        {runStatus === 'cancelled' && '⊘ 已取消'}
      </div>
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
  const positions = useMemo(() => layoutNodes(nodes, edges), [nodes, edges]);

  const latestRun = useMemo(() => {
    const map = new Map<string, { status: string; detail: string | null }>();
    execution?.node_runs.forEach((run) => map.set(run.node_id, run));
    return map;
  }, [execution]);

  const rfNodes: Node<NodeData>[] = useMemo(
    () =>
      nodes.map((node) => {
        const run = latestRun.get(node.id);
        return {
          id: node.id,
          type: 'task',
          position: positions.get(node.id) ?? { x: 0, y: 0 },
          data: {
            node,
            runStatus: run?.status,
            detail: run?.detail,
            waiting: execution?.waiting_for != null && node.type === 'wait' && run?.status === 'waiting',
          },
        };
      }),
    [nodes, positions, latestRun, execution],
  );

  const rfEdges: Edge[] = useMemo(
    () =>
      edges.map((edge) => {
        const run = latestRun.get(edge.source);
        const taken =
          (edge.kind === 'success' && run?.status === 'succeeded') ||
          (edge.kind === 'failure' && run?.status === 'failed');
        return {
          id: edge.id,
          source: edge.source,
          target: edge.target,
          sourceHandle: 'success',
          label: edge.kind === 'failure' ? '失败走向' : undefined,
          animated: taken,
          className: edge.kind === 'failure' ? 'edge-failure' : taken ? 'edge-taken' : '',
          style: edge.kind === 'failure'
            ? { stroke: '#e5484d' }
            : taken
              ? { stroke: '#46a758' }
              : undefined,
          markerEnd: { type: MarkerType.ArrowClosed },
        };
      }),
    [edges, latestRun],
  );

  return (
    <div className="canvas">
      <ReactFlow nodes={rfNodes} edges={rfEdges} nodeTypes={nodeTypes} fitView proOptions={{ hideAttribution: true }}>
        <Background />
      </ReactFlow>
    </div>
  );
}
