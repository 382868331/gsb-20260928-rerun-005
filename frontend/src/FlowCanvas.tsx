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
import type { Execution, ExecEvent, FlowEdge, FlowNode } from './types';
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
  attempt?: number;
  history?: { attempt: number; status: string; detail: string | null }[];
  maxRetries?: number;
}

function TaskNode({ data }: NodeProps<Node<NodeData>>) {
  const { node, runStatus, detail, waiting, attempt, history = [], maxRetries } = data;
  const cls = `task-node ${runStatus ?? 'idle'} ${waiting ? 'waiting-pulse' : ''}`;
  const params = node.type === 'move'
    ? `→(${node.params.x}, ${node.params.y})`
    : node.type === 'wait'
      ? `条件: ${node.params.condition}`
      : node.type === 'pick'
        ? `抓取 ${node.params.item ?? ''}（最多 ${(maxRetries ?? 3) + 1} 次）`
      : node.type === 'log'
          ? String(node.params.message ?? '')
          : node.type === 'manual'
            ? String(node.params.instruction ?? '等待人工处置')
          : '放下物品';
  const statusText =
    waiting && '⏳ 正在等待条件'
    || runStatus === 'running' && `▶ 第 ${attempt} 次执行中`
    || runStatus === 'manual' && '✋ 等待人工处置'
    || runStatus === 'succeeded' && (attempt && attempt > 1 ? `✓ 第 ${attempt} 次成功` : '✓ 完成')
    || runStatus === 'failed' && `✗ 第 ${attempt} 次失败${detail ? `：${detail}` : ''}`
    || runStatus === 'cancelled' && '⊘ 已取消'
    || runStatus === 'aborted' && '⊘ 人工终止'
    || '';
  return (
    <div className={cls}>
      <Handle type="target" position={Position.Left} />
      <div className="task-node__type">{TYPE_LABEL[node.type] ?? node.type}</div>
      <div className="task-node__label">{node.label || node.id}</div>
      <div className="task-node__params" title={params}>{params}</div>
      {history.length > 0 && (
        <div className="task-node__history" title="历史经过（不代表本次状态）">
          {history.map((run) => (
            <span key={run.attempt} className={`attempt-chip attempt-${run.status}`} title={run.detail ?? ''}>
              第{run.attempt}次{run.status === 'succeeded' ? '✓' : run.status === 'cancelled' ? '⊘' : '✗'}
            </span>
          ))}
        </div>
      )}
      <div className="task-node__status">{statusText}</div>
      <Handle type="source" position={Position.Right} id="success" />
    </div>
  );
}

const nodeTypes = { task: TaskNode };

interface Props {
  nodes: FlowNode[];
  edges: FlowEdge[];
  execution: Execution | null;
  events: ExecEvent[];
}

export default function FlowCanvas({ nodes, edges, execution, events }: Props) {
  const positions = useMemo(() => layoutNodes(nodes, edges), [nodes, edges]);

  const runsByNode = useMemo(() => {
    const map = new Map<string, NonNullable<typeof execution>['node_runs']>();
    execution?.node_runs.forEach((run) => {
      map.set(run.node_id, [...(map.get(run.node_id) ?? []), run]);
    });
    return map;
  }, [execution]);
  const currentNode = execution?.current_node ?? null;
  const currentAttempt = execution?.current_attempt ?? 0;

  const rfNodes: Node<NodeData>[] = useMemo(
    () =>
      nodes.map((node) => {
        const runs = runsByNode.get(node.id) ?? [];
        const isCurrent = currentNode === node.id;
        const run = isCurrent
          ? runs.find((r) => r.attempt === currentAttempt)
          : runs[runs.length - 1];
        const history = isCurrent
          ? runs.filter((r) => r.attempt !== currentAttempt)
          : runs.slice(0, -1);
        return {
          id: node.id,
          type: 'task',
          position: positions.get(node.id) ?? { x: 0, y: 0 },
          data: {
            node,
            runStatus: run?.status,
            detail: run?.detail,
            waiting: execution?.waiting_for != null && isCurrent && run?.status === 'waiting',
            attempt: run?.attempt,
            history,
            maxRetries: node.params.max_retries as number | undefined,
          },
        };
      }),
    [nodes, positions, runsByNode, currentNode, currentAttempt, execution],
  );

  const rfEdges: Edge[] = useMemo(
    () => {
      // 以实际发生的分支事件为准：旧回路的走向不会被新一轮状态覆盖
      const takenKeys = new Set(
        events.flatMap((event) => {
          if (event.type === 'branch_taken' && event.data?.edge && event.node_id) {
            return [`${event.node_id}:${event.data.edge}`];
          }
          if (event.type === 'retries_exhausted' && event.node_id) {
            return [`${event.node_id}:exhausted`];
          }
          if (event.type === 'node_succeeded' && event.node_id) {
            return [`${event.node_id}:success`];
          }
          return [];
        }),
      );
      return edges.map((edge) => {
        const taken = takenKeys.has(`${edge.source}:${edge.kind}`);
        return {
          id: edge.id,
          source: edge.source,
          target: edge.target,
          sourceHandle: 'success',
          label: edge.kind === 'failure' ? '失败重试' : edge.kind === 'exhausted' ? '超过上限' : undefined,
          animated: taken,
          className: edge.kind === 'failure'
            ? 'edge-failure'
            : edge.kind === 'exhausted' ? 'edge-exhausted' : taken ? 'edge-taken' : '',
          style: !taken
            ? undefined
            : edge.kind === 'failure'
              ? { stroke: '#e5484d' }
              : edge.kind === 'exhausted'
                ? { stroke: '#b91c1c', strokeWidth: 2 }
                : { stroke: '#46a758' },
          markerEnd: { type: MarkerType.ArrowClosed },
        };
      });
    },
    [edges, events],
  );

  return (
    <div className="canvas">
      <ReactFlow nodes={rfNodes} edges={rfEdges} nodeTypes={nodeTypes} fitView proOptions={{ hideAttribution: true }}>
        <Background />
      </ReactFlow>
    </div>
  );
}
