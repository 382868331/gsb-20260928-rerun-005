import type { FlowEdge, FlowNode } from './types';

export interface LayoutResult {
  positions: Map<string, { x: number; y: number }>;
  backEdges: Set<string>;
}

/** 分层布局：用 DFS 识别回边（流程回路），仅对无环边做最长路径分层，
 *  保证“失败后回到移动节点”这类带回路流程也能稳定打开、不卡死。 */
export function layoutNodes(nodes: FlowNode[], edges: FlowEdge[]): LayoutResult {
  const outgoing = new Map<string, string[]>();
  for (const edge of edges) {
    outgoing.set(edge.source, [...(outgoing.get(edge.source) ?? []), edge.target]);
  }
  const backByPair = new Set<string>();
  const state = new Map(nodes.map((n) => [n.id, 0])); // 0 未访问 1 栈中 2 完成
  const visit = (id: string) => {
    state.set(id, 1);
    for (const next of outgoing.get(id) ?? []) {
      if (state.get(next) === 1) {
        backByPair.add(`${next}->${id}`);
      } else if (state.get(next) === 0) {
        visit(next);
      }
    }
    state.set(id, 2);
  };
  for (const node of nodes) {
    if (state.get(node.id) === 0) visit(node.id);
  }

  const backEdges = new Set<string>();
  const acyclicOut = new Map<string, string[]>();
  const indegree = new Map(nodes.map((n) => [n.id, 0]));
  for (const edge of edges) {
    if (backByPair.has(`${edge.source}->${edge.target}`)) {
      backEdges.add(edge.id);
      continue;
    }
    acyclicOut.set(edge.source, [...(acyclicOut.get(edge.source) ?? []), edge.target]);
    indegree.set(edge.target, (indegree.get(edge.target) ?? 0) + 1);
  }

  const layer = new Map(nodes.map((n) => [n.id, 0]));
  const queue = nodes.filter((n) => (indegree.get(n.id) ?? 0) === 0).map((n) => n.id);
  while (queue.length) {
    const id = queue.shift()!;
    for (const next of acyclicOut.get(id) ?? []) {
      layer.set(next, Math.max(layer.get(next) ?? 0, (layer.get(id) ?? 0) + 1));
      indegree.set(next, (indegree.get(next) ?? 1) - 1);
      if (indegree.get(next) === 0) queue.push(next);
    }
  }

  const byLayer = new Map<number, string[]>();
  for (const node of nodes) {
    const l = layer.get(node.id) ?? 0;
    byLayer.set(l, [...(byLayer.get(l) ?? []), node.id]);
  }
  const positions = new Map<string, { x: number; y: number }>();
  for (const [l, ids] of byLayer) {
    ids.forEach((id, index) => {
      positions.set(id, { x: l * 230, y: index * 150 });
    });
  }
  return { positions, backEdges };
}
