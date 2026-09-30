import type { FlowEdge, FlowNode } from './types';

export function layoutNodes(nodes: FlowNode[], edges: FlowEdge[]) {
  const outgoing = new Map<string, string[]>();
  for (const edge of edges) {
    outgoing.set(edge.source, [...(outgoing.get(edge.source) ?? []), edge.target]);
  }

  // DFS 三色标记：遇到回路边（指向当前递归栈中的节点）跳过，
  // 带回路的流程也能得到稳定的分层，不会无限循环。
  const layer = new Map<string, number>();
  const state = new Map(nodes.map((n) => [n.id, 0]));
  const visit = (id: string): number => {
    const cached = layer.get(id);
    if (cached !== undefined) return cached;
    state.set(id, 1);
    let depth = 0;
    for (const next of outgoing.get(id) ?? []) {
      if (state.get(next) === 1) continue; // 回路边，不参与分层
      depth = Math.max(depth, visit(next) + 1);
    }
    state.set(id, 2);
    layer.set(id, depth);
    return depth;
  };
  nodes.forEach((n) => visit(n.id));

  // DFS 把起点放在最左层：上面算出的是到终点的距离，翻转成层级
  const maxLayer = Math.max(0, ...layer.values());
  const xLayer = new Map<string, number>();
  layer.forEach((l, id) => xLayer.set(id, maxLayer - l));
  const byLayer = new Map<number, string[]>();
  for (const node of nodes) {
    const l = xLayer.get(node.id) ?? 0;
    byLayer.set(l, [...(byLayer.get(l) ?? []), node.id]);
  }
  const positions = new Map<string, { x: number; y: number }>();
  for (const [l, ids] of byLayer) {
    ids.forEach((id, index) => {
      positions.set(id, { x: l * 230, y: index * 140 });
    });
  }
  return positions;
}
