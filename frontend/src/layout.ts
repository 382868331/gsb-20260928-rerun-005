import type { FlowEdge, FlowNode } from './types';

export function layoutNodes(nodes: FlowNode[], edges: FlowEdge[]) {
  const indegree = new Map(nodes.map((n) => [n.id, 0]));
  const outgoing = new Map<string, string[]>();
  for (const edge of edges) {
    outgoing.set(edge.source, [...(outgoing.get(edge.source) ?? []), edge.target]);
    indegree.set(edge.target, (indegree.get(edge.target) ?? 0) + 1);
  }
  const layer = new Map<string, number>();
  const queue = nodes.filter((n) => (indegree.get(n.id) ?? 0) === 0).map((n) => n.id);
  queue.forEach((id) => layer.set(id, 0));
  while (queue.length) {
    const id = queue.shift()!;
    for (const next of outgoing.get(id) ?? []) {
      layer.set(next, Math.max(layer.get(next) ?? 0, (layer.get(id) ?? 0) + 1));
      queue.push(next);
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
      positions.set(id, { x: l * 230, y: index * 140 });
    });
  }
  return positions;
}
