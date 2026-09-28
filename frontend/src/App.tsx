import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Background,
  Controls,
  ReactFlow,
  type Edge,
  type Node,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import {
  api,
  type EventRow,
  type FlowDefinition,
  type NodeRecord,
  type RobotState,
  type RunDetail,
  type RunRow,
  type RunStatus,
} from "./api";
import FlowNodeCard from "./components/FlowNodeCard";
import Scene from "./components/Scene";

const WF_ID = "pickup-demo";
const nodeTypes = { card: FlowNodeCard };
const TERMINAL: RunStatus[] = ["SUCCEEDED", "FAILED", "CANCELLED", "ERROR"];

const BRANCH_LABELS: Record<string, string> = {
  success: "成功", failure: "失败 →", "true": "是", "false": "否",
  timeout: "超时",
};

export default function App() {
  const [flow, setFlow] = useState<FlowDefinition | null>(null);
  const [wfName, setWfName] = useState("");
  const [runId, setRunId] = useState<string | null>(null);
  const [detail, setDetail] = useState<RunDetail | null>(null);
  const [runs, setRuns] = useState<RunRow[]>([]);
  const [robot, setRobot] = useState<RobotState | null>(null);
  const [events, setEvents] = useState<EventRow[]>([]);
  const [progress, setProgress] = useState<Record<string, number>>({});
  const seqRef = useRef(0);

  const loadList = useCallback(() => {
    api.runs().then((r) => setRuns(r.runs)).catch(() => {});
  }, []);

  useEffect(() => {
    api.workflow(WF_ID).then((w) => {
      setFlow(w.definition);
      setWfName(w.name);
    });
    api.robot().then(setRobot).catch(() => {});
    loadList();
  }, [loadList]);

  const applyDetail = useCallback((d: RunDetail) => {
    setDetail(d);
    setRobot(d.robot);
    setEvents(d.events);
    seqRef.current = d.events.length ? d.events[d.events.length - 1].seq : 0;
  }, []);

  const openRun = useCallback((id: string) => {
    setRunId(id);
    seqRef.current = 0;
    api.run(id).then(applyDetail).catch(() => {});
  }, [applyDetail]);

  // SSE：事件只做增量通知，权威状态回源到 /api/runs/:id
  useEffect(() => {
    if (!runId) return;
    let cancelled = false;
    let es: EventSource | null = null;
    let timer: number | undefined;

    const refresh = () => api.run(runId).then((d) => {
      if (cancelled) return;
      applyDetail(d);
      if (TERMINAL.includes(d.run.status)) {
        loadList();
      }
    }).catch(() => {});

    const connect = () => {
      es = new EventSource(
        `/api/runs/${runId}/events?after=${seqRef.current}`);
      es.onmessage = (ev) => {
        try {
          const data = JSON.parse(ev.data) as EventRow;
          setEvents((prev) => {
            if (prev.some((e) => e.seq === data.seq)) return prev;
            const next = [...prev, data];
            seqRef.current = data.seq;
            return next.slice(-300);
          });
          if (data.kind === "robot") {
            const p = data.payload as unknown as Partial<RobotState> &
              { progress?: number };
            const nodeId: string | null = data.node_id;
            if (nodeId && typeof p.progress === "number") {
              const prog = p.progress;
              setProgress((prev) => ({ ...prev, [nodeId]: prog }));
            }
            if (Array.isArray(p.pos)) setRobot(p as RobotState);
          }
        } catch { /* ignore */ }
        // 节流回源
        if (!timer) {
          timer = window.setTimeout(() => { timer = undefined; refresh(); }, 250);
        }
      };
      es.onerror = () => {
        es?.close();
        if (!cancelled) setTimeout(connect, 1000);
      };
    };
    refresh();
    connect();
    const poll = window.setInterval(refresh, 2000);
    return () => {
      cancelled = true;
      es?.close();
      window.clearInterval(poll);
      if (timer) window.clearTimeout(timer);
    };
  }, [runId, applyDetail, loadList]);

  const records: Record<string, NodeRecord> = useMemo(() => {
    const m: Record<string, NodeRecord> = {};
    for (const n of detail?.nodes ?? []) m[n.node_id] = n;
    return m;
  }, [detail]);

  const rfNodes: Node[] = useMemo(() => {
    if (!flow) return [];
    return flow.nodes.map((n) => ({
      id: n.id,
      type: "card",
      position: n.position,
      data: {
        ...n.data,
        record: records[n.id],
        progress: progress[n.id],
      },
      draggable: false,
    }));
  }, [flow, records, progress]);

  const rfEdges: Edge[] = useMemo(() => {
    if (!flow) return [];
    const failed = new Set(Object.values(records)
      .filter((r) => r.status === "FAILED")
      .map((r) => r.node_id));
    return flow.edges.map((e) => {
      const branch = e.sourceHandle;
      const fromFailed = failed.has(e.source);
      const taken = detail?.run.status &&
        records[e.source]?.status === "SUCCEEDED";
      let stroke = "#3b4868";
      if (fromFailed && branch === "failure") stroke = "#f87171";
      else if (fromFailed && branch === "success") stroke = "#33405f";
      else if (taken) stroke = branch === "failure" ? "#fbbf24" : "#34d399";
      return {
        id: e.id,
        source: e.source,
        target: e.target,
        sourceHandle: e.sourceHandle,
        animated: stroke !== "#3b4868" && stroke !== "#33405f",
        label: branch ? BRANCH_LABELS[branch] ?? branch : undefined,
        style: { stroke, strokeWidth: stroke === "#3b4868" ? 1.5 : 2.5 },
      };
    });
  }, [flow, records, detail]);

  const start = async () => {
    setProgress({});
    setEvents([]);
    const { run_id } = await api.startRun(WF_ID);
    setRunId(run_id);
    seqRef.current = 0;
    loadList();
  };

  const run = detail?.run;
  const active = run && !TERMINAL.includes(run.status);
  const waitingNode = Object.values(records).find(
    (r) => r.status === "WAITING");

  const onCellClick = async (x: number, y: number) => {
    const key = `${x},${y}`;
    const isOb = (robot?.obstacles ?? []).some(
      ([ox, oy]) => ox === x && oy === y);
    const next = isOb
      ? await api.clearObstacle(x, y)
      : await api.obstacle(x, y);
    setRobot(next);
    // 若正有等待节点在等障碍清除，同时发信号放行
    if (isOb && runId && waitingNode) {
      await api.signal(runId,
        flow?.nodes.find((n) => n.id === waitingNode.node_id)
          ?.data.signal ?? "obstacle_cleared",
        { x, y });
    }
  };

  return (
    <div className="app">
      <div className="topbar">
        <h1>🤖 机器人任务编排平台</h1>
        <span style={{ color: "var(--muted)" }}>{wfName}</span>
        <div className="spacer" />
        <button className="primary" onClick={start}>启动新执行</button>
        <button onClick={() => api.resetRobot().then(setRobot)}>
          重置机器人
        </button>
      </div>

      <div className="legend">
        <span><i style={{ background: "#4f8cff" }} />执行中</span>
        <span><i style={{ background: "#a78bfa" }} />等待条件</span>
        <span><i style={{ background: "#34d399" }} />成功</span>
        <span><i style={{ background: "#f87171" }} />失败</span>
        <span><i style={{ background: "#fbbf24" }} />走向分支</span>
        <span>点击网格可放置/清除障碍 ▲；清除障碍会放行“等待障碍清除”节点</span>
      </div>

      <div className="main">
        <div className="center">
          <div className="scene-wrap">
            <Scene robot={robot} onCellClick={onCellClick} />
          </div>
          <div className="flow-wrap">
            {flow && (
              <ReactFlow
                nodes={rfNodes}
                edges={rfEdges}
                nodeTypes={nodeTypes}
                nodesConnectable={false}
                fitView
                proOptions={{ hideAttribution: true }}
              >
                <Background color="#1c2740" gap={20} />
                <Controls showInteractive={false} />
              </ReactFlow>
            )}
          </div>
        </div>

        <div className="side">
          <h2>执行控制</h2>
          {run ? (
            <>
              <div className="run-meta">
                <div style={{ display: "flex", justifyContent:
                  "space-between", alignItems: "center" }}>
                  <code>{run.id}</code>
                  <span className={`status-pill st-${run.status}`}>
                    {run.status}
                  </span>
                </div>
                <div className="detail">{run.detail ?? ""}</div>
                <div className="detail">
                  当前节点: {run.current_node_id ?? "-"}
                </div>
                {robot && (
                  <div className="detail">
                    位置 ({robot.pos[0]},{robot.pos[1]}) ·
                    电量 {robot.battery}% ·
                    {robot.carrying ? "携带货物" : "空手"}
                  </div>
                )}
              </div>
              <div className="controls">
                <button disabled={run.status !== "RUNNING"}
                        onClick={() => runId && api.pause(runId)}>
                  暂停
                </button>
                <button
                  disabled={run.status !== "PAUSED"}
                  onClick={() => runId && api.resume(runId)}>
                  恢复
                </button>
                <button className="danger" disabled={!active}
                        onClick={() => runId && api.cancel(runId)}>
                  取消（动作收尾后停止）
                </button>
              </div>
              {waitingNode && (
                <div className="hint" style={{ color: "#c4b5fd" }}>
                  ⏸ {waitingNode.detail}
                  <div style={{ marginTop: 6 }}>
                    <button onClick={() => runId && api.signal(
                      runId,
                      flow?.nodes.find((n) => n.id === waitingNode.node_id)
                        ?.data.signal ?? "signal", true)}>
                      手动发送信号放行
                    </button>
                  </div>
                </div>
              )}
            </>
          ) : (
            <div className="hint">尚未选择执行，点击“启动新执行”。</div>
          )}

          <h3>执行事件（服务端持久化）</h3>
          <div className="log">
            {events.map((e) => (
              <div className="ev" key={e.seq}>
                <span className="seq">{e.seq}</span>
                <span className={`k k-${e.kind}`}>{e.kind}</span>
                <span>
                  {e.node_id ? `[${e.node_id}] ` : ""}
                  {formatPayload(e)}
                </span>
              </div>
            ))}
          </div>

          <h3>历史执行（刷新页面仍在）</h3>
          <div className="runs-list">
            {runs.map((r) => (
              <div key={r.id}
                   className={`item ${r.id === runId ? "active" : ""}`}
                   onClick={() => openRun(r.id)}>
                <span className="name">{r.id} · {r.workflow_name}</span>
                <span className={`status-pill st-${r.status}`}>
                  {r.status}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function formatPayload(e: EventRow): string {
  const p = e.payload ?? {};
  if (e.kind === "node")
    return `${p.status ?? ""}${p.detail ? ` · ${p.detail}` : ""}`;
  if (e.kind === "branch") return `走向: ${p.branch}`;
  if (e.kind === "run") return JSON.stringify(p);
  if (e.kind === "robot") {
    const pos = Array.isArray(p.pos) ? `(${p.pos[0]},${p.pos[1]})` : "";
    const prog = typeof p.progress === "number"
      ? ` ${Math.round(p.progress * 100)}%` : "";
    return `${pos}${prog}${p.detail ? ` · ${p.detail}` : ""}`.trim();
  }
  if (e.kind === "signal") return `信号 ${JSON.stringify(p)}`;
  return JSON.stringify(p);
}
