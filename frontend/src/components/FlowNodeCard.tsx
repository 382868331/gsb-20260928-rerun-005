import { Handle, Position } from "@xyflow/react";
import { memo } from "react";
import type { NodeRecord } from "../api";

const KIND_LABEL: Record<string, string> = {
  START: "开始",
  END: "结束",
  ACTION: "动作",
  CONDITION: "条件分支",
  WAIT: "等待条件",
};

interface Props {
  data: {
    kind: string;
    label: string;
    action?: string;
    params?: Record<string, number | string>;
    expr?: string;
    signal?: string;
    record?: NodeRecord;
    progress?: number;
  };
}

function subtext(d: Props["data"]): string {
  if (d.record?.detail &&
      (d.record.status === "FAILED" || d.record.status === "WAITING")) {
    return d.record.detail;
  }
  if (d.kind === "ACTION") {
    const p = d.params ? JSON.stringify(d.params) : "";
    return `${d.action ?? ""} ${p}`.trim();
  }
  if (d.kind === "CONDITION") return d.expr ?? "";
  if (d.kind === "WAIT") return `信号: ${d.signal ?? "signal"}`;
  return "";
}

function FlowNodeCard({ data }: Props) {
  const status = data.record?.status;
  const progress = data.progress ??
    (status === "SUCCEEDED" ? 1 : 0);
  const showSuccess = data.kind !== "START";
  return (
    <div className={`node-card ${status ? `n-${status}` : ""}`}>
      <Handle type="target" position={Position.Left} />
      <div className="kind">{KIND_LABEL[data.kind] ?? data.kind}</div>
      <div className="label">{data.label}</div>
      <div className="sub">{subtext(data)}</div>
      {status === "RUNNING" && progress > 0 && (
        <div className="bar"><i style={{ width: `${progress * 100}%` }} /></div>
      )}
      {data.kind === "ACTION" && showSuccess && (
        <>
          <Handle id="success" type="source" position={Position.Top}
                  style={{ background: "#34d399" }} />
          <Handle id="failure" type="source" position={Position.Bottom}
                  style={{ background: "#f87171" }} />
        </>
      )}
      {data.kind === "CONDITION" && (
        <>
          <Handle id="true" type="source" position={Position.Top}
                  style={{ background: "#34d399" }} />
          <Handle id="false" type="source" position={Position.Bottom}
                  style={{ background: "#f87171" }} />
        </>
      )}
      {data.kind === "WAIT" && (
        <>
          <Handle id="success" type="source" position={Position.Top}
                  style={{ background: "#34d399" }} />
          <Handle id="timeout" type="source" position={Position.Bottom}
                  style={{ background: "#fbbf24" }} />
        </>
      )}
      {(data.kind === "START" || data.kind === "END") && (
        <Handle type="source" position={Position.Right} />
      )}
    </div>
  );
}

export default memo(FlowNodeCard);
