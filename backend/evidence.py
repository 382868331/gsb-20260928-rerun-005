from __future__ import annotations

from dataclasses import dataclass, field


SUBJECTS = ("position", "holding", "action_result")
SUBJECT_LABELS = {
    "position": "位置",
    "holding": "持物",
    "action_result": "动作结果",
}


def normalize_position(value: str) -> tuple[float, float] | None:
    text = value.strip().strip("()（） ")
    if not text:
        return None
    for sep in (",", "，", "/"):
        if sep in text:
            left, _, right = text.partition(sep)
            try:
                return round(float(left.strip()), 3), round(float(right.strip()), 3)
            except ValueError:
                return None
    return None


def normalize_holding(value: str) -> str | None:
    text = value.strip().lower()
    if text in ("", "empty", "none", "null", "空", "无", "未持物", "空手"):
        return None
    if ":" in text:
        text = text.split(":", 1)[1].strip()
    if "：" in text:
        text = text.split("：", 1)[1].strip()
    return value.strip() or None


def normalize_action_result(value: str) -> str | None:
    text = value.strip().lower()
    if text in ("success", "succeeded", "成功", "完成", "ok"):
        return "success"
    if text in ("failure", "failed", "失败"):
        return "failure"
    return None


def normalize(subject: str, value: str):
    if subject == "position":
        return normalize_position(value)
    if subject == "holding":
        return normalize_holding(value)
    return normalize_action_result(value)


@dataclass
class SubjectResolution:
    subject: str
    status: str  # unknown / confirmed / disputed
    value: object | None = None
    claims: list[dict] = field(default_factory=list)
    evidence_ids: list[int] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "subject": self.subject,
            "label": SUBJECT_LABELS[self.subject],
            "status": self.status,
            "value": self.value,
            "claims": self.claims,
            "evidence_ids": self.evidence_ids,
        }


def resolve_subject(subject: str, entries: list[dict]) -> SubjectResolution:
    """归并同一主题下仍有效的证据条目。

    - 内容一致：confirmed；
    - 内容冲突：disputed（保持待定，不按到达先后自动覆盖）；
    - 无有效条目（全部撤回/被修订）：unknown。
    """
    active = [e for e in entries if e["subject"] == subject and e["status"] == "active"]
    if not active:
        return SubjectResolution(subject, "unknown")
    claims = []
    distinct: dict = {}
    for entry in active:
        norm = normalize(subject, entry["value"])
        claims.append({
            "evidence_id": entry["id"],
            "source": entry["source"],
            "observed_at": entry["observed_at"],
            "raw": entry["value"],
            "normalized": norm,
            "detail": entry.get("detail") or "",
        })
        distinct.setdefault(repr(norm), norm)
    if len(distinct) == 1:
        only = next(iter(distinct.values()))
        return SubjectResolution(
            subject, "confirmed", value=only, claims=claims,
            evidence_ids=[entry["id"] for entry in active])
    # 矛盾：无论观察时刻早晚，都保持待定，不能由最新一条覆盖现场观察。
    return SubjectResolution(
        subject, "disputed", value=None, claims=claims,
        evidence_ids=[entry["id"] for entry in active])


def resolve_ledger(entries: list[dict]) -> dict[str, dict]:
    return {s: resolve_subject(s, entries).as_dict() for s in SUBJECTS}


def blocked_subjects(resolution: dict[str, dict], needed: list[str]) -> list[dict]:
    """返回所需主题中尚不能作为决定依据的主题（待定或无证据）。"""
    blocked = []
    for subject in needed:
        item = resolution.get(subject)
        if item is None or item["status"] != "confirmed":
            blocked.append(item or {"subject": subject, "status": "unknown"})
    return blocked
