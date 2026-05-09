from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal


AgentStatus = Literal["queued", "running", "success", "failed"]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class AgentStepResult:
    agent_name: str
    status: AgentStatus
    output: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    error_message: str = ""
    started_at: str = ""
    completed_at: str = ""
    duration_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AgentRun:
    run_id: str
    customer_id: str
    task_type: str
    status: AgentStatus = "queued"
    input_snapshot: dict[str, Any] = field(default_factory=dict)
    output_report: dict[str, Any] = field(default_factory=dict)
    steps: list[dict[str, Any]] = field(default_factory=list)
    version_fingerprint: dict[str, Any] = field(default_factory=dict)
    error_message: str = ""
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    completed_at: str = ""
    output_path: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
