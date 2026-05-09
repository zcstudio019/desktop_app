from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


def build_agent_observability_summary(agent_run: dict[str, Any]) -> dict[str, Any]:
    steps = list(agent_run.get("steps") or [])
    output_report = agent_run.get("output_report") or agent_run.get("report") or {}
    risk_agent = output_report.get("risk_agent") or {}
    agents = []
    models: set[str] = set()
    warning_count = 0
    error_count = 0
    for step in steps:
        output = step.get("output") or {}
        model = output.get("model")
        if model:
            models.add(str(model))
        warnings = step.get("warnings") or output.get("warnings") or []
        warning_count += len(warnings)
        if step.get("status") == "failed" or step.get("error_message"):
            error_count += 1
        agents.append({
            "agent_name": step.get("agent_name") or "",
            "status": step.get("status") or "",
            "duration_ms": int(step.get("duration_ms") or 0),
            "llm_used": bool(output.get("llm_used")),
            "fallback_used": bool(output.get("fallback_used")),
            "retry_count": int(output.get("retry_count") or 0),
        })
    return {
        "run_id": agent_run.get("run_id") or "",
        "duration_ms": _run_duration_ms(agent_run),
        "step_count": len(steps),
        "llm_steps": sum(1 for item in agents if item["llm_used"]),
        "fallback_steps": sum(1 for item in agents if item["fallback_used"]),
        "warning_count": warning_count,
        "error_count": error_count,
        "risk_level": risk_agent.get("risk_level") or "",
        "used_model": sorted(models),
        "version_fingerprint": agent_run.get("version_fingerprint") or output_report.get("version_fingerprint") or {},
        "agents": agents,
    }


def save_agent_observability(agent_run: dict[str, Any], output_path: str | Path | None = None) -> str:
    summary = build_agent_observability_summary(agent_run)
    if output_path:
        run_dir = Path(output_path).parent
    else:
        customer_id = str(agent_run.get("customer_id") or "unknown")
        run_id = str(agent_run.get("run_id") or "run")
        run_dir = Path("data/agent_runs") / customer_id / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "observability.json"
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _run_duration_ms(agent_run: dict[str, Any]) -> int:
    try:
        started = datetime.fromisoformat(str(agent_run.get("created_at") or ""))
        completed = datetime.fromisoformat(str(agent_run.get("completed_at") or agent_run.get("updated_at") or ""))
        return max(0, int((completed - started).total_seconds() * 1000))
    except Exception:
        return sum(int(step.get("duration_ms") or 0) for step in agent_run.get("steps") or [])
