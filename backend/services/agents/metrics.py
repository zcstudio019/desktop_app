from __future__ import annotations

import json
from pathlib import Path
from statistics import mean
from typing import Any


def build_agent_regression_metrics(results: dict[str, Any]) -> dict[str, Any]:
    cases = list(results.get("cases") or [])
    total = len(cases)
    passed = sum(1 for case in cases if not case.get("failures"))
    drift_cases = [case for case in cases if (case.get("drift") or {}).get("has_drift")]
    critical_cases = [case for case in drift_cases if _has_severity(case, "critical")]
    high_cases = [case for case in drift_cases if _has_severity(case, "high")]
    risk_counts = [int((case.get("snapshot") or {}).get("risk_count") or 0) for case in cases]
    step_outputs = [step.get("output") or {} for case in cases for step in ((case.get("result") or {}).get("report") or {}).get("steps") or []]
    return {
        "total_cases": total,
        "passed_cases": passed,
        "failed_cases": total - passed,
        "drift_cases": len(drift_cases),
        "critical_drift_cases": len(critical_cases),
        "high_drift_cases": len(high_cases),
        "average_risk_count": round(mean(risk_counts), 2) if risk_counts else 0,
        "llm_usage_rate": _rate(step_outputs, "llm_used"),
        "fallback_rate": _rate(step_outputs, "fallback_used"),
        "validation_error_rate": _rate_any(step_outputs, "validation_errors"),
        "compliance_warning_rate": _rate_any(step_outputs, "compliance_warnings"),
        "average_retry_count": round(mean([int(item.get("retry_count") or 0) for item in step_outputs]), 2) if step_outputs else 0,
        "version_fingerprint": results.get("version_fingerprint") or {},
    }


def save_metrics(metrics: dict[str, Any], path: str | Path = "reports/agent_metrics/latest_metrics.json") -> str:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(output)


def _has_severity(case: dict[str, Any], severity: str) -> bool:
    return any((item.get("severity") == severity) for item in ((case.get("drift") or {}).get("changes") or []))


def _rate(items: list[dict[str, Any]], key: str) -> float:
    if not items:
        return 0
    return round(sum(1 for item in items if item.get(key)) / len(items), 4)


def _rate_any(items: list[dict[str, Any]], key: str) -> float:
    if not items:
        return 0
    return round(sum(1 for item in items if item.get(key)) / len(items), 4)
