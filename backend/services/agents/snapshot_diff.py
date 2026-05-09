from __future__ import annotations

import json
from typing import Any

from .compliance_guard import BLOCKED_TERMS, DISCLAIMER
from .versioning import build_agent_version_fingerprint, stable_fingerprint_subset


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _risk_items(report: dict[str, Any]) -> list[dict[str, Any]]:
    return list((report.get("risk_agent") or {}).get("risks") or [])


def _missing_required(report: dict[str, Any]) -> list[str]:
    items = (report.get("missing_material_agent") or {}).get("required_materials") or []
    values = [
        str(item.get("material") or item.get("name") or item.get("document_type") or "")
        for item in items
        if isinstance(item, dict)
    ]
    return sorted(x for x in values if x)


def _missing_optional(report: dict[str, Any]) -> list[str]:
    items = (report.get("missing_material_agent") or {}).get("optional_materials") or []
    values = [
        str(item.get("material") or item.get("name") or item.get("document_type") or "")
        for item in items
        if isinstance(item, dict)
    ]
    return sorted(x for x in values if x)


def _judgement(report: dict[str, Any]) -> dict[str, Any]:
    return (report.get("financing_judgement_agent") or {}).get("judgement") or {}


def _step_output(report: dict[str, Any], agent_name: str) -> dict[str, Any]:
    for step in report.get("steps") or []:
        if step.get("agent_name") == agent_name:
            return step.get("output") or {}
    key = {
        "RiskAgent": "risk_agent",
        "FinancingJudgementAgent": "financing_judgement_agent",
    }.get(agent_name, agent_name)
    return report.get(key) or {}


def build_agent_snapshot(report: dict[str, Any], case_name: str) -> dict[str, Any]:
    risk = report.get("risk_agent") or {}
    risk_items = _risk_items(report)
    judgement_agent = report.get("financing_judgement_agent") or {}
    judgement = _judgement(report)
    all_text = _dump(report)
    risk_step = _step_output(report, "RiskAgent")
    financing_step = _step_output(report, "FinancingJudgementAgent")
    fingerprint = report.get("version_fingerprint") or build_agent_version_fingerprint()
    return {
        "case_name": case_name,
        "risk_level": risk.get("risk_level") or "unknown",
        "risk_types": sorted({
            str(item.get("risk_type") or item.get("description") or "")
            for item in risk_items
            if isinstance(item, dict) and (item.get("risk_type") or item.get("description"))
        }),
        "risk_count": len(risk_items),
        "missing_required_materials": _missing_required(report),
        "missing_optional_materials": _missing_optional(report),
        "estimated_amount_range": str(judgement.get("estimated_amount_range") or ""),
        "strengths_count": len(judgement.get("strengths") or []),
        "weaknesses_count": len(judgement.get("weaknesses") or []),
        "next_actions_count": len(judgement.get("next_actions") or []),
        "has_disclaimer": DISCLAIMER in _dump(judgement_agent),
        "forbidden_terms_found": sorted(term for term in BLOCKED_TERMS if term in all_text),
        "risk_agent_llm_used": bool(risk_step.get("llm_used")),
        "risk_agent_fallback_used": bool(risk_step.get("fallback_used")),
        "financing_agent_llm_used": bool(financing_step.get("llm_used")),
        "financing_agent_fallback_used": bool(financing_step.get("fallback_used")),
        "version_fingerprint": stable_fingerprint_subset(fingerprint),
    }


def diff_agent_snapshot(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    case_name = new.get("case_name") or old.get("case_name") or ""
    changes: list[dict[str, Any]] = []

    def add_change(field: str, severity: str, **payload: Any) -> None:
        changes.append({"field": field, "severity": severity, **payload})

    if old.get("risk_level") != new.get("risk_level"):
        add_change("risk_level", "critical", old=old.get("risk_level"), new=new.get("risk_level"))

    if old.get("has_disclaimer") is True and new.get("has_disclaimer") is False:
        add_change("has_disclaimer", "critical", old=True, new=False)

    old_forbidden = set(old.get("forbidden_terms_found") or [])
    new_forbidden = set(new.get("forbidden_terms_found") or [])
    if not old_forbidden and new_forbidden:
        add_change("forbidden_terms_found", "critical", added=sorted(new_forbidden))
    elif old_forbidden != new_forbidden:
        add_change(
            "forbidden_terms_found",
            "high",
            added=sorted(new_forbidden - old_forbidden),
            removed=sorted(old_forbidden - new_forbidden),
        )

    _diff_list_removed_high(old, new, "risk_types", changes)
    _diff_list_removed_high(old, new, "missing_required_materials", changes)

    for field in ["risk_agent_fallback_used", "financing_agent_fallback_used"]:
        if old.get(field) is False and new.get(field) is True:
            add_change(field, "high", old=False, new=True)

    for field in ["risk_count", "estimated_amount_range", "next_actions_count"]:
        if old.get(field) != new.get(field):
            add_change(field, "medium", old=old.get(field), new=new.get(field))

    for field in ["strengths_count", "weaknesses_count"]:
        if old.get(field) != new.get(field):
            add_change(field, "low", old=old.get(field), new=new.get(field))

    for field in ["risk_agent_llm_used", "financing_agent_llm_used"]:
        if old.get(field) != new.get(field):
            add_change(field, "low", old=old.get(field), new=new.get(field))

    possible_causes = _possible_causes(old.get("version_fingerprint") or {}, new.get("version_fingerprint") or {}, bool(changes))
    return {
        "case_name": case_name,
        "has_drift": bool(changes),
        "changes": changes,
        "possible_causes": possible_causes,
    }


def _diff_list_removed_high(old: dict[str, Any], new: dict[str, Any], field: str, changes: list[dict[str, Any]]) -> None:
    old_values = set(old.get(field) or [])
    new_values = set(new.get(field) or [])
    added = sorted(new_values - old_values)
    removed = sorted(old_values - new_values)
    if removed:
        changes.append({"field": field, "added": added, "removed": removed, "severity": "high"})
    elif added:
        changes.append({"field": field, "added": added, "removed": [], "severity": "low"})


def _possible_causes(old_fp: dict[str, Any], new_fp: dict[str, Any], has_drift: bool) -> list[str]:
    if not has_drift:
        return []

    causes: list[str] = []
    if (old_fp.get("prompt_hashes") or {}) != (new_fp.get("prompt_hashes") or {}):
        causes.append("prompt_changed")
    if (old_fp.get("schema_hashes") or {}) != (new_fp.get("schema_hashes") or {}):
        causes.append("schema_changed")
    if (old_fp.get("rule_hashes") or {}) != (new_fp.get("rule_hashes") or {}):
        causes.append("rule_changed")
    if (old_fp.get("model_config") or {}) != (new_fp.get("model_config") or {}):
        causes.append("model_changed")
    if (old_fp.get("compliance_guard_hash") or "unknown") != (new_fp.get("compliance_guard_hash") or "unknown"):
        causes.append("compliance_guard_changed")
    if has_drift and not causes:
        causes.append("llm_nondeterminism_or_data_change")
    return causes
