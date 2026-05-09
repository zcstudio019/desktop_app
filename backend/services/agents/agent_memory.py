from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def get_agent_output_dir() -> Path:
    return Path(os.getenv("AGENT_OUTPUT_DIR") or "data/agent_runs")


def _customer_dir(customer_id: str) -> Path:
    return get_agent_output_dir() / str(customer_id)


def save_agent_run(run: dict[str, Any]) -> str:
    customer_id = str(run.get("customer_id") or "unknown")
    run_id = str(run.get("run_id") or "run")
    out_dir = _customer_dir(customer_id) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "agent_run.json"
    payload = {**run, "output_path": str(path)}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_path = _customer_dir(customer_id) / "latest.json"
    latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def get_agent_run(run_id: str) -> dict[str, Any] | None:
    root = get_agent_output_dir()
    for path in root.glob(f"*/{run_id}/agent_run.json"):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def get_latest_agent_run(customer_id: str) -> dict[str, Any] | None:
    path = _customer_dir(str(customer_id)) / "latest.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


async def build_customer_ai_context(customer_id: str, storage_service: Any | None = None) -> dict[str, Any]:
    customer_profile: dict[str, Any] = {}
    documents: dict[str, Any] = {}
    credit_summary: dict[str, Any] = {}
    if storage_service is not None:
        try:
            customer_profile = await storage_service.get_customer(str(customer_id)) or {}
        except Exception:
            customer_profile = {}
        try:
            docs = await storage_service.list_documents(str(customer_id))
            documents = {"items": docs, "count": len(docs)}
        except Exception:
            documents = {"items": [], "count": 0}
        try:
            extractions = await storage_service.get_extractions_by_customer(str(customer_id))
            for item in extractions:
                data = item.get("extracted_data") or {}
                if item.get("extraction_type") == "enterprise_credit" or data.get("schema_version", "").startswith("enterprise_credit"):
                    credit_summary = data.get("credit_summary") or data.get("agent_result", {}).get("credit_summary") or {}
                    break
        except Exception:
            credit_summary = {}

    latest = get_latest_agent_run(str(customer_id)) or {}
    report = latest.get("output_report") or {}
    risk_agent = report.get("risk_agent") or {}
    judgement_agent = report.get("financing_judgement_agent") or {}
    return {
        "customer_profile": customer_profile,
        "documents": documents,
        "credit_summary": credit_summary,
        "latest_agent_report": report,
        "risks": (risk_agent.get("risks") or []),
        "missing_materials": ((report.get("missing_material_agent") or {}).get("required_materials") or []),
        "financing_judgement": (judgement_agent.get("judgement") or {}),
        "llm_used": {
            "risk_agent": bool(risk_agent.get("llm_used")),
            "financing_judgement_agent": bool(judgement_agent.get("llm_used")),
        },
        "compliance_disclaimer": judgement_agent.get("compliance_disclaimer") or "以上为资料初判，不构成贷款承诺，最终以银行审批为准。",
    }
