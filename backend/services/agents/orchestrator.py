from __future__ import annotations

import os
import uuid
from copy import deepcopy
from typing import Any

from backend.services import get_storage_service

from .agent_logger import log_step
from .agent_memory import save_agent_run
from .credit_analysis_agent import CreditAnalysisAgent
from .document_agent import DocumentAgent
from .financing_judgement_agent import FinancingJudgementAgent
from .missing_material_agent import MissingMaterialAgent
from .observability import save_agent_observability
from .risk_agent import RiskAgent
from .schemas import AgentRun, utc_now
from .versioning import build_agent_version_fingerprint


def financing_agent_enabled() -> bool:
    return os.getenv("ENABLE_FINANCING_AGENT", "false").lower() in {"1", "true", "yes", "on"}


async def _load_customer_context(customer_id: str) -> dict[str, Any]:
    load_warnings: list[str] = []
    try:
        storage = get_storage_service()
    except Exception as exc:
        return {
            "customer": {},
            "documents": [],
            "extractions": [],
            "customer_profile": {},
            "enterprise_credit": {},
            "steps": {},
            "load_warnings": [f"存储服务初始化失败：{exc}"],
        }
    try:
        customer = await storage.get_customer(str(customer_id))
    except Exception as exc:
        customer = {}
        load_warnings.append(f"客户信息读取失败：{exc}")
    try:
        documents = await storage.list_documents(str(customer_id)) if hasattr(storage, "list_documents") else []
    except Exception as exc:
        documents = []
        load_warnings.append(f"资料列表读取失败：{exc}")
    try:
        extractions = await storage.get_extractions_by_customer(str(customer_id)) if hasattr(storage, "get_extractions_by_customer") else []
    except Exception as exc:
        extractions = []
        load_warnings.append(f"解析结果读取失败：{exc}")
    try:
        profile = await storage.get_customer_profile(str(customer_id)) if hasattr(storage, "get_customer_profile") else None
    except Exception as exc:
        profile = {}
        load_warnings.append(f"资料汇总读取失败：{exc}")

    enterprise_credit: dict[str, Any] = {}
    for item in extractions or []:
        data = item.get("extracted_data") or {}
        if item.get("extraction_type") == "enterprise_credit" or str(data.get("schema_version") or "").startswith("enterprise_credit"):
            enterprise_credit = data
            break

    return {
        "customer": customer or {},
        "documents": documents or [],
        "extractions": extractions or [],
        "customer_profile": profile or {},
        "enterprise_credit": enterprise_credit,
        "steps": {},
        "load_warnings": load_warnings,
    }


def _build_report(context: dict[str, Any], steps: list[dict[str, Any]], version_fingerprint: dict[str, Any]) -> dict[str, Any]:
    step_outputs = {step.get("agent_name"): step.get("output") or {} for step in steps}
    return {
        "customer": context.get("customer") or {},
        "document_agent": step_outputs.get("DocumentAgent") or {},
        "credit_analysis_agent": step_outputs.get("CreditAnalysisAgent") or {},
        "risk_agent": step_outputs.get("RiskAgent") or {},
        "missing_material_agent": step_outputs.get("MissingMaterialAgent") or {},
        "financing_judgement_agent": step_outputs.get("FinancingJudgementAgent") or {},
        "version_fingerprint": version_fingerprint,
        "steps": steps,
    }


def _normalize_workflow_context(context: dict[str, Any]) -> dict[str, Any]:
    source = deepcopy(context or {})
    documents = source.get("documents") or []
    if isinstance(documents, dict):
        documents = documents.get("items") or []
    latest_report = source.get("latest_agent_report") or {}
    enterprise_credit = source.get("enterprise_credit") or {}
    if not enterprise_credit:
        enterprise_credit = {
            "credit_summary": source.get("credit_summary") or {},
            "active_loans": source.get("active_loans") or latest_report.get("active_loans") or [],
            "credit_facilities": source.get("credit_lines") or latest_report.get("credit_lines") or [],
        }
    return {
        "customer": source.get("customer") or source.get("customer_profile") or {},
        "documents": documents,
        "extractions": source.get("extractions") or [],
        "customer_profile": source.get("customer_profile") or {},
        "enterprise_credit": enterprise_credit,
        "steps": {},
        "load_warnings": source.get("load_warnings") or [],
    }


async def _execute_agent_workflow(*, run: AgentRun, context: dict[str, Any]) -> AgentRun:
    run.input_snapshot = {
        "customer_id": str(run.customer_id),
        "document_count": len(context.get("documents") or []),
        "extraction_count": len(context.get("extractions") or []),
        "has_enterprise_credit": bool(context.get("enterprise_credit")),
        "load_warnings": context.get("load_warnings") or [],
    }
    run.version_fingerprint = build_agent_version_fingerprint()
    context["_debug"] = {"customer_id": str(run.customer_id), "run_id": run.run_id}
    agents = [
        DocumentAgent(),
        CreditAnalysisAgent(),
        RiskAgent(),
        MissingMaterialAgent(),
        FinancingJudgementAgent(),
    ]
    for order, agent in enumerate(agents, start=1):
        log_step(run.run_id, agent.agent_name, "running", {"order": order})
        step = await agent.run(context)
        step_dict = step.to_dict()
        step_dict["step_order"] = order
        run.steps.append(step_dict)
        context.setdefault("steps", {})[agent.agent_name] = step.output
        log_step(run.run_id, agent.agent_name, step.status, {"warnings": step.warnings, "error": step.error_message})
    failed = [step for step in run.steps if step.get("status") == "failed"]
    run.status = "failed" if failed else "success"
    run.output_report = _build_report(context, run.steps, run.version_fingerprint)
    run.error_message = "; ".join(step.get("error_message") or "" for step in failed if step.get("error_message"))
    return run


async def run_financing_agent_workflow(customer_id: int | str, task_type: str = "full") -> dict[str, Any]:
    if not financing_agent_enabled():
        return {
            "success": False,
            "status": "disabled",
            "error_message": "ENABLE_FINANCING_AGENT=false，融资 Agent 当前未开启",
        }

    run = AgentRun(
        run_id=uuid.uuid4().hex,
        customer_id=str(customer_id),
        task_type=task_type or "full",
        status="running",
    )
    try:
        context = await _load_customer_context(str(customer_id))
        run = await _execute_agent_workflow(run=run, context=context)
    except Exception as exc:
        run.status = "failed"
        run.error_message = str(exc)
        run.output_report = {"customer_id": str(customer_id), "error_message": str(exc)}
    finally:
        run.updated_at = utc_now()
        run.completed_at = utc_now()
        run.output_path = save_agent_run(run.to_dict())
        save_agent_observability(run.to_dict(), run.output_path)

    return {
        "success": run.status == "success",
        "run_id": run.run_id,
        "status": run.status,
        "report": run.output_report,
        "steps": run.steps,
        "error_message": run.error_message,
        "output_path": run.output_path,
    }


async def run_financing_agent_workflow_from_context(
    context: dict[str, Any],
    task_type: str = "full",
    use_llm: bool = False,
) -> dict[str, Any]:
    old_llm = os.environ.get("ENABLE_AGENT_LLM")
    os.environ["ENABLE_AGENT_LLM"] = "true" if use_llm else "false"
    run = AgentRun(
        run_id=uuid.uuid4().hex,
        customer_id=str((context.get("customer_profile") or context.get("customer") or {}).get("customer_id") or "fixture"),
        task_type=task_type or "full",
        status="running",
    )
    try:
        normalized_context = _normalize_workflow_context(context)
        run = await _execute_agent_workflow(run=run, context=normalized_context)
    except Exception as exc:
        run.status = "failed"
        run.error_message = str(exc)
        run.output_report = {"customer_id": run.customer_id, "error_message": str(exc)}
    finally:
        run.updated_at = utc_now()
        run.completed_at = utc_now()
        run.output_path = save_agent_run(run.to_dict())
        save_agent_observability(run.to_dict(), run.output_path)
        if old_llm is None:
            os.environ.pop("ENABLE_AGENT_LLM", None)
        else:
            os.environ["ENABLE_AGENT_LLM"] = old_llm
    return {
        "success": run.status == "success",
        "run_id": run.run_id,
        "status": run.status,
        "report": run.output_report,
        "steps": run.steps,
        "error_message": run.error_message,
        "output_path": run.output_path,
    }
