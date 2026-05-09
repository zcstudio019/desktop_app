from __future__ import annotations

import asyncio
import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.ai_service import AIService

from .compliance_guard import check_financing_compliance
from .json_guard import parse_json_safely, validate_agent_output
from .sanitize import sanitize_for_debug


def agent_llm_enabled() -> bool:
    return os.getenv("ENABLE_AGENT_LLM", "false").lower() in {"1", "true", "yes", "on"}


async def run_agent_llm_json(
    *,
    agent_name: str,
    system_prompt: str,
    user_payload: dict[str, Any],
    json_schema: dict[str, Any],
    max_retries: int = 2,
    fallback_output: dict[str, Any] | None = None,
    debug_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fallback = deepcopy(fallback_output or {})
    fallback.setdefault("agent_name", agent_name)
    fallback.setdefault("status", "success")
    fallback.setdefault("warnings", [])

    if not agent_llm_enabled():
        fallback.update({
            "llm_used": False,
            "fallback_used": True,
            "fallback_reason": "ENABLE_AGENT_LLM=false",
            "retry_count": 0,
        })
        return fallback

    retries = int(os.getenv("AGENT_LLM_MAX_RETRIES") or max_retries or 2)
    timeout = float(os.getenv("AGENT_LLM_TIMEOUT") or 60)
    model = os.getenv("AGENT_LLM_MODEL") or "deepseek-chat"
    errors: list[str] = []
    debug_records: list[dict[str, Any]] = []
    ai_service = AIService()

    payload_text = json.dumps(
        {"agent_name": agent_name, "input": user_payload, "json_schema": json_schema},
        ensure_ascii=False,
        indent=2,
    )

    for attempt in range(retries + 1):
        prompt = (
            system_prompt
            + "\n\n你必须只返回一个 JSON object，不要输出 Markdown，不要解释。"
            + "\n如果字段无法判断，使用 unknown、null 或空数组。"
            + f"\n这是第 {attempt + 1} 次尝试。"
            + ("\n上一次错误：" + " | ".join(errors[-3:]) if errors else "")
        )
        try:
            raw = await asyncio.to_thread(
                ai_service.extract,
                prompt,
                payload_text,
                model,
                timeout,
                4096,
            )
            parsed, parse_error = parse_json_safely(raw or "")
            debug_record = _build_debug_record(
                agent_name=agent_name,
                attempt=attempt + 1,
                model=model,
                user_payload=user_payload,
                raw_response=raw or "",
                parsed_json=parsed or {},
                validation_errors=[],
                compliance_warnings=[],
                fallback_used=False,
            )
            if parse_error or parsed is None:
                errors.append(parse_error or "json parse failed")
                debug_record["validation_errors"] = [parse_error or "json parse failed"]
                debug_records.append(debug_record)
                continue

            validation_errors = validate_agent_output(parsed, json_schema)
            if validation_errors:
                errors.extend(validation_errors)
                debug_record["parsed_json"] = sanitize_for_debug(parsed)
                debug_record["validation_errors"] = validation_errors
                debug_records.append(debug_record)
                continue

            compliance = check_financing_compliance(parsed)
            if not compliance.get("passed"):
                errors.append(compliance.get("message") or "compliance failed")
                debug_record["parsed_json"] = sanitize_for_debug(parsed)
                debug_record["compliance_warnings"] = compliance.get("blocked_terms") or []
                debug_records.append(debug_record)
                continue

            parsed.update({
                "llm_used": True,
                "fallback_used": False,
                "validated": True,
                "model": model,
                "retry_count": attempt,
                "validation_errors": [],
                "compliance_warnings": [],
            })
            debug_record["parsed_json"] = sanitize_for_debug(parsed)
            debug_records.append(debug_record)
            _save_llm_debug(debug_context, agent_name, debug_records)
            return parsed
        except Exception as exc:
            errors.append(str(exc))
            debug_records.append(
                _build_debug_record(
                    agent_name=agent_name,
                    attempt=attempt + 1,
                    model=model,
                    user_payload=user_payload,
                    raw_response="",
                    parsed_json={},
                    validation_errors=[str(exc)],
                    compliance_warnings=[],
                    fallback_used=False,
                )
            )

    fallback.update({
        "llm_used": False,
        "fallback_used": True,
        "fallback_reason": "; ".join(errors[-5:]) or "llm failed",
        "retry_count": retries,
        "validation_errors": errors,
    })
    fallback.setdefault("compliance_warnings", [])
    fallback.setdefault("warnings", []).append("LLM 增强失败，已回退到规则结果")
    debug_records.append(
        _build_debug_record(
            agent_name=agent_name,
            attempt=retries + 1,
            model=model,
            user_payload=user_payload,
            raw_response="",
            parsed_json=fallback,
            validation_errors=errors,
            compliance_warnings=fallback.get("compliance_warnings") or [],
            fallback_used=True,
        )
    )
    _save_llm_debug(debug_context, agent_name, debug_records)
    return fallback


def _input_summary(payload: dict[str, Any]) -> dict[str, Any]:
    documents = payload.get("documents") or {}
    parsed_credit = payload.get("parsed_credit_fields") or {}
    credit_summary = payload.get("credit_summary") or {}
    rule_risks = payload.get("rule_risks") or []
    return {
        "document_count": documents.get("count") if isinstance(documents, dict) else None,
        "document_types": sorted({
            str(item.get("document_type") or item.get("file_type") or item.get("extraction_type") or "")
            for item in (documents.get("items") or [])
            if isinstance(item, dict)
        }) if isinstance(documents, dict) else [],
        "risk_count": len(rule_risks) if isinstance(rule_risks, list) else 0,
        "credit_summary_keys": sorted(credit_summary.keys()) if isinstance(credit_summary, dict) else [],
        "parsed_credit_keys": sorted(parsed_credit.keys())[:30] if isinstance(parsed_credit, dict) else [],
        "amount_summary": {
            "total_unsettled_balance": credit_summary.get("total_unsettled_balance") if isinstance(credit_summary, dict) else None,
            "short_term_loan_balance": credit_summary.get("short_term_loan_balance") if isinstance(credit_summary, dict) else None,
            "medium_long_term_loan_balance": credit_summary.get("medium_long_term_loan_balance") if isinstance(credit_summary, dict) else None,
            "credit_line_total": credit_summary.get("credit_line_total") if isinstance(credit_summary, dict) else None,
            "used_credit_line": credit_summary.get("used_credit_line") if isinstance(credit_summary, dict) else None,
        },
    }


def _build_debug_record(
    *,
    agent_name: str,
    attempt: int,
    model: str,
    user_payload: dict[str, Any],
    raw_response: str,
    parsed_json: dict[str, Any],
    validation_errors: list[str],
    compliance_warnings: list[str],
    fallback_used: bool,
) -> dict[str, Any]:
    return {
        "agent_name": agent_name,
        "attempt": attempt,
        "model": model,
        "input_summary": sanitize_for_debug(_input_summary(user_payload)),
        "raw_response_preview": sanitize_for_debug((raw_response or "")[:2000]),
        "parsed_json": sanitize_for_debug(parsed_json),
        "validation_errors": validation_errors,
        "compliance_warnings": compliance_warnings,
        "fallback_used": fallback_used,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def _save_llm_debug(debug_context: dict[str, Any] | None, agent_name: str, records: list[dict[str, Any]]) -> None:
    if os.getenv("AGENT_DEBUG", "false").lower() not in {"1", "true", "yes", "on"}:
        return
    context = debug_context or {}
    customer_id = str(context.get("customer_id") or "unknown")
    run_id = str(context.get("run_id") or "manual")
    root = Path(os.getenv("AGENT_OUTPUT_DIR") or "data/agent_runs")
    debug_dir = root / customer_id / run_id / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    path = debug_dir / f"{agent_name}_llm_debug.json"
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
