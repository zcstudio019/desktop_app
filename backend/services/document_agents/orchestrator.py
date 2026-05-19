from __future__ import annotations

import logging
from typing import Any

from backend.document_types import normalize_document_type_code

from .registry import get_document_agent
from .result import DocumentAgentResult

logger = logging.getLogger(__name__)


def _fallback_result(document_type: str, raw_text: str, filename: str) -> DocumentAgentResult:
    normalized_type = normalize_document_type_code(document_type) or str(document_type or "").strip()
    logger.warning("[DocumentAgentOrchestrator] no_agent_registered document_type=%s", normalized_type)
    return DocumentAgentResult(
        document_type=normalized_type,
        agent_name="document_agent_fallback",
        schema_version="fallback.v1",
        confidence=0.0,
        extracted_json={"raw_text_preview": str(raw_text or "")[:3000]},
        markdown_summary="",
        warnings=[f"no document agent registered for {normalized_type}"],
        debug={
            "selected_agent": None,
            "document_type": normalized_type,
            "filename": filename,
            "schema_version": "fallback.v1",
            "confidence": 0.0,
            "fallback": True,
        },
        raw_agent_result=None,
    )


def run_document_extraction_agent(
    document_type: str,
    raw_text: str,
    filename: str,
    customer_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> DocumentAgentResult:
    normalized_type = normalize_document_type_code(document_type) or str(document_type or "").strip()
    agent = get_document_agent(normalized_type)
    if not agent:
        return _fallback_result(normalized_type, raw_text, filename)

    logger.info(
        "[DocumentAgentOrchestrator] selected_agent=%s document_type=%s filename=%s",
        agent.agent_name,
        normalized_type,
        filename,
    )
    try:
        result = agent.extract(
            raw_text=raw_text,
            filename=filename,
            customer_id=customer_id,
            metadata=metadata or {},
        )
        result.debug = {
            **(result.debug or {}),
            "selected_agent": agent.agent_name,
            "document_type": result.document_type or normalized_type,
            "filename": filename,
            "schema_version": result.schema_version,
            "confidence": result.confidence,
        }
        logger.info(
            "[DocumentAgentOrchestrator] extraction_success document_type=%s agent=%s skill=%s",
            result.document_type,
            result.agent_name,
            (result.debug or {}).get("skill_name") or result.agent_name,
        )
        return result
    except Exception as exc:
        logger.exception(
            "[DocumentAgentOrchestrator] extraction_failed document_type=%s agent=%s filename=%s",
            normalized_type,
            agent.agent_name,
            filename,
        )
        fallback = _fallback_result(normalized_type, raw_text, filename)
        fallback.agent_name = agent.agent_name
        fallback.warnings.append(str(exc))
        fallback.debug["selected_agent"] = agent.agent_name
        fallback.debug["error"] = str(exc)
        return fallback
