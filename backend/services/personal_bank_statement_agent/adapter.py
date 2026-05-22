from __future__ import annotations

from typing import Any

from backend.services.document_agents.base import BaseDocumentAgent
from backend.services.document_agents.result import DocumentAgentResult

from .orchestrator import run_personal_bank_statement_agent


def _as_float(value: Any, default: float = 0.8) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class PersonalBankStatementAgentAdapter(BaseDocumentAgent):
    agent_name = "personal_bank_statement_agent"
    supported_document_types = [
        "personal_flow",
        "personal_bank_statement",
        "bank_statement_personal",
        "individual_bank_statement",
        "个人流水",
        "个人银行流水",
    ]
    schema_version = "personal_bank_statement.agent.v1"

    def extract(
        self,
        *,
        raw_text: str,
        filename: str,
        customer_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DocumentAgentResult:
        metadata = metadata or {}
        content = run_personal_bank_statement_agent(
            file_path=str(metadata.get("file_path") or "") or None,
            filename=filename,
            text=metadata.get("text") or raw_text,
            raw_text=raw_text,
            document_type="personal_flow",
            customer_id=customer_id,
            metadata={**metadata, "document_type": "personal_flow"},
        )
        extracted_json = content.get("extracted_json") if isinstance(content.get("extracted_json"), dict) else {}
        return DocumentAgentResult(
            document_type="personal_flow",
            agent_name=self.agent_name,
            schema_version=str(content.get("schema_version") or self.schema_version),
            confidence=_as_float(content.get("confidence"), 0.8),
            extracted_json=extracted_json,
            markdown_summary=str(content.get("markdown_summary") or content.get("markdown") or ""),
            evidence={"items": extracted_json.get("evidence") or []},
            warnings=list(content.get("warnings") or []),
            debug={"skill_name": content.get("skill_name"), "normalized_document_type": content.get("normalized_document_type")},
            raw_agent_result=content,
        )
