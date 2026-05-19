from __future__ import annotations

import logging
from typing import Any

from backend.document_types import get_document_display_name, get_document_storage_label, normalize_document_type_code
from backend.extraction_skills.enterprise_credit import build_enterprise_credit_content
from backend.extraction_skills.personal_credit import build_personal_credit_report_content
from backend.services.enterprise_bank_statement_agent import run_enterprise_bank_statement_agent

from .base import BaseDocumentAgent
from .result import DocumentAgentResult

logger = logging.getLogger(__name__)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class EnterpriseCreditReportAgentAdapter(BaseDocumentAgent):
    agent_name = "enterprise_credit_report_agent"
    supported_document_types = ["enterprise_credit_report", "enterprise_credit"]
    schema_version = "enterprise_credit.v2"

    def extract(
        self,
        *,
        raw_text: str,
        filename: str,
        customer_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DocumentAgentResult:
        metadata = metadata or {}
        content = build_enterprise_credit_content(
            text=str(raw_text or ""),
            customer_id=customer_id or "",
            customer_name=str(metadata.get("customer_name") or ""),
            file_name=filename,
            file_path=str(metadata.get("file_path") or ""),
            document_id=str(metadata.get("document_id") or ""),
            raw_pages=metadata.get("raw_pages") if isinstance(metadata.get("raw_pages"), list) else None,
        )
        extracted_json = content.get("extracted_json") if isinstance(content.get("extracted_json"), dict) else {}
        markdown_summary = str(content.get("markdown_summary") or content.get("markdown") or "")
        return DocumentAgentResult(
            document_type="enterprise_credit_report",
            agent_name=self.agent_name,
            schema_version=str(content.get("schema_version") or self.schema_version),
            confidence=_as_float(content.get("confidence"), 0.0),
            extracted_json=extracted_json,
            markdown_summary=markdown_summary,
            evidence={},
            warnings=list(content.get("warnings") or []),
            debug={"legacy_skill_name": content.get("skill_name"), "legacy_type": content.get("type")},
            raw_agent_result=content,
        )


class PersonalCreditReportAgentAdapter(BaseDocumentAgent):
    agent_name = "personal_credit_report_agent"
    supported_document_types = ["personal_credit_report", "personal_credit"]
    schema_version = "personal_credit_report.agent.v1"

    def extract(
        self,
        *,
        raw_text: str,
        filename: str,
        customer_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DocumentAgentResult:
        metadata = metadata or {}
        content = build_personal_credit_report_content(
            text=str(raw_text or ""),
            customer_id=customer_id or "",
            customer_name=str(metadata.get("customer_name") or ""),
            file_name=filename,
            file_path=str(metadata.get("file_path") or ""),
            document_id=str(metadata.get("document_id") or ""),
            raw_pages=metadata.get("raw_pages") if isinstance(metadata.get("raw_pages"), list) else None,
        )
        report_json = content.get("extracted_json") if isinstance(content.get("extracted_json"), dict) else {}
        markdown = str(content.get("markdown_summary") or content.get("markdown") or "")
        debug = content.get("debug") if isinstance(content.get("debug"), dict) else {}
        debug = {
            **debug,
            "skill_name": content.get("skill_name"),
            "legacy_type": content.get("type"),
        }
        return DocumentAgentResult(
            document_type="personal_credit_report",
            agent_name=self.agent_name,
            schema_version=str(content.get("schema_version") or self.schema_version),
            confidence=_as_float(content.get("confidence"), 0.0),
            extracted_json=report_json,
            markdown_summary=markdown,
            evidence=content.get("evidence") if isinstance(content.get("evidence"), dict) else {},
            warnings=list(content.get("warnings") or []),
            debug=debug,
            raw_agent_result=content,
        )


class EnterpriseBankStatementAgentAdapter(BaseDocumentAgent):
    agent_name = "enterprise_bank_statement_agent"
    supported_document_types = [
        "enterprise_bank_statement",
        "bank_statement_enterprise",
        "company_bank_statement",
    ]
    schema_version = "enterprise_bank_statement.agent.v1"

    def extract(
        self,
        *,
        raw_text: str,
        filename: str,
        customer_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DocumentAgentResult:
        metadata = metadata or {}
        content = run_enterprise_bank_statement_agent(
            text=str(raw_text or ""),
            document_type=str(metadata.get("document_type") or "enterprise_bank_statement"),
            metadata={
                **metadata,
                "filename": filename,
                "customer_id": customer_id or "",
            },
        )
        extracted_json = content.get("extracted_json") if isinstance(content.get("extracted_json"), dict) else {}
        markdown = str(content.get("markdown_summary") or content.get("markdown") or "")
        evidence = content.get("evidence")
        return DocumentAgentResult(
            document_type="enterprise_bank_statement",
            agent_name=self.agent_name,
            schema_version=str(content.get("schema_version") or self.schema_version),
            confidence=_as_float(content.get("confidence"), 0.0),
            extracted_json=extracted_json,
            markdown_summary=markdown,
            evidence=evidence if isinstance(evidence, dict) else {"items": evidence or []},
            warnings=list(content.get("warnings") or []),
            debug={"skill_name": content.get("skill_name"), "legacy_type": content.get("type")},
            raw_agent_result=content,
        )


_ENTERPRISE_CREDIT_AGENT = EnterpriseCreditReportAgentAdapter()
_PERSONAL_CREDIT_AGENT = PersonalCreditReportAgentAdapter()
_ENTERPRISE_BANK_STATEMENT_AGENT = EnterpriseBankStatementAgentAdapter()

DOCUMENT_AGENT_REGISTRY: dict[str, BaseDocumentAgent] = {
    "enterprise_credit_report": _ENTERPRISE_CREDIT_AGENT,
    "enterprise_credit": _ENTERPRISE_CREDIT_AGENT,
    "personal_credit_report": _PERSONAL_CREDIT_AGENT,
    "personal_credit": _PERSONAL_CREDIT_AGENT,
    "enterprise_bank_statement": _ENTERPRISE_BANK_STATEMENT_AGENT,
    "bank_statement_enterprise": _ENTERPRISE_BANK_STATEMENT_AGENT,
    "company_bank_statement": _ENTERPRISE_BANK_STATEMENT_AGENT,
}

# Future agents:
# personal_bank_statement
# financial_data
# real_estate_certificate
# business_license
# contract
# tax_social_security


def get_document_agent(document_type: str) -> BaseDocumentAgent | None:
    normalized = normalize_document_type_code(document_type) or str(document_type or "").strip()
    return DOCUMENT_AGENT_REGISTRY.get(normalized) or DOCUMENT_AGENT_REGISTRY.get(str(document_type or "").strip())


def list_document_agents() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for document_type, agent in DOCUMENT_AGENT_REGISTRY.items():
        items.append(
            {
                "document_type": document_type,
                "document_type_name": get_document_display_name(document_type),
                "storage_label": get_document_storage_label(document_type),
                "agent_name": agent.agent_name,
                "schema_version": agent.schema_version,
                "supported_document_types": list(agent.supported_document_types),
            }
        )
    return items
