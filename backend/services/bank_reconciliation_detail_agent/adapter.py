from __future__ import annotations

from typing import Any

from backend.services.document_agents.base import BaseDocumentAgent
from backend.services.document_agents.result import DocumentAgentResult

from .agent import BankReconciliationDetailAgent


class BankReconciliationDetailAgentAdapter(BaseDocumentAgent):
    agent_name = "bank_reconciliation_detail_agent"
    supported_document_types = ["bank_reconciliation_detail"]
    schema_version = "bank_reconciliation_detail.agent.v1"

    def extract(
        self,
        *,
        raw_text: str,
        filename: str,
        customer_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DocumentAgentResult:
        result = BankReconciliationDetailAgent().extract(
            raw_text=raw_text,
            filename=filename,
            customer_id=customer_id,
            metadata=metadata,
        )
        content = {
            "title": "银行对账明细",
            "type": "bank_reconciliation_detail",
            "document_type": "bank_reconciliation_detail",
            "document_type_code": "bank_reconciliation_detail",
            "document_type_name": "银行对账明细",
            "doc_type": "bank_reconciliation_detail",
            "doc_type_name": "银行对账明细",
            "agent_type": self.agent_name,
            "schema_version": result.schema_version,
            "skill_name": result.skill_name,
            "skill_version": result.skill_version,
            "extracted_json": result.extracted_json,
            "data": result.extracted_json,
            "markdown_summary": result.markdown_summary,
            "markdown": result.markdown_summary,
            "display_markdown": result.markdown_summary,
            "report_markdown": result.markdown_summary,
            "summary": result.markdown_summary,
            "warnings": result.warnings,
            "confidence": result.confidence,
            "extraction_status": result.extracted_json.get("extraction_status") or "success",
        }
        return DocumentAgentResult(
            document_type="bank_reconciliation_detail",
            agent_name=self.agent_name,
            schema_version=result.schema_version,
            confidence=result.confidence,
            extracted_json=result.extracted_json,
            markdown_summary=result.markdown_summary,
            evidence={},
            warnings=result.warnings,
            debug={"skill_name": result.skill_name},
            raw_agent_result=content,
        )
