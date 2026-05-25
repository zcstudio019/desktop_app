from __future__ import annotations

from typing import Any

from backend.services.document_agents.base import BaseDocumentAgent
from backend.services.document_agents.result import DocumentAgentResult

from .orchestrator import run_financial_report_agent


class FinancialReportAgentAdapter(BaseDocumentAgent):
    agent_name = "financial_report_agent"
    supported_document_types = ["financial_report", "financial_data", "财务报表", "财务数据"]
    schema_version = "financial_report.agent.v1"

    def extract(
        self, *, raw_text: str, filename: str, customer_id: str | None = None, metadata: dict[str, Any] | None = None
    ) -> DocumentAgentResult:
        content = run_financial_report_agent(
            raw_text=raw_text,
            text=(metadata or {}).get("text") or raw_text,
            file_path=str((metadata or {}).get("file_path") or "") or None,
            filename=filename,
            customer_id=customer_id,
            metadata=metadata or {},
        )
        return DocumentAgentResult(
            document_type="financial_report",
            agent_name=self.agent_name,
            schema_version=self.schema_version,
            confidence=float(content.get("confidence") or 0),
            extracted_json=content["extracted_json"],
            markdown_summary=content["markdown_summary"],
            evidence=content["evidence"],
            warnings=content["warnings"],
            debug={"skill_name": content["skill_name"], "normalized_document_type": "financial_report"},
            raw_agent_result=content,
        )
