from __future__ import annotations

from typing import Any

from backend.services.document_agents.base import BaseDocumentAgent
from backend.services.document_agents.result import DocumentAgentResult

from .agent import ContractAgent
from .schema import DOC_TYPE, SCHEMA_VERSION


class ContractAgentAdapter(BaseDocumentAgent):
    agent_name = "contract_agent"
    supported_document_types = ["contract"]
    schema_version = SCHEMA_VERSION

    def extract(
        self,
        *,
        raw_text: str,
        filename: str,
        customer_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DocumentAgentResult:
        metadata = metadata or {}
        result = ContractAgent().run(
            {
                "text": raw_text,
                "raw_pages": metadata.get("raw_pages") if isinstance(metadata.get("raw_pages"), list) else [],
                "filename": filename,
                "customer_id": customer_id or str(metadata.get("customer_id") or ""),
                "customer_name": str(metadata.get("customer_name") or ""),
                "source": str(metadata.get("source") or "upload"),
            }
        )
        content = result.to_dict()
        return DocumentAgentResult(
            document_type=DOC_TYPE,
            agent_name=self.agent_name,
            schema_version=self.schema_version,
            confidence=0.88 if result.extraction_status == "success" else 0.72,
            extracted_json=content,
            markdown_summary=result.markdown,
            evidence=result.evidence,
            warnings=list(result.warnings),
            debug={"skill_name": "contract_skill", "normalized_document_type": DOC_TYPE},
            raw_agent_result=content,
        )
