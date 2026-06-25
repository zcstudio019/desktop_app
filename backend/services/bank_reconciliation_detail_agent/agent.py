"""Bank reconciliation detail Agent; parsing is delegated to bank_reconciliation_detail_skill."""

from __future__ import annotations

from typing import Any

from backend.extraction_skills import extract_with_skill
from backend.extraction_skills.base import ExtractionInput, ExtractionResult


class BankReconciliationDetailAgent:
    agent_name = "bank_reconciliation_detail_agent"
    schema_version = "bank_reconciliation_detail.agent.v1"

    def extract(
        self,
        *,
        raw_text: str,
        filename: str,
        customer_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ExtractionResult:
        metadata = metadata or {}
        result = extract_with_skill(
            ExtractionInput(
                customer_id=customer_id or "",
                document_id=str(metadata.get("document_id") or ""),
                document_type="bank_reconciliation_detail",
                file_name=filename,
                file_path=str(metadata.get("file_path") or ""),
                mime_type=str(metadata.get("mime_type") or ""),
                raw_text=raw_text,
                metadata=metadata,
            )
        )
        if not isinstance(result, ExtractionResult):
            raise RuntimeError("bank_reconciliation_detail_skill 未注册或未返回有效结果")
        return result
