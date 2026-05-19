from __future__ import annotations

from typing import Any

from .result import DocumentAgentResult


class BaseDocumentAgent:
    agent_name: str = ""
    supported_document_types: list[str] = []
    schema_version: str = "v1"

    def can_handle(self, document_type: str) -> bool:
        return str(document_type or "").strip() in set(self.supported_document_types)

    def extract(
        self,
        *,
        raw_text: str,
        filename: str,
        customer_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DocumentAgentResult:
        raise NotImplementedError
