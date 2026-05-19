from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class DocumentAgentResult:
    document_type: str
    agent_name: str
    schema_version: str
    confidence: float
    extracted_json: dict[str, Any] = field(default_factory=dict)
    markdown_summary: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    debug: dict[str, Any] = field(default_factory=dict)
    raw_agent_result: dict[str, Any] | None = None

    @property
    def data(self) -> dict[str, Any]:
        return self.extracted_json

    @property
    def type(self) -> str:
        return self.document_type

    @property
    def title(self) -> str:
        if isinstance(self.extracted_json, dict):
            title = self.extracted_json.get("title") or self.extracted_json.get("name")
            if title:
                return str(title)
        return self.document_type

    def to_legacy_content(self) -> dict[str, Any]:
        return {
            "type": self.document_type,
            "title": self.title,
            "document_type_code": self.document_type,
            "skill_name": self.agent_name,
            "schema_version": self.schema_version,
            "confidence": self.confidence,
            "warnings": self.warnings,
            "markdown": self.markdown_summary,
            "markdown_summary": self.markdown_summary,
            "summary": self.markdown_summary,
            "extracted_json": self.extracted_json,
            "data": self.extracted_json,
            "debug": self.debug,
        }
