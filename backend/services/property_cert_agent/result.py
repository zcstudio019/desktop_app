from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class PropertyCertAgentResult:
    doc_type: str = "property_cert"
    doc_type_name: str = "房产证/不动产权证"
    owner_type: str = "asset"
    agent_type: str = "property_cert_agent"
    extraction_status: str = "failed"
    fields: dict[str, Any] = field(default_factory=dict)
    pages: list[dict[str, Any]] = field(default_factory=list)
    merged_fields: dict[str, Any] = field(default_factory=dict)
    page_roles: list[str] = field(default_factory=list)
    validation: dict[str, Any] = field(default_factory=dict)
    confidence: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    missing_fields: list[str] = field(default_factory=list)
    raw_text_preview: str = ""
    markdown: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    supplemental_files: list[str] = field(default_factory=list)
    risk_sections: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "doc_type": self.doc_type,
            "doc_type_name": self.doc_type_name,
            "owner_type": self.owner_type,
            "agent_type": self.agent_type,
            "extraction_status": self.extraction_status,
            "fields": self.fields,
            "pages": self.pages,
            "merged_fields": self.merged_fields,
            "page_roles": self.page_roles,
            "validation": self.validation,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "missing_fields": self.missing_fields,
            "raw_text_preview": self.raw_text_preview,
            "markdown": self.markdown,
            "metadata": self.metadata,
            "supplemental_files": self.supplemental_files,
            "risk_sections": self.risk_sections,
            "document_type_code": self.doc_type,
            "document_type_name": self.doc_type_name,
            "storage_label": self.doc_type_name,
        }
