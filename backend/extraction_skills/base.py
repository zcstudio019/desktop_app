from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ExtractionInput:
    customer_id: str
    document_id: str
    document_type: str
    file_name: str
    file_path: str
    mime_type: str | None = None
    raw_text: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExtractionResult:
    document_type: str
    schema_version: str
    extracted_json: dict[str, Any]
    markdown_summary: str
    confidence: float
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    skill_name: str = ""
    skill_version: str = "v1"


class BaseExtractionSkill:
    document_type: str = ""
    supported_extensions: set[str] = set()

    def extract(self, input_data: ExtractionInput) -> ExtractionResult:
        raise NotImplementedError
