from __future__ import annotations

from typing import Any

from backend.document_types import normalize_document_type_code

from .base import BaseExtractionSkill, ExtractionInput, ExtractionResult
from .enterprise_credit import EnterpriseCreditSkill

_ALIASES = {
    "企业征信": "enterprise_credit",
    "enterprise_credit": "enterprise_credit",
}

_SKILLS: dict[str, BaseExtractionSkill] = {
    "enterprise_credit": EnterpriseCreditSkill(),
}


def normalize_document_type(document_type: str) -> str:
    raw = str(document_type or "").strip()
    normalized = normalize_document_type_code(raw)
    if normalized:
        return normalized
    return _ALIASES.get(raw, raw)


def get_skill(document_type: str) -> BaseExtractionSkill | None:
    return _SKILLS.get(normalize_document_type(document_type))


def extract_with_skill(input_data: ExtractionInput) -> ExtractionResult | None:
    skill = get_skill(input_data.document_type)
    if not skill:
        return None
    return skill.extract(input_data)
