from __future__ import annotations

from typing import Any

from backend.document_types import normalize_document_type_code

from .base import BaseExtractionSkill, ExtractionInput, ExtractionResult
from .bank_receipt_bundle import BankReceiptBundleSkill
from .bank_statement import BankStatementSkill
from .enterprise_credit import EnterpriseCreditSkill
from .personal_credit import PersonalCreditSkill

_ALIASES = {
    "企业征信": "enterprise_credit",
    "enterprise_credit": "enterprise_credit",
    "enterprise_credit_report": "enterprise_credit",
    "个人征信": "personal_credit_report",
    "personal_credit": "personal_credit_report",
    "personal_credit_report": "personal_credit_report",
}

_ENTERPRISE_CREDIT_SKILL = EnterpriseCreditSkill()
_PERSONAL_CREDIT_SKILL = PersonalCreditSkill()
_BANK_STATEMENT_SKILL = BankStatementSkill()
_BANK_RECEIPT_BUNDLE_SKILL = BankReceiptBundleSkill()

_SKILLS: dict[str, BaseExtractionSkill] = {
    "enterprise_credit": _ENTERPRISE_CREDIT_SKILL,
    "enterprise_credit_report": _ENTERPRISE_CREDIT_SKILL,
    "personal_credit_report": _PERSONAL_CREDIT_SKILL,
    "personal_credit": _PERSONAL_CREDIT_SKILL,
    "bank_statement": _BANK_STATEMENT_SKILL,
    "bank_receipt_bundle": _BANK_RECEIPT_BUNDLE_SKILL,
}


def normalize_document_type(document_type: str) -> str:
    raw = str(document_type or "").strip()
    normalized = normalize_document_type_code(raw)
    if normalized in {"enterprise_credit_report", "enterprise_credit"}:
        return "enterprise_credit"
    if normalized in {"personal_credit_report", "personal_credit"}:
        return "personal_credit_report"
    if normalized:
        return normalized
    return _ALIASES.get(raw, raw)


def get_skill(document_type: str) -> BaseExtractionSkill | None:
    return _SKILLS.get(normalize_document_type(document_type))


def extract_with_skill(input_data: ExtractionInput) -> Any | None:
    skill = get_skill(input_data.document_type)
    if not skill:
        return None
    return skill.extract(input_data)
