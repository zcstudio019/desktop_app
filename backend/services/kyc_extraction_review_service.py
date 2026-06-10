from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


KYC_AGENT_TYPE = "kyc_document_agent"
CONFIRM_STATUSES = {"unconfirmed", "partial", "confirmed"}
UPDATE_ROLES = {"admin", "operator"}
BUSINESS_LICENSE_FIELD_ORDER = (
    "unified_social_credit_code",
    "license_number",
    "company_name",
    "company_type",
    "legal_representative",
    "registered_capital",
    "establishment_date",
    "business_term",
    "registered_address",
    "business_scope",
    "registration_authority",
    "issue_date",
)
MISSING_FIELD_ALIASES = {
    "登记机关": "registration_authority",
}


def can_update_review(role: str | None) -> bool:
    return str(role or "").lower() in UPDATE_ROLES


def _data(extraction: dict[str, Any]) -> dict[str, Any]:
    value = extraction.get("extracted_data")
    return value if isinstance(value, dict) else {}


def _confirmed_data(extraction: dict[str, Any]) -> dict[str, Any]:
    value = extraction.get("confirmed_data")
    return value if isinstance(value, dict) else {}


def is_kyc_extraction(extraction: dict[str, Any]) -> bool:
    return _data(extraction).get("agent_type") == KYC_AGENT_TYPE


def _has_value(value: Any) -> bool:
    return value not in (None, "", [], {})


def _ensure_review_fields(doc_type: str, fields: dict[str, Any]) -> dict[str, Any]:
    next_fields = dict(fields)
    if doc_type == "business_license":
        for field_name in BUSINESS_LICENSE_FIELD_ORDER:
            next_fields.setdefault(field_name, "")
    return next_fields


def _effective_missing_fields(missing_fields: Any, confirmed_fields: dict[str, Any]) -> list[Any]:
    if not isinstance(missing_fields, list):
        return []
    return [
        field_name
        for field_name in missing_fields
        if not _has_value(confirmed_fields.get(MISSING_FIELD_ALIASES.get(str(field_name), str(field_name))))
    ]


def build_extraction_review(document_id: str, extraction: dict[str, Any]) -> dict[str, Any]:
    extracted_data = _data(extraction)
    confirmed_data = _confirmed_data(extraction)
    extracted_fields = extracted_data.get("fields") if isinstance(extracted_data.get("fields"), dict) else {}
    confirmed_fields = confirmed_data.get("confirmed_fields") if isinstance(confirmed_data.get("confirmed_fields"), dict) else {}
    doc_type = extracted_data.get("doc_type") or extraction.get("extraction_type") or ""
    merged_fields = _ensure_review_fields(str(doc_type), {**extracted_fields, **confirmed_fields})
    confirm_status = str(extraction.get("confirm_status") or confirmed_data.get("confirm_status") or "unconfirmed")
    if confirm_status not in CONFIRM_STATUSES:
        confirm_status = "unconfirmed"
    return {
        "document_id": document_id,
        "doc_type": doc_type,
        "doc_type_name": extracted_data.get("doc_type_name") or "",
        "agent_type": extracted_data.get("agent_type") or "",
        "extracted_data": extracted_data,
        "confirmed_data": confirmed_data,
        "merged_fields": merged_fields,
        "confirm_status": confirm_status,
        "confirmed_by": extraction.get("confirmed_by") or confirmed_data.get("confirmed_by") or "",
        "confirmed_at": extraction.get("confirmed_at") or confirmed_data.get("confirmed_at") or "",
        "validation": extracted_data.get("validation") if isinstance(extracted_data.get("validation"), dict) else {},
        "evidence": extracted_data.get("evidence") if isinstance(extracted_data.get("evidence"), dict) else {},
        "missing_fields": _effective_missing_fields(extracted_data.get("missing_fields"), confirmed_fields),
    }


def build_confirmed_data(
    *,
    existing: dict[str, Any] | None,
    confirmed_fields: dict[str, Any],
    confirm_status: str,
    confirmed_by: str,
    confirmed_at: datetime | None = None,
) -> dict[str, Any]:
    if confirm_status not in {"partial", "confirmed"}:
        raise ValueError("confirm_status must be partial or confirmed")
    confirmed_at = confirmed_at or datetime.now(timezone.utc)
    current = dict(existing or {})
    current_fields = current.get("confirmed_fields") if isinstance(current.get("confirmed_fields"), dict) else {}
    current["confirmed_fields"] = {**current_fields, **(confirmed_fields or {})}
    current["confirm_status"] = confirm_status
    current["confirmed_by"] = confirmed_by
    current["confirmed_at"] = confirmed_at.isoformat()
    return current
