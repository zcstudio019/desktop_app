"""Utilities for making large extracted payloads safe for database storage."""

from __future__ import annotations

import re
from typing import Any

_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_BANK_STATEMENT_TYPES = {
    "enterprise_flow",
    "enterprise_bank_statement",
    "bank_statement_enterprise",
    "company_bank_statement",
    "企业流水",
    "银行流水",
}
_RAW_VALUE_MAX_LEN = 500
_RAW_MAX_FIELDS = 80


def sanitize_text_for_db(text: str | None) -> str | None:
    """Remove characters that commonly break MySQL text/JSON storage."""
    if text is None:
        return text
    cleaned = str(text).replace("\x00", "")
    cleaned = _CONTROL_CHARS_RE.sub("", cleaned)
    return cleaned.encode("utf-8", errors="ignore").decode("utf-8", errors="ignore")


def sanitize_for_db(value: Any) -> Any:
    """Recursively sanitize strings, dict keys, and list/tuple members."""
    if isinstance(value, str):
        return sanitize_text_for_db(value)
    if isinstance(value, dict):
        return {sanitize_text_for_db(str(key)) or "": sanitize_for_db(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_for_db(item) for item in value]
    if isinstance(value, tuple):
        return tuple(sanitize_for_db(item) for item in value)
    return value


def _is_enterprise_bank_statement_payload(payload: dict[str, Any]) -> bool:
    doc_type = str(payload.get("document_type") or payload.get("type") or "").strip()
    normalized_type = str(payload.get("normalized_document_type") or "").strip()
    return (
        doc_type in _BANK_STATEMENT_TYPES
        or normalized_type == "enterprise_bank_statement"
        or any(key in payload for key in ("summary", "accounts", "monthly_summary", "counterparty_summary", "financing_view"))
    )


def _compact_raw(raw: Any) -> Any:
    if not isinstance(raw, dict):
        return sanitize_for_db(raw)
    compacted: dict[str, Any] = {}
    for index, (key, value) in enumerate(raw.items()):
        if index >= _RAW_MAX_FIELDS:
            break
        if value is None or value == "":
            continue
        clean_key = sanitize_text_for_db(str(key)) or ""
        if isinstance(value, (dict, list, tuple)):
            clean_value = sanitize_for_db(value)
        else:
            clean_value = sanitize_text_for_db(str(value))
            if clean_value and len(clean_value) > _RAW_VALUE_MAX_LEN:
                clean_value = clean_value[:_RAW_VALUE_MAX_LEN]
        if clean_key:
            compacted[clean_key] = clean_value
    return compacted


def compact_enterprise_bank_statement_payload(value: Any) -> Any:
    """Reduce noisy transaction.raw blocks without changing summary-level analysis."""
    if isinstance(value, list):
        return [compact_enterprise_bank_statement_payload(item) for item in value]
    if not isinstance(value, dict):
        return value

    payload = dict(value)
    for nested_key in ("extracted_json", "data", "result", "payload"):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            payload[nested_key] = compact_enterprise_bank_statement_payload(nested)

    if _is_enterprise_bank_statement_payload(payload):
        transactions = payload.get("transactions")
        if isinstance(transactions, list):
            cleaned_transactions: list[Any] = []
            for transaction in transactions:
                if not isinstance(transaction, dict):
                    cleaned_transactions.append(transaction)
                    continue
                next_transaction = dict(transaction)
                if "raw" in next_transaction:
                    next_transaction["raw"] = _compact_raw(next_transaction.get("raw"))
                cleaned_transactions.append(next_transaction)
            payload["transactions"] = cleaned_transactions
    return payload


def sanitize_payload_for_db(value: Any, *, compact_enterprise_flow: bool = True) -> Any:
    payload = compact_enterprise_bank_statement_payload(value) if compact_enterprise_flow else value
    return sanitize_for_db(payload)
