from __future__ import annotations

import re
from typing import Any

from .schema import (
    CREDIT_CARD_ACCOUNT_FIELDS,
    GUARANTEE_FIELDS,
    LOAN_ACCOUNT_FIELDS,
    OVERDUE_RECORD_FIELDS,
    PUBLIC_RECORD_FIELDS,
    QUERY_RECORD_FIELDS,
    clone_default_report_json,
    default_basic_info,
    default_credit_summary,
    ensure_record_fields,
)

LIST_FIELDS = (
    "loan_accounts",
    "credit_card_accounts",
    "guarantees",
    "overdue_records",
    "public_records",
    "query_records",
    "risk_flags",
    "missing_fields",
    "warnings",
)

RECORD_FIELDS_BY_LIST = {
    "loan_accounts": LOAN_ACCOUNT_FIELDS,
    "credit_card_accounts": CREDIT_CARD_ACCOUNT_FIELDS,
    "guarantees": GUARANTEE_FIELDS,
    "overdue_records": OVERDUE_RECORD_FIELDS,
    "public_records": PUBLIC_RECORD_FIELDS,
    "query_records": QUERY_RECORD_FIELDS,
}

AMOUNT_KEYS = {
    "issued_amount",
    "balance",
    "credit_limit",
    "used_amount",
    "overdue_amount",
    "guarantee_amount",
    "guarantee_balance",
    "amount",
    "used_limit",
    "latest_repayment_amount",
}


def _clean_scalar(value: Any, *, is_amount: bool = False) -> Any:
    if value is None:
        return "" if is_amount else value
    if isinstance(value, str):
        text = re.sub(r"[ \t\u3000]+", " ", value).strip()
        if is_amount:
            text = re.sub(r"\s+", "", text)
        return text
    return value


def _normalize_record(record: Any, fields: tuple[str, ...]) -> dict[str, Any]:
    if not isinstance(record, dict):
        record = {}
    normalized = ensure_record_fields(record, fields)
    for key, value in list(normalized.items()):
        normalized[key] = _clean_scalar(value, is_amount=key in AMOUNT_KEYS)
    return normalized


def normalize_report_json(report: dict[str, Any] | None) -> dict[str, Any]:
    normalized = clone_default_report_json()
    if isinstance(report, dict):
        normalized.update({key: value for key, value in report.items() if key not in {"basic_info", "credit_summary"}})
        basic = report.get("basic_info") if isinstance(report.get("basic_info"), dict) else {}
        summary = report.get("credit_summary") if isinstance(report.get("credit_summary"), dict) else {}
        normalized["basic_info"] = {**default_basic_info(), **basic}
        normalized["credit_summary"] = {**default_credit_summary(), **summary}

    for key, value in list(normalized["basic_info"].items()):
        normalized["basic_info"][key] = _clean_scalar(value) or ""
    for key, value in list(normalized["credit_summary"].items()):
        normalized["credit_summary"][key] = value if isinstance(value, int) or value is None else _clean_scalar(value)

    for field in LIST_FIELDS:
        value = normalized.get(field)
        if not isinstance(value, list):
            value = []
        record_fields = RECORD_FIELDS_BY_LIST.get(field)
        if record_fields:
            normalized[field] = [_normalize_record(item, record_fields) for item in value]
        else:
            normalized[field] = [item for item in value if item is not None]

    normalized["report_type"] = "personal_credit_report"
    if not isinstance(normalized.get("personal_credit_indicators"), dict):
        normalized["personal_credit_indicators"] = {}
    return normalized
