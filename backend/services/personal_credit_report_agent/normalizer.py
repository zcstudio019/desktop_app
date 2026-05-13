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

ID_CARD_PATTERN = re.compile(
    r"(?<!\d)([1-9]\d{5}(?:(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx]|\d{9}))(?!\d)"
)
MARRIAGE_VALUES = ("未婚", "已婚", "离异", "丧偶")

SUMMARY_ALIASES = {
    "active_credit_card_account_count": ("credit_card_active_count",),
    "credit_card_overdue_account_count": ("credit_card_overdue_count",),
    "credit_card_90d_overdue_account_count": ("credit_card_90d_overdue_count",),
}


def _warn_once(report: dict[str, Any], warning: str) -> None:
    warnings = report.setdefault("warnings", [])
    if isinstance(warnings, list) and warning not in warnings:
        warnings.append(warning)


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


def _clean_id_number(value: Any) -> str:
    text = re.sub(r"\s+", "", str(value or ""))
    text = re.split(r"[:：]", text)[-1] if "证件号码" in text else text
    match = ID_CARD_PATTERN.search(text)
    return match.group(1).upper() if match else ""


def _split_report_number_time(report_number: Any, report_time: Any) -> tuple[str, str]:
    number = _clean_scalar(report_number) or ""
    time = _clean_scalar(report_time) or ""
    if "报告时间" in number:
        parts = re.split(r"报告时间\s*[:：]?", number, maxsplit=1)
        number = re.sub(r"报告编号\s*[:：]?", "", parts[0]).strip()
        if len(parts) > 1 and not time:
            time = parts[1].strip()
    number_match = re.search(r"([A-Za-z0-9\-]{6,80})", str(number))
    cleaned_number = number_match.group(1) if number_match else str(number).strip()
    time_match = re.search(r"((?:19|20)\d{2}[-/年.]\d{1,2}[-/月.]\d{1,2}(?:日)?(?:\s+\d{1,2}:\d{1,2}:\d{1,2})?)", str(time))
    cleaned_time = time_match.group(1) if time_match else str(time).strip()
    return cleaned_number, cleaned_time


def _clean_marital_status(value: Any) -> str:
    text = str(value or "")
    for item in MARRIAGE_VALUES:
        if item in text:
            return item
    return ""


def _summary_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    match = re.search(r"\d+", str(value or ""))
    return int(match.group(0)) if match else None


def _summary_sum(*values: Any) -> str:
    numbers = [_summary_int(value) for value in values]
    numbers = [number for number in numbers if number is not None]
    return str(sum(numbers)) if numbers else ""


def _normalize_credit_summary(summary: dict[str, Any]) -> dict[str, Any]:
    normalized = {**default_credit_summary(), **summary}
    for target, aliases in SUMMARY_ALIASES.items():
        if normalized.get(target) in (None, ""):
            for alias in aliases:
                if summary.get(alias) not in (None, ""):
                    normalized[target] = summary.get(alias)
                    break
    if normalized.get("loan_account_count") in (None, ""):
        normalized["loan_account_count"] = _summary_sum(
            summary.get("housing_loan_account_count"),
            summary.get("other_loan_account_count"),
        ) or None
    if normalized.get("outstanding_loan_account_count") in (None, ""):
        normalized["outstanding_loan_account_count"] = _summary_sum(
            summary.get("housing_loan_outstanding_count"),
            summary.get("other_loan_outstanding_count"),
        ) or None
    if normalized.get("loan_overdue_account_count") in (None, ""):
        normalized["loan_overdue_account_count"] = _summary_sum(
            summary.get("housing_loan_overdue_count"),
            summary.get("other_loan_overdue_count"),
        ) or None
    for key, value in list(normalized.items()):
        normalized[key] = value if isinstance(value, int) or value is None else _clean_scalar(value)
    return normalized


def normalize_report_json(report: dict[str, Any] | None) -> dict[str, Any]:
    normalized = clone_default_report_json()
    if isinstance(report, dict):
        normalized.update({key: value for key, value in report.items() if key not in {"basic_info", "credit_summary"}})
        basic = report.get("basic_info") if isinstance(report.get("basic_info"), dict) else {}
        summary = report.get("credit_summary") if isinstance(report.get("credit_summary"), dict) else {}
        normalized["basic_info"] = {**default_basic_info(), **basic}
        normalized["credit_summary"] = _normalize_credit_summary(summary)

    for key, value in list(normalized["basic_info"].items()):
        normalized["basic_info"][key] = _clean_scalar(value) or ""
    report_number, report_time = _split_report_number_time(
        normalized["basic_info"].get("report_number"),
        normalized["basic_info"].get("report_time"),
    )
    normalized["basic_info"]["report_number"] = report_number
    normalized["basic_info"]["report_time"] = report_time
    cleaned_id_number = _clean_id_number(normalized["basic_info"].get("id_number"))
    if normalized["basic_info"].get("id_number") and not cleaned_id_number:
        _warn_once(normalized, "证件号码未识别或格式异常")
    normalized["basic_info"]["id_number"] = cleaned_id_number
    normalized["basic_info"]["marital_status"] = _clean_marital_status(normalized["basic_info"].get("marital_status"))
    normalized["credit_summary"] = _normalize_credit_summary(normalized["credit_summary"])

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
