from __future__ import annotations

from typing import Any


def _append_missing(missing: list[str], field: str, value: Any) -> None:
    if value in (None, ""):
        missing.append(field)


def _summary_count(summary: dict[str, Any], *keys: str) -> int:
    total = 0
    for key in keys:
        value = summary.get(key)
        if isinstance(value, int):
            total += value
    return total


def validate_report_json(report: dict[str, Any]) -> tuple[list[str], list[str]]:
    warnings = list(report.get("warnings") or [])
    missing_fields = list(report.get("missing_fields") or [])
    basic = report.get("basic_info") if isinstance(report.get("basic_info"), dict) else {}
    summary = report.get("credit_summary") if isinstance(report.get("credit_summary"), dict) else {}

    for field in ("report_number", "name", "id_number"):
        path = f"basic_info.{field}"
        if path not in missing_fields:
            _append_missing(missing_fields, path, basic.get(field))

    if not isinstance(report.get("loan_accounts"), list):
        warnings.append("loan_accounts_not_array")
        report["loan_accounts"] = []
    if not isinstance(report.get("credit_card_accounts"), list):
        warnings.append("credit_card_accounts_not_array")
        report["credit_card_accounts"] = []
    if not isinstance(report.get("query_records"), list):
        warnings.append("query_records_not_array")
        report["query_records"] = []

    expected_loans = _summary_count(
        summary,
        "housing_loan_account_count",
        "other_loan_account_count",
    )
    actual_loans = len(report.get("loan_accounts") or [])
    if expected_loans and abs(expected_loans - actual_loans) >= 3:
        warnings.append(f"loan_account_count_mismatch: expected={expected_loans}, actual={actual_loans}")

    expected_cards = summary.get("credit_card_account_count")
    actual_cards = len(report.get("credit_card_accounts") or [])
    if isinstance(expected_cards, int) and expected_cards and abs(expected_cards - actual_cards) >= 3:
        warnings.append(f"credit_card_account_count_mismatch: expected={expected_cards}, actual={actual_cards}")

    report["warnings"] = warnings
    report["missing_fields"] = missing_fields
    return warnings, missing_fields
