"""Shared read-only credit facts adapted from the stable credit report model."""

from __future__ import annotations

from typing import Any

from backend.services.assistant_credit_report_service import (
    _declared_unit,
    _enterprise_credit_model,
    _money,
    _money_sum_objects,
    _number,
    _personal_credit_model,
)


def _value(money: Any) -> float | None:
    value = _number(money)
    return float(value) if value is not None else None


def _unit(money: Any) -> str | None:
    return str(money.get("unit") or "").strip() or None if isinstance(money, dict) else None


def _query_window(matrix: list[dict[str, Any]], label: str) -> dict[str, Any]:
    return next((dict(row) for row in matrix if row.get("window") == label), {})


def _count(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None else None


def _with_unit(money: Any, fallback_unit: str | None) -> Any:
    if not isinstance(money, dict) or money.get("unit") or not fallback_unit:
        return money
    return {**money, "unit": fallback_unit}


def build_stable_personal_credit_facts(
    payload: dict[str, Any],
    person_name: str,
    roles: list[str],
    source_report_date: str | None = None,
) -> dict[str, Any]:
    """Reuse the verified personal credit normalization used by the credit report."""
    normalized = _personal_credit_model(payload, person_name)
    summary = normalized.get("summary") or {}
    loans = normalized.get("loans") or []
    cards = normalized.get("credit_cards") or []
    related = normalized.get("related_repayment_responsibilities") or []
    loan_balance = _money_sum_objects([row.get("balance") for row in loans])
    card_limit = _money_sum_objects([row.get("credit_limit") for row in cards])
    card_used = _money_sum_objects([row.get("used_amount") for row in cards])
    related_balance = _money_sum_objects([row.get("balance") for row in related])
    limit_value, used_value = _value(card_limit), _value(card_used)
    query_matrix = normalized.get("query_matrix") or []
    overdue = normalized.get("overdue_summary") or {}
    overdue_90 = overdue.get("overdue_90_plus_account_count")
    if _number(overdue_90) is None:
        loan_90 = _number(overdue.get("loan_90d_overdue_account_count"))
        card_90 = _number(overdue.get("credit_card_90d_overdue_account_count"))
        overdue_90 = None if loan_90 is None and card_90 is None else int((loan_90 or 0) + (card_90 or 0))
    report_date = source_report_date or (normalized.get("basic_info") or {}).get("report_time")
    return {
        "name": person_name,
        "roles": list(dict.fromkeys(roles)),
        "loan_balance": _value(loan_balance),
        "loan_balance_money": loan_balance,
        "loan_balance_unit": _unit(loan_balance),
        "loan_account_count": summary.get("outstanding_loan_account_count") or len(loans) or None,
        "credit_card_limit": _value(card_limit),
        "credit_card_limit_money": card_limit,
        "credit_card_used": _value(card_used),
        "credit_card_used_money": card_used,
        "credit_card_utilization": round(used_value / limit_value, 4) if limit_value and used_value is not None else None,
        "overdue_summary": {
            "loan_overdue_account_count": _count(overdue.get("loan_overdue_account_count")),
            "credit_card_overdue_account_count": _count(overdue.get("credit_card_overdue_account_count")),
            "overdue_90d_account_count": _count(overdue_90),
        },
        "query_summary": {
            "windows": query_matrix,
            "near_1_month": _query_window(query_matrix, "近1月"),
            "near_2_months": _query_window(query_matrix, "近2月"),
            "near_3_months": _query_window(query_matrix, "近3月"),
            "near_6_months": _query_window(query_matrix, "近6月"),
            "near_9_months": _query_window(query_matrix, "近9月"),
            "near_1_year": _query_window(query_matrix, "近1年"),
            "near_2_years": _query_window(query_matrix, "近2年"),
        },
        "related_repayment_balance": _value(related_balance),
        "related_repayment_balance_money": related_balance,
        "related_repayment_balance_unit": _unit(related_balance),
        "related_repayment_records": [
            {
                "institution": row.get("institution"),
                "balance": _value(row.get("balance")),
                "balance_money": row.get("balance"),
                "responsibility_type": row.get("responsibility_type"),
                "related_party": row.get("related_party"),
            }
            for row in related
        ],
        "source_report_date": report_date,
        "data_status": "available",
    }


def build_stable_enterprise_credit_facts(
    payload: dict[str, Any],
    source_report_date: str | None = None,
) -> dict[str, Any]:
    """Reuse the verified enterprise credit normalization used by the credit report."""
    normalized = _enterprise_credit_model(payload)
    summary = normalized.get("summary") or {}
    loans = normalized.get("loans") or []
    document_unit = _declared_unit(payload, ("normalized_unit", "document_unit", "amount_unit", "currency_unit", "unit"))
    report_basic = payload.get("report_basic") or payload.get("basic_info") or {}
    document_unit = document_unit or _declared_unit(report_basic, ("normalized_unit", "currency_unit", "unit"))
    if not loans:
        loans = [
            {"institution": row.get("institution") or row.get("institution_name"),
             "balance": _money(row, "balance", "outstanding_balance", document_unit=document_unit),
             "due_date": row.get("due_date"), "status": row.get("classification") or row.get("five_category")}
            for row in payload.get("loans") or payload.get("loan_accounts") or [] if isinstance(row, dict)
        ]
    balance = summary.get("unsettled_credit_balance") or _money_sum_objects([row.get("balance") for row in loans])
    if balance is None:
        raw_summary = payload.get("credit_summary") or {}
        balance = _money(
            raw_summary,
            "unsettled_credit_balance", "total_unsettled_balance", "active_borrowing_balance",
            document_unit=document_unit,
        )
    balance = _with_unit(balance, document_unit)
    guarantee = normalized.get("external_guarantee_balance")
    external = normalized.get("external_guarantees") or []
    if not external:
        external = [
            {"guaranteed_subject": row.get("beneficiary"),
             "balance": _money(row, "guarantee_balance", "balance", "amount", document_unit=document_unit)}
            for row in payload.get("guarantees") or payload.get("external_guarantees") or [] if isinstance(row, dict)
        ]
    if guarantee is None:
        guarantee = _money_sum_objects([row.get("balance") for row in external])
    guarantee = _with_unit(guarantee, document_unit)
    report_meta = normalized.get("report_meta") or {}
    return {
        "subject_name": report_meta.get("customer_name"),
        "outstanding_loan_balance": _value(balance),
        "outstanding_loan_balance_money": balance,
        "unit": _unit(balance) or document_unit,
        "outstanding_loan_institution_count": summary.get("unsettled_credit_institution_count") or len({row.get("institution") for row in loans if row.get("institution")}) or None,
        "outstanding_loans": [
            {
                "institution": row.get("institution"),
                "balance": _value(row.get("balance")),
                "balance_money": row.get("balance"),
                "due_date": row.get("due_date"),
                "classification": row.get("status"),
            }
            for row in loans[:30]
        ],
        "overdue_summary": {"count": len(normalized.get("overdue_records") or [])},
        "nonperforming_summary": {"count": None},
        "five_classification": sorted({str(row.get("status")) for row in loans if row.get("status")}),
        "guarantee_balance": _value(guarantee),
        "guarantee_balance_money": guarantee,
        "guarantee_records": [
            {
                "beneficiary": row.get("guaranteed_subject"),
                "balance": _value(row.get("balance")),
                "balance_money": row.get("balance"),
            }
            for row in external
        ],
        "query_summary": {"count": None},
        "upcoming_or_past_due_records": [
            {"institution": row.get("institution"), "balance": _value(row.get("balance")),
             "balance_money": row.get("balance"), "due_date": row.get("due_date")}
            for row in loans[:30] if row.get("due_date")
        ],
        "source_report_date": source_report_date or report_meta.get("report_time"),
    }
