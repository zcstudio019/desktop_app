from __future__ import annotations

from collections import defaultdict
from typing import Any

from ..normalizer import round2


def _month(date: Any) -> str:
    text = str(date or "")
    return text[:7] if len(text) >= 7 else "unknown"


def analyze_expenses(transactions: list[dict[str, Any]], month_count: int | None = None) -> dict[str, Any]:
    monthly: dict[str, dict[str, float]] = defaultdict(lambda: {
        "expense": 0.0,
        "loan_repayment_expense": 0.0,
        "credit_card_repayment_expense": 0.0,
        "quick_payment_expense": 0.0,
        "living_expense": 0.0,
        "operating_expense": 0.0,
    })
    totals = defaultdict(float)
    repayment_frequency = 0
    for tx in transactions:
        debit = float(tx.get("debit_amount") or 0)
        if tx.get("direction") != "expense" or debit <= 0:
            continue
        category = str(tx.get("category") or "")
        month = _month(tx.get("transaction_date"))
        monthly[month]["expense"] += debit
        totals["raw_total_expense"] += debit
        if category == "loan_repayment_expense":
            totals["loan_repayment_expense"] += debit
            monthly[month]["loan_repayment_expense"] += debit
            repayment_frequency += 1
        elif category == "credit_card_repayment_expense":
            totals["credit_card_repayment_expense"] += debit
            monthly[month]["credit_card_repayment_expense"] += debit
        elif category == "quick_payment_expense":
            totals["quick_payment_expense"] += debit
            monthly[month]["quick_payment_expense"] += debit
        elif category == "living_expense":
            totals["living_expense"] += debit
            monthly[month]["living_expense"] += debit
        elif category == "operating_expense":
            totals["operating_expense"] += debit
            monthly[month]["operating_expense"] += debit
        elif category == "internal_transfer":
            totals["internal_transfer_expense"] += debit
        elif category == "investment_expense":
            totals["investment_expense"] += debit
        else:
            totals["other_expense"] += debit
    divisor = max(1, int(month_count or len([m for m in monthly if m != "unknown"]) or 1))
    raw_total = totals["raw_total_expense"]
    totals["avg_monthly_loan_repayment"] = totals["loan_repayment_expense"] / divisor
    totals["loan_repayment_ratio"] = totals["loan_repayment_expense"] / raw_total if raw_total else 0.0
    return {
        **{key: round2(value) for key, value in totals.items()},
        "monthly_expense": [{"month": month, **{k: round2(v) for k, v in item.items()}} for month, item in sorted(monthly.items())],
        "repayment_frequency": repayment_frequency,
        "has_frequent_loan_or_credit_card_repayment": repayment_frequency >= 3 or totals["credit_card_repayment_expense"] >= 30000,
        "has_abnormal_large_expense": any("abnormal_large_expense" in (tx.get("risk_tags") or []) for tx in transactions),
    }
