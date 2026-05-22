from __future__ import annotations

from collections import defaultdict
from typing import Any


def _month(date: Any) -> str:
    text = str(date or "")
    return text[:7] if len(text) >= 7 else "unknown"


def analyze_expenses(transactions: list[dict[str, Any]]) -> dict[str, Any]:
    monthly: dict[str, dict[str, float]] = defaultdict(lambda: {"expense": 0.0, "living_expense": 0.0, "operating_expense": 0.0, "loan_repayment": 0.0, "credit_card_repayment": 0.0})
    loan_repayment_count = 0
    credit_card_amount = 0.0
    abnormal_large_expense = False
    for tx in transactions:
        debit = float(tx.get("debit_amount") or 0)
        if debit <= 0:
            continue
        month = _month(tx.get("transaction_date"))
        category = tx.get("category")
        monthly[month]["expense"] += debit
        if category == "living_expense":
            monthly[month]["living_expense"] += debit
        elif category == "operating_expense":
            monthly[month]["operating_expense"] += debit
        elif category == "loan_repayment":
            monthly[month]["loan_repayment"] += debit
            loan_repayment_count += 1
        elif category == "credit_card_repayment":
            monthly[month]["credit_card_repayment"] += debit
            credit_card_amount += debit
        elif category == "abnormal_large_expense":
            abnormal_large_expense = True
    return {
        "monthly_expense": [{"month": month, **{k: round(v, 2) for k, v in item.items()}} for month, item in sorted(monthly.items())],
        "has_frequent_loan_or_credit_card_repayment": loan_repayment_count >= 3 or credit_card_amount >= 30000,
        "has_abnormal_large_expense": abnormal_large_expense,
    }
