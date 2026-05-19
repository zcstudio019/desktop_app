from __future__ import annotations

from collections import defaultdict
from typing import Any


def analyze_monthly_trends(transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    months: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "month": "",
            "credit_amount": 0.0,
            "debit_amount": 0.0,
            "net_inflow": 0.0,
            "transaction_count": 0,
            "month_end_balance": None,
            "is_abnormal_volatility": False,
        }
    )
    for tx in transactions:
        date = str(tx.get("transaction_date") or "")
        if len(date) < 7:
            continue
        key = date[:7]
        item = months[key]
        item["month"] = key
        item["credit_amount"] += float(tx.get("credit_amount") or 0)
        item["debit_amount"] += float(tx.get("debit_amount") or 0)
        item["transaction_count"] += 1
        if tx.get("balance") is not None:
            item["month_end_balance"] = tx.get("balance")
    values = list(months.values())
    avg_credit = sum(item["credit_amount"] for item in values) / len(values) if values else 0
    for item in values:
        item["credit_amount"] = round(item["credit_amount"], 2)
        item["debit_amount"] = round(item["debit_amount"], 2)
        item["net_inflow"] = round(item["credit_amount"] - item["debit_amount"], 2)
        item["is_abnormal_volatility"] = bool(avg_credit and item["credit_amount"] < avg_credit * 0.4)
    return sorted(values, key=lambda x: x["month"])
