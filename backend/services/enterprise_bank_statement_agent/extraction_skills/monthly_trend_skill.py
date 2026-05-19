from __future__ import annotations

from collections import defaultdict
from typing import Any


def analyze_monthly_trends(transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_month: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"month": "", "inflow": 0.0, "outflow": 0.0, "net_cashflow": 0.0, "inflow_count": 0, "outflow_count": 0, "ending_balance": None}
    )
    for tx in transactions:
        date = str(tx.get("transaction_date") or "")
        if len(date) < 7:
            continue
        month = date[:7]
        item = by_month[month]
        item["month"] = month
        if tx.get("direction") == "inflow":
            item["inflow"] += float(tx.get("credit_amount") or 0)
            item["inflow_count"] += 1
        elif tx.get("direction") == "outflow":
            item["outflow"] += float(tx.get("debit_amount") or 0)
            item["outflow_count"] += 1
        if tx.get("balance") is not None:
            item["ending_balance"] = tx.get("balance")
    result = []
    for item in by_month.values():
        item["inflow"] = round(item["inflow"], 2)
        item["outflow"] = round(item["outflow"], 2)
        item["net_cashflow"] = round(item["inflow"] - item["outflow"], 2)
        result.append(item)
    return sorted(result, key=lambda x: x["month"])
