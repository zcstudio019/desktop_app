from __future__ import annotations

from collections import defaultdict
from typing import Any


def _top(transactions: list[dict[str, Any]], amount_field: str) -> list[dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"name": "未知对手方", "account": "", "amount": 0.0, "count": 0})
    for tx in transactions:
        amount = float(tx.get(amount_field) or 0)
        if amount <= 0:
            continue
        key = f"{tx.get('counterparty_name') or '未知对手方'}|{tx.get('counterparty_account') or ''}"
        item = stats[key]
        item["name"] = tx.get("counterparty_name") or "未知对手方"
        item["account"] = tx.get("counterparty_account") or ""
        item["amount"] = round(float(item["amount"]) + amount, 2)
        item["count"] = int(item["count"]) + 1
    return sorted(stats.values(), key=lambda item: float(item.get("amount") or 0), reverse=True)[:10]


def analyze_counterparties(transactions: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    income = [tx for tx in transactions if tx.get("direction") == "income" and float(tx.get("credit_amount") or 0) > 0]
    expense = [tx for tx in transactions if tx.get("direction") == "expense" and float(tx.get("debit_amount") or 0) > 0]
    return {
        "top_income_counterparties": _top(income, "credit_amount"),
        "top_expense_counterparties": _top(expense, "debit_amount"),
    }
