from __future__ import annotations

from collections import defaultdict
from typing import Any


def detect_large_transactions(
    transactions: list[dict[str, Any]],
    statement_summary: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    summary = statement_summary or {}
    monthly_avg_credit = float(summary.get("monthly_average_credit") or 0)
    threshold = max(100000.0, monthly_avg_credit * 0.3 if monthly_avg_credit else 0.0)
    result: list[dict[str, Any]] = []
    by_counterparty: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for tx in transactions:
        amount = float(tx.get("credit_amount") or tx.get("debit_amount") or 0)
        name = str(tx.get("counterparty_name") or "")
        if name:
            by_counterparty[name].append(tx)
        reasons = []
        if amount >= threshold:
            reasons.append("单笔大额收入" if tx.get("credit_amount") else "单笔大额支出")
        if amount >= 10000 and abs(amount % 10000) < 0.01:
            reasons.append("整数金额交易")
        if reasons:
            result.append({"reason": "、".join(reasons), "amount": round(amount, 2), "transaction": tx})
    for name, items in by_counterparty.items():
        large_in = sum(1 for tx in items if float(tx.get("credit_amount") or 0) >= threshold)
        large_out = sum(1 for tx in items if float(tx.get("debit_amount") or 0) >= threshold)
        if large_in and large_out:
            result.append({"reason": "同一对手方大额往返", "counterparty_name": name, "transaction_count": len(items)})
    return result
