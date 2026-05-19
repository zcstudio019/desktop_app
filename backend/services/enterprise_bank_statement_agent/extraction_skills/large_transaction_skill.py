from __future__ import annotations

from collections import defaultdict
from typing import Any


def detect_large_transactions(transactions: list[dict[str, Any]], summary: dict[str, Any]) -> list[dict[str, Any]]:
    threshold = max(100000.0, float(summary.get("average_monthly_inflow") or 0) * 0.3)
    result: list[dict[str, Any]] = []
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for tx in transactions:
        amount = float(tx.get("normalized_amount") or 0)
        date = str(tx.get("transaction_date") or "")
        if date:
            by_day[date].append(tx)
        reasons = []
        if amount >= threshold:
            reasons.append("单笔大额收入" if tx.get("direction") == "inflow" else "单笔大额支出")
        if amount >= 10000 and abs(amount % 10000) < 0.01:
            reasons.append("整数金额交易")
        if reasons:
            result.append({"reason": "、".join(reasons), "amount": round(amount, 2), "transaction_id": tx.get("transaction_id"), "transaction": tx})
    for date, items in by_day.items():
        inflows = [tx for tx in items if tx.get("direction") == "inflow" and float(tx.get("normalized_amount") or 0) >= threshold]
        outflows = [tx for tx in items if tx.get("direction") == "outflow" and float(tx.get("normalized_amount") or 0) >= threshold]
        for inflow in inflows:
            for outflow in outflows:
                if abs(float(inflow.get("normalized_amount") or 0) - float(outflow.get("normalized_amount") or 0)) <= max(1000, float(inflow.get("normalized_amount") or 0) * 0.05):
                    result.append({"reason": "同日同额/近似金额进出交易组合", "amount": float(inflow.get("normalized_amount") or 0), "transaction_id": inflow.get("transaction_id"), "paired_transaction_id": outflow.get("transaction_id")})
    return result
