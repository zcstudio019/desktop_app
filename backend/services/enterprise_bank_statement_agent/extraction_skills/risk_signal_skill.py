from __future__ import annotations

from typing import Any


def detect_risk_signals(
    transactions: list[dict[str, Any]],
    monthly_trends: list[dict[str, Any]],
    counterparties: list[dict[str, Any]],
    loan_related_transactions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    personal_count = sum(1 for item in counterparties if item.get("is_personal_account"))
    if personal_count >= 3:
        signals.append({"type": "大量个人账户往来", "level": "medium", "detail": f"疑似个人账户对手方 {personal_count} 个"})
    integer_count = 0
    for tx in transactions:
        amount = float(tx.get("credit_amount") or tx.get("debit_amount") or 0)
        if amount >= 10000 and abs(amount % 10000) < 0.01:
            integer_count += 1
    if integer_count >= 3:
        signals.append({"type": "大额整数交易频繁", "level": "medium", "detail": f"整数金额交易 {integer_count} 笔"})
    for item in monthly_trends:
        if item.get("is_abnormal_volatility"):
            signals.append({"type": "月度收入断崖式下滑", "level": "medium", "detail": f"{item.get('month')} 收入明显低于平均水平"})
    low_balance_months = [item for item in monthly_trends if item.get("month_end_balance") is not None and float(item.get("month_end_balance") or 0) < 10000]
    if len(low_balance_months) >= 2:
        signals.append({"type": "余额长期很低", "level": "medium", "detail": f"低余额月份 {len(low_balance_months)} 个"})
    if len(loan_related_transactions) >= 3:
        signals.append({"type": "贷款还款压力大", "level": "medium", "detail": f"融资相关交易 {len(loan_related_transactions)} 笔"})
    for index in range(1, len(transactions)):
        prev = transactions[index - 1]
        curr = transactions[index]
        if prev.get("credit_amount") and curr.get("debit_amount"):
            credit = float(prev.get("credit_amount") or 0)
            debit = float(curr.get("debit_amount") or 0)
            if credit >= 100000 and debit >= credit * 0.8:
                signals.append({"type": "快进快出", "level": "high", "detail": "大额收入后相邻交易出现大额转出"})
                break
    return signals
