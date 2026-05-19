from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from ..normalizer import normalize_amount, safe_int


def _round(value: float | None) -> float | None:
    return round(value, 2) if value is not None else None


def _months_between(start: str, end: str) -> int:
    try:
        a = datetime.strptime(start, "%Y-%m-%d")
        b = datetime.strptime(end, "%Y-%m-%d")
    except ValueError:
        return 1
    return max(1, (b.year - a.year) * 12 + b.month - a.month + 1)


def _extract_summary_from_text(text: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    patterns = {
        "total_debit_amount": r"(?:借方总金额|支出合计|借方发生额合计)\s*[:：]?\s*(￥?[\d,]+(?:\.\d+)?)",
        "total_credit_amount": r"(?:贷方总金额|收入合计|贷方发生额合计)\s*[:：]?\s*(￥?[\d,]+(?:\.\d+)?)",
        "total_debit_count": r"(?:借方总笔数|支出笔数)\s*[:：]?\s*(\d+)",
        "total_credit_count": r"(?:贷方总笔数|收入笔数)\s*[:：]?\s*(\d+)",
        "average_daily_balance": r"(?:日均余额|平均日余额)\s*[:：]?\s*(￥?[\d,]+(?:\.\d+)?)",
    }
    for field, pattern in patterns.items():
        match = re.search(pattern, text)
        if not match:
            continue
        if field.endswith("_count"):
            result[field] = safe_int(match.group(1))
        else:
            result[field] = normalize_amount(match.group(1))
    return result


def extract_or_derive_account_summary(
    segments: dict[str, Any],
    transactions: list[dict[str, Any]],
    account_basic_info: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    from_text = _extract_summary_from_text(str(segments.get("text") or ""))
    debit_total = sum(float(tx.get("debit_amount") or 0) for tx in transactions)
    credit_total = sum(float(tx.get("credit_amount") or 0) for tx in transactions)
    debit_count = sum(1 for tx in transactions if tx.get("debit_amount") not in (None, 0))
    credit_count = sum(1 for tx in transactions if tx.get("credit_amount") not in (None, 0))
    derived = {
        "total_debit_amount": _round(debit_total),
        "total_credit_amount": _round(credit_total),
        "total_debit_count": debit_count,
        "total_credit_count": credit_count,
        "total_transaction_count": len(transactions),
        "average_daily_balance": None,
    }
    result = {**derived, **{k: v for k, v in from_text.items() if v is not None}}
    if "total_transaction_count" not in result or result.get("total_transaction_count") is None:
        result["total_transaction_count"] = (result.get("total_debit_count") or 0) + (result.get("total_credit_count") or 0)
    for field in ("total_debit_amount", "total_credit_amount"):
        if field in from_text and abs(float(from_text[field] or 0) - float(derived[field] or 0)) > 1:
            warnings.append(f"原文汇总字段 {field} 与明细反算不一致，保留原文汇总值")
    months = _months_between(account_basic_info.get("statement_period_start") or "", account_basic_info.get("statement_period_end") or "")
    result["monthly_average_credit"] = _round(float(result.get("total_credit_amount") or 0) / months)
    result["monthly_average_debit"] = _round(float(result.get("total_debit_amount") or 0) / months)
    return result, warnings
