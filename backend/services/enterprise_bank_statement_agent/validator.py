from __future__ import annotations

from datetime import datetime
from typing import Any


def _amount_close(a: Any, b: Any, tolerance: float = 1.0) -> bool:
    try:
        return abs(float(a or 0) - float(b or 0)) <= tolerance
    except (TypeError, ValueError):
        return True


def validate_enterprise_bank_statement_result(result: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    basic = result.get("account_basic_info") or {}
    summary = result.get("statement_summary") or {}
    transactions = result.get("transactions") or []
    start = basic.get("statement_period_start")
    end = basic.get("statement_period_end")
    if start and end:
        try:
            if datetime.strptime(start, "%Y-%m-%d") > datetime.strptime(end, "%Y-%m-%d"):
                warnings.append("流水期间开始日期晚于结束日期")
        except ValueError:
            warnings.append("流水期间日期格式不合法")
    debit_total = round(sum(float(tx.get("debit_amount") or 0) for tx in transactions), 2)
    credit_total = round(sum(float(tx.get("credit_amount") or 0) for tx in transactions), 2)
    debit_count = sum(1 for tx in transactions if tx.get("debit_amount") not in (None, 0))
    credit_count = sum(1 for tx in transactions if tx.get("credit_amount") not in (None, 0))
    if summary.get("total_debit_count") is not None and int(summary.get("total_debit_count") or 0) != debit_count:
        warnings.append("借方笔数与交易明细反算不一致")
    if summary.get("total_credit_count") is not None and int(summary.get("total_credit_count") or 0) != credit_count:
        warnings.append("贷方笔数与交易明细反算不一致")
    if summary.get("total_debit_amount") is not None and not _amount_close(summary.get("total_debit_amount"), debit_total):
        warnings.append("借方金额与交易明细反算不一致")
    if summary.get("total_credit_amount") is not None and not _amount_close(summary.get("total_credit_amount"), credit_total):
        warnings.append("贷方金额与交易明细反算不一致")
    opening = basic.get("opening_balance")
    closing = basic.get("closing_balance")
    if opening is not None and closing is not None:
        expected = float(opening or 0) + credit_total - debit_total
        if not _amount_close(closing, expected, tolerance=5.0):
            warnings.append("期末余额无法由期初余额 + 收入 - 支出近似校验通过")
    return warnings
