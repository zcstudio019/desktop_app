from __future__ import annotations

from datetime import datetime
from typing import Any


def _close(a: float | None, b: float | None, tolerance: float = 1.0) -> bool:
    return abs(float(a or 0) - float(b or 0)) <= tolerance


def validate_enterprise_bank_statement_result(result: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    period = result.get("statement_period") or {}
    summary = result.get("summary") or {}
    accounts = result.get("accounts") or []
    monthly = result.get("monthly_summary") or []
    transactions = result.get("transactions") or []
    start = period.get("start_date")
    end = period.get("end_date")
    if start and end:
        try:
            if datetime.strptime(start, "%Y-%m-%d") > datetime.strptime(end, "%Y-%m-%d"):
                warnings.append("流水期间开始日期晚于结束日期")
        except ValueError:
            warnings.append("流水期间日期格式不合法")
    derived_inflow = round(sum(float(tx.get("credit_amount") or 0) for tx in transactions), 2)
    derived_outflow = round(sum(float(tx.get("debit_amount") or 0) for tx in transactions), 2)
    if not _close(summary.get("total_inflow"), derived_inflow):
        warnings.append("总收入与交易明细反算不一致")
    if not _close(summary.get("total_outflow"), derived_outflow):
        warnings.append("总支出与交易明细反算不一致")
    if int(summary.get("inflow_count") or 0) != sum(1 for tx in transactions if tx.get("direction") == "inflow"):
        warnings.append("收入笔数与交易明细反算不一致")
    if int(summary.get("outflow_count") or 0) != sum(1 for tx in transactions if tx.get("direction") == "outflow"):
        warnings.append("支出笔数与交易明细反算不一致")
    monthly_inflow = round(sum(float(item.get("inflow") or 0) for item in monthly), 2)
    monthly_outflow = round(sum(float(item.get("outflow") or 0) for item in monthly), 2)
    if monthly and not _close(summary.get("total_inflow"), monthly_inflow):
        warnings.append("月度收入汇总与总收入不一致")
    if monthly and not _close(summary.get("total_outflow"), monthly_outflow):
        warnings.append("月度支出汇总与总支出不一致")
    account_inflow = round(sum(float(item.get("total_inflow") or 0) for item in accounts), 2)
    account_outflow = round(sum(float(item.get("total_outflow") or 0) for item in accounts), 2)
    if accounts and not _close(summary.get("total_inflow"), account_inflow):
        warnings.append("账户维度收入汇总与总收入不一致")
    if accounts and not _close(summary.get("total_outflow"), account_outflow):
        warnings.append("账户维度支出汇总与总支出不一致")
    for account in accounts:
        opening = account.get("opening_balance")
        ending = account.get("ending_balance")
        if opening is not None and ending is not None:
            expected = float(opening or 0) + float(account.get("total_inflow") or 0) - float(account.get("total_outflow") or 0)
            if not _close(ending, expected, tolerance=10.0):
                warnings.append(f"账户 {account.get('account_number') or account.get('sheet_name')} 期末余额无法由期初余额 + 收入 - 支出近似校验通过")
    return warnings
