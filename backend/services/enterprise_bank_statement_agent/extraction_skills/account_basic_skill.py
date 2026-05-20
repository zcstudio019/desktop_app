from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
from typing import Any

from ..normalizer import normalize_account_number, normalize_amount, normalize_currency, normalize_date, normalize_text


def _months_count(start: str | None, end: str | None) -> int | None:
    if not start or not end:
        return None
    try:
        a = datetime.strptime(start, "%Y-%m-%d")
        b = datetime.strptime(end, "%Y-%m-%d")
    except ValueError:
        return None
    return max(1, (b.year - a.year) * 12 + b.month - a.month + 1)


def _looks_like_bank_name(value: str | None) -> bool:
    text = normalize_text(value)
    return bool(text and any(word in text for word in ("银行", "支行", "分行", "开户行", "开户机构")))


def extract_account_basic_info(workbook: dict[str, Any], metadata: dict[str, Any] | None = None) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    metadata = metadata or {}
    warnings: list[str] = []
    accounts: OrderedDict[str, dict[str, Any]] = OrderedDict()
    dates: list[str] = []
    company_name = normalize_text(metadata.get("customer_name") or metadata.get("customerName")) or None

    for sheet in workbook.get("sheets") or []:
        sheet_name = sheet.get("sheet_name") or ""
        meta = sheet.get("meta") or {}
        account_number = normalize_account_number(meta.get("account_number"))
        account_name = normalize_text(meta.get("account_name")) or company_name
        if _looks_like_bank_name(account_name):
            warnings.append(f"{sheet_name} 户名疑似银行名称，已不作为客户名称")
            account_name = company_name
        if account_name and not company_name and not _looks_like_bank_name(account_name):
            company_name = account_name

        bank_name = normalize_text(meta.get("bank_name")) or sheet_name
        account_id = normalize_text(meta.get("account_id")) or f"{bank_name}:{account_number or sheet_name}"
        summary_inflow = normalize_amount(meta.get("summary_inflow"))
        summary_outflow = normalize_amount(meta.get("summary_outflow"))
        summary_inflow_count = int(meta.get("summary_inflow_count") or 0)
        summary_outflow_count = int(meta.get("summary_outflow_count") or 0)
        account = accounts.setdefault(
            account_id,
            {
                "account_id": account_id,
                "bank_name": bank_name,
                "account_name": account_name,
                "account_number": account_number,
                "branch_name": normalize_text(meta.get("branch_name")) or None,
                "currency": normalize_currency(meta.get("currency")),
                "sheet_name": sheet_name,
                "opening_balance": None,
                "ending_balance": None,
                "total_inflow": float(summary_inflow or 0),
                "total_outflow": float(summary_outflow or 0),
                "net_cashflow": float((summary_inflow or 0) - (summary_outflow or 0)),
                "transaction_count": summary_inflow_count + summary_outflow_count,
                "summary_inflow": summary_inflow,
                "summary_outflow": summary_outflow,
                "summary_inflow_count": summary_inflow_count or None,
                "summary_outflow_count": summary_outflow_count or None,
            },
        )

        for candidate in (meta.get("period_start"), meta.get("period_end")):
            date = normalize_date(candidate)
            if date:
                dates.append(date)

        balances = []
        for row in sheet.get("rows") or []:
            date = normalize_date(row.get("transaction_date") or row.get("post_date"))
            if date:
                dates.append(date)
            balance = normalize_amount(row.get("balance"))
            if balance is not None:
                balances.append(balance)
        if balances:
            account["opening_balance"] = balances[0] if account["opening_balance"] is None else account["opening_balance"]
            account["ending_balance"] = balances[-1]

    start_date = min(dates) if dates else None
    end_date = max(dates) if dates else None
    period = {"start_date": start_date, "end_date": end_date, "months_count": _months_count(start_date, end_date)}
    if not accounts:
        warnings.append("未识别到企业流水账户")
    return {"company_name": company_name, "statement_period": period}, list(accounts.values()), warnings
