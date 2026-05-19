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


def extract_account_basic_info(workbook: dict[str, Any], metadata: dict[str, Any] | None = None) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    metadata = metadata or {}
    warnings: list[str] = []
    accounts: OrderedDict[str, dict[str, Any]] = OrderedDict()
    dates: list[str] = []
    company_name = normalize_text(metadata.get("customer_name")) or None

    for sheet in workbook.get("sheets") or []:
        meta = sheet.get("meta") or {}
        account_number = normalize_account_number(meta.get("account_number"))
        account_name = normalize_text(meta.get("account_name")) or company_name
        if account_name and any(word in account_name for word in ("银行", "支行", "分行")):
            warnings.append(f"{sheet.get('sheet_name')} 户名疑似银行名称，已不作为客户名称")
            account_name = company_name
        if account_name and not company_name and not any(word in account_name for word in ("银行", "支行", "分行")):
            company_name = account_name
        account_id = account_number or f"sheet:{sheet.get('sheet_name')}"
        account = accounts.setdefault(
            account_id,
            {
                "account_id": account_id,
                "bank_name": meta.get("bank_name"),
                "account_name": account_name,
                "account_number": account_number,
                "currency": normalize_currency(meta.get("currency")),
                "sheet_name": sheet.get("sheet_name"),
                "opening_balance": None,
                "ending_balance": None,
                "total_inflow": 0.0,
                "total_outflow": 0.0,
                "net_cashflow": 0.0,
                "transaction_count": 0,
            },
        )
        rows = sheet.get("rows") or []
        balances = []
        for row in rows:
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
