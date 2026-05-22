from __future__ import annotations

from collections import OrderedDict
from typing import Any

from ..normalizer import normalize_account_number, normalize_currency, normalize_date, normalize_text


def extract_account_info(workbook: dict[str, Any], owner: dict[str, Any] | None = None) -> tuple[list[dict[str, Any]], dict[str, str], list[str]]:
    owner = owner or {}
    warnings: list[str] = []
    accounts: OrderedDict[str, dict[str, Any]] = OrderedDict()
    dates: list[str] = []
    for sheet in workbook.get("sheets") or []:
        meta = sheet.get("meta") or {}
        sheet_name = normalize_text(sheet.get("sheet_name"))
        account_no = normalize_account_number(meta.get("account_number")) or ""
        bank_name = normalize_text(meta.get("bank_name")) or sheet_name
        account_name = normalize_text(meta.get("account_name")) or normalize_text(owner.get("name"))
        account_id = f"{bank_name}:{account_no or sheet_name}"
        accounts.setdefault(
            account_id,
            {
                "account_id": account_id,
                "bank_name": bank_name,
                "account_name": account_name,
                "account_no": account_no,
                "currency": normalize_currency(meta.get("currency")) or "人民币",
                "sheet_name": sheet_name,
                "statement_period": {"start_date": "", "end_date": ""},
            },
        )
        for value in (meta.get("period_start"), meta.get("period_end")):
            date = normalize_date(value)
            if date:
                dates.append(date)
        for row in sheet.get("rows") or []:
            date = normalize_date(row.get("transaction_date") or row.get("post_date"))
            if date:
                dates.append(date)
    period = {"start_date": min(dates) if dates else "", "end_date": max(dates) if dates else ""}
    for account in accounts.values():
        account["statement_period"] = dict(period)
    if not accounts:
        warnings.append("未识别到个人流水账户信息")
    return list(accounts.values()), period, warnings
