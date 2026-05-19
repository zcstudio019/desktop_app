from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any


def safe_float(value: Any) -> float | None:
    amount = normalize_amount(value)
    return amount


def safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return None


def normalize_amount(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"-", "--", "—", "无", "null", "None"}:
        return None
    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]
    text = (
        text.replace("￥", "")
        .replace("¥", "")
        .replace("人民币", "")
        .replace("元", "")
        .replace(",", "")
        .replace("，", "")
        .replace(" ", "")
    )
    if text.endswith("-"):
        negative = True
        text = text[:-1]
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError):
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return None
        try:
            number = Decimal(match.group(0))
        except InvalidOperation:
            return None
    if negative and number > 0:
        number = -number
    return float(number)


def normalize_date(value: Any, default_year: int | str | None = None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("年", "-").replace("月", "-").replace("日", "")
    text = text.replace("/", "-").replace(".", "-")
    text = re.sub(r"\s+", "", text)
    patterns = (
        r"((?:19|20)\d{2})-(\d{1,2})-(\d{1,2})",
        r"((?:19|20)\d{2})(\d{2})(\d{2})",
        r"(\d{1,2})-(\d{1,2})",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        if len(match.groups()) == 3:
            year, month, day = match.groups()
        else:
            if not default_year:
                return ""
            year = str(default_year)
            month, day = match.groups()
        try:
            return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    return ""


def normalize_currency(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return "人民币"
    if any(item in text for item in ("CNY", "RMB", "人民币", "￥", "¥")):
        return "人民币"
    if "USD" in text or "美元" in text:
        return "美元"
    if "EUR" in text or "欧元" in text:
        return "欧元"
    return str(value).strip()


def normalize_account_number(value: Any) -> str:
    text = str(value or "").strip()
    return re.sub(r"[^\dA-Za-z]", "", text)


def normalize_transaction_direction(
    summary: str = "",
    debit_amount: Any = None,
    credit_amount: Any = None,
) -> str:
    debit = normalize_amount(debit_amount)
    credit = normalize_amount(credit_amount)
    if credit not in (None, 0):
        return "credit"
    if debit not in (None, 0):
        return "debit"
    text = str(summary or "")
    if any(word in text for word in ("收入", "贷方", "转入", "存入", "收款")):
        return "credit"
    if any(word in text for word in ("支出", "借方", "转出", "扣款", "付款")):
        return "debit"
    return ""


def normalize_transactions(transactions: list[dict[str, Any]], default_year: int | str | None = None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for tx in transactions or []:
        item = dict(tx)
        item["transaction_date"] = normalize_date(item.get("transaction_date"), default_year=default_year) or str(item.get("transaction_date") or "")
        item["posting_date"] = normalize_date(item.get("posting_date"), default_year=default_year) or item["transaction_date"]
        item["debit_amount"] = normalize_amount(item.get("debit_amount"))
        item["credit_amount"] = normalize_amount(item.get("credit_amount"))
        item["balance"] = normalize_amount(item.get("balance"))
        item["currency"] = normalize_currency(item.get("currency"))
        item["counterparty_account"] = normalize_account_number(item.get("counterparty_account"))
        item["transaction_type"] = item.get("transaction_type") or normalize_transaction_direction(
            item.get("summary") or "",
            item.get("debit_amount"),
            item.get("credit_amount"),
        )
        normalized.append(item)
    return normalized
