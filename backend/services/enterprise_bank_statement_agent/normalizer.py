from __future__ import annotations

import re
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any


def normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\u3000", " ")).strip()


def normalize_amount(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    text = normalize_text(value)
    if not text or text in {"-", "--", "—", "无", "None", "null"}:
        return None
    text = text.replace("¥", "").replace("￥", "").replace("元", "")
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


def safe_float(value: Any) -> float | None:
    return normalize_amount(value)


def safe_int(value: Any) -> int | None:
    amount = normalize_amount(value)
    return int(amount) if amount is not None else None


def normalize_date(value: Any, default_year: int | str | None = None) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (int, float)) and 1 <= float(value) <= 60000:
        try:
            return (datetime(1899, 12, 30) + timedelta(days=float(value))).strftime("%Y-%m-%d")
        except (OverflowError, ValueError):
            return None
    text = normalize_text(value)
    if not text:
        return None
    text = text.replace("年", "-").replace("月", "-").replace("日", "")
    text = text.replace("/", "-").replace(".", "-")
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
                return None
            year = str(default_year)
            month, day = match.groups()
        try:
            return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None


def normalize_currency(value: Any) -> str | None:
    text = normalize_text(value).upper()
    if not text:
        return "人民币"
    if any(word in text for word in ("人民币", "RMB", "CNY", "￥", "¥")):
        return "人民币"
    if "USD" in text or "美元" in text:
        return "美元"
    if "EUR" in text or "欧元" in text:
        return "欧元"
    return normalize_text(value)


def normalize_account_number(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    cleaned = re.sub(r"[^\dA-Za-z]", "", text)
    return cleaned or None


def normalize_transaction_direction(debit_amount: Any, credit_amount: Any) -> str:
    debit = normalize_amount(debit_amount)
    credit = normalize_amount(credit_amount)
    if debit not in (None, 0) and credit not in (None, 0):
        return "unknown"
    if credit not in (None, 0):
        return "inflow"
    if debit not in (None, 0):
        return "outflow"
    return "unknown"


def guess_bank_name(text: Any) -> str | None:
    source = normalize_text(text)
    banks = (
        "中国工商银行",
        "中国建设银行",
        "中国农业银行",
        "中国银行",
        "交通银行",
        "招商银行",
        "民生银行",
        "平安银行",
        "泰隆银行",
        "浙江网商银行",
        "网商银行",
        "浦发银行",
        "兴业银行",
        "中信银行",
        "光大银行",
        "广发银行",
    )
    for bank in banks:
        if bank in source:
            return bank
    match = re.search(r"([\u4e00-\u9fff]{2,20}银行)", source)
    return match.group(1) if match else None


def guess_company_core_name(company_name: Any) -> str:
    text = normalize_text(company_name)
    text = re.sub(r"^(上海|北京|天津|重庆|浙江|江苏|安徽|广东|深圳|杭州|宁波|苏州|南京|中国)", "", text)
    text = re.sub(r"(有限责任公司|股份有限公司|有限公司|集团|公司|科技|贸易|工程|建设|材料|建材|市政|供应链|新材料).*$", "", text)
    return text[:4]


def is_probable_person_name(name: Any) -> bool:
    text = normalize_text(name)
    if not re.fullmatch(r"[\u4e00-\u9fff]{2,4}", text):
        return False
    org_words = ("公司", "有限", "银行", "合作社", "中心", "集团", "工程", "建材", "供应链", "材料", "矿业")
    return not any(word in text for word in org_words)


def normalize_transactions(transactions: list[dict[str, Any]], default_year: int | str | None = None) -> list[dict[str, Any]]:
    normalized = []
    for tx in transactions or []:
        item = dict(tx)
        item["transaction_date"] = normalize_date(item.get("transaction_date"), default_year) or item.get("transaction_date")
        item["post_date"] = normalize_date(item.get("post_date"), default_year) or item.get("post_date") or item.get("transaction_date")
        item["debit_amount"] = normalize_amount(item.get("debit_amount"))
        item["credit_amount"] = normalize_amount(item.get("credit_amount"))
        item["balance"] = normalize_amount(item.get("balance"))
        item["currency"] = normalize_currency(item.get("currency"))
        item["account_number"] = normalize_account_number(item.get("account_number"))
        item["counterparty_account"] = normalize_account_number(item.get("counterparty_account"))
        item["direction"] = normalize_transaction_direction(item.get("debit_amount"), item.get("credit_amount"))
        item["normalized_amount"] = float(item.get("credit_amount") or item.get("debit_amount") or 0)
        normalized.append(item)
    return normalized
