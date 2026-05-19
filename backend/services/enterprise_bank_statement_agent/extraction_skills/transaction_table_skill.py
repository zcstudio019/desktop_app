from __future__ import annotations

import re
from typing import Any

from ..normalizer import normalize_account_number, normalize_amount, normalize_date
from ..schema import empty_transaction

DATE_TOKEN = r"(?:20\d{2}[年./-]?\d{1,2}[月./-]?\d{1,2}日?|\d{1,2}[./-]\d{1,2})"
AMOUNT_TOKEN = r"(?:[-+]?￥?\d[\d,]*(?:\.\d+)?|\(|\)-?)"


def _default_year(segments: dict[str, Any]) -> str | None:
    match = re.search(r"(20\d{2})", str(segments.get("text") or ""))
    return match.group(1) if match else None


def _looks_like_header(line: str) -> bool:
    return sum(word in line for word in ("日期", "摘要", "借方", "贷方", "余额", "对方", "交易")) >= 3


def _extract_amounts(line: str) -> list[tuple[str, float | None]]:
    matches = []
    for match in re.finditer(r"[-+]?￥?\d[\d,]*(?:\.\d+)?", line):
        raw = match.group(0)
        # Avoid treating account numbers as amounts.
        if len(raw.replace(",", "").replace(".", "")) > 13:
            continue
        matches.append((raw, normalize_amount(raw)))
    return matches


def extract_transactions(segments: dict[str, Any], account_basic_info: dict[str, Any] | None = None) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    transactions: list[dict[str, Any]] = []
    default_year = _default_year(segments)
    pending: dict[str, Any] | None = None

    for item in segments.get("lines") or []:
        line = str(item.get("text") or "").strip()
        if not line or _looks_like_header(line):
            continue
        date_match = re.search(DATE_TOKEN, line)
        if not date_match:
            if pending and line and not re.search(r"^\s*第?\d+\s*页", line):
                pending["summary"] = " ".join(filter(None, [pending.get("summary"), line]))
                pending["source_text"] = f"{pending.get('source_text', '')}\n{line}".strip()
            continue
        if pending:
            transactions.append(pending)
            pending = None

        tx = empty_transaction()
        tx["transaction_date"] = normalize_date(date_match.group(0), default_year=default_year)
        tx["posting_date"] = tx["transaction_date"]
        tx["source_page"] = item.get("page")
        tx["source_text"] = line

        tail = line[date_match.end() :].strip()
        amounts = _extract_amounts(tail)
        account_match = re.search(r"\b\d{8,32}\b", tail.replace(" ", ""))
        if account_match:
            tx["counterparty_account"] = normalize_account_number(account_match.group(0))

        if len(amounts) >= 3:
            tx["debit_amount"] = amounts[-3][1]
            tx["credit_amount"] = amounts[-2][1]
            tx["balance"] = amounts[-1][1]
        elif len(amounts) == 2:
            amount = amounts[0][1]
            tx["balance"] = amounts[1][1]
            if any(word in line for word in ("贷方", "收入", "转入", "收款", "存入")):
                tx["credit_amount"] = amount
            elif any(word in line for word in ("借方", "支出", "转出", "付款", "扣款")):
                tx["debit_amount"] = amount
            else:
                tx["credit_amount"] = amount
        elif len(amounts) == 1:
            amount = amounts[0][1]
            if any(word in line for word in ("余额", "结余")):
                tx["balance"] = amount
            elif any(word in line for word in ("贷方", "收入", "转入", "收款", "存入")):
                tx["credit_amount"] = amount
            else:
                tx["debit_amount"] = amount

        cleaned_tail = tail
        for raw, _ in amounts:
            cleaned_tail = cleaned_tail.replace(raw, " ")
        cleaned_tail = re.sub(r"\b\d{8,32}\b", " ", cleaned_tail)
        cleaned_tail = re.sub(r"\s+", " ", cleaned_tail).strip(" |,，")
        parts = cleaned_tail.split()
        if parts:
            tx["summary"] = parts[0]
            if len(parts) > 1:
                tx["counterparty_name"] = parts[-1]
                tx["usage"] = " ".join(parts[1:-1])
        pending = tx

    if pending:
        transactions.append(pending)
    if not transactions:
        warnings.append("未识别到交易明细，未编造交易记录")
    return transactions, warnings
