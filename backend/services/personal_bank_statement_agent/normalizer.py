from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.services.enterprise_bank_statement_agent.normalizer import (
    normalize_account_number,
    normalize_amount,
    normalize_currency,
    normalize_date,
    normalize_text,
)


def direction_from_amounts(debit: Any, credit: Any) -> str:
    debit_amount = float(normalize_amount(debit) or 0)
    credit_amount = float(normalize_amount(credit) or 0)
    if credit_amount > 0 and debit_amount <= 0:
        return "income"
    if debit_amount > 0 and credit_amount <= 0:
        return "expense"
    return "unknown"


def months_count(start: str | None, end: str | None, observed_months: int = 0) -> int:
    if start and end:
        try:
            a = datetime.strptime(start[:10], "%Y-%m-%d")
            b = datetime.strptime(end[:10], "%Y-%m-%d")
            return max(1, (b.year - a.year) * 12 + b.month - a.month + 1)
        except Exception:
            pass
    return max(1, observed_months or 1)


def round2(value: Any) -> float:
    try:
        return round(float(value or 0), 2)
    except Exception:
        return 0.0


__all__ = [
    "direction_from_amounts",
    "months_count",
    "normalize_account_number",
    "normalize_amount",
    "normalize_currency",
    "normalize_date",
    "normalize_text",
    "round2",
]
