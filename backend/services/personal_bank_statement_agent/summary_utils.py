from __future__ import annotations

from datetime import datetime
from typing import Any

from .normalizer import normalize_amount, normalize_text, round2


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _date(value: Any) -> str:
    text = str(value or "")[:10]
    return text if len(text) >= 7 else ""


def _month_count(dates: list[str]) -> int:
    months = sorted({date[:7] for date in dates if len(date) >= 7})
    if not months:
        return 1
    try:
        start = datetime.strptime(months[0], "%Y-%m")
        end = datetime.strptime(months[-1], "%Y-%m")
        return max(1, (end.year - start.year) * 12 + end.month - start.month + 1)
    except ValueError:
        return max(1, len(months))


def _amount(tx: dict[str, Any]) -> float:
    raw_direction = normalize_text(tx.get("direction") or tx.get("收支") or tx.get("支/收"))
    credit = normalize_amount(tx.get("credit_amount") or tx.get("creditAmount") or tx.get("收入"))
    debit = normalize_amount(tx.get("debit_amount") or tx.get("debitAmount") or tx.get("支出"))
    if raw_direction in {"expense", "debit", "outflow", "支", "支出"} and debit and debit > 0:
        return -float(debit)
    if raw_direction in {"income", "credit", "inflow", "收", "收入"} and credit and credit > 0:
        return float(credit)
    value = (
        tx.get("transaction_amount")
        if tx.get("transaction_amount") not in (None, "")
        else tx.get("amount")
        if tx.get("amount") not in (None, "")
        else tx.get("交易金额")
        if tx.get("交易金额") not in (None, "")
        else tx.get("金额")
    )
    amount = normalize_amount(value)
    if amount is not None:
        return float(amount)
    if credit and credit > 0:
        return float(credit)
    if debit and debit > 0:
        return -float(debit)
    return 0.0


def _summary(tx: dict[str, Any]) -> str:
    return normalize_text(tx.get("summary") or tx.get("摘要") or tx.get("交易摘要") or tx.get("Transaction Type") or tx.get("description"))


def _counterparty(tx: dict[str, Any]) -> str:
    return normalize_text(
        tx.get("counterparty_name")
        or tx.get("counterparty")
        or tx.get("对手信息")
        or tx.get("对方户名")
        or tx.get("对方名称")
        or tx.get("交易对手")
        or tx.get("Counter Party")
    )


def _direction(tx: dict[str, Any], amount: float) -> str:
    raw = normalize_text(tx.get("direction") or tx.get("收支") or tx.get("支/收"))
    if raw in {"收", "收入", "income", "credit", "inflow"} or amount > 0:
        return "income"
    if raw in {"支", "支出", "expense", "debit", "outflow"} or amount < 0:
        return "expense"
    return "unknown"


def collect_transaction_details(payload: dict[str, Any]) -> list[dict[str, Any]]:
    transactions: list[dict[str, Any]] = []
    transactions.extend(_dict(item) for item in _list(payload.get("transactions")))
    transactions.extend(_dict(item) for item in _list(payload.get("交易明细列表")))
    extracted_json = _dict(payload.get("extracted_json"))
    transactions.extend(_dict(item) for item in _list(extracted_json.get("transactions")))
    transactions.extend(_dict(item) for item in _list(extracted_json.get("交易明细列表")))
    summary = _dict(payload.get("summary"))
    transactions.extend(_dict(item) for item in _list(summary.get("交易明细列表")))
    for account in _list(payload.get("accounts")):
        transactions.extend(_dict(item) for item in _list(_dict(account).get("transactions")))
    return transactions


def build_deterministic_summary(transactions: list[dict[str, Any]], month_count: int | None = None) -> dict[str, Any]:
    total_income = 0.0
    total_expense = 0.0
    income_count = 0
    expense_count = 0
    dates: list[str] = []
    max_income: dict[str, Any] | None = None
    max_expense: dict[str, Any] | None = None
    for tx in transactions:
        amount = _amount(tx)
        direction = _direction(tx, amount)
        date = _date(tx.get("transaction_date") or tx.get("交易日期") or tx.get("date"))
        if date:
            dates.append(date)
        if direction == "income" and amount > 0:
            total_income += amount
            income_count += 1
            if max_income is None or amount > float(max_income.get("amount") or 0):
                max_income = {"amount": round2(amount), "summary": _summary(tx), "date": date, "counterparty_name": _counterparty(tx)}
        elif direction == "expense":
            expense_amount = abs(amount)
            total_expense += expense_amount
            expense_count += 1
            if max_expense is None or expense_amount > float(max_expense.get("amount") or 0):
                max_expense = {"amount": round2(expense_amount), "summary": _summary(tx), "date": date, "counterparty_name": _counterparty(tx)}
    months = max(1, int(month_count or _month_count(dates)))
    return {
        "total_income": round2(total_income),
        "total_expense": round2(total_expense),
        "income_count": income_count,
        "expense_count": expense_count,
        "net_cash_flow": round2(total_income - total_expense),
        "avg_monthly_income": round2(total_income / months),
        "avg_monthly_expense": round2(total_expense / months),
        "month_count": months,
        "max_income_transaction": max_income or {"amount": 0.0, "summary": "", "date": "", "counterparty_name": ""},
        "max_expense_transaction": max_expense or {"amount": 0.0, "summary": "", "date": "", "counterparty_name": ""},
    }


def get_ai_summary_raw(payload: dict[str, Any]) -> dict[str, Any]:
    scale = _dict(payload.get("收支规模汇总"))
    raw = _dict(payload.get("raw_summary"))
    customer = _dict(payload.get("customer_level_summary"))
    return {
        "total_income": raw.get("total_income") or customer.get("raw_total_income") or scale.get("总收入金额") or 0,
        "total_expense": raw.get("total_expense") or customer.get("raw_total_expense") or scale.get("总支出金额") or 0,
        "income_count": raw.get("income_count") or scale.get("总收入笔数") or 0,
        "expense_count": raw.get("expense_count") or scale.get("总支出笔数") or 0,
        "net_cash_flow": raw.get("net_cash_flow") or scale.get("净现金流") or 0,
        "avg_monthly_income": customer.get("avg_monthly_income") or scale.get("月均收入") or 0,
        "avg_monthly_expense": scale.get("月均支出") or 0,
    }


def build_summary_mismatch_warning(ai_summary: dict[str, Any], detail_summary: dict[str, Any]) -> dict[str, str] | None:
    ai_income = abs(float(normalize_amount(ai_summary.get("total_income")) or 0))
    ai_expense = abs(float(normalize_amount(ai_summary.get("total_expense")) or 0))
    detail_income = float(detail_summary.get("total_income") or 0)
    detail_expense = float(detail_summary.get("total_expense") or 0)
    if abs(ai_income - detail_income) <= 1 and abs(ai_expense - detail_expense) <= 1:
        return None
    return {
        "code": "summary_detail_mismatch",
        "level": "medium",
        "message": "AI 汇总与交易明细重算结果不一致，已优先采用交易明细重算结果",
        "evidence": f"AI total_income={ai_income:.2f}, detail total_income={detail_income:.2f}; AI total_expense={ai_expense:.2f}, detail total_expense={detail_expense:.2f}",
    }
