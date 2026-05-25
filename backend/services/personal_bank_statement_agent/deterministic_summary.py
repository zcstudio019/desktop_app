from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from hashlib import sha1
from typing import Any

from .normalizer import normalize_amount, normalize_text, round2
from .skills.salary_income_detection_skill import detect_salary_income


TRANSACTION_LIST_KEYS = (
    "交易明细列表",
    "transactions",
    "transaction_details",
    "明细",
    "流水明细",
    "三、交易明细列表",
)

DATE_KEYS = ("交易日期", "记账日期", "transaction_date", "date", "accounting_date")
SUMMARY_KEYS = ("摘要", "交易摘要", "Transaction Type", "summary", "transaction_type")
AMOUNT_KEYS = ("signed_amount", "金额", "交易金额", "Transaction Amount", "transaction_amount", "amount")
DIRECTION_KEYS = ("收支", "支/收", "direction", "income_or_expense")
BALANCE_KEYS = ("余额", "联机余额", "balance")
COUNTERPARTY_KEYS = (
    "对手信息",
    "对方户名",
    "对方名称",
    "交易对手",
    "对手方",
    "counterparty",
    "counterparty_name",
    "Counter Party",
)

UNKNOWN_INFLOW_KEYWORDS = ("汇款汇入", "汇入汇款", "转账收入", "跨行汇入", "他行汇入", "普通汇款", "电子汇入")
INTEREST_KEYWORDS = ("存款利息", "结息")
LOAN_REPAYMENT_KEYWORDS = ("个贷还款", "贷款回收", "贷款还款", "贷款扣款", "按揭", "房贷", "车贷", "小贷", "消费贷", "网贷", "还本", "还息")
QUICK_PAYMENT_KEYWORDS = ("快捷支付", "支付宝", "微信支付", "POS", "消费")
FAST_OUT_EXPENSE_KEYWORDS = LOAN_REPAYMENT_KEYWORDS + QUICK_PAYMENT_KEYWORDS + ("转账", "转出", "汇款")
OPERATING_INCOME_KEYWORDS = ("货款", "服务费", "销售款", "客户付款", "经营回款", "工程款", "材料款", "结算款")
OTHER_STABLE_INCOME_KEYWORDS = ("租金", "分红", "固定收入")
EXCLUDE_OPERATING_INCOME_KEYWORDS = ("借款", "贷款", "还款", "转存", "理财赎回", "赎回")


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _first(record: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in record and record.get(key) not in (None, ""):
            return record.get(key)
    return None


def _date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("/", "-").replace(".", "-")
    return text[:10]


def _month_count_from_period(payload: dict[str, Any], dates: list[str]) -> int:
    period = _dict(payload.get("statement_period") or payload.get("流水期间"))
    start = _date(period.get("start_date") or period.get("开始日期") or payload.get("period_start"))
    end = _date(period.get("end_date") or period.get("结束日期") or payload.get("period_end"))
    try:
        if start and end:
            a = datetime.strptime(start[:7], "%Y-%m")
            b = datetime.strptime(end[:7], "%Y-%m")
            return max(1, (b.year - a.year) * 12 + b.month - a.month + 1)
    except ValueError:
        pass
    months = sorted({date[:7] for date in dates if len(date) >= 7})
    if not months:
        return 1
    try:
        a = datetime.strptime(months[0], "%Y-%m")
        b = datetime.strptime(months[-1], "%Y-%m")
        return max(1, (b.year - a.year) * 12 + b.month - a.month + 1)
    except ValueError:
        return max(1, len(months))


def _contains(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def collect_raw_transactions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    containers = [payload, _dict(payload.get("extracted_json")), _dict(payload.get("summary"))]
    for container in containers:
        for key in TRANSACTION_LIST_KEYS:
            transactions = [_dict(item) for item in _list(container.get(key))]
            if transactions:
                return transactions
    transactions: list[dict[str, Any]] = []
    for account in _list(payload.get("accounts")):
        transactions.extend(_dict(item) for item in _list(_dict(account).get("transactions")))
    return transactions


def normalize_personal_flow_transaction(tx: dict[str, Any], index: int = 0) -> dict[str, Any]:
    raw_amount = _first(tx, AMOUNT_KEYS)
    amount_value = normalize_amount(raw_amount)
    credit = normalize_amount(tx.get("credit_amount") or tx.get("creditAmount") or tx.get("收入"))
    debit = normalize_amount(tx.get("debit_amount") or tx.get("debitAmount") or tx.get("支出"))
    direction_text = normalize_text(_first(tx, DIRECTION_KEYS)).lower()

    if direction_text in {"支", "支出", "expense", "debit", "outflow"} or (debit and debit > 0 and not credit):
        direction = "expense"
        credit_amount = 0.0
        debit_amount = abs(float(debit or amount_value or 0))
    elif direction_text in {"收", "收入", "income", "credit", "inflow"} or (credit and credit > 0 and not debit):
        direction = "income"
        credit_amount = abs(float(credit or amount_value or 0))
        debit_amount = 0.0
    elif amount_value is not None and amount_value > 0:
        direction = "income"
        credit_amount = float(amount_value)
        debit_amount = 0.0
    elif amount_value is not None and amount_value < 0:
        direction = "expense"
        credit_amount = 0.0
        debit_amount = abs(float(amount_value))
    elif credit and credit > 0:
        direction = "income"
        credit_amount = float(credit)
        debit_amount = 0.0
    elif debit and debit > 0:
        direction = "expense"
        credit_amount = 0.0
        debit_amount = float(debit)
    else:
        direction = "unknown"
        credit_amount = 0.0
        debit_amount = 0.0

    signed_amount = credit_amount if direction == "income" else -debit_amount if direction == "expense" else float(amount_value or 0)
    date = _date(_first(tx, DATE_KEYS))
    summary = normalize_text(_first(tx, SUMMARY_KEYS))
    counterparty_name = normalize_text(_first(tx, COUNTERPARTY_KEYS))
    normalized = {
        **tx,
        "transaction_id": str(tx.get("transaction_id") or tx.get("流水号") or f"tx_{index + 1}"),
        "transaction_date": date,
        "accounting_date": _date(tx.get("accounting_date") or tx.get("记账日期") or date),
        "transaction_time": str(tx.get("transaction_time") or tx.get("交易时间") or ""),
        "summary": summary,
        "direction": direction,
        "amount": round2(abs(signed_amount)),
        "signed_amount": round2(signed_amount),
        "debit_amount": round2(debit_amount),
        "credit_amount": round2(credit_amount),
        "balance": normalize_amount(_first(tx, BALANCE_KEYS)),
        "counterparty_name": counterparty_name,
        "counterparty_account": normalize_text(tx.get("counterparty_account") or tx.get("对方账号") or ""),
        "raw": _dict(tx.get("raw")) or dict(tx),
    }
    return normalized


def normalize_personal_flow_transactions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [normalize_personal_flow_transaction(tx, index) for index, tx in enumerate(collect_raw_transactions(payload))]


def _raw_summary(payload: dict[str, Any], transactions: list[dict[str, Any]]) -> dict[str, Any]:
    total_income = 0.0
    total_expense = 0.0
    income_count = 0
    expense_count = 0
    dates: list[str] = []
    max_income: dict[str, Any] | None = None
    max_expense: dict[str, Any] | None = None
    for tx in transactions:
        date = str(tx.get("transaction_date") or "")
        if date:
            dates.append(date)
        credit = float(tx.get("credit_amount") or 0)
        debit = float(tx.get("debit_amount") or 0)
        if credit > 0:
            total_income += credit
            income_count += 1
            if max_income is None or credit > float(max_income.get("amount") or 0):
                max_income = {"amount": round2(credit), "summary": tx.get("summary") or "", "date": date, "counterparty_name": tx.get("counterparty_name") or ""}
        if debit > 0:
            total_expense += debit
            expense_count += 1
            if max_expense is None or debit > float(max_expense.get("amount") or 0):
                max_expense = {"amount": round2(debit), "summary": tx.get("summary") or "", "date": date, "counterparty_name": tx.get("counterparty_name") or ""}
    months = _month_count_from_period(payload, dates)
    return {
        "total_income": round2(total_income),
        "total_expense": round2(total_expense),
        "income_count": income_count,
        "expense_count": expense_count,
        "net_cash_flow": round2(total_income - total_expense),
        "avg_monthly_income": round2(total_income / months),
        "avg_monthly_expense": round2(total_expense / months),
        "month_count": months,
        "max_income_amount": (max_income or {}).get("amount", 0.0),
        "max_income_summary": (max_income or {}).get("summary", ""),
        "max_income_date": (max_income or {}).get("date", ""),
        "max_expense_amount": (max_expense or {}).get("amount", 0.0),
        "max_expense_summary": (max_expense or {}).get("summary", ""),
        "max_expense_date": (max_expense or {}).get("date", ""),
        "max_income_transaction": max_income or {"amount": 0.0, "summary": "", "date": "", "counterparty_name": ""},
        "max_expense_transaction": max_expense or {"amount": 0.0, "summary": "", "date": "", "counterparty_name": ""},
    }


def _ai_summary_raw(payload: dict[str, Any]) -> dict[str, Any]:
    scale = _dict(payload.get("收支规模汇总"))
    raw = _dict(payload.get("raw_summary"))
    customer = _dict(payload.get("customer_level_summary"))
    return {
        "total_income": raw.get("total_income") or customer.get("raw_total_income") or scale.get("总收入金额") or 0,
        "total_expense": raw.get("total_expense") or customer.get("raw_total_expense") or scale.get("总支出金额") or 0,
        "income_count": raw.get("income_count") or scale.get("总收入笔数") or 0,
        "expense_count": raw.get("expense_count") or scale.get("总支出笔数") or 0,
        "net_cash_flow": raw.get("net_cash_flow") or customer.get("net_cash_flow") or scale.get("净现金流") or 0,
        "avg_monthly_income": customer.get("avg_monthly_income") or scale.get("月均收入") or 0,
        "avg_monthly_expense": scale.get("月均支出") or 0,
    }


def _summary_mismatch_warning(ai_summary: dict[str, Any], detail_summary: dict[str, Any]) -> dict[str, str] | None:
    ai_income = abs(float(normalize_amount(ai_summary.get("total_income")) or 0))
    ai_expense = abs(float(normalize_amount(ai_summary.get("total_expense")) or 0))
    detail_income = float(detail_summary.get("total_income") or 0)
    detail_expense = float(detail_summary.get("total_expense") or 0)
    if abs(ai_income - detail_income) <= 1 and abs(ai_expense - detail_expense) <= 1:
        return None
    return {
        "code": "summary_detail_mismatch",
        "level": "high",
        "message": "AI汇总与交易明细重算结果不一致，已优先采用交易明细重算结果",
        "evidence": f"ai_income={ai_income:.2f}, detail_income={detail_income:.2f}, ai_expense={ai_expense:.2f}, detail_expense={detail_expense:.2f}",
    }


def _income_and_expense(payload: dict[str, Any], transactions: list[dict[str, Any]], raw_summary: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    salary = detect_salary_income(transactions)
    verified_operating_income = 0.0
    verified_other_stable_income = 0.0
    unknown_inflow = 0.0
    interest_income = 0.0
    loan_repayment_expense = 0.0
    quick_payment_expense = 0.0
    other_expense = 0.0
    monthly_loan_months: set[str] = set()

    for tx in transactions:
        summary = str(tx.get("summary") or "")
        counterparty = str(tx.get("counterparty_name") or "")
        credit = float(tx.get("credit_amount") or 0)
        debit = float(tx.get("debit_amount") or 0)
        if credit > 0:
            if _contains(summary, OPERATING_INCOME_KEYWORDS) and not _contains(summary, EXCLUDE_OPERATING_INCOME_KEYWORDS):
                verified_operating_income += credit
            elif _contains(summary, OTHER_STABLE_INCOME_KEYWORDS):
                verified_other_stable_income += credit
            if _contains(summary, UNKNOWN_INFLOW_KEYWORDS) and not counterparty:
                unknown_inflow += credit
            elif summary in {"汇款汇入", "汇入汇款", "转账收入"} and not counterparty:
                unknown_inflow += credit
            if _contains(summary, INTEREST_KEYWORDS):
                interest_income += credit
        if debit > 0:
            if _contains(summary, LOAN_REPAYMENT_KEYWORDS):
                loan_repayment_expense += debit
                month = str(tx.get("transaction_date") or "")[:7]
                if month:
                    monthly_loan_months.add(month)
            elif _contains(summary, QUICK_PAYMENT_KEYWORDS):
                quick_payment_expense += debit
            else:
                other_expense += debit

    confirmed_salary = float(salary.get("confirmed_salary_income") or 0)
    verified_income = round2(confirmed_salary + verified_operating_income + verified_other_stable_income)
    month_count = int(raw_summary.get("month_count") or 1)
    income_verification = {
        "raw_total_income": raw_summary["total_income"],
        "confirmed_salary_income": round2(confirmed_salary),
        "suspected_salary_income": salary.get("suspected_salary_income") or 0,
        "verified_salary_income": round2(confirmed_salary),
        "salary_income_count": salary.get("salary_income_count") or 0,
        "suspected_salary_count": salary.get("suspected_salary_count") or 0,
        "salary_months": salary.get("salary_months") or 0,
        "salary_avg_monthly_amount": salary.get("salary_avg_monthly_amount") or 0,
        "salary_continuity_level": salary.get("salary_continuity_level") or "none",
        "salary_confidence": salary.get("salary_confidence") or 0,
        "salary_sources": salary.get("salary_sources") or [],
        "salary_detection_notes": salary.get("salary_detection_notes") or [],
        "verified_operating_income": round2(verified_operating_income),
        "verified_other_stable_income": round2(verified_other_stable_income),
        "unknown_inflow": round2(unknown_inflow),
        "interest_income": round2(interest_income),
        "verified_income": verified_income,
        "stable_income": verified_income,
        "avg_monthly_verified_income": round2(verified_income / month_count),
        "avg_monthly_stable_income": round2(verified_income / month_count),
    }
    expense_analysis = {
        "raw_total_expense": raw_summary["total_expense"],
        "loan_repayment_expense": round2(loan_repayment_expense),
        "credit_card_repayment_expense": 0.0,
        "quick_payment_expense": round2(quick_payment_expense),
        "living_expense": round2(quick_payment_expense),
        "operating_expense": 0.0,
        "internal_transfer_expense": 0.0,
        "investment_expense": 0.0,
        "other_expense": round2(other_expense),
        "avg_monthly_loan_repayment": round2(loan_repayment_expense / month_count),
        "loan_repayment_ratio": round(loan_repayment_expense / raw_summary["total_expense"], 6) if raw_summary["total_expense"] else 0.0,
        "repayment_frequency": len(monthly_loan_months),
    }
    return income_verification, expense_analysis


def _monthly_trend(transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    monthly: dict[str, dict[str, Any]] = defaultdict(lambda: {"month": "", "raw_income": 0.0, "raw_expense": 0.0, "verified_income": 0.0, "loan_repayment_expense": 0.0, "net_cash_flow": 0.0})
    for tx in transactions:
        month = str(tx.get("transaction_date") or "")[:7]
        if not month:
            continue
        target = monthly[month]
        target["month"] = month
        credit = float(tx.get("credit_amount") or 0)
        debit = float(tx.get("debit_amount") or 0)
        target["raw_income"] = round2(target["raw_income"] + credit)
        target["raw_expense"] = round2(target["raw_expense"] + debit)
        if (tx.get("salary_detection") or {}).get("salary_type") == "confirmed_salary":
            target["verified_income"] = round2(target["verified_income"] + credit)
        if debit > 0 and _contains(str(tx.get("summary") or ""), LOAN_REPAYMENT_KEYWORDS):
            target["loan_repayment_expense"] = round2(target["loan_repayment_expense"] + debit)
        target["net_cash_flow"] = round2(target["raw_income"] - target["raw_expense"])
    return [monthly[key] for key in sorted(monthly)]


def _top_counterparties(transactions: list[dict[str, Any]], direction: str) -> list[dict[str, Any]]:
    field = "credit_amount" if direction == "income" else "debit_amount"
    totals: dict[str, dict[str, Any]] = defaultdict(lambda: {"name": "未知对手方", "account": "", "amount": 0.0, "count": 0})
    for tx in transactions:
        amount = float(tx.get(field) or 0)
        if amount <= 0:
            continue
        name = str(tx.get("counterparty_name") or "未知对手方")
        target = totals[name]
        target["name"] = name
        target["account"] = tx.get("counterparty_account") or ""
        target["amount"] = round2(target["amount"] + amount)
        target["count"] += 1
    return sorted(totals.values(), key=lambda item: float(item.get("amount") or 0), reverse=True)[:10]


def _fast_in_fast_out_analysis(transactions: list[dict[str, Any]], raw_income: float) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    expenses = [
        tx for tx in transactions
        if float(tx.get("debit_amount") or 0) > 0 and _contains(str(tx.get("summary") or ""), FAST_OUT_EXPENSE_KEYWORDS)
    ]
    used_expenses: set[str] = set()
    for income in [tx for tx in transactions if float(tx.get("credit_amount") or 0) > 0]:
        income_date = _date(income.get("transaction_date"))
        try:
            income_day = datetime.strptime(income_date, "%Y-%m-%d")
        except ValueError:
            continue
        amount = float(income.get("credit_amount") or 0)
        candidates: list[tuple[dict[str, Any], int]] = []
        for expense in expenses:
            expense_id = str(expense.get("transaction_id") or "")
            if expense_id in used_expenses:
                continue
            try:
                days = (datetime.strptime(_date(expense.get("transaction_date")), "%Y-%m-%d") - income_day).days
            except ValueError:
                continue
            if 0 <= days <= 3:
                candidates.append((expense, days))
        single = next(
            (
                (expense, days)
                for expense, days in candidates
                if 0.85 <= float(expense.get("debit_amount") or 0) / amount <= 1.05
            ),
            None,
        )
        if single:
            expense, days = single
            expense_amount = float(expense.get("debit_amount") or 0)
            used_expenses.add(str(expense.get("transaction_id") or ""))
            matches.append({
                "income_transaction_id": income.get("transaction_id") or "",
                "expense_transaction_id": expense.get("transaction_id") or "",
                "income_date": income_date,
                "expense_date": expense.get("transaction_date") or "",
                "income_amount": round2(amount),
                "expense_amount": round2(expense_amount),
                "days_between": days,
                "match_ratio": round(expense_amount / amount, 6),
                "reason": "收入后3日内发生金额接近的贷款还款或转出支出",
            })
            continue
        combination_amount = sum(float(expense.get("debit_amount") or 0) for expense, _days in candidates)
        if candidates and 0.85 <= combination_amount / amount <= 1.05:
            for expense, _days in candidates:
                used_expenses.add(str(expense.get("transaction_id") or ""))
            max_days = max(days for _expense, days in candidates)
            matches.append({
                "income_transaction_id": income.get("transaction_id") or "",
                "expense_transaction_id": "",
                "income_date": income_date,
                "expense_date": max(str(expense.get("transaction_date") or "") for expense, _days in candidates),
                "income_amount": round2(amount),
                "expense_amount": round2(combination_amount),
                "days_between": max_days,
                "match_ratio": round(combination_amount / amount, 6),
                "reason": "收入后3日内多笔贷款还款或转出支出合计金额接近收入",
                "expense_transactions": [
                    {"transaction_id": expense.get("transaction_id") or "", "expense_date": expense.get("transaction_date") or "", "expense_amount": expense.get("debit_amount") or 0}
                    for expense, _days in candidates
                ],
            })
    matched_amount = round2(sum(float(item.get("expense_amount") or 0) for item in matches))
    return {
        "has_fast_in_fast_out": bool(matches),
        "matched_count": len(matches),
        "matched_amount": matched_amount,
        "matched_amount_ratio": round(matched_amount / raw_income, 6) if raw_income else 0.0,
        "matches": matches,
    }


def transaction_signature(transactions: list[dict[str, Any]]) -> str:
    parts = [
        f"{tx.get('transaction_date')}|{tx.get('summary')}|{tx.get('counterparty_name')}|{tx.get('signed_amount')}"
        for tx in transactions
    ]
    return sha1("\n".join(sorted(parts)).encode("utf-8")).hexdigest()


def build_deterministic_personal_flow_summary(extracted_json: dict[str, Any]) -> dict[str, Any]:
    payload = _dict(extracted_json)
    transactions = normalize_personal_flow_transactions(payload)
    raw_summary = _raw_summary(payload, transactions)
    ai_raw = _ai_summary_raw(payload)
    warnings = [item for item in _list(payload.get("warnings")) if item]
    mismatch = _summary_mismatch_warning(ai_raw, raw_summary) if transactions else None
    if mismatch:
        warnings.append(mismatch)
    income_verification, expense_analysis = _income_and_expense(payload, transactions, raw_summary)
    fast_analysis = _fast_in_fast_out_analysis(transactions, raw_summary["total_income"])
    month_count = int(raw_summary.get("month_count") or 1)
    customer_level_summary = {
        **_dict(payload.get("customer_level_summary")),
        "account_count": len(_list(payload.get("accounts"))) or 1 if transactions else 0,
        "period_start": (_dict(payload.get("statement_period")).get("start_date") or ""),
        "period_end": (_dict(payload.get("statement_period")).get("end_date") or ""),
        "raw_total_income": raw_summary["total_income"],
        "raw_total_expense": raw_summary["total_expense"],
        "net_cash_flow": raw_summary["net_cash_flow"],
        "avg_monthly_income": raw_summary["avg_monthly_income"],
        "avg_monthly_expense": raw_summary["avg_monthly_expense"],
        "salary_income": income_verification["verified_salary_income"],
        "confirmed_salary_income": income_verification["confirmed_salary_income"],
        "suspected_salary_income": income_verification["suspected_salary_income"],
        "operating_income": income_verification["verified_operating_income"],
        "stable_income": income_verification["stable_income"],
        "verified_income": income_verification["verified_income"],
        "unknown_inflow": income_verification["unknown_inflow"],
        "loan_repayment_expense": expense_analysis["loan_repayment_expense"],
        "avg_monthly_stable_income": round2(income_verification["stable_income"] / month_count),
    }
    return {
        **payload,
        "doc_type": "personal_flow",
        "document_type": "personal_flow",
        "deterministic_summary": raw_summary,
        "raw_summary": raw_summary,
        "ai_summary_raw": ai_raw,
        "income_verification": income_verification,
        "expense_analysis": expense_analysis,
        "fast_in_fast_out_analysis": fast_analysis,
        "customer_level_summary": customer_level_summary,
        "monthly_trend": _monthly_trend(transactions),
        "top_income_counterparties": _top_counterparties(transactions, "income"),
        "top_expense_counterparties": _top_counterparties(transactions, "expense"),
        "transactions": transactions,
        "transaction_signature": transaction_signature(transactions),
        "warnings": warnings,
        "summary_warnings": [mismatch] if mismatch else [],
    }
