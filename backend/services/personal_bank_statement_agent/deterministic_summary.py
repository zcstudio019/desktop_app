from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from hashlib import sha1
import logging
import re
from typing import Any

from .normalizer import normalize_amount, normalize_text, round2
from .skills.salary_income_detection_skill import detect_salary_income
from .skills.transaction_classification_skill import classify_transactions

logger = logging.getLogger(__name__)


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
    "Counterparty",
)

UNKNOWN_INFLOW_KEYWORDS = ("汇款汇入", "汇入汇款", "转账收入", "跨行汇入", "他行汇入", "普通汇款", "电子汇入")
INTEREST_KEYWORDS = ("存款利息", "结息")
LOAN_REPAYMENT_KEYWORDS = ("个贷还款", "贷款回收", "贷款还款", "贷款扣款", "按揭", "房贷", "车贷", "小贷", "消费贷", "网贷", "还本", "还息")
QUICK_PAYMENT_KEYWORDS = ("快捷支付", "支付宝", "微信支付", "POS", "消费")
FAST_OUT_EXPENSE_KEYWORDS = LOAN_REPAYMENT_KEYWORDS + QUICK_PAYMENT_KEYWORDS + ("信用卡还款", "中融小贷", "支付宝信贷业务待还款账户", "美团月付还款", "转账", "转出", "汇款")
OPERATING_INCOME_KEYWORDS = ("货款", "服务费", "销售款", "客户付款", "经营回款", "工程款", "材料款", "结算款")
OTHER_STABLE_INCOME_KEYWORDS = ("租金", "分红", "固定收入")
EXCLUDE_OPERATING_INCOME_KEYWORDS = ("借款", "贷款", "还款", "转存", "理财赎回", "赎回")
BASE_INFO_KEYS = ("账户基础信息", "一、账户基础信息", "base_info", "account_info")
BANK_NAME_KEYS = ("银行", "银行名称", "Bank Name")
BRANCH_NAME_KEYS = ("开户行", "开户银行", "开户机构", "Sub Branch", "支行", "网点")
ACCOUNT_NAME_KEYS = ("户名", "账户名称", "客户名称", "姓名", "Account Name")
ACCOUNT_NO_KEYS = ("账号", "账户", "银行卡号", "Account No.", "Account No")
CURRENCY_KEYS = ("币种", "Currency")
ACCOUNT_TYPE_KEYS = ("账户类型", "Account Type")
START_DATE_KEYS = ("流水起始日期", "起始日期", "开始日期", "交易起始日期", "statement_start_date")
END_DATE_KEYS = ("流水结束日期", "结束日期", "截止日期", "交易结束日期", "statement_end_date")
PRINT_DATE_KEYS = ("文档打印日期", "打印日期", "Print Time", "打印时间")
CMB_TEXT_SUMMARY_KEYWORDS = (
    "代发款项",
    "代发工资",
    "工资发放",
    "转账汇款",
    "汇入汇款",
    "汇款汇入",
    "快捷支付",
    "个贷还款",
    "贷款回收",
    "存款利息",
    "结息",
)


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


def _infer_bank_brand(*values: Any) -> str:
    text = " ".join(str(value or "") for value in values)
    for keyword, bank_name in (
        ("招商银行", "招商银行"),
        ("Transaction Statement of China Merchants Bank", "招商银行"),
        ("兴业银行", "兴业银行"),
        ("中国建设银行", "中国建设银行"),
        ("建设银行", "中国建设银行"),
        ("建行", "中国建设银行"),
        ("中国农业银行", "中国农业银行"),
        ("农业银行", "中国农业银行"),
        ("农行", "中国农业银行"),
        ("上海银行", "上海银行"),
        ("北京银行", "北京银行"),
    ):
        if keyword in text:
            return bank_name
    return ""


def _full_bank_name(bank_brand: str, branch_name: str, provided_bank_name: str) -> str:
    provided = normalize_text(provided_bank_name)
    brand = normalize_text(bank_brand)
    branch = normalize_text(branch_name)
    if provided and brand and brand in provided:
        return provided
    if branch and brand and brand in branch:
        return branch
    if brand and branch:
        return f"{brand}{branch}"
    return provided or brand or branch


def normalize_personal_flow_base_info(payload: dict[str, Any], transactions: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    transactions = transactions or []
    base_info: dict[str, Any] = {}
    for key in BASE_INFO_KEYS:
        candidate = _dict(payload.get(key))
        if candidate:
            base_info = candidate
            break
    period = _dict(payload.get("statement_period") or payload.get("流水期间"))
    dates = sorted(str(tx.get("transaction_date") or "")[:10] for tx in transactions if str(tx.get("transaction_date") or "")[:10])
    source_file = payload.get("source_file") or payload.get("original_filename") or payload.get("file_name") or ""
    raw_text = payload.get("raw_text") or payload.get("text") or payload.get("ocr_text") or ""
    provided_bank_name = normalize_text(payload.get("bank_name") or _first(base_info, BANK_NAME_KEYS))
    branch_name = normalize_text(payload.get("branch_name") or _first(base_info, BRANCH_NAME_KEYS))
    bank_brand = normalize_text(
        payload.get("bank_brand")
        or _infer_bank_brand(source_file, raw_text, provided_bank_name, branch_name)
    )
    if provided_bank_name and not branch_name and ("支行" in provided_bank_name or "分行" in provided_bank_name):
        branch_name = provided_bank_name
    return {
        "bank_brand": bank_brand,
        "branch_name": branch_name,
        "bank_name": _full_bank_name(bank_brand, branch_name, provided_bank_name),
        "account_name": normalize_text(payload.get("account_name") or _first(base_info, ACCOUNT_NAME_KEYS)),
        "account_no": normalize_text(payload.get("account_no") or _first(base_info, ACCOUNT_NO_KEYS)),
        "currency": normalize_text(payload.get("currency") or _first(base_info, CURRENCY_KEYS)) or "人民币",
        "account_type": normalize_text(payload.get("account_type") or _first(base_info, ACCOUNT_TYPE_KEYS)),
        "statement_period": {
            "start_date": _date(period.get("start_date") or _first(base_info, START_DATE_KEYS) or (dates[0] if dates else "")),
            "end_date": _date(period.get("end_date") or _first(base_info, END_DATE_KEYS) or (dates[-1] if dates else "")),
        },
        "print_date": _date(payload.get("print_date") or _first(base_info, PRINT_DATE_KEYS)),
    }


def collect_raw_transactions_with_source(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    containers = [payload, _dict(payload.get("extracted_json")), _dict(payload.get("summary"))]
    for container_index, container in enumerate(containers):
        for key in TRANSACTION_LIST_KEYS:
            transactions = [_dict(item) for item in _list(container.get(key))]
            if transactions:
                prefix = ("", "extracted_json.", "summary.")[container_index]
                return transactions, f"{prefix}{key}"
    transactions: list[dict[str, Any]] = []
    for account in _list(payload.get("accounts")):
        transactions.extend(_dict(item) for item in _list(_dict(account).get("transactions")))
    return transactions, "accounts.transactions" if transactions else ""


def collect_raw_transactions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return collect_raw_transactions_with_source(payload)[0]


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


def extract_china_merchants_transactions_from_text(raw_text: str) -> list[dict[str, Any]]:
    """Extract CMB row-shaped evidence used only to fill missing counterparties."""
    summary_pattern = "|".join(re.escape(keyword) for keyword in CMB_TEXT_SUMMARY_KEYWORDS)
    row_pattern = re.compile(
        rf"(?P<date>(?:19|20)\d{{2}}[-/.]\d{{1,2}}[-/.]\d{{1,2}})"
        rf"\s+(?:(?:CNY|RMB|人民币)\s+)?"
        rf"(?P<amount>[+-]?\d[\d,]*\.\d{{1,2}})"
        rf"\s+(?P<balance>[+-]?\d[\d,]*\.\d{{1,2}})"
        rf"\s+(?P<summary>{summary_pattern})"
        rf"(?:\s+(?P<counterparty>.+?))?\s*$",
        re.IGNORECASE,
    )
    parsed: list[dict[str, Any]] = []
    for raw_line in str(raw_text or "").splitlines():
        line = re.sub(r"\s+", " ", str(raw_line or "")).strip()
        match = row_pattern.search(line)
        if not match:
            continue
        counterparty = normalize_text(match.group("counterparty") or "")
        parsed.append(
            {
                "transaction_date": _date(match.group("date")),
                "amount": normalize_amount(match.group("amount")) or 0,
                "summary": normalize_text(match.group("summary")),
                "counterparty_name": counterparty,
                "raw_text_line": line,
            }
        )
    return parsed


def fill_missing_counterparties_from_text(
    transactions: list[dict[str, Any]],
    raw_text: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    fallback_rows = extract_china_merchants_transactions_from_text(raw_text)
    recovered: list[dict[str, Any]] = []
    if not fallback_rows:
        return transactions, recovered
    for tx in transactions:
        if tx.get("counterparty_name"):
            continue
        amount = float(tx.get("credit_amount") or -(float(tx.get("debit_amount") or 0)))
        for row in fallback_rows:
            if not row.get("counterparty_name"):
                continue
            if (
                row.get("transaction_date") == tx.get("transaction_date")
                and row.get("summary") == tx.get("summary")
                and abs(float(row.get("amount") or 0) - amount) <= 0.01
            ):
                tx["counterparty_name"] = row["counterparty_name"]
                tx["counterparty_recovered_from_raw_text"] = True
                recovered.append(
                    {
                        "transaction_id": tx.get("transaction_id") or "",
                        "transaction_date": tx.get("transaction_date") or "",
                        "summary": tx.get("summary") or "",
                        "amount": amount,
                        "counterparty_name": tx["counterparty_name"],
                    }
                )
                break
    return transactions, recovered


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
    persisted_ai_raw = _dict(payload.get("ai_summary_raw"))
    if persisted_ai_raw:
        return persisted_ai_raw
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


def _income_and_expense(
    payload: dict[str, Any],
    transactions: list[dict[str, Any]],
    raw_summary: dict[str, Any],
    account_name: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    salary = detect_salary_income(transactions, account_name=account_name)
    for tx in transactions:
        salary_type = _dict(tx.get("salary_detection")).get("salary_type")
        if salary_type in {"confirmed_salary", "suspected_salary", "low_confidence_suspected_salary"}:
            logger.info(
                "[PersonalFlow][SALARY_CANDIDATE] type=%s date=%s amount=%s summary=%s counterparty=%s reason=%s",
                salary_type.replace("_salary", ""),
                tx.get("transaction_date") or "",
                tx.get("credit_amount") or 0,
                tx.get("summary") or "",
                tx.get("counterparty_name") or "",
                (
                    "payroll_keyword_and_company_counterparty"
                    if salary_type == "suspected_salary"
                    else "payroll_keyword_missing_counterparty"
                    if salary_type == "low_confidence_suspected_salary"
                    else "strong_salary_keyword"
                ),
            )
    verified_operating_income = 0.0
    verified_other_stable_income = 0.0
    unknown_inflow = 0.0
    self_transfer_income = 0.0
    personal_transfer_income = 0.0
    related_party_income = 0.0
    platform_collection_income = 0.0
    company_business_inflow = 0.0
    interest_income = 0.0
    loan_inflow = 0.0
    investment_income = 0.0
    refund_income = 0.0
    loan_repayment_expense = 0.0
    credit_card_repayment_expense = 0.0
    online_loan_repayment_expense = 0.0
    quick_payment_expense = 0.0
    living_expense = 0.0
    investment_expense = 0.0
    internal_transfer_expense = 0.0
    related_party_transfer_expense = 0.0
    business_or_company_outflow = 0.0
    cash_withdrawal = 0.0
    fee_expense = 0.0
    other_expense = 0.0
    monthly_loan_months: set[str] = set()

    for tx in transactions:
        summary = str(tx.get("summary") or "")
        counterparty = str(tx.get("counterparty_name") or "")
        credit = float(tx.get("credit_amount") or 0)
        debit = float(tx.get("debit_amount") or 0)
        if credit > 0:
            category = str(tx.get("category") or "")
            income_nature = _dict(tx.get("salary_detection")).get("income_nature") or tx.get("income_nature")
            if income_nature == "self_transfer_income" or category in {"self_transfer_income", "internal_transfer_income"}:
                self_transfer_income += credit
                if category != "internal_transfer_income":
                    tx["category"] = "self_transfer_income"
                tx["is_internal_transfer"] = True
                tx["is_salary"] = False
                tx["is_verified_income"] = False
                tx["is_stable_income"] = False
            elif income_nature == "personal_transfer_income" or category == "personal_transfer_income":
                personal_transfer_income += credit
                tx["category"] = "personal_transfer_income"
                tx["is_salary"] = False
                tx["is_verified_income"] = False
                tx["is_stable_income"] = False
            if category == "operating_income" or (_contains(summary, OPERATING_INCOME_KEYWORDS) and not _contains(summary, EXCLUDE_OPERATING_INCOME_KEYWORDS)):
                verified_operating_income += credit
            elif _contains(summary, OTHER_STABLE_INCOME_KEYWORDS):
                verified_other_stable_income += credit
            elif category == "platform_collection_income":
                platform_collection_income += credit
            elif category == "company_business_inflow":
                company_business_inflow += credit
            elif category == "loan_inflow":
                loan_inflow += credit
            elif category == "related_party_income":
                related_party_income += credit
            elif category == "investment_income":
                investment_income += credit
            elif category == "refund_income":
                refund_income += credit
            salary_type = _dict(tx.get("salary_detection")).get("salary_type")
            if category == "unknown_inflow":
                unknown_inflow += credit
            elif salary_type not in {"confirmed_salary", "suspected_salary", "low_confidence_suspected_salary"} and _contains(summary, UNKNOWN_INFLOW_KEYWORDS) and not counterparty:
                unknown_inflow += credit
            elif salary_type not in {"confirmed_salary", "suspected_salary", "low_confidence_suspected_salary"} and summary in {"汇款汇入", "汇入汇款", "转账收入"} and not counterparty:
                unknown_inflow += credit
            if _contains(summary, INTEREST_KEYWORDS):
                interest_income += credit
        if debit > 0:
            category = str(tx.get("category") or "")
            if category == "online_loan_repayment_expense":
                online_loan_repayment_expense += debit
                loan_repayment_expense += debit
                month = str(tx.get("transaction_date") or "")[:7]
                if month:
                    monthly_loan_months.add(month)
            elif category == "credit_card_repayment_expense":
                credit_card_repayment_expense += debit
            elif category == "loan_repayment_expense" or _contains(summary, LOAN_REPAYMENT_KEYWORDS):
                loan_repayment_expense += debit
                month = str(tx.get("transaction_date") or "")[:7]
                if month:
                    monthly_loan_months.add(month)
            elif category in {"platform_payment_expense", "quick_payment_expense"} or _contains(summary, QUICK_PAYMENT_KEYWORDS):
                quick_payment_expense += debit
                living_expense += debit
            elif category == "investment_expense":
                investment_expense += debit
            elif category == "internal_transfer_expense":
                internal_transfer_expense += debit
            elif category == "related_party_transfer_expense":
                related_party_transfer_expense += debit
            elif category == "business_or_company_outflow":
                business_or_company_outflow += debit
            elif category == "cash_withdrawal":
                cash_withdrawal += debit
            elif category == "fee_expense":
                fee_expense += debit
            else:
                other_expense += debit

    confirmed_salary = float(salary.get("confirmed_salary_income") or 0)
    verified_income = round2(confirmed_salary + verified_operating_income + verified_other_stable_income)
    month_count = int(raw_summary.get("month_count") or 1)
    income_verification = {
        "raw_total_income": raw_summary["total_income"],
        "confirmed_salary_income": round2(confirmed_salary),
        "suspected_salary_income": salary.get("suspected_salary_income") or 0,
        "low_confidence_suspected_salary_income": salary.get("low_confidence_suspected_salary_income") or salary.get("suspected_salary_income_low_confidence") or 0,
        "suspected_salary_income_low_confidence": salary.get("suspected_salary_income_low_confidence") or 0,
        "verified_salary_income": round2(confirmed_salary),
        "salary_income_count": salary.get("salary_income_count") or 0,
        "suspected_salary_count": salary.get("suspected_salary_count") or 0,
        "suspected_salary_count_low_confidence": salary.get("suspected_salary_count_low_confidence") or 0,
        "salary_months": salary.get("salary_months") or 0,
        "salary_avg_monthly_amount": salary.get("salary_avg_monthly_amount") or 0,
        "salary_continuity_level": salary.get("salary_continuity_level") or "none",
        "salary_confidence": salary.get("salary_confidence") or 0,
        "salary_sources": salary.get("salary_sources") or [],
        "salary_detection_notes": salary.get("salary_detection_notes") or [],
        "verified_operating_income": round2(verified_operating_income),
        "verified_other_stable_income": round2(verified_other_stable_income),
        "self_transfer_income": round2(self_transfer_income),
        "internal_transfer_income": round2(self_transfer_income),
        "personal_transfer_income": round2(personal_transfer_income),
        "related_party_income": round2(related_party_income),
        "platform_collection_income": round2(platform_collection_income),
        "company_business_inflow": round2(company_business_inflow),
        "unknown_inflow": round2(unknown_inflow),
        "interest_income": round2(interest_income),
        "loan_inflow": round2(loan_inflow),
        "investment_income": round2(investment_income),
        "refund_income": round2(refund_income),
        "verified_income": verified_income,
        "stable_income": verified_income,
        "avg_monthly_verified_income": round2(verified_income / month_count),
        "avg_monthly_stable_income": round2(verified_income / month_count),
    }
    expense_analysis = {
        "raw_total_expense": raw_summary["total_expense"],
        "loan_repayment_expense": round2(loan_repayment_expense),
        "credit_card_repayment_expense": round2(credit_card_repayment_expense),
        "online_loan_repayment_expense": round2(online_loan_repayment_expense),
        "quick_payment_expense": round2(quick_payment_expense),
        "living_expense": round2(living_expense),
        "operating_expense": 0.0,
        "internal_transfer_expense": round2(internal_transfer_expense),
        "investment_expense": round2(investment_expense),
        "related_party_transfer_expense": round2(related_party_transfer_expense),
        "business_or_company_outflow": round2(business_or_company_outflow),
        "cash_withdrawal": round2(cash_withdrawal),
        "fee_expense": round2(fee_expense),
        "other_expense": round2(other_expense),
        "avg_monthly_loan_repayment": round2(loan_repayment_expense / month_count),
        "loan_repayment_ratio": round(loan_repayment_expense / raw_summary["total_expense"], 6) if raw_summary["total_expense"] else 0.0,
        "repayment_frequency": len(monthly_loan_months),
    }
    logger.info(
        "[PersonalFlow][SALARY_RESULT] confirmed=%s suspected=%s low_confidence=%s months=%s sources=%s",
        income_verification["confirmed_salary_income"],
        income_verification["suspected_salary_income"],
        income_verification["suspected_salary_income_low_confidence"],
        income_verification["salary_months"],
        income_verification["salary_sources"],
    )
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


def build_deterministic_personal_flow_summary(extracted_json: dict[str, Any], raw_text: str = "") -> dict[str, Any]:
    payload = _dict(extracted_json)
    raw_transactions, transaction_source_key = collect_raw_transactions_with_source(payload)
    for index, raw_tx in enumerate(raw_transactions[:10]):
        logger.info("[PersonalFlow][RAW_TX_KEYS] index=%s keys=%s", index, list(raw_tx.keys()))
        logger.info("[PersonalFlow][RAW_TX_SAMPLE] raw_tx_%s=%s", index, raw_tx)
    transactions = [normalize_personal_flow_transaction(tx, index) for index, tx in enumerate(raw_transactions)]
    fallback_text = str(raw_text or payload.get("raw_text") or "")
    transactions, recovered_counterparties = fill_missing_counterparties_from_text(transactions, fallback_text)
    if recovered_counterparties:
        logger.info("[PersonalFlow][COUNTERPARTY_FALLBACK] recovered=%s rows=%s", len(recovered_counterparties), recovered_counterparties)
    base_info = normalize_personal_flow_base_info(
        {**payload, "raw_text": payload.get("raw_text") or fallback_text},
        transactions,
    )
    transactions = classify_transactions(transactions, account_name=str(base_info.get("account_name") or ""))
    raw_summary = _raw_summary(payload, transactions)
    ai_raw = _ai_summary_raw(payload)
    warnings = [item for item in _list(payload.get("warnings")) if item]
    has_ai_summary = bool(
        _dict(payload.get("ai_summary_raw"))
        or _dict(payload.get("收支规模汇总"))
        or _dict(payload.get("raw_summary"))
        or _dict(payload.get("customer_level_summary"))
    )
    mismatch = _summary_mismatch_warning(ai_raw, raw_summary) if transactions and has_ai_summary else None
    if mismatch:
        warnings.append(mismatch)
    income_verification, expense_analysis = _income_and_expense(
        payload,
        transactions,
        raw_summary,
        account_name=str(base_info.get("account_name") or ""),
    )
    fast_analysis = _fast_in_fast_out_analysis(transactions, raw_summary["total_income"])
    payroll_like_transactions = [
        {
            "transaction_date": tx.get("transaction_date") or "",
            "amount": tx.get("credit_amount") or tx.get("amount") or 0,
            "direction": tx.get("direction") or "",
            "summary": tx.get("summary") or "",
            "counterparty_name": tx.get("counterparty_name") or "",
            "salary_type": _dict(tx.get("salary_detection")).get("salary_type") or "",
        }
        for tx in transactions
        if "代发" in str(tx.get("summary") or "")
    ]
    salary_candidate_transactions = [
        tx for tx in payroll_like_transactions
        if tx.get("salary_type") in {"confirmed_salary", "suspected_salary", "low_confidence_suspected_salary"}
    ]
    month_count = int(raw_summary.get("month_count") or 1)
    customer_level_summary = {
        **_dict(payload.get("customer_level_summary")),
        "account_count": len(_list(payload.get("accounts"))) or 1 if transactions else 0,
        "period_start": base_info["statement_period"]["start_date"],
        "period_end": base_info["statement_period"]["end_date"],
        "raw_total_income": raw_summary["total_income"],
        "raw_total_expense": raw_summary["total_expense"],
        "net_cash_flow": raw_summary["net_cash_flow"],
        "avg_monthly_income": raw_summary["avg_monthly_income"],
        "avg_monthly_expense": raw_summary["avg_monthly_expense"],
        "salary_income": income_verification["verified_salary_income"],
        "confirmed_salary_income": income_verification["confirmed_salary_income"],
        "suspected_salary_income": income_verification["suspected_salary_income"],
        "low_confidence_suspected_salary_income": income_verification["low_confidence_suspected_salary_income"],
        "suspected_salary_income_low_confidence": income_verification["suspected_salary_income_low_confidence"],
        "operating_income": income_verification["verified_operating_income"],
        "stable_income": income_verification["stable_income"],
        "verified_income": income_verification["verified_income"],
        "unknown_inflow": income_verification["unknown_inflow"],
        "self_transfer_income": income_verification["self_transfer_income"],
        "internal_transfer_income": income_verification["internal_transfer_income"],
        "personal_transfer_income": income_verification["personal_transfer_income"],
        "related_party_income": income_verification["related_party_income"],
        "platform_collection_income": income_verification["platform_collection_income"],
        "company_business_inflow": income_verification["company_business_inflow"],
        "loan_inflow": income_verification["loan_inflow"],
        "investment_income": income_verification["investment_income"],
        "refund_income": income_verification["refund_income"],
        "interest_income": income_verification["interest_income"],
        "loan_repayment_expense": expense_analysis["loan_repayment_expense"],
        "avg_monthly_stable_income": round2(income_verification["stable_income"] / month_count),
    }
    logger.info(
        "[PersonalFlow][START] customer_id=%s file_name=%s document_type=%s",
        payload.get("customer_id") or "",
        payload.get("source_file") or payload.get("original_filename") or payload.get("file_name") or "",
        payload.get("document_type") or payload.get("doc_type") or "personal_flow",
    )
    logger.info(
        "[PersonalFlow][BASE_INFO] bank=%s account_name=%s account_no=%s period=%s currency=%s",
        base_info.get("bank_name") or "",
        base_info.get("account_name") or "",
        base_info.get("account_no") or "",
        base_info.get("statement_period") or {},
        base_info.get("currency") or "",
    )
    logger.info("[PersonalFlow][TX_SOURCE] source_key=%s raw_count=%s", transaction_source_key or "missing", len(raw_transactions))
    logger.info(
        "[PersonalFlow][TX_NORMALIZED] count=%s income_count=%s expense_count=%s",
        len(transactions),
        raw_summary["income_count"],
        raw_summary["expense_count"],
    )
    for tx in transactions[:10]:
        logger.info(
            "[PersonalFlow][TX_SAMPLE] date=%s amount=%s direction=%s summary=%s counterparty=%s",
            tx.get("transaction_date") or "",
            tx.get("credit_amount") or -(float(tx.get("debit_amount") or 0)),
            tx.get("direction") or "",
            tx.get("summary") or "",
            tx.get("counterparty_name") or "",
        )
    for tx in payroll_like_transactions:
        logger.info(
            "[PersonalFlow][PAYROLL_LIKE_TX] date=%s amount=%s summary=%s counterparty=%s",
            tx.get("transaction_date") or "",
            tx.get("amount") or 0,
            tx.get("summary") or "",
            tx.get("counterparty_name") or "",
        )
    logger.info("[PersonalFlow][SALARY_CANDIDATES] count=%s", len(salary_candidate_transactions))
    logger.info(
        "[PersonalFlow][AI_SUMMARY_RAW] income=%s expense=%s net=%s",
        ai_raw.get("total_income") or 0,
        ai_raw.get("total_expense") or 0,
        ai_raw.get("net_cash_flow") or 0,
    )
    logger.info(
        "[PersonalFlow][DETAIL_SUMMARY] income=%s expense=%s net=%s income_count=%s expense_count=%s",
        raw_summary["total_income"],
        raw_summary["total_expense"],
        raw_summary["net_cash_flow"],
        raw_summary["income_count"],
        raw_summary["expense_count"],
    )
    logger.info("[PersonalFlow][TRANSACTIONS_COUNT] count=%s", len(transactions))
    logger.info("[PersonalFlow][SUMMARY_SOURCE] deterministic_from_transactions")
    logger.info("[PersonalFlow][SUMMARY_MISMATCH] %s", bool(mismatch))
    logger.info("[PersonalFlow][WARNINGS] %s", warnings)
    return {
        **payload,
        **base_info,
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
        "debug": {
            "summary_source": "deterministic_from_transactions",
            "transaction_source_key": transaction_source_key,
            "transaction_count": len(transactions),
            "salary_candidate_count": len(salary_candidate_transactions),
            "payroll_like_transactions": payroll_like_transactions,
            "recovered_counterparties": recovered_counterparties,
            "ai_summary_raw": ai_raw,
            "warnings": warnings,
        },
    }
