"""Structured extraction helpers for upload/chat document parsing."""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from backend.document_types import (
    get_document_display_name,
    get_document_storage_label,
    normalize_document_type_code,
)
from backend.services.extraction_utils import normalize_amount, normalize_text
from prompts import get_prompt_for_type, load_prompts
from utils.json_parser import parse_json


DATE_PATTERN = re.compile(r"((?:19|20)\d{2}[年/\-.](?:0?[1-9]|1[0-2])[月/\-.](?:0?[1-9]|[12]\d|3[01])日?)")
MONEY_PATTERN = re.compile(r"([+-]?(?:\d[\d,]*)(?:\.\d+)?)")
ID_CARD_PATTERN = re.compile(r"([1-9]\d{5}(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx])")
UNIFIED_CODE_PATTERN = re.compile(r"\b([0-9A-Z]{18})\b")

TYPE_KEYWORD_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("business_license", ("营业执照", "统一社会信用代码", "法定代表人")),
    ("account_license", ("开户许可证", "开户银行", "核准号")),
    ("company_articles", ("公司章程", "股东", "注册资本")),
    ("bank_statement_detail", ("交易明细", "对方户名", "借方发生额", "贷方发生额")),
    ("bank_statement", ("对账单", "账户余额", "期末余额")),
    ("contract", ("合同", "甲方", "乙方", "金额", "期限")),
    ("id_card", ("居民身份证", "公民身份号码", "住址")),
    ("marriage_cert", ("结婚证", "婚姻登记", "持证人")),
    ("hukou", ("户口本", "户主", "与户主关系")),
    ("property_report", ("不动产", "房屋坐落", "权利人")),
    ("special_license", ("许可证", "有效期", "发证机关")),
)

BANK_DATE_KEYS = ("交易日期", "记账日期", "日期", "入账日期", "交易时间")
BANK_CREDIT_KEYS = ("收入", "贷方发生额", "贷方金额", "贷方", "credit", "入账金额")
BANK_DEBIT_KEYS = ("支出", "借方发生额", "借方金额", "借方", "debit", "出账金额")
BANK_BALANCE_KEYS = ("余额", "账户余额", "可用余额", "期末余额", "balance")
BANK_COUNTERPARTY_KEYS = ("对手方", "对方户名", "对手名称", "交易对手", "对方名称", "对方账号名称")
BANK_SUMMARY_KEYS = ("摘要", "交易摘要", "用途", "附言", "备注", "交易说明")
BANK_ACCOUNT_KEYS = ("账号", "账户", "银行卡号", "账户号码", "账号/卡号")
BANK_ACCOUNT_NAME_KEYS = ("户名", "账户名称", "账户名", "账号名称", "客户名称")
BANK_BANK_NAME_KEYS = ("开户行", "银行名称", "所属银行", "开户银行")
BANK_LICENSE_NUMBER_KEYS = ("许可证编号", "核准号", "许可证号")


def detect_document_type_code(
    text_content: str,
    explicit_type: str | None = None,
    *,
    rows: list[dict[str, Any]] | None = None,
    ai_service: Any | None = None,
) -> str:
    normalized_explicit = normalize_document_type_code(explicit_type)
    if normalized_explicit:
        return normalized_explicit

    rows = rows or []
    header_names = " ".join(key for row in rows[:5] for key in row.keys())
    bank_header_type = _detect_bank_type_from_headers(header_names)
    if bank_header_type:
        return bank_header_type

    lower_text = (text_content or "").lower()
    for code, keywords in TYPE_KEYWORD_RULES:
        if any(keyword.lower() in lower_text for keyword in keywords):
            return code

    if ai_service is not None:
        classified = ai_service.classify(text_content)
        normalized_ai = normalize_document_type_code(classified)
        if normalized_ai:
            return normalized_ai

    return "enterprise_credit"


def build_structured_extraction(
    text_content: str,
    document_type_code: str,
    *,
    rows: list[dict[str, Any]] | None = None,
    ai_service: Any | None = None,
) -> dict[str, Any]:
    normalized_code = normalize_document_type_code(document_type_code) or document_type_code
    rows = rows or []

    if normalized_code == "business_license":
        content = extract_business_license(text_content)
    elif normalized_code == "account_license":
        content = extract_account_license(text_content)
    elif normalized_code == "company_articles":
        content = extract_company_articles(text_content, ai_service=ai_service)
    elif normalized_code == "bank_statement":
        content = extract_bank_statement_from_rows(rows, text_content)
    elif normalized_code == "bank_statement_detail":
        content = extract_bank_statement_detail_from_rows(rows, text_content)
    elif normalized_code == "contract":
        content = extract_contract(text_content)
    elif normalized_code == "id_card":
        content = extract_id_card(text_content)
    elif normalized_code == "marriage_cert":
        content = extract_marriage_cert(text_content)
    elif normalized_code == "hukou":
        content = extract_hukou(text_content)
    elif normalized_code == "property_report":
        content = extract_property_report(text_content)
    elif normalized_code == "special_license":
        content = extract_special_license(text_content)
    else:
        content = generic_extract(text_content, normalized_code, ai_service)

    content.setdefault("document_type_code", normalized_code)
    content.setdefault("document_type_name", get_document_display_name(normalized_code))
    content.setdefault("storage_label", get_document_storage_label(normalized_code))
    return content


def _extract_with_ai(text_content: str, document_type_code: str, ai_service: Any | None) -> dict[str, Any]:
    if ai_service is None:
        return {
            "summary": _clean_line(text_content[:400]) or "暂无可解析内容",
            "raw_text_excerpt": _clean_line(text_content[:1000]),
        }

    load_prompts()
    prompt = get_prompt_for_type(get_document_storage_label(document_type_code))
    if not prompt:
        prompt = (
            f"请从以下{get_document_display_name(document_type_code)}内容中提取关键信息，"
            "并返回 JSON。字段尽量贴近业务语义，只返回 JSON。"
        )
    result = ai_service.extract(prompt, text_content)
    parsed = parse_json(result)
    if parsed is None:
        return {
            "raw_text": result,
            "parse_error": True,
            "raw_text_excerpt": _clean_line(text_content[:1000]),
        }
    return parsed


def generic_extract(text_content: str, document_type_code: str, ai_service: Any | None = None) -> dict[str, Any]:
    return _extract_with_ai(text_content, document_type_code, ai_service)


def _clean_line(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _find_after_labels(text: str, labels: tuple[str, ...]) -> str:
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}[:：]?\s*([^\n\r]+)")
        match = pattern.search(text or "")
        if match:
            return _clean_line(match.group(1))
    return ""


def _find_first_date(text: str) -> str:
    match = DATE_PATTERN.search(text or "")
    return _normalize_date(match.group(1)) if match else ""


def _normalize_date(value: str) -> str:
    normalized = normalize_text(value).replace("年", "-").replace("月", "-").replace("日", "")
    normalized = normalized.replace("/", "-").replace(".", "-")
    parts = [part.zfill(2) if index else part for index, part in enumerate(normalized.split("-")) if part]
    if len(parts) == 3:
        return "-".join(parts)
    return normalized


def _money_after_labels(text: str, labels: tuple[str, ...]) -> str:
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}[:：]?\s*([¥￥]?\s*[0-9,]+(?:\.\d+)?)")
        match = pattern.search(text or "")
        if match:
            return normalize_amount(match.group(1))
    return ""


def _first_nonempty(values: list[str]) -> str:
    for value in values:
        if normalize_text(value):
            return normalize_text(value)
    return ""


def extract_business_license(text: str) -> dict[str, Any]:
    return {
        "company_name": _first_nonempty([
            _find_after_labels(text, ("名称", "企业名称", "公司名称")),
            _find_after_labels(text, ("市场主体名称",)),
        ]),
        "credit_code": _find_first_match(text, UNIFIED_CODE_PATTERN),
        "legal_person": _find_after_labels(text, ("法定代表人", "法人", "负责人")),
        "registered_capital": _money_after_labels(text, ("注册资本", "注册资金")),
        "establish_date": _find_after_labels(text, ("成立日期", "注册日期", "营业期限自")),
        "business_scope": _find_after_labels(text, ("经营范围",)),
        "address": _find_after_labels(text, ("住所", "营业场所", "地址")),
        "company_type": _find_after_labels(text, ("类型", "主体类型")),
    }


def extract_account_license(text: str) -> dict[str, Any]:
    return {
        "account_name": _find_after_labels(text, ("存款人名称", "账户名称", "户名")),
        "account_number": _find_after_labels(text, ("账号", "银行账号", "账户号码")),
        "bank_name": _find_after_labels(text, ("开户银行", "开户行", "银行名称")),
        "bank_branch": _find_after_labels(text, ("开户银行机构", "开户网点", "开户银行支行")),
        "license_number": _find_after_labels(text, ("核准号", "许可证编号", "许可证号")),
        "account_type": _find_after_labels(text, ("账户性质", "账户类型")),
        "open_date": _find_after_labels(text, ("开户日期", "开立日期")),
    }


def extract_company_articles(text: str, ai_service: Any | None = None) -> dict[str, Any]:
    shareholder_sentences = _extract_keyword_sentences(text, ("股东", "出资", "持股"))
    summary = _build_summary(text, shareholder_sentences, ai_service=ai_service)
    return {
        "company_name": _find_after_labels(text, ("公司名称", "名称")),
        "registered_capital": _money_after_labels(text, ("注册资本", "注册资金")),
        "legal_person": _find_after_labels(text, ("法定代表人", "执行董事", "董事长")),
        "shareholders": shareholder_sentences[:5],
        "business_scope": _find_after_labels(text, ("经营范围",)),
        "address": _find_after_labels(text, ("住所", "公司住所", "地址")),
        "management_structure": "；".join(_extract_keyword_sentences(text, ("董事会", "监事", "经理", "治理结构"))[:3]),
        "summary": summary,
    }


def extract_contract(text: str) -> dict[str, Any]:
    return {
        "counterparty": _find_after_labels(text, ("乙方", "相对方", "合作方", "客户名称")),
        "amount": _money_after_labels(text, ("合同金额", "总金额", "价税合计", "借款金额")),
        "term": _find_after_labels(text, ("合同期限", "服务期限", "履行期限", "租赁期限")),
        "summary": _clean_line(text[:240]),
    }


def extract_id_card(text: str) -> dict[str, Any]:
    return {
        "name": _find_after_labels(text, ("姓名",)),
        "id_number": _find_first_match(text, ID_CARD_PATTERN),
        "address": _find_after_labels(text, ("住址", "地址")),
    }


def extract_marriage_cert(text: str) -> dict[str, Any]:
    return {
        "holder_one": _find_after_labels(text, ("持证人", "姓名")),
        "holder_two": _find_after_labels(text, ("配偶姓名", "另一方")),
        "register_date": _find_after_labels(text, ("登记日期", "领证日期", "结婚登记日期")),
    }


def extract_hukou(text: str) -> dict[str, Any]:
    return {
        "householder": _find_after_labels(text, ("户主姓名", "户主")),
        "address": _find_after_labels(text, ("住址", "地址")),
        "relation": _find_after_labels(text, ("与户主关系", "关系")),
    }


def extract_property_report(text: str) -> dict[str, Any]:
    return {
        "property_location": _find_after_labels(text, ("房屋坐落", "坐落")),
        "owner": _find_after_labels(text, ("权利人", "所有权人")),
        "building_area": _find_after_labels(text, ("建筑面积", "面积")),
    }


def extract_special_license(text: str) -> dict[str, Any]:
    return {
        "license_name": _find_after_labels(text, ("许可证名称", "许可项目", "证书名称")),
        "license_number": _find_after_labels(text, ("许可证编号", "证书编号", "编号")),
        "valid_until": _find_after_labels(text, ("有效期", "有效期限")),
    }


def extract_bank_statement_from_rows(rows: list[dict], raw_text: str = "") -> dict:
    analysis = _analyze_bank_rows(rows)
    transactions = analysis["transactions"]
    top_inflows = sorted(
        [row for row in transactions if row["credit_amount_decimal"] > 0],
        key=lambda item: item["credit_amount_decimal"],
        reverse=True,
    )[:5]
    top_outflows = sorted(
        [row for row in transactions if row["debit_amount_decimal"] > 0],
        key=lambda item: item["debit_amount_decimal"],
        reverse=True,
    )[:5]

    total_income = _sum_decimal(item["credit_amount_decimal"] for item in transactions)
    total_expense = _sum_decimal(item["debit_amount_decimal"] for item in transactions)
    transaction_count = len(transactions)
    monthly_avg_income = _average_by_month(transactions, "credit_amount_decimal")
    monthly_avg_expense = _average_by_month(transactions, "debit_amount_decimal")

    opening_balance = _first_nonempty([item["balance"] for item in transactions])
    closing_balance = _first_nonempty([item["balance"] for item in reversed(transactions)])

    return {
        "account_name": analysis["account_name"] or _find_after_labels(raw_text, ("户名", "账户名称", "客户名称")),
        "account_number": analysis["account_number"] or _find_after_labels(raw_text, ("账号", "账户号码", "银行卡号")),
        "bank_name": analysis["bank_name"] or _find_after_labels(raw_text, ("开户行", "银行名称", "开户银行")),
        "currency": analysis["currency"] or _find_after_labels(raw_text, ("币种",)),
        "start_date": analysis["start_date"] or _find_first_date(raw_text),
        "end_date": analysis["end_date"] or _find_last_date(raw_text),
        "opening_balance": opening_balance or _money_after_labels(raw_text, ("期初余额", "上期余额", "起始余额")),
        "closing_balance": closing_balance or _money_after_labels(raw_text, ("期末余额", "当前余额", "账户余额")),
        "total_income": _format_decimal(total_income),
        "total_expense": _format_decimal(total_expense),
        "monthly_avg_income": _format_decimal(monthly_avg_income),
        "monthly_avg_expense": _format_decimal(monthly_avg_expense),
        "transaction_count": str(transaction_count),
        "top_inflows": [_serialize_bank_transaction(item) for item in top_inflows],
        "top_outflows": [_serialize_bank_transaction(item) for item in top_outflows],
    }


def extract_bank_statement_detail_from_rows(rows: list[dict], raw_text: str = "") -> dict:
    analysis = _analyze_bank_rows(rows)
    transactions = analysis["transactions"]
    total_debit = _sum_decimal(item["debit_amount_decimal"] for item in transactions)
    total_credit = _sum_decimal(item["credit_amount_decimal"] for item in transactions)
    top_transactions = sorted(
        transactions,
        key=lambda item: max(item["credit_amount_decimal"], item["debit_amount_decimal"]),
        reverse=True,
    )[:10]
    frequent_counterparties = [
        name for name, _ in Counter(
            normalize_text(item["counterparty"]) for item in transactions if normalize_text(item["counterparty"])
        ).most_common(5)
    ]

    abnormal_parts: list[str] = []
    large_transactions = [
        item for item in transactions
        if max(item["credit_amount_decimal"], item["debit_amount_decimal"]) >= Decimal("100000")
    ]
    if large_transactions:
        abnormal_parts.append(f"检测到{len(large_transactions)}笔大额交易")
    if analysis["start_date"] and analysis["end_date"] and not transactions:
        abnormal_parts.append("账单时间范围存在，但未识别出有效交易行")

    return {
        "account_name": analysis["account_name"] or _find_after_labels(raw_text, ("户名", "账户名称", "客户名称")),
        "account_number": analysis["account_number"] or _find_after_labels(raw_text, ("账号", "账户号码", "银行卡号")),
        "bank_name": analysis["bank_name"] or _find_after_labels(raw_text, ("开户行", "银行名称", "开户银行")),
        "start_date": analysis["start_date"] or _find_first_date(raw_text),
        "end_date": analysis["end_date"] or _find_last_date(raw_text),
        "transaction_count": str(len(transactions)),
        "total_debit": _format_decimal(total_debit),
        "total_credit": _format_decimal(total_credit),
        "top_transactions": [_serialize_bank_transaction(item) for item in top_transactions],
        "frequent_counterparties": frequent_counterparties,
        "abnormal_summary": "；".join(abnormal_parts),
    }


def _detect_bank_type_from_headers(header_text: str) -> str | None:
    lower = (header_text or "").lower()
    if any(keyword in lower for keyword in ("摘要", "对方", "借方", "贷方", "交易")):
        if "明细" in lower or "摘要" in lower or "对方" in lower:
            return "bank_statement_detail"
    if any(keyword in lower for keyword in ("余额", "收入", "支出", "对账")):
        return "bank_statement"
    return None


def _analyze_bank_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    transactions: list[dict[str, Any]] = []
    account_name = ""
    account_number = ""
    bank_name = ""
    currency = ""
    dates: list[str] = []

    for row in rows:
        account_name = account_name or _find_value_by_aliases(row, BANK_ACCOUNT_NAME_KEYS)
        account_number = account_number or _find_value_by_aliases(row, BANK_ACCOUNT_KEYS)
        bank_name = bank_name or _find_value_by_aliases(row, BANK_BANK_NAME_KEYS)
        currency = currency or _find_value_by_aliases(row, ("币种", "currency"))

        date_value = _normalize_date(_find_value_by_aliases(row, BANK_DATE_KEYS))
        credit_amount = Decimal(normalize_amount(_find_value_by_aliases(row, BANK_CREDIT_KEYS)) or "0")
        debit_amount = Decimal(normalize_amount(_find_value_by_aliases(row, BANK_DEBIT_KEYS)) or "0")
        balance = normalize_amount(_find_value_by_aliases(row, BANK_BALANCE_KEYS))
        counterparty = _find_value_by_aliases(row, BANK_COUNTERPARTY_KEYS)
        summary = _find_value_by_aliases(row, BANK_SUMMARY_KEYS)

        if date_value:
            dates.append(date_value)
        if not any([date_value, credit_amount, debit_amount, balance, counterparty, summary]):
            continue

        transactions.append(
            {
                "date": date_value,
                "credit_amount_decimal": credit_amount,
                "debit_amount_decimal": debit_amount,
                "credit_amount": _format_decimal(credit_amount),
                "debit_amount": _format_decimal(debit_amount),
                "balance": balance,
                "counterparty": counterparty,
                "summary": summary,
            }
        )

    dates = sorted(date for date in dates if date)
    return {
        "account_name": account_name,
        "account_number": account_number,
        "bank_name": bank_name,
        "currency": currency,
        "start_date": dates[0] if dates else "",
        "end_date": dates[-1] if dates else "",
        "transactions": transactions,
    }


def _find_value_by_aliases(row: dict[str, Any], aliases: tuple[str, ...]) -> str:
    for key, value in row.items():
        if key.startswith("_"):
            continue
        normalized_key = normalize_text(key).lower()
        for alias in aliases:
            if alias.lower() in normalized_key:
                return normalize_text(value)
    return ""


def _serialize_bank_transaction(item: dict[str, Any]) -> dict[str, str]:
    return {
        "date": item.get("date", ""),
        "counterparty": item.get("counterparty", ""),
        "summary": item.get("summary", ""),
        "income": item.get("credit_amount", ""),
        "expense": item.get("debit_amount", ""),
        "balance": item.get("balance", ""),
    }


def _sum_decimal(values: Any) -> Decimal:
    total = Decimal("0")
    for value in values:
        total += value
    return total


def _average_by_month(transactions: list[dict[str, Any]], amount_key: str) -> Decimal:
    monthly_totals: dict[str, Decimal] = {}
    for item in transactions:
        date = item.get("date", "")
        month_key = date[:7] if len(date) >= 7 else ""
        if not month_key:
            continue
        monthly_totals[month_key] = monthly_totals.get(month_key, Decimal("0")) + item[amount_key]
    if not monthly_totals:
        return Decimal("0")
    total = sum(monthly_totals.values(), Decimal("0"))
    return total / Decimal(len(monthly_totals))


def _format_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _find_first_match(text: str, pattern: re.Pattern[str]) -> str:
    match = pattern.search(text or "")
    return match.group(1).strip() if match else ""


def _extract_keyword_sentences(text: str, keywords: tuple[str, ...]) -> list[str]:
    sentences = re.split(r"[。\n；;]+", text or "")
    matches = []
    for sentence in sentences:
        cleaned = _clean_line(sentence)
        if cleaned and any(keyword in cleaned for keyword in keywords):
            matches.append(cleaned)
    return matches


def _build_summary(text: str, shareholder_sentences: list[str], ai_service: Any | None = None) -> str:
    if ai_service is not None and text.strip():
        prompt = (
            "请用不超过120字总结这份公司章程的核心信息，"
            "重点概括公司名称、注册资本、股东结构和经营范围，只返回纯文本。"
        )
        try:
            result = ai_service.extract(prompt, text[:6000])
            if isinstance(result, str) and result.strip():
                return _clean_line(result)
        except Exception:
            pass
    summary_parts = []
    if shareholder_sentences:
        summary_parts.append(shareholder_sentences[0])
    summary_parts.append(_clean_line(text[:180]))
    return "；".join(part for part in summary_parts if part)[:240]


def _find_last_date(text: str) -> str:
    matches = DATE_PATTERN.findall(text or "")
    return _normalize_date(matches[-1]) if matches else ""
