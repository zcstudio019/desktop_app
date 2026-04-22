"""Structured extraction helpers for upload/chat document parsing."""

from __future__ import annotations

import json
import logging
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
from backend.services.extraction_utils import normalize_amount, normalize_text, only_digits
from prompts import get_prompt_for_type, load_prompts
from utils.json_parser import parse_json

logger = logging.getLogger(__name__)

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
        content = extract_bank_statement_from_rows(rows, text_content) if rows else extract_bank_statement_pdf_fields(text_content)
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


def _extract_label_value(
    text: str,
    labels: tuple[str, ...],
    *,
    stop_labels: tuple[str, ...] = (),
    allow_multiline: bool = False,
    max_length: int = 200,
) -> str:
    source = text or ""
    flags = re.MULTILINE | (re.DOTALL if allow_multiline else 0)
    stop_pattern = "|".join(re.escape(item) for item in stop_labels if item)

    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*", flags)
        match = pattern.search(source)
        if not match:
            continue
        start = match.end()
        candidate = source[start : start + max_length]
        if stop_pattern:
            stop_match = re.search(rf"(?=\b(?:{stop_pattern})\b\s*[:：]?)", candidate, flags)
            if stop_match:
                candidate = candidate[: stop_match.start()]
        candidate = candidate.split("\n")[0] if not allow_multiline else candidate
        cleaned = _clean_field_value(candidate)
        if cleaned:
            return cleaned
    return ""


def extract_labeled_field(text: str, labels: list[str], stop_labels: list[str]) -> str:
    """Extract `label: value` text and stop at the next recognized label."""
    return _extract_label_value(text, tuple(labels), stop_labels=tuple(stop_labels), allow_multiline=True)


def _clean_field_value(value: str) -> str:
    cleaned = normalize_text(value)
    if not cleaned:
        return ""
    cleaned = re.sub(r"[|｜]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip("：:;；，,。 ")
    return cleaned


def extract_company_articles_registered_capital(text: str) -> str:
    """Extract registered capital from company articles without defaulting to 0."""
    source = normalize_text(text)
    if not source:
        return ""
    patterns = (
        r"(?:公司)?注册资本\s*[:：]?\s*(人民币\s*[0-9,]+(?:\.\d+)?\s*(?:万元|元|亿元))",
        r"(?:公司)?注册资本\s*为\s*(人民币\s*[0-9,]+(?:\.\d+)?\s*(?:万元|元|亿元))",
        r"注册资本(?:总额)?\s*[:：]?\s*(人民币\s*[0-9,]+(?:\.\d+)?\s*(?:万元|元|亿元))",
        r"(?:公司)?注册资本\s*[:：]?\s*([0-9,]+(?:\.\d+)?\s*(?:万元|元|亿元))",
        r"(?:公司)?注册资本\s*为\s*([0-9,]+(?:\.\d+)?\s*(?:万元|元|亿元))",
    )
    for pattern in patterns:
        match = re.search(pattern, source)
        if not match:
            continue
        value = re.sub(r"\s+", "", match.group(1))
        if value and value != "0":
            return value
    fallback = _registered_capital_cn(source)
    return "" if fallback == "0" else fallback


def clean_business_scope(value: str) -> str:
    return _clean_scope_or_address(value, stop_words=("住所", "地址", "类型", "法定代表人", "统一社会信用代码", "成立日期"))


def clean_address(value: str) -> str:
    return _clean_scope_or_address(value, stop_words=("经营范围", "类型", "法定代表人", "统一社会信用代码", "成立日期"))


def _clean_scope_or_address(value: str, *, stop_words: tuple[str, ...]) -> str:
    cleaned = _clean_field_value(value)
    for stop_word in stop_words:
        idx = cleaned.find(stop_word)
        if idx > 0:
            cleaned = cleaned[:idx].strip("：:;；，,。 ")
    return cleaned


def _extract_branch_from_bank_name(bank_name: str) -> tuple[str, str]:
    cleaned = _clean_field_value(bank_name)
    if not cleaned:
        return "", ""
    branch_match = re.search(r"(.+?银行)(.+?(?:支行|分行|营业部|营业室|分理处))", cleaned)
    if branch_match:
        return branch_match.group(1).strip(), branch_match.group(2).strip()
    return cleaned, ""


def split_bank_name_and_branch(value: str) -> tuple[str, str]:
    """Split bank_name / bank_branch where possible."""
    return _extract_branch_from_bank_name(value)


def _pick_first_nonempty(*values: str) -> str:
    for value in values:
        cleaned = _clean_field_value(value)
        if cleaned:
            return cleaned
    return ""


def _extract_account_number_from_text(text: str) -> str:
    candidates = [
        _extract_label_value(text, ("账号", "账户号码", "银行账号", "结算账户"), stop_labels=("开户行", "开户银行", "币种", "账户名称")),
        _extract_label_value(text, ("卡号",), stop_labels=("开户行", "开户银行", "币种")),
    ]
    for candidate in candidates:
        normalized = re.sub(r"\s+", "", candidate)
        normalized = normalized.replace("账号", "").replace("账户", "")
        normalized = normalized.strip("：:")
        if re.search(r"\d{8,}", normalized):
            return re.search(r"\d{8,}", normalized).group(0)
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
    stop_labels = (
        "统一社会信用代码",
        "法定代表人",
        "注册资本",
        "成立日期",
        "住所",
        "地址",
        "经营范围",
        "类型",
    )
    return {
        "company_name": _pick_first_nonempty(
            _extract_label_value(text, ("名称", "企业名称", "公司名称", "市场主体名称"), stop_labels=stop_labels),
            _find_after_labels(text, ("名称", "企业名称", "公司名称")),
        ),
        "credit_code": _find_first_match(text, UNIFIED_CODE_PATTERN),
        "legal_person": _extract_label_value(text, ("法定代表人", "法人", "负责人"), stop_labels=stop_labels),
        "registered_capital": extract_company_articles_registered_capital(text),
        "establish_date": _pick_first_nonempty(
            _extract_label_value(text, ("成立日期", "注册日期", "营业期限自"), stop_labels=stop_labels),
            _find_first_date(text),
        ),
        "business_scope": _clean_scope_or_address(
            _extract_label_value(text, ("经营范围",), stop_labels=("住所", "地址", "类型", "法定代表人"), allow_multiline=True, max_length=600),
            stop_words=("住所", "地址", "类型", "法定代表人"),
        ),
        "address": _clean_scope_or_address(
            _extract_label_value(text, ("住所", "营业场所", "地址"), stop_labels=("经营范围", "类型", "法定代表人"), allow_multiline=True, max_length=240),
            stop_words=("经营范围", "类型", "法定代表人"),
        ),
        "company_type": _extract_label_value(text, ("类型", "主体类型"), stop_labels=("法定代表人", "注册资本", "成立日期", "经营范围")),
    }


def extract_account_license(text: str) -> dict[str, Any]:
    stop_labels = (
        "开户银行",
        "开户行",
        "开户银行机构",
        "核准号",
        "许可证编号",
        "账户性质",
        "账户类型",
        "开户日期",
        "开立日期",
        "存款人名称",
        "账户名称",
        "户名",
    )
    bank_full = _pick_first_nonempty(
        _extract_label_value(text, ("开户银行", "开户行", "银行名称"), stop_labels=stop_labels),
        _extract_label_value(text, ("开户银行机构", "开户网点"), stop_labels=stop_labels),
    )
    bank_name, bank_branch = _extract_branch_from_bank_name(bank_full)
    return {
        "account_name": _extract_label_value(text, ("存款人名称", "账户名称", "户名"), stop_labels=stop_labels),
        "account_number": _extract_account_number_from_text(text),
        "bank_name": bank_name,
        "bank_branch": _pick_first_nonempty(
            bank_branch,
            _extract_label_value(text, ("开户银行机构", "开户网点", "开户银行支行"), stop_labels=stop_labels),
        ),
        "license_number": _extract_label_value(text, ("核准号", "许可证编号", "许可证号"), stop_labels=stop_labels),
        "account_type": _extract_label_value(text, ("账户性质", "账户类型"), stop_labels=stop_labels),
        "open_date": _pick_first_nonempty(
            _extract_label_value(text, ("开户日期", "开立日期"), stop_labels=stop_labels),
            _find_first_date(text),
        ),
    }


def extract_company_articles(text: str, ai_service: Any | None = None) -> dict[str, Any]:
    shareholder_sentences = _extract_keyword_sentences(text, ("股东", "出资", "持股"))
    summary = _build_summary(text, shareholder_sentences, ai_service=ai_service)
    return {
        "company_name": _find_after_labels(text, ("公司名称", "名称")),
        "registered_capital": extract_company_articles_registered_capital(text),
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


def extract_bank_statement_pdf(raw_text: str) -> dict[str, Any]:
    account_name = _extract_label_value(
        raw_text,
        ("户名", "账户名称", "客户名称", "单位名称"),
        stop_labels=("账号", "账户号码", "卡号", "开户行", "开户银行", "币种"),
        max_length=120,
    )
    account_number = _extract_account_number_from_text(raw_text)
    bank_full = _pick_first_nonempty(
        _extract_label_value(
            raw_text,
            ("开户行", "开户银行", "银行名称"),
            stop_labels=("币种", "账号", "账户号码", "户名", "账户名称"),
            max_length=160,
        ),
        _extract_label_value(
            raw_text,
            ("所属银行",),
            stop_labels=("币种", "账号", "账户号码", "户名", "账户名称"),
            max_length=160,
        ),
    )
    bank_name, bank_branch = _extract_branch_from_bank_name(bank_full)
    currency = _extract_label_value(
        raw_text,
        ("币种",),
        stop_labels=("账号", "账户号码", "开户行", "开户银行", "交易日期", "记账日期"),
        max_length=20,
    )
    start_date = _pick_first_nonempty(
        _extract_label_value(raw_text, ("起始日期", "开始日期", "账单起始日", "自"), stop_labels=("截止日期", "结束日期", "至")),
        _find_first_date(raw_text),
    )
    end_date = _pick_first_nonempty(
        _extract_label_value(raw_text, ("截止日期", "结束日期", "账单截止日", "至"), stop_labels=("期初余额", "期末余额", "余额")),
        _find_last_date(raw_text),
    )
    opening_balance = _money_after_labels(raw_text, ("期初余额", "上期余额", "起始余额"))
    closing_balance = _money_after_labels(raw_text, ("期末余额", "当前余额", "账户余额"))

    return {
        "account_name": account_name,
        "account_number": account_number,
        "bank_name": bank_name,
        "currency": currency,
        "start_date": start_date,
        "end_date": end_date,
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
        "total_income": "",
        "total_expense": "",
        "monthly_avg_income": "",
        "monthly_avg_expense": "",
        "transaction_count": "",
        "top_inflows": [],
        "top_outflows": [],
        "bank_branch": bank_branch,
    }


def _v2_only_digits(value: str) -> str:
    return only_digits(value)


def _v2_extract_currency(value: str) -> str:
    text = normalize_text(value).upper()
    if not text:
        return ""
    if "人民币" in text or "CNY" in text or "RMB" in text:
        return "人民币"
    for currency in ("USD", "HKD", "EUR"):
        if currency in text:
            return currency
    return ""


def _v2_extract_date_range(text: str) -> tuple[str, str]:
    source = text or ""
    patterns = (
        re.compile(r"((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)\s*(?:--|-|至|~)\s*((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)"),
        re.compile(r"(?:记账日期|查询日期范围|起止日期)[:：]?\s*((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)\s*(?:至|-|--|~)\s*((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)"),
    )
    for pattern in patterns:
        match = pattern.search(source)
        if match:
            return _normalize_date(match.group(1)), _normalize_date(match.group(2))
    return "", ""


def _v2_extract_labeled_field(text: str, labels: list[str], stop_labels: list[str], *, max_length: int = 240, allow_multiline: bool = False) -> str:
    source = text or ""
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*", re.MULTILINE)
        match = pattern.search(source)
        if not match:
            continue
        start = match.end()
        candidate = source[start : start + max_length]
        end_indexes = []
        for stop_label in stop_labels:
            stop_match = re.search(rf"{re.escape(stop_label)}\s*[:：]?", candidate)
            if stop_match:
                end_indexes.append(stop_match.start())
        if end_indexes:
            candidate = candidate[: min(end_indexes)]
        if not allow_multiline:
            candidate = candidate.splitlines()[0]
        cleaned = re.sub(r"\s+", " ", normalize_text(candidate)).strip("：:;；，,。 ")
        if cleaned:
            return cleaned
    return ""


def clean_business_scope(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    for marker in ("住所", "地址", "类型", "法定代表人", "统一社会信用代码", "成立日期"):
        idx = cleaned.find(marker)
        if idx > 0:
            cleaned = cleaned[:idx].strip("：:;；，,。 ")
    return cleaned


def clean_address(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    for marker in ("经营范围", "类型", "法定代表人", "统一社会信用代码", "成立日期"):
        idx = cleaned.find(marker)
        if idx > 0:
            cleaned = cleaned[:idx].strip("：:;；，,。 ")
    return cleaned


def split_bank_name_and_branch(value: str) -> tuple[str, str]:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    if not cleaned:
        return "", ""
    match = re.search(r"(.+?(?:银行|信用社|农商行|农村商业银行|股份有限公司))(.+?(?:支行|分行|营业部|营业室|分理处))", cleaned)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return cleaned, ""


def _v2_extract_registered_capital(text: str) -> str:
    for label in ("注册资本", "注册资金"):
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*((?:人民币)?\s*[0-9,]+(?:\.\d+)?\s*(?:万元|万人民币|元|亿元|万美元|亿美元)?)")
        match = pattern.search(text or "")
        if match:
            return re.sub(r"\s+", "", match.group(1))
    return ""


def extract_business_license(text: str) -> dict[str, Any]:
    stop_labels = ["统一社会信用代码", "社会信用代码", "法定代表人", "法人", "负责人", "注册资本", "成立日期", "住所", "地址", "经营范围", "类型"]
    scope_raw = _v2_extract_labeled_field(text, ["经营范围"], ["住所", "地址", "类型", "法定代表人"], max_length=800, allow_multiline=True)
    address_raw = _v2_extract_labeled_field(text, ["住所", "地址", "营业场所"], ["经营范围", "类型", "法定代表人"], max_length=260, allow_multiline=True)
    return {
        "company_name": _pick_first_nonempty(
            _v2_extract_labeled_field(text, ["名称", "企业名称", "公司名称", "市场主体名称"], stop_labels, max_length=180),
            _find_after_labels(text, ("名称", "企业名称", "公司名称")),
        ),
        "credit_code": _find_first_match(text, UNIFIED_CODE_PATTERN),
        "legal_person": _v2_extract_labeled_field(text, ["法定代表人", "法人", "负责人"], stop_labels, max_length=60),
        "registered_capital": _v2_extract_registered_capital(text),
        "establish_date": _pick_first_nonempty(
            _v2_extract_labeled_field(text, ["成立日期", "注册日期"], stop_labels, max_length=80),
            _find_first_date(text),
        ),
        "business_scope": clean_business_scope(scope_raw),
        "address": clean_address(address_raw),
        "company_type": _v2_extract_labeled_field(text, ["类型", "主体类型"], ["法定代表人", "注册资本", "成立日期", "经营范围"], max_length=80),
    }


def extract_account_license(text: str) -> dict[str, Any]:
    stop_labels = ["开户银行", "开户行", "开户机构", "核准号", "许可证号", "账户性质", "账户类型", "开户日期", "存款人名称", "账户名称", "户名", "币种"]
    bank_full = _pick_first_nonempty(
        _v2_extract_labeled_field(text, ["开户银行", "开户行"], stop_labels, max_length=180),
        _v2_extract_labeled_field(text, ["开户机构", "开户银行机构"], stop_labels, max_length=180),
    )
    bank_name, bank_branch = split_bank_name_and_branch(bank_full)
    return {
        "account_name": _v2_extract_labeled_field(text, ["账户名称", "存款人名称", "户名"], stop_labels, max_length=120),
        "account_number": _v2_extract_account_number(text),
        "bank_name": bank_name,
        "bank_branch": _pick_first_nonempty(bank_branch, _v2_extract_labeled_field(text, ["开户机构", "开户银行机构", "开户网点"], stop_labels, max_length=120)),
        "license_number": _v2_extract_labeled_field(text, ["核准号", "许可证号", "许可证编号"], stop_labels, max_length=80),
        "account_type": _v2_extract_labeled_field(text, ["账户性质", "账户类型"], stop_labels, max_length=80),
        "open_date": _pick_first_nonempty(_v2_extract_labeled_field(text, ["开户日期", "开立日期"], stop_labels, max_length=60), _find_first_date(text)),
    }


def _v2_extract_labeled_field(text: str, labels: list[str], stop_labels: list[str], *, max_length: int = 240, allow_multiline: bool = False) -> str:
    source = text or ""
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}\s*[:锛歖?\s*", re.MULTILINE)
        match = pattern.search(source)
        if not match:
            continue
        candidate = source[match.end() : match.end() + max_length]
        stop_indexes: list[int] = []
        for stop_label in stop_labels:
            stop_match = re.search(rf"{re.escape(stop_label)}\s*[:锛歖?", candidate)
            if stop_match:
                stop_indexes.append(stop_match.start())
        if stop_indexes:
            candidate = candidate[: min(stop_indexes)]
        candidate = str(candidate or "").strip()
        if not candidate:
            return ""
        if not allow_multiline:
            lines = [line.strip() for line in candidate.splitlines() if line.strip()]
            candidate = lines[0] if lines else ""
        candidate = str(candidate or "").strip()
        if not candidate:
            return ""
        cleaned = re.sub(r"\s+", " ", normalize_text(candidate)).strip("锛?;锛涳紝,銆?")
        return cleaned if cleaned else ""
    return ""


_FINAL_COLON_PATTERN = r"(?:\s*[:：]\s*|\s+)"


def _normalize_date(value: str) -> str:
    normalized = normalize_text(value)
    if not normalized:
        return ""
    normalized = (
        normalized.replace("\u5e74", "-")
        .replace("\u6708", "-")
        .replace("\u65e5", "")
        .replace("/", "-")
        .replace(".", "-")
        .replace("--", "-")
    )
    match = re.search(r"((?:19|20)\d{2})-(\d{1,2})-(\d{1,2})", normalized)
    if not match:
        return normalized
    return f"{match.group(1)}-{match.group(2).zfill(2)}-{match.group(3).zfill(2)}"


def _find_first_date(text: str) -> str:
    source = text or ""
    pattern = re.compile(r"(?:19|20)\d{2}[年./-]\d{1,2}[月./-]\d{1,2}日?")
    match = pattern.search(source)
    return _normalize_date(match.group(0)) if match else ""


def _find_last_date(text: str) -> str:
    source = text or ""
    matches = re.findall(r"(?:19|20)\d{2}[年./-]\d{1,2}[月./-]\d{1,2}日?", source)
    return _normalize_date(matches[-1]) if matches else ""


def _money_after_labels(text: str, labels: tuple[str, ...]) -> str:
    source = text or ""
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}{_FINAL_COLON_PATTERN}?([^\n\r ]{{0,8}})?([+-]?\d[\d,]*(?:\.\d+)?)")
        match = pattern.search(source)
        if match:
            return normalize_amount(match.group(2))
    return ""


def extract_labeled_field(text: str, labels: list[str], stop_labels: list[str]) -> str:
    return _extract_labeled_field_final(text, labels, stop_labels, allow_multiline=True)


def _extract_labeled_field_final(
    text: str,
    labels: list[str],
    stop_labels: list[str],
    *,
    max_length: int = 240,
    allow_multiline: bool = False,
) -> str:
    source = text or ""
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}{_FINAL_COLON_PATTERN}?")
        match = pattern.search(source)
        if not match:
            continue
        candidate = source[match.end() : match.end() + max_length]
        stop_indexes: list[int] = []
        for stop_label in stop_labels:
            stop_match = re.search(rf"{re.escape(stop_label)}{_FINAL_COLON_PATTERN}?", candidate)
            if stop_match:
                stop_indexes.append(stop_match.start())
        if stop_indexes:
            candidate = candidate[: min(stop_indexes)]
        if not allow_multiline:
            candidate = candidate.splitlines()[0]
        cleaned = _clean_extracted_field(candidate)
        if cleaned:
            return cleaned
    return ""


def _clean_extracted_field(value: str) -> str:
    cleaned = normalize_text(value)
    if not cleaned:
        return ""
    cleaned = re.sub(r"[\|\u00a0]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(
        r"^(?:账号|账户号码|账户名称|开户银行|开户行|开户机构|币种|户名|客户名称|单位名称|名称|地址|住所|经营范围|类型|法定代表人|法人|负责人)\s*[:：]?\s*",
        "",
        cleaned,
    )
    cleaned = cleaned.strip("：:;；，,。 ")
    return cleaned


def _extract_registered_capital_final(text: str) -> str:
    labels = ["注册资本", "注册资金"]
    stop_labels = ["成立日期", "住所", "地址", "经营范围", "类型", "法定代表人", "法人", "负责人"]
    candidate = _extract_labeled_field_final(text, labels, stop_labels, max_length=80)
    candidate = re.sub(r"\s+", "", candidate)
    if candidate:
        match = re.search(
            r"((?:人民币)?[0-9一二三四五六七八九十百千万亿零〇,\.]+(?:万?元(?:人民币)?|亿元|万元|元|万美元|万欧元|欧元))",
            candidate,
        )
        if match:
            return match.group(1)
    source = text or ""
    for label in labels:
        match = re.search(
            rf"{re.escape(label)}{_FINAL_COLON_PATTERN}?((?:人民币)?[0-9一二三四五六七八九十百千万亿零〇,\.]+(?:万?元(?:人民币)?|亿元|万元|元|万美元|万欧元|欧元))",
            source,
        )
        if match:
            return re.sub(r"\s+", "", match.group(1))
    return ""


def clean_business_scope(value: str) -> str:
    cleaned = _clean_extracted_field(value)
    if not cleaned:
        return ""
    for marker in (
        "\u4f4f\u6240",
        "\u5730\u5740",
        "\u7c7b\u578b",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba",
        "\u6cd5\u4eba",
        "\u8d1f\u8d23\u4eba",
        "\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u6210\u7acb\u65e5\u671f",
        "\u6ce8\u518c\u8d44\u672c",
    ):
        idx = cleaned.find(marker)
        if idx > 0:
            cleaned = cleaned[:idx]
    if re.search(r"(省|市|区|县|路|街|号|室).*(经营范围)", cleaned):
        cleaned = re.split(r"\u7ecf\u8425\u8303\u56f4", cleaned, maxsplit=1)[-1]
    cleaned = cleaned.strip("：:;；，,。 ")
    if len(cleaned) < 4:
        return ""
    address_like_hits = len(re.findall(r"(省|市|区|县|路|街|号|室)", cleaned))
    if address_like_hits >= 3 and "；" not in cleaned and ";" not in cleaned and "、" not in cleaned:
        return ""
    return cleaned


def clean_address(value: str) -> str:
    cleaned = _clean_extracted_field(value)
    if not cleaned:
        return ""
    for marker in (
        "\u7ecf\u8425\u8303\u56f4",
        "\u7c7b\u578b",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba",
        "\u6cd5\u4eba",
        "\u8d1f\u8d23\u4eba",
        "\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u6210\u7acb\u65e5\u671f",
        "\u6ce8\u518c\u8d44\u672c",
    ):
        idx = cleaned.find(marker)
        if idx > 0:
            cleaned = cleaned[:idx]
    cleaned = cleaned.strip("：:;；，,。 ")
    if len(cleaned) < 4:
        return ""
    scope_markers = len(re.findall(r"(经营|销售|服务|咨询|生产|加工|开发)", cleaned))
    if scope_markers >= 2 and not re.search(r"(省|市|区|县|路|街|号|室)", cleaned):
        return ""
    return cleaned


def split_bank_name_and_branch(value: str) -> tuple[str, str]:
    cleaned = _clean_extracted_field(value)
    if not cleaned:
        return "", ""
    cleaned = re.sub(r"(账号|账户号码|币种).*$", "", cleaned).strip("：:;；，,。 ")
    patterns = (
        r"(.+?(?:银行股份有限公司|银行有限责任公司|银行股份|银行|信用合作联社|农村商业银行|农商银行|信用社))(.+?(?:支行|分行|营业部|营业室|分理处|分中心))$",
        r"(.+?(?:银行|信用社))(.+?(?:支行|分行|营业部|营业室|分理处|分中心))$",
    )
    for pattern in patterns:
        match = re.search(pattern, cleaned)
        if match:
            return match.group(1).strip(), match.group(2).strip()
    return cleaned, ""


def _extract_account_number_final(text: str) -> str:
    source = text or ""
    labels = ["账号", "银行账号", "账户号码", "账号信息", "选择账号", "卡号"]
    stop_labels = ["开户行", "开户银行", "币种", "户名", "客户名称", "单位名称", "账户名称"]
    candidate = _extract_labeled_field_final(source, labels, stop_labels, max_length=80)
    digits = only_digits(candidate)
    if 8 <= len(digits) <= 40:
        return digits
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}{_FINAL_COLON_PATTERN}?([0-9][0-9\s]{{7,39}})")
        match = pattern.search(source)
        if match:
            digits = only_digits(match.group(1))
            if 8 <= len(digits) <= 40:
                return digits
    all_matches = re.findall(r"(?<!\d)(\d{8,40})(?!\d)", source)
    return max(all_matches, key=len) if all_matches else ""


def extract_currency(value: str) -> str:
    text = normalize_text(value).upper()
    if not text:
        return ""
    if "\u4eba\u6c11\u5e01" in value or "CNY" in text or "RMB" in text:
        return "\u4eba\u6c11\u5e01"
    for currency in ("USD", "HKD", "EUR"):
        if currency in text:
            return currency
    return ""


def extract_date_range(text: str) -> tuple[str, str]:
    source = text or ""
    normalized = (
        source.replace("\u5e74", "-")
        .replace("\u6708", "-")
        .replace("\u65e5", "")
        .replace("/", "-")
        .replace(".", "-")
    )
    patterns = (
        re.compile(r"((?:19|20)\d{2}-\d{1,2}-\d{1,2})\s*(?:--|-|至|~|—)\s*((?:19|20)\d{2}-\d{1,2}-\d{1,2})"),
        re.compile(r"(?:记账日期范围|查询日期范围|起止日期|记账日期)\s*[:：]?\s*((?:19|20)\d{2}-\d{1,2}-\d{1,2})\s*(?:--|-|至|~|—)\s*((?:19|20)\d{2}-\d{1,2}-\d{1,2})"),
    )
    for pattern in patterns:
        match = pattern.search(normalized)
        if match:
            return _normalize_date(match.group(1)), _normalize_date(match.group(2))
    dates = re.findall(r"(?:19|20)\d{2}-\d{1,2}-\d{1,2}", normalized)
    if len(dates) >= 2:
        return _normalize_date(dates[0]), _normalize_date(dates[1])
    return "", ""


def extract_business_license(text: str) -> dict[str, Any]:
    stop_labels = [
        "\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba",
        "\u6cd5\u4eba",
        "\u8d1f\u8d23\u4eba",
        "\u6ce8\u518c\u8d44\u672c",
        "\u6210\u7acb\u65e5\u671f",
        "\u4f4f\u6240",
        "\u5730\u5740",
        "\u7ecf\u8425\u8303\u56f4",
        "\u7c7b\u578b",
    ]
    raw_scope = _extract_labeled_field_final(
        text,
        ["\u7ecf\u8425\u8303\u56f4"],
        ["\u4f4f\u6240", "\u5730\u5740", "\u7c7b\u578b", "\u6cd5\u5b9a\u4ee3\u8868\u4eba", "\u6cd5\u4eba", "\u8d1f\u8d23\u4eba"],
        max_length=900,
        allow_multiline=True,
    )
    raw_address = _extract_labeled_field_final(
        text,
        ["\u4f4f\u6240", "\u5730\u5740", "\u8425\u4e1a\u573a\u6240"],
        ["\u7ecf\u8425\u8303\u56f4", "\u7c7b\u578b", "\u6cd5\u5b9a\u4ee3\u8868\u4eba", "\u6cd5\u4eba", "\u8d1f\u8d23\u4eba"],
        max_length=320,
        allow_multiline=True,
    )
    business_scope = clean_business_scope(raw_scope)
    address = clean_address(raw_address)
    if business_scope and address and business_scope == address:
        if len(re.findall(r"(省|市|区|县|路|街|号|室)", business_scope)) >= 3:
            business_scope = ""
        else:
            address = ""
    return {
        "company_name": _pick_first_nonempty(
            _extract_labeled_field_final(text, ["\u540d\u79f0", "\u4f01\u4e1a\u540d\u79f0", "\u516c\u53f8\u540d\u79f0"], stop_labels, max_length=180),
            _find_after_labels(text, ("\u540d\u79f0", "\u4f01\u4e1a\u540d\u79f0", "\u516c\u53f8\u540d\u79f0")),
        ),
        "credit_code": _pick_first_nonempty(
            _find_first_match(text, UNIFIED_CODE_PATTERN),
            _extract_labeled_field_final(text, ["\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801", "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801"], stop_labels, max_length=64),
        ),
        "legal_person": _extract_labeled_field_final(text, ["\u6cd5\u5b9a\u4ee3\u8868\u4eba", "\u6cd5\u4eba", "\u8d1f\u8d23\u4eba"], stop_labels, max_length=60),
        "registered_capital": _extract_registered_capital_final(text),
        "establish_date": _pick_first_nonempty(
            _extract_labeled_field_final(text, ["\u6210\u7acb\u65e5\u671f", "\u6ce8\u518c\u65e5\u671f"], stop_labels, max_length=80),
            _find_first_date(text),
        ),
        "business_scope": business_scope,
        "address": address,
        "company_type": _extract_labeled_field_final(text, ["\u7c7b\u578b", "\u4e3b\u4f53\u7c7b\u578b"], ["\u6cd5\u5b9a\u4ee3\u8868\u4eba", "\u6ce8\u518c\u8d44\u672c", "\u6210\u7acb\u65e5\u671f", "\u7ecf\u8425\u8303\u56f4"], max_length=80),
    }


def extract_account_license(text: str) -> dict[str, Any]:
    stop_labels = [
        "\u5f00\u6237\u94f6\u884c",
        "\u5f00\u6237\u884c",
        "\u5f00\u6237\u673a\u6784",
        "\u5f00\u6237\u94f6\u884c\u673a\u6784",
        "\u6838\u51c6\u53f7",
        "\u8bb8\u53ef\u8bc1\u53f7",
        "\u8bb8\u53ef\u8bc1\u7f16\u53f7",
        "\u8d26\u6237\u6027\u8d28",
        "\u8d26\u6237\u7c7b\u578b",
        "\u5f00\u6237\u65e5\u671f",
        "\u5f00\u7acb\u65e5\u671f",
        "\u5b58\u6b3e\u4eba\u540d\u79f0",
        "\u8d26\u6237\u540d\u79f0",
        "\u6237\u540d",
        "\u5e01\u79cd",
    ]
    bank_full = _pick_first_nonempty(
        _extract_labeled_field_final(text, ["\u5f00\u6237\u94f6\u884c", "\u5f00\u6237\u884c"], stop_labels, max_length=180),
        _extract_labeled_field_final(text, ["\u5f00\u6237\u673a\u6784", "\u5f00\u6237\u94f6\u884c\u673a\u6784"], stop_labels, max_length=180),
    )
    bank_name, branch_from_name = split_bank_name_and_branch(bank_full)
    bank_branch = _pick_first_nonempty(
        branch_from_name,
        _extract_labeled_field_final(text, ["\u5f00\u6237\u673a\u6784", "\u5f00\u6237\u94f6\u884c\u673a\u6784", "\u5f00\u6237\u7f51\u70b9"], stop_labels, max_length=120),
    )
    return {
        "account_name": _extract_labeled_field_final(text, ["\u8d26\u6237\u540d\u79f0", "\u5b58\u6b3e\u4eba\u540d\u79f0", "\u6237\u540d"], stop_labels, max_length=120),
        "account_number": _extract_account_number_final(text),
        "bank_name": bank_name,
        "bank_branch": "" if bank_branch == bank_name else bank_branch,
        "license_number": _extract_labeled_field_final(text, ["\u6838\u51c6\u53f7", "\u8bb8\u53ef\u8bc1\u53f7", "\u8bb8\u53ef\u8bc1\u7f16\u53f7"], stop_labels, max_length=80),
        "account_type": _extract_labeled_field_final(text, ["\u8d26\u6237\u6027\u8d28", "\u8d26\u6237\u7c7b\u578b"], stop_labels, max_length=80),
        "open_date": _pick_first_nonempty(
            _extract_labeled_field_final(text, ["\u5f00\u6237\u65e5\u671f", "\u5f00\u7acb\u65e5\u671f"], stop_labels, max_length=60),
            _find_first_date(text),
        ),
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


def _label_value_cn(text: str, labels: list[str], stop_labels: list[str], *, max_length: int = 240, allow_multiline: bool = False) -> str:
    source = text or ""
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*", re.MULTILINE)
        match = pattern.search(source)
        if not match:
            continue
        candidate = source[match.end() : match.end() + max_length]
        stop_indexes: list[int] = []
        for stop_label in stop_labels:
            stop_match = re.search(rf"{re.escape(stop_label)}\s*[:：]?", candidate)
            if stop_match:
                stop_indexes.append(stop_match.start())
        if stop_indexes:
            candidate = candidate[: min(stop_indexes)]
        if not allow_multiline:
            candidate = candidate.splitlines()[0]
        cleaned = re.sub(r"\s+", " ", normalize_text(candidate)).strip("：:;；，,。 ")
        if cleaned:
            return cleaned
    return ""


def _registered_capital_cn(text: str) -> str:
    for label in ("\u6ce8\u518c\u8d44\u672c", "\u6ce8\u518c\u8d44\u91d1"):
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*((?:\u4eba\u6c11\u5e01)?\s*[0-9,]+(?:\.\d+)?\s*(?:\u4e07\u5143|\u4e07\u4eba\u6c11\u5e01|\u5143|\u4ebf\u5143|\u4e07\u7f8e\u5143|\u4ebf\u7f8e\u5143)?)")
        match = pattern.search(text or "")
        if match:
            return re.sub(r"\s+", "", match.group(1))
    return ""


def clean_business_scope(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    for marker in (
        "\u4f4f\u6240",
        "\u5730\u5740",
        "\u7c7b\u578b",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba",
        "\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u6210\u7acb\u65e5\u671f",
    ):
        idx = cleaned.find(marker)
        if idx > 0:
            cleaned = cleaned[:idx].strip("：:;；，,。 ")
    return cleaned


def clean_address(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    for marker in (
        "\u7ecf\u8425\u8303\u56f4",
        "\u7c7b\u578b",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba",
        "\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u6210\u7acb\u65e5\u671f",
    ):
        idx = cleaned.find(marker)
        if idx > 0:
            cleaned = cleaned[:idx].strip("：:;；，,。 ")
    return cleaned


def _remove_business_license_bottom_noise(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    for marker in (
        "\u767b\u8bb0\u673a\u5173",
        "\u767b\u8bb0\u673a\u6784",
        "\u53d1\u7167\u673a\u5173",
        "\u5e02\u573a\u76d1\u7763\u7ba1\u7406\u5c40",
        "\u884c\u653f\u5ba1\u6279\u5c40",
        "\u5de5\u5546\u884c\u653f\u7ba1\u7406\u5c40",
        "\u56fd\u5bb6\u4f01\u4e1a\u4fe1\u7528\u4fe1\u606f\u516c\u793a\u7cfb\u7edf",
        "\u56fd\u5bb6\u5e02\u573a\u76d1\u7763\u7ba1\u7406\u603b\u5c40\u76d1\u5236",
        "http",
        "www",
    ):
        idx = cleaned.find(marker)
        if idx > 0:
            if marker in ("\u5e02\u573a\u76d1\u7763\u7ba1\u7406\u5c40", "\u884c\u653f\u5ba1\u6279\u5c40", "\u5de5\u5546\u884c\u653f\u7ba1\u7406\u5c40"):
                sentence_boundary = max(cleaned.rfind(boundary, 0, idx) for boundary in ("。", "；", ";"))
                if sentence_boundary >= 0:
                    cleaned = cleaned[: sentence_boundary + 1].strip("：:;；，,。 ")
                    continue
            cleaned = cleaned[:idx].strip("：:;；，,。 ")
    return cleaned


def _looks_like_address_tail(value: str) -> bool:
    cleaned = re.sub(r"\s+", "", normalize_text(value))
    if not cleaned or len(cleaned) > 80:
        return False
    if any(marker in cleaned for marker in ("\u4e00\u822c\u9879\u76ee", "\u8bb8\u53ef\u9879\u76ee", "\u6280\u672f\u670d\u52a1", "\u6280\u672f\u5f00\u53d1")):
        return False
    return bool(
        re.search(
            r"(?:^\u53f7|^\u5ba4|^\u697c|^\u5e62|^\u680b|^\u5355\u5143|[0-9\uff10-\uff19\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341]+(?:\u53f7|\u5ba4|\u697c|\u5e62|\u680b|\u5355\u5143)|[A-Za-z\uff21-\uff3a]\u533a)",
            cleaned,
        )
    )


def _split_address_tail_from_scope(raw_scope: str) -> tuple[str, str]:
    cleaned = _remove_business_license_bottom_noise(raw_scope)
    if not cleaned:
        return "", ""
    marker_positions = [
        idx
        for marker in ("\u4e00\u822c\u9879\u76ee", "\u8bb8\u53ef\u9879\u76ee", "\u7ecf\u8425\u9879\u76ee")
        for idx in [cleaned.find(marker)]
        if idx > 0
    ]
    if not marker_positions:
        return "", cleaned
    start = min(marker_positions)
    prefix = cleaned[:start].strip("：:;；，,。 ")
    scope = cleaned[start:].strip("：:;；，,。 ")
    if _looks_like_address_tail(prefix):
        return prefix, scope
    return "", cleaned


def _extract_business_license_address_and_scope(text: str) -> tuple[str, str]:
    raw_address = _label_value_cn(
        text,
        ["\u4f4f\u6240", "\u5730\u5740", "\u8425\u4e1a\u573a\u6240", "\u7ecf\u8425\u573a\u6240"],
        [
            "\u7c7b\u578b",
            "\u6cd5\u5b9a\u4ee3\u8868\u4eba",
            "\u6ce8\u518c\u8d44\u672c",
            "\u6210\u7acb\u65e5\u671f",
            "\u7ecf\u8425\u8303\u56f4",
            "\u767b\u8bb0\u673a\u5173",
            "\u767b\u8bb0\u673a\u6784",
            "\u53d1\u7167\u673a\u5173",
        ],
        max_length=360,
        allow_multiline=True,
    )
    raw_scope = _label_value_cn(
        text,
        ["\u7ecf\u8425\u8303\u56f4", "\u7ecf\u8425\u9879\u76ee", "\u4e00\u822c\u9879\u76ee", "\u8bb8\u53ef\u9879\u76ee"],
        [
            "\u767b\u8bb0\u673a\u5173",
            "\u767b\u8bb0\u673a\u6784",
            "\u53d1\u7167\u673a\u5173",
            "\u56fd\u5bb6\u4f01\u4e1a\u4fe1\u7528\u4fe1\u606f\u516c\u793a\u7cfb\u7edf",
            "\u56fd\u5bb6\u5e02\u573a\u76d1\u7763\u7ba1\u7406\u603b\u5c40\u76d1\u5236",
        ],
        max_length=900,
        allow_multiline=True,
    )
    address_tail, scope_body = _split_address_tail_from_scope(raw_scope)
    address = clean_address(_remove_business_license_bottom_noise(raw_address))
    if address_tail and address_tail not in address:
        address = clean_address(f"{address}{address_tail}")
    business_scope = clean_business_scope(_remove_business_license_bottom_noise(scope_body or raw_scope))
    return address, business_scope


def _clean_registration_authority(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    cleaned = re.sub(r"(?:19|20)\d{2}\s*\u5e74\s*\d{1,2}\s*\u6708\s*\d{1,2}\s*\u65e5", "", cleaned).strip("：:;；，,。 ")
    for marker in ("\u767b\u8bb0\u673a\u5173", "\u767b\u8bb0\u673a\u6784", "\u53d1\u7167\u673a\u5173"):
        cleaned = cleaned.replace(marker, "").strip("：:;；，,。 ")
    if not cleaned or re.fullmatch(r"[\d\s./\-\u5e74\u6708\u65e5]+", cleaned):
        return ""
    if any(noise in cleaned for noise in ("\u56fd\u5bb6\u4f01\u4e1a\u4fe1\u7528\u4fe1\u606f\u516c\u793a\u7cfb\u7edf", "\u56fd\u5bb6\u5e02\u573a\u76d1\u7763\u7ba1\u7406\u603b\u5c40\u76d1\u5236", "http", "www")):
        return ""
    return cleaned


_BUSINESS_LICENSE_AUTHORITY_SUFFIXES = (
    "\u5e02\u573a\u76d1\u7763\u7ba1\u7406\u5c40",
    "\u884c\u653f\u5ba1\u6279\u5c40",
    "\u5de5\u5546\u884c\u653f\u7ba1\u7406\u5c40",
    "\u5e02\u573a\u76d1\u7763\u7ba1\u7406\u6240",
)


def _extract_authority_candidates_from_text(value: str) -> list[str]:
    source = normalize_text(value)
    if not source:
        return []
    source = re.sub(r"(?:19|20)\d{2}\s*\u5e74\s*\d{1,2}\s*\u6708\s*\d{1,2}\s*\u65e5", "", source)
    source = re.sub(r"\s+", "", source)
    for marker in ("\u767b\u8bb0\u673a\u5173", "\u767b\u8bb0\u673a\u6784", "\u53d1\u7167\u673a\u5173", "\u5370\u7ae0"):
        source = source.replace(marker, "")
    if any(noise in source for noise in ("\u56fd\u5bb6\u4f01\u4e1a\u4fe1\u7528\u4fe1\u606f\u516c\u793a\u7cfb\u7edf", "\u56fd\u5bb6\u5e02\u573a\u76d1\u7763\u7ba1\u7406\u603b\u5c40\u76d1\u5236", "http", "www")):
        return []

    suffix_pattern = "|".join(re.escape(suffix) for suffix in _BUSINESS_LICENSE_AUTHORITY_SUFFIXES)
    candidates: list[str] = []
    for match in re.finditer(rf"([\u4e00-\u9fff]{{0,40}}(?:{suffix_pattern}))", source):
        candidate = match.group(1)
        starts = [
            candidate.rfind(prefix)
            for prefix in (
                "\u4e0a\u6d77\u5e02",
                "\u5317\u4eac\u5e02",
                "\u5929\u6d25\u5e02",
                "\u91cd\u5e86\u5e02",
                "\u5e7f\u4e1c\u7701",
                "\u6c5f\u82cf\u7701",
                "\u6d59\u6c5f\u7701",
                "\u5c71\u4e1c\u7701",
                "\u56db\u5ddd\u7701",
                "\u6cb3\u5357\u7701",
                "\u6cb3\u5317\u7701",
                "\u6e56\u5357\u7701",
                "\u6e56\u5317\u7701",
                "\u5b89\u5fbd\u7701",
                "\u798f\u5efa\u7701",
            )
        ]
        start = max(starts)
        if start > 0:
            candidate = candidate[start:]
        cleaned = _clean_registration_authority(candidate)
        if cleaned:
            candidates.append(cleaned)
    return candidates


def _pick_registration_authority_candidate(candidates: list[tuple[str, int]]) -> str:
    cleaned: list[tuple[str, int]] = []
    for candidate, score in candidates:
        value = _clean_registration_authority(candidate)
        if not value:
            continue
        if not any(suffix in value for suffix in _BUSINESS_LICENSE_AUTHORITY_SUFFIXES):
            continue
        cleaned.append((value, score))
    if not cleaned:
        return ""
    return sorted(cleaned, key=lambda item: (item[1] + len(item[0]), len(item[0])), reverse=True)[0][0]


def _extract_registration_authority_cn(text: str) -> str:
    source = text or ""
    explicit_patterns = (
        re.compile(r"\u767b\u8bb0\u673a\u5173\s*[:：]?\s*([^\n]+)"),
        re.compile(r"\u767b\u8bb0\u673a\u6784\s*[:：]?\s*([^\n]+)"),
        re.compile(r"\u53d1\u7167\u673a\u5173\s*[:：]?\s*([^\n]+)"),
    )
    for pattern in explicit_patterns:
        match = pattern.search(source)
        if match:
            cleaned = _clean_registration_authority(match.group(1))
            if cleaned:
                logger.info("[business_license] registration_authority candidate=%s", cleaned)
                return cleaned

    labeled = _label_value_cn(
        source,
        ["\u767b\u8bb0\u673a\u5173", "\u767b\u8bb0\u673a\u6784", "\u53d1\u7167\u673a\u5173"],
        [
            "\u6210\u7acb\u65e5\u671f",
            "\u7b7e\u53d1\u65e5\u671f",
            "\u53d1\u7167\u65e5\u671f",
            "\u56fd\u5bb6\u4f01\u4e1a\u4fe1\u7528\u4fe1\u606f\u516c\u793a\u7cfb\u7edf",
            "\u56fd\u5bb6\u5e02\u573a\u76d1\u7763\u7ba1\u7406\u603b\u5c40\u76d1\u5236",
        ],
        max_length=140,
        allow_multiline=False,
    )
    cleaned = _clean_registration_authority(labeled)
    if cleaned:
        logger.info("[business_license] registration_authority candidate=%s", cleaned)
        return cleaned

    candidates: list[tuple[str, int]] = []
    lines = [line.strip() for line in source.splitlines() if line.strip()]
    bottom_lines = lines[-20:]
    registration_date = _extract_registration_date_cn(source)
    for idx, line in enumerate(bottom_lines):
        window = "\n".join(bottom_lines[max(0, idx - 1) : min(len(bottom_lines), idx + 2)])
        score = 20
        if registration_date and registration_date in window:
            score += 40
        score += idx
        candidates.extend((candidate, score) for candidate in _extract_authority_candidates_from_text(window))

    bottom_text = "\n".join(bottom_lines)
    candidates.extend((candidate, 10) for candidate in _extract_authority_candidates_from_text(bottom_text))
    candidates.extend((candidate, 1) for candidate in _extract_authority_candidates_from_text(source))
    picked = _pick_registration_authority_candidate(candidates)
    if picked:
        logger.info("[business_license] registration_authority candidate=%s", picked)
    return picked


def _extract_registration_date_cn(text: str) -> str:
    source = text or ""
    date_pattern = r"(?:19|20)\d{2}\s*\u5e74\s*\d{1,2}\s*\u6708\s*\d{1,2}\s*\u65e5"
    authority_markers = (
        "\u767b\u8bb0\u673a\u5173",
        "\u767b\u8bb0\u673a\u6784",
        "\u53d1\u7167\u673a\u5173",
        "\u5e02\u573a\u76d1\u7763\u7ba1\u7406\u5c40",
        "\u884c\u653f\u5ba1\u6279\u5c40",
        "\u5de5\u5546\u884c\u653f\u7ba1\u7406\u5c40",
        "\u5e02\u573a\u76d1\u7763\u7ba1\u7406\u6240",
    )

    for marker in authority_markers:
        idx = source.find(marker)
        if idx < 0:
            continue
        window = source[idx : idx + 240]
        matches = re.findall(date_pattern, window)
        if matches:
            return re.sub(r"\s+", "", matches[-1])

    lines = [line.strip() for line in source.splitlines() if line.strip()]
    bottom_text = "\n".join(lines[-8:])
    bottom_dates = re.findall(date_pattern, bottom_text)
    if not bottom_dates:
        return ""

    establish_date = _label_value_cn(
        source,
        ["\u6210\u7acb\u65e5\u671f", "\u6ce8\u518c\u65e5\u671f"],
        ["\u4f4f\u6240", "\u5730\u5740", "\u7ecf\u8425\u8303\u56f4", "\u767b\u8bb0\u673a\u5173"],
        max_length=80,
    )
    normalized_establish = re.sub(r"\s+", "", establish_date)
    for candidate in reversed(bottom_dates):
        cleaned = re.sub(r"\s+", "", candidate)
        if cleaned and cleaned != normalized_establish:
            return cleaned
    return ""


def split_bank_name_and_branch(value: str) -> tuple[str, str]:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    if not cleaned:
        return "", ""
    match = re.search(r"(.+?(?:\u94f6\u884c|\u4fe1\u7528\u793e|\u519c\u5546\u884c|\u519c\u6751\u5546\u4e1a\u94f6\u884c|\u80a1\u4efd\u6709\u9650\u516c\u53f8))(.+?(?:\u652f\u884c|\u5206\u884c|\u8425\u4e1a\u90e8|\u8425\u4e1a\u5ba4|\u5206\u7406\u5904))", cleaned)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return cleaned, ""


def extract_currency(value: str) -> str:
    text = normalize_text(value).upper()
    if not text:
        return ""
    if "\u4eba\u6c11\u5e01" in text or "CNY" in text or "RMB" in text:
        return "\u4eba\u6c11\u5e01"
    for currency in ("USD", "HKD", "EUR"):
        if currency in text:
            return currency
    return ""


def extract_date_range(text: str) -> tuple[str, str]:
    source = text or ""
    patterns = (
        re.compile(r"((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)\s*(?:--|-|至|~)\s*((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)"),
        re.compile(r"(?:\u8bb0\u8d26\u65e5\u671f|\u67e5\u8be2\u65e5\u671f\u8303\u56f4|\u8d77\u6b62\u65e5\u671f)[:：]?\s*((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)\s*(?:\u81f3|-|--|~)\s*((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)"),
    )
    for pattern in patterns:
        match = pattern.search(source)
        if match:
            return _normalize_date(match.group(1)), _normalize_date(match.group(2))
    return "", ""


def extract_business_license(text: str) -> dict[str, Any]:
    stop_labels = [
        "\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba",
        "\u6cd5\u4eba",
        "\u8d1f\u8d23\u4eba",
        "\u6ce8\u518c\u8d44\u672c",
        "\u6210\u7acb\u65e5\u671f",
        "\u4f4f\u6240",
        "\u5730\u5740",
        "\u7ecf\u8425\u8303\u56f4",
        "\u7c7b\u578b",
        "\u767b\u8bb0\u673a\u5173",
        "\u767b\u8bb0\u673a\u6784",
        "\u53d1\u7167\u673a\u5173",
    ]
    address, business_scope = _extract_business_license_address_and_scope(text)
    registration_authority = _extract_registration_authority_cn(text)
    registration_date = _extract_registration_date_cn(text)
    return {
        "company_name": _pick_first_nonempty(
            _label_value_cn(text, ["\u540d\u79f0", "\u4f01\u4e1a\u540d\u79f0", "\u516c\u53f8\u540d\u79f0", "\u5e02\u573a\u4e3b\u4f53\u540d\u79f0"], stop_labels, max_length=180),
            _find_after_labels(text, ("\u540d\u79f0", "\u4f01\u4e1a\u540d\u79f0", "\u516c\u53f8\u540d\u79f0")),
        ),
        "credit_code": _pick_first_nonempty(
            _find_first_match(text, UNIFIED_CODE_PATTERN),
            _label_value_cn(text, ["\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801", "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801"], stop_labels, max_length=60),
        ),
        "legal_person": _label_value_cn(text, ["\u6cd5\u5b9a\u4ee3\u8868\u4eba", "\u6cd5\u4eba", "\u8d1f\u8d23\u4eba"], stop_labels, max_length=60),
        "registered_capital": _registered_capital_cn(text),
        "establish_date": _pick_first_nonempty(
            _label_value_cn(text, ["\u6210\u7acb\u65e5\u671f", "\u6ce8\u518c\u65e5\u671f"], stop_labels, max_length=80),
            _find_first_date(text),
        ),
        "business_scope": business_scope if len(business_scope) >= 4 else "",
        "address": address if len(address) >= 4 else "",
        "company_type": _label_value_cn(text, ["\u7c7b\u578b", "\u4e3b\u4f53\u7c7b\u578b"], ["\u6cd5\u5b9a\u4ee3\u8868\u4eba", "\u6ce8\u518c\u8d44\u672c", "\u6210\u7acb\u65e5\u671f", "\u7ecf\u8425\u8303\u56f4"], max_length=80),
        "registration_authority": registration_authority,
        "registration_date": registration_date,
    }


def extract_account_license(text: str) -> dict[str, Any]:
    stop_labels = [
        "\u5f00\u6237\u94f6\u884c",
        "\u5f00\u6237\u884c",
        "\u5f00\u6237\u673a\u6784",
        "\u5f00\u6237\u94f6\u884c\u673a\u6784",
        "\u6838\u51c6\u53f7",
        "\u8bb8\u53ef\u8bc1\u53f7",
        "\u8d26\u6237\u6027\u8d28",
        "\u8d26\u6237\u7c7b\u578b",
        "\u5f00\u6237\u65e5\u671f",
        "\u5f00\u7acb\u65e5\u671f",
        "\u5b58\u6b3e\u4eba\u540d\u79f0",
        "\u8d26\u6237\u540d\u79f0",
        "\u6237\u540d",
        "\u5e01\u79cd",
    ]
    bank_full = _pick_first_nonempty(
        _label_value_cn(text, ["\u5f00\u6237\u94f6\u884c", "\u5f00\u6237\u884c"], stop_labels, max_length=180),
        _label_value_cn(text, ["\u5f00\u6237\u673a\u6784", "\u5f00\u6237\u94f6\u884c\u673a\u6784"], stop_labels, max_length=180),
    )
    bank_name, bank_branch = split_bank_name_and_branch(bank_full)
    return {
        "account_name": _label_value_cn(text, ["\u8d26\u6237\u540d\u79f0", "\u5b58\u6b3e\u4eba\u540d\u79f0", "\u6237\u540d"], stop_labels, max_length=120),
        "account_number": _v2_only_digits(
            _pick_first_nonempty(
                _label_value_cn(text, ["\u8d26\u53f7", "\u94f6\u884c\u8d26\u53f7", "\u8d26\u6237\u53f7\u7801"], stop_labels, max_length=120),
                _label_value_cn(text, ["\u5361\u53f7"], stop_labels, max_length=120),
            )
        ) or _v2_extract_account_number(text),
        "bank_name": bank_name,
        "bank_branch": _pick_first_nonempty(bank_branch, _label_value_cn(text, ["\u5f00\u6237\u673a\u6784", "\u5f00\u6237\u94f6\u884c\u673a\u6784", "\u5f00\u6237\u7f51\u70b9"], stop_labels, max_length=120)),
        "license_number": _label_value_cn(text, ["\u6838\u51c6\u53f7", "\u8bb8\u53ef\u8bc1\u53f7", "\u8bb8\u53ef\u8bc1\u7f16\u53f7"], stop_labels, max_length=80),
        "account_type": _label_value_cn(text, ["\u8d26\u6237\u6027\u8d28", "\u8d26\u6237\u7c7b\u578b"], stop_labels, max_length=80),
        "open_date": _pick_first_nonempty(_label_value_cn(text, ["\u5f00\u6237\u65e5\u671f", "\u5f00\u7acb\u65e5\u671f"], stop_labels, max_length=60), _find_first_date(text)),
    }


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
        content = extract_bank_statement_from_rows(rows, text_content) if rows else extract_bank_statement_pdf_fields(text_content)
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


def _extract_label_value(
    text: str,
    labels: tuple[str, ...],
    *,
    stop_labels: tuple[str, ...] = (),
    allow_multiline: bool = False,
    max_length: int = 200,
) -> str:
    source = text or ""
    flags = re.MULTILINE | (re.DOTALL if allow_multiline else 0)
    stop_pattern = "|".join(re.escape(item) for item in stop_labels if item)

    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*", flags)
        match = pattern.search(source)
        if not match:
            continue
        start = match.end()
        candidate = source[start : start + max_length]
        if stop_pattern:
            stop_match = re.search(rf"(?=\b(?:{stop_pattern})\b\s*[:：]?)", candidate, flags)
            if stop_match:
                candidate = candidate[: stop_match.start()]
        candidate = candidate.split("\n")[0] if not allow_multiline else candidate
        cleaned = _clean_field_value(candidate)
        if cleaned:
            return cleaned
    return ""


def extract_labeled_field(text: str, labels: list[str], stop_labels: list[str]) -> str:
    """Extract `label: value` text and stop at the next recognized label."""
    return _extract_label_value(text, tuple(labels), stop_labels=tuple(stop_labels), allow_multiline=True)


def _clean_field_value(value: str) -> str:
    cleaned = normalize_text(value)
    if not cleaned:
        return ""
    cleaned = re.sub(r"[|｜]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip("：:;；，,。 ")
    return cleaned


def clean_business_scope(value: str) -> str:
    return _clean_scope_or_address(value, stop_words=("住所", "地址", "类型", "法定代表人", "统一社会信用代码", "成立日期"))


def clean_address(value: str) -> str:
    return _clean_scope_or_address(value, stop_words=("经营范围", "类型", "法定代表人", "统一社会信用代码", "成立日期"))


def _clean_scope_or_address(value: str, *, stop_words: tuple[str, ...]) -> str:
    cleaned = _clean_field_value(value)
    for stop_word in stop_words:
        idx = cleaned.find(stop_word)
        if idx > 0:
            cleaned = cleaned[:idx].strip("：:;；，,。 ")
    return cleaned


def _extract_branch_from_bank_name(bank_name: str) -> tuple[str, str]:
    cleaned = _clean_field_value(bank_name)
    if not cleaned:
        return "", ""
    branch_match = re.search(r"(.+?银行)(.+?(?:支行|分行|营业部|营业室|分理处))", cleaned)
    if branch_match:
        return branch_match.group(1).strip(), branch_match.group(2).strip()
    return cleaned, ""


def split_bank_name_and_branch(value: str) -> tuple[str, str]:
    """Split bank_name / bank_branch where possible."""
    return _extract_branch_from_bank_name(value)


def _pick_first_nonempty(*values: str) -> str:
    for value in values:
        cleaned = _clean_field_value(value)
        if cleaned:
            return cleaned
    return ""


def _extract_account_number_from_text(text: str) -> str:
    candidates = [
        _extract_label_value(text, ("账号", "账户号码", "银行账号", "结算账户"), stop_labels=("开户行", "开户银行", "币种", "账户名称")),
        _extract_label_value(text, ("卡号",), stop_labels=("开户行", "开户银行", "币种")),
    ]
    for candidate in candidates:
        normalized = re.sub(r"\s+", "", candidate)
        normalized = normalized.replace("账号", "").replace("账户", "")
        normalized = normalized.strip("：:")
        if re.search(r"\d{8,}", normalized):
            return re.search(r"\d{8,}", normalized).group(0)
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
    stop_labels = (
        "统一社会信用代码",
        "法定代表人",
        "注册资本",
        "成立日期",
        "住所",
        "地址",
        "经营范围",
        "类型",
    )
    return {
        "company_name": _pick_first_nonempty(
            _extract_label_value(text, ("名称", "企业名称", "公司名称", "市场主体名称"), stop_labels=stop_labels),
            _find_after_labels(text, ("名称", "企业名称", "公司名称")),
        ),
        "credit_code": _find_first_match(text, UNIFIED_CODE_PATTERN),
        "legal_person": _extract_label_value(text, ("法定代表人", "法人", "负责人"), stop_labels=stop_labels),
        "registered_capital": extract_company_articles_registered_capital(text),
        "establish_date": _pick_first_nonempty(
            _extract_label_value(text, ("成立日期", "注册日期", "营业期限自"), stop_labels=stop_labels),
            _find_first_date(text),
        ),
        "business_scope": _clean_scope_or_address(
            _extract_label_value(text, ("经营范围",), stop_labels=("住所", "地址", "类型", "法定代表人"), allow_multiline=True, max_length=600),
            stop_words=("住所", "地址", "类型", "法定代表人"),
        ),
        "address": _clean_scope_or_address(
            _extract_label_value(text, ("住所", "营业场所", "地址"), stop_labels=("经营范围", "类型", "法定代表人"), allow_multiline=True, max_length=240),
            stop_words=("经营范围", "类型", "法定代表人"),
        ),
        "company_type": _extract_label_value(text, ("类型", "主体类型"), stop_labels=("法定代表人", "注册资本", "成立日期", "经营范围")),
    }


def extract_account_license(text: str) -> dict[str, Any]:
    stop_labels = (
        "开户银行",
        "开户行",
        "开户银行机构",
        "核准号",
        "许可证编号",
        "账户性质",
        "账户类型",
        "开户日期",
        "开立日期",
        "存款人名称",
        "账户名称",
        "户名",
    )
    bank_full = _pick_first_nonempty(
        _extract_label_value(text, ("开户银行", "开户行", "银行名称"), stop_labels=stop_labels),
        _extract_label_value(text, ("开户银行机构", "开户网点"), stop_labels=stop_labels),
    )
    bank_name, bank_branch = _extract_branch_from_bank_name(bank_full)
    return {
        "account_name": _extract_label_value(text, ("存款人名称", "账户名称", "户名"), stop_labels=stop_labels),
        "account_number": _extract_account_number_from_text(text),
        "bank_name": bank_name,
        "bank_branch": _pick_first_nonempty(
            bank_branch,
            _extract_label_value(text, ("开户银行机构", "开户网点", "开户银行支行"), stop_labels=stop_labels),
        ),
        "license_number": _extract_label_value(text, ("核准号", "许可证编号", "许可证号"), stop_labels=stop_labels),
        "account_type": _extract_label_value(text, ("账户性质", "账户类型"), stop_labels=stop_labels),
        "open_date": _pick_first_nonempty(
            _extract_label_value(text, ("开户日期", "开立日期"), stop_labels=stop_labels),
            _find_first_date(text),
        ),
    }


def extract_company_articles(text: str, ai_service: Any | None = None) -> dict[str, Any]:
    shareholder_sentences = _extract_keyword_sentences(text, ("股东", "出资", "持股"))
    summary = _build_summary(text, shareholder_sentences, ai_service=ai_service)
    return {
        "company_name": _find_after_labels(text, ("公司名称", "名称")),
        "registered_capital": extract_company_articles_registered_capital(text),
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


def extract_bank_statement_pdf(raw_text: str) -> dict[str, Any]:
    account_name = _extract_label_value(
        raw_text,
        ("户名", "账户名称", "客户名称", "单位名称"),
        stop_labels=("账号", "账户号码", "卡号", "开户行", "开户银行", "币种"),
        max_length=120,
    )
    account_number = _extract_account_number_from_text(raw_text)
    bank_full = _pick_first_nonempty(
        _extract_label_value(
            raw_text,
            ("开户行", "开户银行", "银行名称"),
            stop_labels=("币种", "账号", "账户号码", "户名", "账户名称"),
            max_length=160,
        ),
        _extract_label_value(
            raw_text,
            ("所属银行",),
            stop_labels=("币种", "账号", "账户号码", "户名", "账户名称"),
            max_length=160,
        ),
    )
    bank_name, bank_branch = _extract_branch_from_bank_name(bank_full)
    currency = _extract_label_value(
        raw_text,
        ("币种",),
        stop_labels=("账号", "账户号码", "开户行", "开户银行", "交易日期", "记账日期"),
        max_length=20,
    )
    start_date = _pick_first_nonempty(
        _extract_label_value(raw_text, ("起始日期", "开始日期", "账单起始日", "自"), stop_labels=("截止日期", "结束日期", "至")),
        _find_first_date(raw_text),
    )
    end_date = _pick_first_nonempty(
        _extract_label_value(raw_text, ("截止日期", "结束日期", "账单截止日", "至"), stop_labels=("期初余额", "期末余额", "余额")),
        _find_last_date(raw_text),
    )
    opening_balance = _money_after_labels(raw_text, ("期初余额", "上期余额", "起始余额"))
    closing_balance = _money_after_labels(raw_text, ("期末余额", "当前余额", "账户余额"))

    return {
        "account_name": account_name,
        "account_number": account_number,
        "bank_name": bank_name,
        "currency": currency,
        "start_date": start_date,
        "end_date": end_date,
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
        "total_income": "",
        "total_expense": "",
        "monthly_avg_income": "",
        "monthly_avg_expense": "",
        "transaction_count": "",
        "top_inflows": [],
        "top_outflows": [],
        "bank_branch": bank_branch,
    }


def _v2_only_digits(value: str) -> str:
    return only_digits(value)


def _v2_extract_currency(value: str) -> str:
    text = normalize_text(value).upper()
    if not text:
        return ""
    if "人民币" in text or "CNY" in text or "RMB" in text:
        return "人民币"
    for currency in ("USD", "HKD", "EUR"):
        if currency in text:
            return currency
    return ""


def _v2_extract_date_range(text: str) -> tuple[str, str]:
    source = text or ""
    patterns = (
        re.compile(r"((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)\s*(?:--|-|至|~)\s*((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)"),
        re.compile(r"(?:记账日期|查询日期范围|起止日期)[:：]?\s*((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)\s*(?:至|-|--|~)\s*((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)"),
    )
    for pattern in patterns:
        match = pattern.search(source)
        if match:
            return _normalize_date(match.group(1)), _normalize_date(match.group(2))
    return "", ""


def _v2_extract_labeled_field(text: str, labels: list[str], stop_labels: list[str], *, max_length: int = 240, allow_multiline: bool = False) -> str:
    source = text or ""
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*", re.MULTILINE)
        match = pattern.search(source)
        if not match:
            continue
        start = match.end()
        candidate = source[start : start + max_length]
        end_indexes = []
        for stop_label in stop_labels:
            stop_match = re.search(rf"{re.escape(stop_label)}\s*[:：]?", candidate)
            if stop_match:
                end_indexes.append(stop_match.start())
        if end_indexes:
            candidate = candidate[: min(end_indexes)]
        if not allow_multiline:
            candidate = candidate.splitlines()[0]
        cleaned = re.sub(r"\s+", " ", normalize_text(candidate)).strip("：:;；，,。 ")
        if cleaned:
            return cleaned
    return ""


def clean_business_scope(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    for marker in ("住所", "地址", "类型", "法定代表人", "统一社会信用代码", "成立日期"):
        idx = cleaned.find(marker)
        if idx > 0:
            cleaned = cleaned[:idx].strip("：:;；，,。 ")
    return cleaned


def clean_address(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    for marker in ("经营范围", "类型", "法定代表人", "统一社会信用代码", "成立日期"):
        idx = cleaned.find(marker)
        if idx > 0:
            cleaned = cleaned[:idx].strip("：:;；，,。 ")
    return cleaned


def split_bank_name_and_branch(value: str) -> tuple[str, str]:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    if not cleaned:
        return "", ""
    match = re.search(r"(.+?(?:银行|信用社|农商行|农村商业银行|股份有限公司))(.+?(?:支行|分行|营业部|营业室|分理处))", cleaned)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return cleaned, ""


def _v2_extract_registered_capital(text: str) -> str:
    for label in ("注册资本", "注册资金"):
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*((?:人民币)?\s*[0-9,]+(?:\.\d+)?\s*(?:万元|万人民币|元|亿元|万美元|亿美元)?)")
        match = pattern.search(text or "")
        if match:
            return re.sub(r"\s+", "", match.group(1))
    return ""


def extract_business_license(text: str) -> dict[str, Any]:
    stop_labels = ["统一社会信用代码", "社会信用代码", "法定代表人", "法人", "负责人", "注册资本", "成立日期", "住所", "地址", "经营范围", "类型"]
    scope_raw = _v2_extract_labeled_field(text, ["经营范围"], ["住所", "地址", "类型", "法定代表人"], max_length=800, allow_multiline=True)
    address_raw = _v2_extract_labeled_field(text, ["住所", "地址", "营业场所"], ["经营范围", "类型", "法定代表人"], max_length=260, allow_multiline=True)
    return {
        "company_name": _pick_first_nonempty(
            _v2_extract_labeled_field(text, ["名称", "企业名称", "公司名称", "市场主体名称"], stop_labels, max_length=180),
            _find_after_labels(text, ("名称", "企业名称", "公司名称")),
        ),
        "credit_code": _find_first_match(text, UNIFIED_CODE_PATTERN),
        "legal_person": _v2_extract_labeled_field(text, ["法定代表人", "法人", "负责人"], stop_labels, max_length=60),
        "registered_capital": _v2_extract_registered_capital(text),
        "establish_date": _pick_first_nonempty(
            _v2_extract_labeled_field(text, ["成立日期", "注册日期"], stop_labels, max_length=80),
            _find_first_date(text),
        ),
        "business_scope": clean_business_scope(scope_raw),
        "address": clean_address(address_raw),
        "company_type": _v2_extract_labeled_field(text, ["类型", "主体类型"], ["法定代表人", "注册资本", "成立日期", "经营范围"], max_length=80),
    }


def extract_account_license(text: str) -> dict[str, Any]:
    stop_labels = ["开户银行", "开户行", "开户机构", "核准号", "许可证号", "账户性质", "账户类型", "开户日期", "存款人名称", "账户名称", "户名", "币种"]
    bank_full = _pick_first_nonempty(
        _v2_extract_labeled_field(text, ["开户银行", "开户行"], stop_labels, max_length=180),
        _v2_extract_labeled_field(text, ["开户机构", "开户银行机构"], stop_labels, max_length=180),
    )
    bank_name, bank_branch = split_bank_name_and_branch(bank_full)
    return {
        "account_name": _v2_extract_labeled_field(text, ["账户名称", "存款人名称", "户名"], stop_labels, max_length=120),
        "account_number": _v2_extract_account_number(text),
        "bank_name": bank_name,
        "bank_branch": _pick_first_nonempty(bank_branch, _v2_extract_labeled_field(text, ["开户机构", "开户银行机构", "开户网点"], stop_labels, max_length=120)),
        "license_number": _v2_extract_labeled_field(text, ["核准号", "许可证号", "许可证编号"], stop_labels, max_length=80),
        "account_type": _v2_extract_labeled_field(text, ["账户性质", "账户类型"], stop_labels, max_length=80),
        "open_date": _pick_first_nonempty(_v2_extract_labeled_field(text, ["开户日期", "开立日期"], stop_labels, max_length=60), _find_first_date(text)),
    }


def _v2_extract_labeled_field(text: str, labels: list[str], stop_labels: list[str], *, max_length: int = 240, allow_multiline: bool = False) -> str:
    source = text or ""
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}\s*[:锛歖?\s*", re.MULTILINE)
        match = pattern.search(source)
        if not match:
            continue
        candidate = source[match.end() : match.end() + max_length]
        stop_indexes: list[int] = []
        for stop_label in stop_labels:
            stop_match = re.search(rf"{re.escape(stop_label)}\s*[:锛歖?", candidate)
            if stop_match:
                stop_indexes.append(stop_match.start())
        if stop_indexes:
            candidate = candidate[: min(stop_indexes)]
        candidate = str(candidate or "").strip()
        if not candidate:
            return ""
        if not allow_multiline:
            lines = [line.strip() for line in candidate.splitlines() if line.strip()]
            candidate = lines[0] if lines else ""
        candidate = str(candidate or "").strip()
        if not candidate:
            return ""
        cleaned = re.sub(r"\s+", " ", normalize_text(candidate)).strip("锛?;锛涳紝,銆?")
        return cleaned if cleaned else ""
    return ""


_FINAL_COLON_PATTERN = r"(?:\s*[:：]\s*|\s+)"


def _normalize_date(value: str) -> str:
    normalized = normalize_text(value)
    if not normalized:
        return ""
    normalized = (
        normalized.replace("\u5e74", "-")
        .replace("\u6708", "-")
        .replace("\u65e5", "")
        .replace("/", "-")
        .replace(".", "-")
        .replace("--", "-")
    )
    match = re.search(r"((?:19|20)\d{2})-(\d{1,2})-(\d{1,2})", normalized)
    if not match:
        return normalized
    return f"{match.group(1)}-{match.group(2).zfill(2)}-{match.group(3).zfill(2)}"


def _find_first_date(text: str) -> str:
    source = text or ""
    pattern = re.compile(r"(?:19|20)\d{2}[年./-]\d{1,2}[月./-]\d{1,2}日?")
    match = pattern.search(source)
    return _normalize_date(match.group(0)) if match else ""


def _find_last_date(text: str) -> str:
    source = text or ""
    matches = re.findall(r"(?:19|20)\d{2}[年./-]\d{1,2}[月./-]\d{1,2}日?", source)
    return _normalize_date(matches[-1]) if matches else ""


def _money_after_labels(text: str, labels: tuple[str, ...]) -> str:
    source = text or ""
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}{_FINAL_COLON_PATTERN}?([^\n\r ]{{0,8}})?([+-]?\d[\d,]*(?:\.\d+)?)")
        match = pattern.search(source)
        if match:
            return normalize_amount(match.group(2))
    return ""


def extract_labeled_field(text: str, labels: list[str], stop_labels: list[str]) -> str:
    return _extract_labeled_field_final(text, labels, stop_labels, allow_multiline=True)


def _extract_labeled_field_final(
    text: str,
    labels: list[str],
    stop_labels: list[str],
    *,
    max_length: int = 240,
    allow_multiline: bool = False,
) -> str:
    source = text or ""
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}{_FINAL_COLON_PATTERN}?")
        match = pattern.search(source)
        if not match:
            continue
        candidate = source[match.end() : match.end() + max_length]
        stop_indexes: list[int] = []
        for stop_label in stop_labels:
            stop_match = re.search(rf"{re.escape(stop_label)}{_FINAL_COLON_PATTERN}?", candidate)
            if stop_match:
                stop_indexes.append(stop_match.start())
        if stop_indexes:
            candidate = candidate[: min(stop_indexes)]
        if not allow_multiline:
            candidate = candidate.splitlines()[0]
        cleaned = _clean_extracted_field(candidate)
        if cleaned:
            return cleaned
    return ""


def _clean_extracted_field(value: str) -> str:
    cleaned = normalize_text(value)
    if not cleaned:
        return ""
    cleaned = re.sub(r"[\|\u00a0]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(
        r"^(?:账号|账户号码|账户名称|开户银行|开户行|开户机构|币种|户名|客户名称|单位名称|名称|地址|住所|经营范围|类型|法定代表人|法人|负责人)\s*[:：]?\s*",
        "",
        cleaned,
    )
    cleaned = cleaned.strip("：:;；，,。 ")
    return cleaned


def _extract_registered_capital_final(text: str) -> str:
    labels = ["注册资本", "注册资金"]
    stop_labels = ["成立日期", "住所", "地址", "经营范围", "类型", "法定代表人", "法人", "负责人"]
    candidate = _extract_labeled_field_final(text, labels, stop_labels, max_length=80)
    candidate = re.sub(r"\s+", "", candidate)
    if candidate:
        match = re.search(
            r"((?:人民币)?[0-9一二三四五六七八九十百千万亿零〇,\.]+(?:万?元(?:人民币)?|亿元|万元|元|万美元|万欧元|欧元))",
            candidate,
        )
        if match:
            return match.group(1)
    source = text or ""
    for label in labels:
        match = re.search(
            rf"{re.escape(label)}{_FINAL_COLON_PATTERN}?((?:人民币)?[0-9一二三四五六七八九十百千万亿零〇,\.]+(?:万?元(?:人民币)?|亿元|万元|元|万美元|万欧元|欧元))",
            source,
        )
        if match:
            return re.sub(r"\s+", "", match.group(1))
    return ""


def clean_business_scope(value: str) -> str:
    cleaned = _clean_extracted_field(value)
    if not cleaned:
        return ""
    for marker in (
        "\u4f4f\u6240",
        "\u5730\u5740",
        "\u7c7b\u578b",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba",
        "\u6cd5\u4eba",
        "\u8d1f\u8d23\u4eba",
        "\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u6210\u7acb\u65e5\u671f",
        "\u6ce8\u518c\u8d44\u672c",
    ):
        idx = cleaned.find(marker)
        if idx > 0:
            cleaned = cleaned[:idx]
    if re.search(r"(省|市|区|县|路|街|号|室).*(经营范围)", cleaned):
        cleaned = re.split(r"\u7ecf\u8425\u8303\u56f4", cleaned, maxsplit=1)[-1]
    cleaned = cleaned.strip("：:;；，,。 ")
    if len(cleaned) < 4:
        return ""
    address_like_hits = len(re.findall(r"(省|市|区|县|路|街|号|室)", cleaned))
    if address_like_hits >= 3 and "；" not in cleaned and ";" not in cleaned and "、" not in cleaned:
        return ""
    return cleaned


def clean_address(value: str) -> str:
    cleaned = _clean_extracted_field(value)
    if not cleaned:
        return ""
    for marker in (
        "\u7ecf\u8425\u8303\u56f4",
        "\u7c7b\u578b",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba",
        "\u6cd5\u4eba",
        "\u8d1f\u8d23\u4eba",
        "\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u6210\u7acb\u65e5\u671f",
        "\u6ce8\u518c\u8d44\u672c",
    ):
        idx = cleaned.find(marker)
        if idx > 0:
            cleaned = cleaned[:idx]
    cleaned = cleaned.strip("：:;；，,。 ")
    if len(cleaned) < 4:
        return ""
    scope_markers = len(re.findall(r"(经营|销售|服务|咨询|生产|加工|开发)", cleaned))
    if scope_markers >= 2 and not re.search(r"(省|市|区|县|路|街|号|室)", cleaned):
        return ""
    return cleaned


def split_bank_name_and_branch(value: str) -> tuple[str, str]:
    cleaned = _clean_extracted_field(value)
    if not cleaned:
        return "", ""
    cleaned = re.sub(r"(账号|账户号码|币种).*$", "", cleaned).strip("：:;；，,。 ")
    patterns = (
        r"(.+?(?:银行股份有限公司|银行有限责任公司|银行股份|银行|信用合作联社|农村商业银行|农商银行|信用社))(.+?(?:支行|分行|营业部|营业室|分理处|分中心))$",
        r"(.+?(?:银行|信用社))(.+?(?:支行|分行|营业部|营业室|分理处|分中心))$",
    )
    for pattern in patterns:
        match = re.search(pattern, cleaned)
        if match:
            return match.group(1).strip(), match.group(2).strip()
    return cleaned, ""


def _extract_account_number_final(text: str) -> str:
    source = text or ""
    labels = ["账号", "银行账号", "账户号码", "账号信息", "选择账号", "卡号"]
    stop_labels = ["开户行", "开户银行", "币种", "户名", "客户名称", "单位名称", "账户名称"]
    candidate = _extract_labeled_field_final(source, labels, stop_labels, max_length=80)
    digits = only_digits(candidate)
    if 8 <= len(digits) <= 40:
        return digits
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}{_FINAL_COLON_PATTERN}?([0-9][0-9\s]{{7,39}})")
        match = pattern.search(source)
        if match:
            digits = only_digits(match.group(1))
            if 8 <= len(digits) <= 40:
                return digits
    all_matches = re.findall(r"(?<!\d)(\d{8,40})(?!\d)", source)
    return max(all_matches, key=len) if all_matches else ""


def extract_currency(value: str) -> str:
    text = normalize_text(value).upper()
    if not text:
        return ""
    if "\u4eba\u6c11\u5e01" in value or "CNY" in text or "RMB" in text:
        return "\u4eba\u6c11\u5e01"
    for currency in ("USD", "HKD", "EUR"):
        if currency in text:
            return currency
    return ""


def extract_date_range(text: str) -> tuple[str, str]:
    source = text or ""
    normalized = (
        source.replace("\u5e74", "-")
        .replace("\u6708", "-")
        .replace("\u65e5", "")
        .replace("/", "-")
        .replace(".", "-")
    )
    patterns = (
        re.compile(r"((?:19|20)\d{2}-\d{1,2}-\d{1,2})\s*(?:--|-|至|~|—)\s*((?:19|20)\d{2}-\d{1,2}-\d{1,2})"),
        re.compile(r"(?:记账日期范围|查询日期范围|起止日期|记账日期)\s*[:：]?\s*((?:19|20)\d{2}-\d{1,2}-\d{1,2})\s*(?:--|-|至|~|—)\s*((?:19|20)\d{2}-\d{1,2}-\d{1,2})"),
    )
    for pattern in patterns:
        match = pattern.search(normalized)
        if match:
            return _normalize_date(match.group(1)), _normalize_date(match.group(2))
    dates = re.findall(r"(?:19|20)\d{2}-\d{1,2}-\d{1,2}", normalized)
    if len(dates) >= 2:
        return _normalize_date(dates[0]), _normalize_date(dates[1])
    return "", ""


def extract_business_license(text: str) -> dict[str, Any]:
    stop_labels = [
        "\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba",
        "\u6cd5\u4eba",
        "\u8d1f\u8d23\u4eba",
        "\u6ce8\u518c\u8d44\u672c",
        "\u6210\u7acb\u65e5\u671f",
        "\u4f4f\u6240",
        "\u5730\u5740",
        "\u7ecf\u8425\u8303\u56f4",
        "\u7c7b\u578b",
    ]
    raw_scope = _extract_labeled_field_final(
        text,
        ["\u7ecf\u8425\u8303\u56f4"],
        ["\u4f4f\u6240", "\u5730\u5740", "\u7c7b\u578b", "\u6cd5\u5b9a\u4ee3\u8868\u4eba", "\u6cd5\u4eba", "\u8d1f\u8d23\u4eba"],
        max_length=900,
        allow_multiline=True,
    )
    raw_address = _extract_labeled_field_final(
        text,
        ["\u4f4f\u6240", "\u5730\u5740", "\u8425\u4e1a\u573a\u6240"],
        ["\u7ecf\u8425\u8303\u56f4", "\u7c7b\u578b", "\u6cd5\u5b9a\u4ee3\u8868\u4eba", "\u6cd5\u4eba", "\u8d1f\u8d23\u4eba"],
        max_length=320,
        allow_multiline=True,
    )
    business_scope = clean_business_scope(raw_scope)
    address = clean_address(raw_address)
    if business_scope and address and business_scope == address:
        if len(re.findall(r"(省|市|区|县|路|街|号|室)", business_scope)) >= 3:
            business_scope = ""
        else:
            address = ""
    return {
        "company_name": _pick_first_nonempty(
            _extract_labeled_field_final(text, ["\u540d\u79f0", "\u4f01\u4e1a\u540d\u79f0", "\u516c\u53f8\u540d\u79f0"], stop_labels, max_length=180),
            _find_after_labels(text, ("\u540d\u79f0", "\u4f01\u4e1a\u540d\u79f0", "\u516c\u53f8\u540d\u79f0")),
        ),
        "credit_code": _pick_first_nonempty(
            _find_first_match(text, UNIFIED_CODE_PATTERN),
            _extract_labeled_field_final(text, ["\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801", "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801"], stop_labels, max_length=64),
        ),
        "legal_person": _extract_labeled_field_final(text, ["\u6cd5\u5b9a\u4ee3\u8868\u4eba", "\u6cd5\u4eba", "\u8d1f\u8d23\u4eba"], stop_labels, max_length=60),
        "registered_capital": _extract_registered_capital_final(text),
        "establish_date": _pick_first_nonempty(
            _extract_labeled_field_final(text, ["\u6210\u7acb\u65e5\u671f", "\u6ce8\u518c\u65e5\u671f"], stop_labels, max_length=80),
            _find_first_date(text),
        ),
        "business_scope": business_scope,
        "address": address,
        "company_type": _extract_labeled_field_final(text, ["\u7c7b\u578b", "\u4e3b\u4f53\u7c7b\u578b"], ["\u6cd5\u5b9a\u4ee3\u8868\u4eba", "\u6ce8\u518c\u8d44\u672c", "\u6210\u7acb\u65e5\u671f", "\u7ecf\u8425\u8303\u56f4"], max_length=80),
    }


def extract_account_license(text: str) -> dict[str, Any]:
    stop_labels = [
        "\u5f00\u6237\u94f6\u884c",
        "\u5f00\u6237\u884c",
        "\u5f00\u6237\u673a\u6784",
        "\u5f00\u6237\u94f6\u884c\u673a\u6784",
        "\u6838\u51c6\u53f7",
        "\u8bb8\u53ef\u8bc1\u53f7",
        "\u8bb8\u53ef\u8bc1\u7f16\u53f7",
        "\u8d26\u6237\u6027\u8d28",
        "\u8d26\u6237\u7c7b\u578b",
        "\u5f00\u6237\u65e5\u671f",
        "\u5f00\u7acb\u65e5\u671f",
        "\u5b58\u6b3e\u4eba\u540d\u79f0",
        "\u8d26\u6237\u540d\u79f0",
        "\u6237\u540d",
        "\u5e01\u79cd",
    ]
    bank_full = _pick_first_nonempty(
        _extract_labeled_field_final(text, ["\u5f00\u6237\u94f6\u884c", "\u5f00\u6237\u884c"], stop_labels, max_length=180),
        _extract_labeled_field_final(text, ["\u5f00\u6237\u673a\u6784", "\u5f00\u6237\u94f6\u884c\u673a\u6784"], stop_labels, max_length=180),
    )
    bank_name, branch_from_name = split_bank_name_and_branch(bank_full)
    bank_branch = _pick_first_nonempty(
        branch_from_name,
        _extract_labeled_field_final(text, ["\u5f00\u6237\u673a\u6784", "\u5f00\u6237\u94f6\u884c\u673a\u6784", "\u5f00\u6237\u7f51\u70b9"], stop_labels, max_length=120),
    )
    return {
        "account_name": _extract_labeled_field_final(text, ["\u8d26\u6237\u540d\u79f0", "\u5b58\u6b3e\u4eba\u540d\u79f0", "\u6237\u540d"], stop_labels, max_length=120),
        "account_number": _extract_account_number_final(text),
        "bank_name": bank_name,
        "bank_branch": "" if bank_branch == bank_name else bank_branch,
        "license_number": _extract_labeled_field_final(text, ["\u6838\u51c6\u53f7", "\u8bb8\u53ef\u8bc1\u53f7", "\u8bb8\u53ef\u8bc1\u7f16\u53f7"], stop_labels, max_length=80),
        "account_type": _extract_labeled_field_final(text, ["\u8d26\u6237\u6027\u8d28", "\u8d26\u6237\u7c7b\u578b"], stop_labels, max_length=80),
        "open_date": _pick_first_nonempty(
            _extract_labeled_field_final(text, ["\u5f00\u6237\u65e5\u671f", "\u5f00\u7acb\u65e5\u671f"], stop_labels, max_length=60),
            _find_first_date(text),
        ),
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


def _label_value_cn(text: str, labels: list[str], stop_labels: list[str], *, max_length: int = 240, allow_multiline: bool = False) -> str:
    source = text or ""
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*", re.MULTILINE)
        match = pattern.search(source)
        if not match:
            continue
        candidate = source[match.end() : match.end() + max_length]
        stop_indexes: list[int] = []
        for stop_label in stop_labels:
            stop_match = re.search(rf"{re.escape(stop_label)}\s*[:：]?", candidate)
            if stop_match:
                stop_indexes.append(stop_match.start())
        if stop_indexes:
            candidate = candidate[: min(stop_indexes)]
        if not allow_multiline:
            candidate = candidate.splitlines()[0]
        cleaned = re.sub(r"\s+", " ", normalize_text(candidate)).strip("：:;；，,。 ")
        if cleaned:
            return cleaned
    return ""


def _registered_capital_cn(text: str) -> str:
    for label in ("\u6ce8\u518c\u8d44\u672c", "\u6ce8\u518c\u8d44\u91d1"):
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*((?:\u4eba\u6c11\u5e01)?\s*[0-9,]+(?:\.\d+)?\s*(?:\u4e07\u5143|\u4e07\u4eba\u6c11\u5e01|\u5143|\u4ebf\u5143|\u4e07\u7f8e\u5143|\u4ebf\u7f8e\u5143)?)")
        match = pattern.search(text or "")
        if match:
            return re.sub(r"\s+", "", match.group(1))
    return ""


def clean_business_scope(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    for marker in (
        "\u4f4f\u6240",
        "\u5730\u5740",
        "\u7c7b\u578b",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba",
        "\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u6210\u7acb\u65e5\u671f",
    ):
        idx = cleaned.find(marker)
        if idx > 0:
            cleaned = cleaned[:idx].strip("：:;；，,。 ")
    return cleaned


def clean_address(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    for marker in (
        "\u7ecf\u8425\u8303\u56f4",
        "\u7c7b\u578b",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba",
        "\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u6210\u7acb\u65e5\u671f",
    ):
        idx = cleaned.find(marker)
        if idx > 0:
            cleaned = cleaned[:idx].strip("：:;；，,。 ")
    return cleaned


def split_bank_name_and_branch(value: str) -> tuple[str, str]:
    cleaned = re.sub(r"\s+", " ", normalize_text(value)).strip("：:;；，,。 ")
    if not cleaned:
        return "", ""
    match = re.search(r"(.+?(?:\u94f6\u884c|\u4fe1\u7528\u793e|\u519c\u5546\u884c|\u519c\u6751\u5546\u4e1a\u94f6\u884c|\u80a1\u4efd\u6709\u9650\u516c\u53f8))(.+?(?:\u652f\u884c|\u5206\u884c|\u8425\u4e1a\u90e8|\u8425\u4e1a\u5ba4|\u5206\u7406\u5904))", cleaned)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return cleaned, ""


def extract_currency(value: str) -> str:
    text = normalize_text(value).upper()
    if not text:
        return ""
    if "\u4eba\u6c11\u5e01" in text or "CNY" in text or "RMB" in text:
        return "\u4eba\u6c11\u5e01"
    for currency in ("USD", "HKD", "EUR"):
        if currency in text:
            return currency
    return ""


def extract_date_range(text: str) -> tuple[str, str]:
    source = text or ""
    patterns = (
        re.compile(r"((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)\s*(?:--|-|至|~)\s*((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)"),
        re.compile(r"(?:\u8bb0\u8d26\u65e5\u671f|\u67e5\u8be2\u65e5\u671f\u8303\u56f4|\u8d77\u6b62\u65e5\u671f)[:：]?\s*((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)\s*(?:\u81f3|-|--|~)\s*((?:19|20)\d{2}[-/.年](?:0?\d|1[0-2])[-/.月](?:0?\d|[12]\d|3[01])日?)"),
    )
    for pattern in patterns:
        match = pattern.search(source)
        if match:
            return _normalize_date(match.group(1)), _normalize_date(match.group(2))
    return "", ""


def extract_business_license(text: str) -> dict[str, Any]:
    stop_labels = [
        "\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba",
        "\u6cd5\u4eba",
        "\u8d1f\u8d23\u4eba",
        "\u6ce8\u518c\u8d44\u672c",
        "\u6210\u7acb\u65e5\u671f",
        "\u4f4f\u6240",
        "\u5730\u5740",
        "\u7ecf\u8425\u8303\u56f4",
        "\u7c7b\u578b",
        "\u767b\u8bb0\u673a\u5173",
        "\u767b\u8bb0\u673a\u6784",
        "\u53d1\u7167\u673a\u5173",
    ]
    address, business_scope = _extract_business_license_address_and_scope(text)
    registration_authority = _extract_registration_authority_cn(text)
    registration_date = _extract_registration_date_cn(text)
    return {
        "company_name": _pick_first_nonempty(
            _label_value_cn(text, ["\u540d\u79f0", "\u4f01\u4e1a\u540d\u79f0", "\u516c\u53f8\u540d\u79f0", "\u5e02\u573a\u4e3b\u4f53\u540d\u79f0"], stop_labels, max_length=180),
            _find_after_labels(text, ("\u540d\u79f0", "\u4f01\u4e1a\u540d\u79f0", "\u516c\u53f8\u540d\u79f0")),
        ),
        "credit_code": _pick_first_nonempty(
            _find_first_match(text, UNIFIED_CODE_PATTERN),
            _label_value_cn(text, ["\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801", "\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801"], stop_labels, max_length=60),
        ),
        "legal_person": _label_value_cn(text, ["\u6cd5\u5b9a\u4ee3\u8868\u4eba", "\u6cd5\u4eba", "\u8d1f\u8d23\u4eba"], stop_labels, max_length=60),
        "registered_capital": _registered_capital_cn(text),
        "establish_date": _pick_first_nonempty(
            _label_value_cn(text, ["\u6210\u7acb\u65e5\u671f", "\u6ce8\u518c\u65e5\u671f"], stop_labels, max_length=80),
            _find_first_date(text),
        ),
        "business_scope": business_scope if len(business_scope) >= 4 else "",
        "address": address if len(address) >= 4 else "",
        "company_type": _label_value_cn(text, ["\u7c7b\u578b", "\u4e3b\u4f53\u7c7b\u578b"], ["\u6cd5\u5b9a\u4ee3\u8868\u4eba", "\u6ce8\u518c\u8d44\u672c", "\u6210\u7acb\u65e5\u671f", "\u7ecf\u8425\u8303\u56f4"], max_length=80),
        "registration_authority": registration_authority,
        "registration_date": registration_date,
    }


def extract_account_license(text: str) -> dict[str, Any]:
    stop_labels = [
        "\u5f00\u6237\u94f6\u884c",
        "\u5f00\u6237\u884c",
        "\u5f00\u6237\u673a\u6784",
        "\u5f00\u6237\u94f6\u884c\u673a\u6784",
        "\u6838\u51c6\u53f7",
        "\u8bb8\u53ef\u8bc1\u53f7",
        "\u8d26\u6237\u6027\u8d28",
        "\u8d26\u6237\u7c7b\u578b",
        "\u5f00\u6237\u65e5\u671f",
        "\u5f00\u7acb\u65e5\u671f",
        "\u5b58\u6b3e\u4eba\u540d\u79f0",
        "\u8d26\u6237\u540d\u79f0",
        "\u6237\u540d",
        "\u5e01\u79cd",
    ]
    bank_full = _pick_first_nonempty(
        _label_value_cn(text, ["\u5f00\u6237\u94f6\u884c", "\u5f00\u6237\u884c"], stop_labels, max_length=180),
        _label_value_cn(text, ["\u5f00\u6237\u673a\u6784", "\u5f00\u6237\u94f6\u884c\u673a\u6784"], stop_labels, max_length=180),
    )
    bank_name, bank_branch = split_bank_name_and_branch(bank_full)
    return {
        "account_name": _label_value_cn(text, ["\u8d26\u6237\u540d\u79f0", "\u5b58\u6b3e\u4eba\u540d\u79f0", "\u6237\u540d"], stop_labels, max_length=120),
        "account_number": _v2_only_digits(
            _pick_first_nonempty(
                _label_value_cn(text, ["\u8d26\u53f7", "\u94f6\u884c\u8d26\u53f7", "\u8d26\u6237\u53f7\u7801"], stop_labels, max_length=120),
                _label_value_cn(text, ["\u5361\u53f7"], stop_labels, max_length=120),
            )
        ) or _v2_extract_account_number(text),
        "bank_name": bank_name,
        "bank_branch": _pick_first_nonempty(bank_branch, _label_value_cn(text, ["\u5f00\u6237\u673a\u6784", "\u5f00\u6237\u94f6\u884c\u673a\u6784", "\u5f00\u6237\u7f51\u70b9"], stop_labels, max_length=120)),
        "license_number": _label_value_cn(text, ["\u6838\u51c6\u53f7", "\u8bb8\u53ef\u8bc1\u53f7", "\u8bb8\u53ef\u8bc1\u7f16\u53f7"], stop_labels, max_length=80),
        "account_type": _label_value_cn(text, ["\u8d26\u6237\u6027\u8d28", "\u8d26\u6237\u7c7b\u578b"], stop_labels, max_length=80),
        "open_date": _pick_first_nonempty(_label_value_cn(text, ["\u5f00\u6237\u65e5\u671f", "\u5f00\u7acb\u65e5\u671f"], stop_labels, max_length=60), _find_first_date(text)),
    }




def _v2_extract_account_number(text: str) -> str:
    try:
        candidates = (
            _v2_extract_labeled_field(
                text,
                ["账号", "账户号码", "银行账号", "结算账户", "选择账号"],
                ["开户行", "开户银行", "币种", "户名", "账户名称"],
                max_length=120,
            ),
            _v2_extract_labeled_field(
                text,
                ["卡号"],
                ["开户行", "开户银行", "币种"],
                max_length=120,
            ),
        )
        for candidate in candidates:
            candidate = str(candidate or "").strip()
            if not candidate:
                continue
            candidate = re.split(
                r"(开户行|开户银行|币种|户名|账户名称|客户名称|公司名称)",
                candidate,
                maxsplit=1,
            )[0]
            digits = _v2_only_digits(candidate)
            if len(digits) >= 8:
                return digits
        for match in re.findall(r"\d{8,}", text or ""):
            digits = _v2_only_digits(match)
            if len(digits) >= 10:
                return digits
    except Exception as exc:
        logger.warning("bank_statement pdf field parse fallback: account_number (%s)", exc)
    return ""


def extract_bank_statement_pdf_fields(text: str) -> dict[str, Any]:
    source = text or ""

    def _safe_field(field_name: str, extractor: Callable[[], str]) -> str:
        try:
            value = str(extractor() or "").strip()
            if not value:
                logger.warning("bank_statement pdf field missing: %s", field_name)
            return value
        except Exception as exc:
            logger.warning("bank_statement pdf field parse fallback: %s (%s)", field_name, exc)
            return ""

    try:
        start_date, end_date = extract_date_range(source)
    except Exception as exc:
        logger.warning("bank_statement pdf field parse fallback: date_range (%s)", exc)
        start_date, end_date = "", ""

    account_name = _safe_field(
        "account_name",
        lambda: _v2_extract_labeled_field(
            source,
            ["户名", "账户名称", "客户名称", "单位名称"],
            ["账号", "账户号码", "卡号", "开户行", "开户银行", "币种"],
            max_length=120,
        ),
    )
    account_number = _safe_field("account_number", lambda: _v2_extract_account_number(source))
    bank_full = _safe_field(
        "bank_name",
        lambda: _pick_first_nonempty(
            _v2_extract_labeled_field(
                source,
                ["开户行", "开户银行", "银行名称"],
                ["币种", "账号", "账户号码", "户名", "账户名称"],
                max_length=200,
            ),
            _v2_extract_labeled_field(
                source,
                ["所属银行"],
                ["币种", "账号", "账户号码", "户名", "账户名称"],
                max_length=200,
            ),
        ),
    )
    try:
        bank_name, _ = split_bank_name_and_branch(bank_full)
    except Exception as exc:
        logger.warning("bank_statement pdf field parse fallback: bank_name (%s)", exc)
        bank_name = ""
    if not bank_name:
        logger.warning("bank_statement pdf field missing: bank_name")

    currency = _safe_field(
        "currency",
        lambda: extract_currency(
            _pick_first_nonempty(
                _v2_extract_labeled_field(
                    source,
                    ["币种"],
                    ["账号", "账户号码", "开户行", "开户银行", "交易日期", "记账日期"],
                    max_length=40,
                ),
                source[:200],
            )
        ),
    )
    opening_balance = _safe_field("opening_balance", lambda: _money_after_labels(source, ("期初余额", "上期余额", "起始余额")))
    closing_balance = _safe_field("closing_balance", lambda: _money_after_labels(source, ("期末余额", "当前余额", "账户余额")))
    total_income = _safe_field("total_income", lambda: _money_after_labels(source, ("贷方总金额", "收入合计", "总收入")))
    total_expense = _safe_field("total_expense", lambda: _money_after_labels(source, ("借方总金额", "支出合计", "总支出")))
    transaction_count = _safe_field(
        "transaction_count",
        lambda: only_digits(
            _v2_extract_labeled_field(
                source,
                ["总笔数", "交易笔数", "明细笔数"],
                ["借方总金额", "贷方总金额", "收入合计", "支出合计"],
                max_length=30,
            )
        ),
    )

    return {
        "account_name": account_name,
        "account_number": account_number,
        "bank_name": bank_name,
        "currency": currency,
        "start_date": start_date or _safe_field("start_date", lambda: _find_first_date(source)),
        "end_date": end_date or _safe_field("end_date", lambda: _find_last_date(source)),
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
        "total_income": total_income,
        "total_expense": total_expense,
        "monthly_avg_income": "",
        "monthly_avg_expense": "",
        "transaction_count": transaction_count,
        "top_inflows": [],
        "top_outflows": [],
    }


def _safe_label_pattern(labels: list[str]) -> str:
    return "|".join(re.escape(label) for label in labels if label)


def _normalize_line_value(value: str) -> str:
    cleaned = str(value or "").strip()
    cleaned = re.sub(r"^[：:\-\s]+", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _find_line_value_by_labels(lines: list[str], labels: list[str]) -> str:
    pattern_str = _safe_label_pattern(labels)
    if not pattern_str:
        return ""
    pattern = re.compile(rf"(?:{pattern_str})\s*[：: ]?\s*(.*)")
    for raw_line in lines:
        line = str(raw_line or "").strip()
        if not line:
            continue
        match = pattern.search(line)
        if match:
            return _normalize_line_value(match.group(1))
    return ""


def _extract_currency_from_text(value: str) -> str:
    text = str(value or "").upper()
    if not text:
        return ""
    if "\u4eba\u6c11\u5e01" in value or "CNY" in text or "RMB" in text:
        return "\u4eba\u6c11\u5e01"
    for currency in ("USD", "HKD", "EUR"):
        if currency in text:
            return currency
    return ""


def _extract_date_range_from_text(text: str) -> tuple[str, str]:
    source = str(text or "")
    match = re.search(r"(\d{4}[-/\.]\d{2}[-/\.]\d{2}).{0,5}(\d{4}[-/\.]\d{2}[-/\.]\d{2})", source)
    if not match:
        return "", ""
    return _normalize_date(match.group(1)), _normalize_date(match.group(2))


def _extract_longest_company_name(text: str) -> str:
    candidates = re.findall(r"([^\n\r]{0,40}有限公司)", str(text or ""))
    candidates = [candidate.strip("：:;；，,。 ").strip() for candidate in candidates if candidate.strip()]
    return max(candidates, key=len) if candidates else ""


def _extract_longest_bank_name(text: str) -> str:
    candidates = re.findall(r"([^\n\r]{0,40}银行[^\n\r]{0,20})", str(text or ""))
    cleaned_candidates = []
    for candidate in candidates:
        cleaned = candidate.strip("：:;；，,。 ").strip()
        cleaned = re.split(r"(\d{8,}|人民币|CNY|USD|HKD|EUR)", cleaned, maxsplit=1)[0].strip()
        if cleaned:
            cleaned_candidates.append(cleaned)
    return max(cleaned_candidates, key=len) if cleaned_candidates else ""


def _find_longest_digits(text: str, min_length: int = 10) -> str:
    matches = re.findall(r"\d{%d,}" % min_length, str(text or ""))
    return max(matches, key=len) if matches else ""


def _v2_extract_labeled_field(text: str, labels: list[str], stop_labels: list[str], *, max_length: int = 240, allow_multiline: bool = False) -> str:
    source = str(text or "")
    pattern_str = _safe_label_pattern(labels)
    if not pattern_str:
        return ""
    stop_pattern_str = _safe_label_pattern(stop_labels)
    pattern = re.compile(rf"(?:{pattern_str})\s*[：: ]?\s*(.*)")
    stop_pattern = re.compile(rf"(?:{stop_pattern_str})\s*[：: ]?") if stop_pattern_str else None

    for raw_line in source.splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        match = pattern.search(line)
        if not match:
            continue
        candidate = str(match.group(1) or "").strip()
        if not candidate:
            return ""
        if stop_pattern:
            stop_match = stop_pattern.search(candidate)
            if stop_match:
                candidate = candidate[: stop_match.start()]
        candidate = str(candidate or "").strip()
        if not candidate:
            return ""
        if not allow_multiline:
            lines = [item.strip() for item in candidate.splitlines() if item.strip()]
            candidate = lines[0] if lines else ""
        candidate = str(candidate or "").strip()
        if not candidate:
            return ""
        cleaned = re.sub(r"\s+", " ", normalize_text(candidate)).strip("：:;；，,。 ")
        if cleaned:
            return cleaned
    return ""


def _v2_extract_account_number(text: str) -> str:
    source = str(text or "")
    lines = source.splitlines()
    labels = ["\u9009\u62e9\u8d26\u53f7", "\u8d26\u53f7", "\u8d26\u6237\u53f7\u7801", "\u94f6\u884c\u8d26\u53f7", "\u5361\u53f7"]
    try:
        pattern_str = _safe_label_pattern(labels)
        pattern = re.compile(rf"(?:{pattern_str})\s*[：: ]?\s*(.*)")
        for raw_line in lines:
            line = str(raw_line or "").strip()
            if not line:
                continue
            match = pattern.search(line)
            if not match:
                continue
            value = _normalize_line_value(match.group(1))
            if not value:
                continue
            value = re.split(r"(\u5f00\u6237\u884c|\u5f00\u6237\u94f6\u884c|\u5e01\u79cd|\u6237\u540d|\u8d26\u6237\u540d\u79f0|\u516c\u53f8\u540d\u79f0)", value, maxsplit=1)[0]
            digits_match = re.search(r"\d{8,}", value)
            if digits_match:
                return digits_match.group(0)
        return _find_longest_digits(source, min_length=10)
    except Exception as exc:
        logger.warning("bank_statement pdf field parse fallback: account_number (%s)", exc)
        return ""


def extract_bank_statement_pdf_fields(text: str) -> dict[str, Any]:
    source = str(text or "")
    lines = [str(line or "").strip() for line in source.splitlines() if str(line or "").strip()]

    def _safe_field(field_name: str, extractor: Callable[[], str]) -> str:
        try:
            value = str(extractor() or "").strip()
            if not value:
                logger.warning("bank_statement pdf field missing: %s", field_name)
            return value
        except Exception as exc:
            logger.warning("bank_statement pdf field parse fallback: %s (%s)", field_name, exc)
            return ""

    start_date, end_date = _extract_date_range_from_text(source)

    account_name = _safe_field(
        "account_name",
        lambda: _find_line_value_by_labels(lines, ["\u6237\u540d", "\u8d26\u6237\u540d\u79f0", "\u8d26\u6237\u540d"])
        or _extract_longest_company_name(source),
    )
    account_number = _safe_field("account_number", lambda: _v2_extract_account_number(source))
    bank_name = _safe_field(
        "bank_name",
        lambda: _find_line_value_by_labels(lines, ["\u5f00\u6237\u884c", "\u5f00\u6237\u94f6\u884c"])
        or _extract_longest_bank_name(source),
    )
    currency = _safe_field(
        "currency",
        lambda: _extract_currency_from_text(_find_line_value_by_labels(lines, ["\u5e01\u79cd"]) or source[:200]),
    )
    opening_balance = _safe_field("opening_balance", lambda: _money_after_labels(source, ("\u671f\u521d\u4f59\u989d", "\u4e0a\u671f\u4f59\u989d", "\u8d77\u59cb\u4f59\u989d")))
    closing_balance = _safe_field("closing_balance", lambda: _money_after_labels(source, ("\u671f\u672b\u4f59\u989d", "\u5f53\u524d\u4f59\u989d", "\u8d26\u6237\u4f59\u989d")))
    total_income = _safe_field("total_income", lambda: _money_after_labels(source, ("\u8d37\u65b9\u603b\u91d1\u989d", "\u6536\u5165\u5408\u8ba1", "\u603b\u6536\u5165")))
    total_expense = _safe_field("total_expense", lambda: _money_after_labels(source, ("\u501f\u65b9\u603b\u91d1\u989d", "\u652f\u51fa\u5408\u8ba1", "\u603b\u652f\u51fa")))
    transaction_count = _safe_field(
        "transaction_count",
        lambda: only_digits(_find_line_value_by_labels(lines, ["\u603b\u7b14\u6570", "\u4ea4\u6613\u7b14\u6570", "\u660e\u7ec6\u7b14\u6570"])),
    )

    return {
        "account_name": account_name,
        "account_number": account_number,
        "bank_name": bank_name,
        "currency": currency,
        "start_date": start_date or _safe_field("start_date", lambda: _find_first_date(source)),
        "end_date": end_date or _safe_field("end_date", lambda: _find_last_date(source)),
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
        "total_income": total_income,
        "total_expense": total_expense,
        "monthly_avg_income": "",
        "monthly_avg_expense": "",
        "transaction_count": transaction_count,
        "top_inflows": [],
        "top_outflows": [],
    }


def _clean_account_name_candidate(value: str) -> str:
    candidate = str(value or "").strip()
    if not candidate:
        return ""
    candidate = re.sub(r"\d{4,}", " ", candidate)
    candidate = re.sub(r"[+-]?\d[\d,]*(?:\.\d+)?", " ", candidate)
    candidate = re.sub(r"(人民币|CNY|USD|HKD|EUR)", " ", candidate, flags=re.I)
    candidate = re.sub(r"(开户行|开户银行|账号|账户名称|账户名|户名|币种)", " ", candidate)
    candidate = re.sub(r"\s+", " ", candidate).strip("：:;；，,。 ")
    if re.search(r"\d", candidate):
        return ""
    if len(candidate) < 4:
        return ""
    return candidate


def _extract_company_name_from_line(line: str) -> str:
    source = str(line or "")
    candidates = re.findall(r"([一-龥A-Za-z（）()·]{2,60}(?:有限责任公司|股份有限公司|有限公司))", source)
    cleaned = [_clean_account_name_candidate(candidate) for candidate in candidates]
    cleaned = [candidate for candidate in cleaned if candidate]
    return max(cleaned, key=len) if cleaned else ""


def _extract_bank_name_from_line(line: str) -> str:
    source = str(line or "")
    candidates = re.findall(r"([一-龥A-Za-z]{2,40}银行[一-龥A-Za-z]{0,30}(?:支行|分行|营业部|营业室|分理处)?)", source)
    cleaned_candidates: list[str] = []
    for candidate in candidates:
        cleaned = str(candidate or "").strip("：:;；，,。 ").strip()
        cleaned = re.split(r"(\d{8,}|人民币|CNY|USD|HKD|EUR)", cleaned, maxsplit=1, flags=re.I)[0].strip()
        if cleaned:
            cleaned_candidates.append(cleaned)
    if not cleaned_candidates:
        return ""
    branch_candidates = [candidate for candidate in cleaned_candidates if re.search(r"(支行|分行|营业部|营业室|分理处)", candidate)]
    pool = branch_candidates or cleaned_candidates
    return max(pool, key=len)


def _split_header_line(line: str) -> dict[str, str]:
    source = str(line or "").strip()
    if not source:
        return {
            "account_number": "",
            "account_name": "",
            "bank_name": "",
            "currency": "",
        }

    digit_matches = re.findall(r"\d{8,}", source)
    account_number = digit_matches[0] if digit_matches else ""
    currency = _extract_currency_from_text(source)

    bank_name = ""
    if "\u5f00\u6237\u884c" in source or "\u5f00\u6237\u94f6\u884c" in source:
        parts = re.split(r"(开户行|开户银行)\s*[：: ]?", source, maxsplit=1)
        if len(parts) >= 3:
            bank_name = _extract_bank_name_from_line(parts[-1])
    bank_name = bank_name or _extract_bank_name_from_line(source)

    account_name = _extract_company_name_from_line(source)
    if not account_name and any(label in source for label in ("\u6237\u540d", "\u8d26\u6237\u540d\u79f0", "\u8d26\u6237\u540d")):
        parts = re.split(r"(户名|账户名称|账户名)\s*[：: ]?", source, maxsplit=1)
        if len(parts) >= 3:
            account_name = _extract_company_name_from_line(parts[-1]) or _clean_account_name_candidate(parts[-1])

    return {
        "account_number": account_number,
        "account_name": account_name,
        "bank_name": bank_name,
        "currency": currency,
    }


def _find_line_value_by_labels(lines: list[str], labels: list[str]) -> str:
    pattern_str = _safe_label_pattern(labels)
    if not pattern_str:
        return ""
    pattern = re.compile(rf"(?:{pattern_str})\s*[：: ]?\s*(.*)")
    for raw_line in lines:
        line = str(raw_line or "").strip()
        if not line:
            continue
        match = pattern.search(line)
        if match:
            return _normalize_line_value(match.group(1))
    return ""


def _extract_currency_from_text(value: str) -> str:
    text = str(value or "")
    upper_text = text.upper()
    if "\u4eba\u6c11\u5e01" in text or "CNY" in upper_text or "RMB" in upper_text:
        return "\u4eba\u6c11\u5e01"
    if "USD" in upper_text:
        return "USD"
    if "HKD" in upper_text:
        return "HKD"
    if "EUR" in upper_text:
        return "EUR"
    return ""


def _extract_date_range_from_text(text: str) -> tuple[str, str]:
    source = str(text or "")
    match = re.search(r"(\d{4}[-/\.]\d{2}[-/\.]\d{2}).{0,5}(\d{4}[-/\.]\d{2}[-/\.]\d{2})", source)
    if not match:
        return "", ""
    return _normalize_date(match.group(1)), _normalize_date(match.group(2))


def _extract_longest_company_name(text: str) -> str:
    source = str(text or "")
    candidates = re.findall(r"([一-龥A-Za-z（）()·]{2,60}(?:有限责任公司|股份有限公司|有限公司))", source)
    cleaned = [_clean_account_name_candidate(candidate) for candidate in candidates]
    cleaned = [candidate for candidate in cleaned if candidate]
    return max(cleaned, key=len) if cleaned else ""


def _extract_bank_name_from_line(line: str) -> str:
    source = str(line or "")
    candidates = re.findall(r"([一-龥A-Za-z]{2,40}银行[一-龥A-Za-z]{0,30}(?:支行|分行|营业部|营业室|分理处)?)", source)
    cleaned_candidates: list[str] = []
    for candidate in candidates:
        cleaned = str(candidate or "").strip("：:;；，,。 ").strip()
        cleaned = re.split(r"(\d{8,}|人民币|CNY|USD|HKD|EUR)", cleaned, maxsplit=1, flags=re.I)[0].strip()
        if cleaned:
            cleaned_candidates.append(cleaned)
    if not cleaned_candidates:
        return ""
    branch_candidates = [candidate for candidate in cleaned_candidates if re.search(r"(支行|分行|营业部|营业室|分理处)", candidate)]
    pool = branch_candidates or cleaned_candidates
    return max(pool, key=len)


def _split_header_line(line: str) -> dict[str, str]:
    source = str(line or "").strip()
    if not source:
        return {
            "account_number": "",
            "account_name": "",
            "bank_name": "",
            "currency": "",
        }

    digit_matches = re.findall(r"\d{8,}", source)
    account_number = digit_matches[0] if digit_matches else ""

    currency = _extract_currency_from_text(source)

    bank_name = ""
    if "\u5f00\u6237\u884c" in source or "\u5f00\u6237\u94f6\u884c" in source:
        parts = re.split(r"(开户行|开户银行)\s*[：: ]?", source, maxsplit=1)
        if len(parts) >= 3:
            bank_name = _extract_bank_name_from_line(parts[-1])
    bank_name = bank_name or _extract_bank_name_from_line(source)

    account_name = _extract_company_name_from_line(source)
    if not account_name and any(label in source for label in ("\u6237\u540d", "\u8d26\u6237\u540d\u79f0", "\u8d26\u6237\u540d")):
        parts = re.split(r"(户名|账户名称|账户名)\s*[：: ]?", source, maxsplit=1)
        if len(parts) >= 3:
            account_name = _extract_company_name_from_line(parts[-1]) or _clean_account_name_candidate(parts[-1])

    return {
        "account_number": account_number,
        "account_name": account_name,
        "bank_name": bank_name,
        "currency": currency,
    }


def extract_bank_statement_pdf_fields(text: str) -> dict[str, Any]:
    source = str(text or "")
    lines = [str(line or "").strip() for line in source.splitlines() if str(line or "").strip()]

    def _safe_field(field_name: str, extractor: Callable[[], str]) -> str:
        try:
            value = str(extractor() or "").strip()
            if not value:
                logger.warning("bank_statement pdf field missing: %s", field_name)
            return value
        except Exception as exc:
            logger.warning("bank_statement pdf field parse fallback: %s (%s)", field_name, exc)
            return ""

    def _find_header_line(labels: list[str]) -> str:
        for line in lines:
            if _line_has_any_label(line, labels):
                return line
        return ""

    header_candidates = [
        _find_header_line(["\u9009\u62e9\u8d26\u53f7", "\u8d26\u53f7"]),
        _find_header_line(["\u6237\u540d", "\u8d26\u6237\u540d\u79f0", "\u8d26\u6237\u540d"]),
        _find_header_line(["\u5f00\u6237\u884c", "\u5f00\u6237\u94f6\u884c"]),
        _find_header_line(["\u5e01\u79cd"]),
    ]
    header_candidates = [line for line in header_candidates if line]
    merged_header = " ".join(dict.fromkeys(header_candidates)) if header_candidates else source[:400]
    split_result = _split_header_line(merged_header)
    start_date, end_date = _extract_date_range_from_text(source)

    account_number = _safe_field("account_number", lambda: split_result["account_number"] or _find_longest_digits(source, min_length=10))
    account_name = _safe_field("account_name", lambda: split_result["account_name"] or _extract_longest_company_name(source))
    bank_name = _safe_field("bank_name", lambda: split_result["bank_name"] or _extract_longest_bank_name(source))
    currency = _safe_field("currency", lambda: split_result["currency"] or _extract_currency_from_text(merged_header or source[:200]))
    opening_balance = _safe_field("opening_balance", lambda: _money_after_labels(source, ("\u671f\u521d\u4f59\u989d", "\u4e0a\u671f\u4f59\u989d", "\u8d77\u59cb\u4f59\u989d")))
    closing_balance = _safe_field("closing_balance", lambda: _money_after_labels(source, ("\u671f\u672b\u4f59\u989d", "\u5f53\u524d\u4f59\u989d", "\u8d26\u6237\u4f59\u989d")))
    total_income = _safe_field("total_income", lambda: _money_after_labels(source, ("\u8d37\u65b9\u603b\u91d1\u989d", "\u6536\u5165\u5408\u8ba1", "\u603b\u6536\u5165")))
    total_expense = _safe_field("total_expense", lambda: _money_after_labels(source, ("\u501f\u65b9\u603b\u91d1\u989d", "\u652f\u51fa\u5408\u8ba1", "\u603b\u652f\u51fa")))
    transaction_count = _safe_field("transaction_count", lambda: only_digits(_find_line_value_by_labels(lines, ["\u603b\u7b14\u6570", "\u4ea4\u6613\u7b14\u6570", "\u660e\u7ec6\u7b14\u6570"])))

    return {
        "account_name": account_name,
        "account_number": account_number,
        "bank_name": bank_name,
        "currency": currency,
        "start_date": start_date or _safe_field("start_date", lambda: _find_first_date(source)),
        "end_date": end_date or _safe_field("end_date", lambda: _find_last_date(source)),
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
        "total_income": total_income,
        "total_expense": total_expense,
        "monthly_avg_income": "",
        "monthly_avg_expense": "",
        "transaction_count": transaction_count,
        "top_inflows": [],
        "top_outflows": [],
    }


def _line_has_any_label(line: str, labels: list[str]) -> bool:
    normalized = str(line or "")
    return any(label in normalized for label in labels if label)


def _clean_account_name_candidate(value: str) -> str:
    candidate = str(value or "").strip()
    if not candidate:
        return ""
    candidate = re.sub(r"\d{4,}", " ", candidate)
    candidate = re.sub(r"[+-]?\d[\d,]*(?:\.\d+)?", " ", candidate)
    candidate = re.sub(r"(人民币|CNY|USD|HKD|EUR)", " ", candidate, flags=re.I)
    candidate = re.sub(r"(开户行|开户银行|账号|账户名称|账户名|户名|币种)", " ", candidate)
    candidate = re.sub(r"\s+", " ", candidate).strip("：:;；，,。 ")
    if re.search(r"\d", candidate):
        return ""
    if len(candidate) < 4:
        return ""
    return candidate


def _extract_company_name_from_line(line: str) -> str:
    source = str(line or "")
    candidates = re.findall(r"([一-龥A-Za-z（）()·]{2,60}(?:有限责任公司|股份有限公司|有限公司))", source)
    cleaned = [_clean_account_name_candidate(candidate) for candidate in candidates]
    cleaned = [candidate for candidate in cleaned if candidate]
    return max(cleaned, key=len) if cleaned else ""


def _extract_bank_name_from_line(line: str) -> str:
    source = str(line or "")
    candidates = re.findall(r"([一-龥A-Za-z]{2,40}银行[一-龥A-Za-z]{0,30}(?:支行|分行|营业部|营业室|分理处)?)", source)
    cleaned_candidates: list[str] = []
    for candidate in candidates:
        cleaned = str(candidate or "").strip("：:;；，,。 ")
        cleaned = re.split(r"(\d{8,}|人民币|CNY|USD|HKD|EUR)", cleaned, maxsplit=1, flags=re.I)[0].strip()
        if cleaned:
            cleaned_candidates.append(cleaned)
    if not cleaned_candidates:
        return ""
    branch_candidates = [candidate for candidate in cleaned_candidates if re.search(r"(支行|分行|营业部|营业室|分理处)", candidate)]
    pool = branch_candidates or cleaned_candidates
    return max(pool, key=len)


def _split_header_line(line: str) -> dict[str, str]:
    source = str(line or "").strip()
    if not source:
        return {
            "account_number": "",
            "account_name": "",
            "bank_name": "",
            "currency": "",
        }

    account_number = ""
    digit_matches = re.findall(r"\d{8,}", source)
    if digit_matches:
        account_number = digit_matches[0]

    currency = _extract_currency_from_text(source)

    bank_name = ""
    if "\u5f00\u6237\u884c" in source or "\u5f00\u6237\u94f6\u884c" in source:
        bank_part = re.split(r"(开户行|开户银行)\s*[：: ]?", source, maxsplit=1)
        if len(bank_part) >= 3:
            bank_name = _extract_bank_name_from_line(bank_part[-1])
    bank_name = bank_name or _extract_bank_name_from_line(source)

    account_name = _extract_company_name_from_line(source)
    if not account_name and ("\u6237\u540d" in source or "\u8d26\u6237\u540d\u79f0" in source or "\u8d26\u6237\u540d" in source):
        parts = re.split(r"(户名|账户名称|账户名)\s*[：: ]?", source, maxsplit=1)
        if len(parts) >= 3:
            account_name = _extract_company_name_from_line(parts[-1]) or _clean_account_name_candidate(parts[-1])

    return {
        "account_number": account_number,
        "account_name": account_name,
        "bank_name": bank_name,
        "currency": currency,
    }


def extract_bank_statement_pdf_fields(text: str) -> dict[str, Any]:
    source = str(text or "")
    lines = [str(line or "").strip() for line in source.splitlines() if str(line or "").strip()]

    def _safe_field(field_name: str, extractor: Callable[[], str]) -> str:
        try:
            value = str(extractor() or "").strip()
            if not value:
                logger.warning("bank_statement pdf field missing: %s", field_name)
            return value
        except Exception as exc:
            logger.warning("bank_statement pdf field parse fallback: %s (%s)", field_name, exc)
            return ""

    def _find_header_line(labels: list[str]) -> str:
        for line in lines:
            if _line_has_any_label(line, labels):
                return line
        return ""

    header_candidates = [
        _find_header_line(["\u9009\u62e9\u8d26\u53f7", "\u8d26\u53f7"]),
        _find_header_line(["\u6237\u540d", "\u8d26\u6237\u540d\u79f0", "\u8d26\u6237\u540d"]),
        _find_header_line(["\u5f00\u6237\u884c", "\u5f00\u6237\u94f6\u884c"]),
        _find_header_line(["\u5e01\u79cd"]),
    ]
    header_candidates = [line for line in header_candidates if line]

    merged_header = " ".join(dict.fromkeys(header_candidates)) if header_candidates else source[:400]
    split_result = _split_header_line(merged_header)
    start_date, end_date = _extract_date_range_from_text(source)

    def _fallback_account_number() -> str:
        return split_result["account_number"] or _find_longest_digits(source, min_length=10)

    def _fallback_account_name() -> str:
        return split_result["account_name"] or _extract_longest_company_name(source)

    def _fallback_bank_name() -> str:
        return split_result["bank_name"] or _extract_longest_bank_name(source)

    def _fallback_currency() -> str:
        return split_result["currency"] or _extract_currency_from_text(merged_header or source[:200])

    account_number = _safe_field("account_number", _fallback_account_number)
    account_name = _safe_field("account_name", _fallback_account_name)
    bank_name = _safe_field("bank_name", _fallback_bank_name)
    currency = _safe_field("currency", _fallback_currency)
    opening_balance = _safe_field("opening_balance", lambda: _money_after_labels(source, ("\u671f\u521d\u4f59\u989d", "\u4e0a\u671f\u4f59\u989d", "\u8d77\u59cb\u4f59\u989d")))
    closing_balance = _safe_field("closing_balance", lambda: _money_after_labels(source, ("\u671f\u672b\u4f59\u989d", "\u5f53\u524d\u4f59\u989d", "\u8d26\u6237\u4f59\u989d")))
    total_income = _safe_field("total_income", lambda: _money_after_labels(source, ("\u8d37\u65b9\u603b\u91d1\u989d", "\u6536\u5165\u5408\u8ba1", "\u603b\u6536\u5165")))
    total_expense = _safe_field("total_expense", lambda: _money_after_labels(source, ("\u501f\u65b9\u603b\u91d1\u989d", "\u652f\u51fa\u5408\u8ba1", "\u603b\u652f\u51fa")))
    transaction_count = _safe_field(
        "transaction_count",
        lambda: only_digits(_find_line_value_by_labels(lines, ["\u603b\u7b14\u6570", "\u4ea4\u6613\u7b14\u6570", "\u660e\u7ec6\u7b14\u6570"])),
    )

    return {
        "account_name": account_name,
        "account_number": account_number,
        "bank_name": bank_name,
        "currency": currency,
        "start_date": start_date or _safe_field("start_date", lambda: _find_first_date(source)),
        "end_date": end_date or _safe_field("end_date", lambda: _find_last_date(source)),
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
        "total_income": total_income,
        "total_expense": total_expense,
        "monthly_avg_income": "",
        "monthly_avg_expense": "",
        "transaction_count": transaction_count,
        "top_inflows": [],
        "top_outflows": [],
    }

# === Final bank_statement PDF accuracy overrides ===
def _header_search_region(lines: list[str], limit: int = 24) -> list[str]:
    return [str(line or '').strip() for line in lines[:limit] if str(line or '').strip()]


def _contains_transaction_row(line: str) -> bool:
    source = str(line or '')
    return bool(re.search(r'(\u5bf9\u624b|\u6458\u8981|\u4ea4\u6613|\u501f\u65b9|\u8d37\u65b9|\u4f59\u989d|\u8bb0\u8d26\u65e5\u671f|\u4ea4\u6613\u65e5\u671f)', source))


def _clean_account_name_candidate(value: str) -> str:
    candidate = str(value or '').strip()
    if not candidate:
        return ''
    candidate = re.sub(r'\d{4,}', ' ', candidate)
    candidate = re.sub(r'[+-]?\d[\d,]*(?:\.\d+)?', ' ', candidate)
    candidate = re.sub(r'(\u4eba\u6c11\u5e01|CNY|USD|HKD|EUR)', ' ', candidate, flags=re.I)
    candidate = re.sub(r'(\u5f00\u6237\u884c|\u5f00\u6237\u94f6\u884c|\u8d26\u53f7|\u8d26\u6237\u53f7\u7801|\u8d26\u6237\u540d\u79f0|\u8d26\u6237\u540d|\u6237\u540d|\u5e01\u79cd|\u501f\u65b9\u603b\u91d1\u989d|\u8d37\u65b9\u603b\u91d1\u989d)', ' ', candidate)
    candidate = re.sub(r'\s+', ' ', candidate).strip('\uff1a:;\uff1b\uff0c,\u3002 ')
    if re.search(r'\d', candidate):
        return ''
    if len(candidate) < 4:
        return ''
    return candidate


def _extract_company_name_from_line(line: str) -> str:
    source = str(line or '')
    patterns = [
        r'([\u4e00-\u9fffA-Za-z\uff08\uff09()\u00b7]{2,80}?\u6709\u9650\u516c\u53f8)',
        r'([\u4e00-\u9fffA-Za-z\uff08\uff09()\u00b7]{2,80}?\u6709\u9650\u8d23\u4efb\u516c\u53f8)',
        r'([\u4e00-\u9fffA-Za-z\uff08\uff09()\u00b7]{2,80}?\u80a1\u4efd\u6709\u9650\u516c\u53f8)',
    ]
    candidates: list[str] = []
    for pattern in patterns:
        candidates.extend(re.findall(pattern, source))
    cleaned = [_clean_account_name_candidate(candidate) for candidate in candidates]
    cleaned = [candidate for candidate in cleaned if candidate and not _contains_transaction_row(candidate)]
    return max(cleaned, key=len) if cleaned else ''


def _extract_bank_name_from_line(line: str) -> str:
    source = str(line or '')
    patterns = [
        r'([\u4e00-\u9fffA-Za-z]{2,50}?\u94f6\u884c[\u4e00-\u9fffA-Za-z]{0,40}?(?:\u652f\u884c|\u5206\u884c|\u8425\u4e1a\u90e8|\u8425\u4e1a\u5ba4|\u5206\u7406\u5904))',
        r'([\u4e00-\u9fffA-Za-z]{2,50}?\u94f6\u884c[\u4e00-\u9fffA-Za-z]{0,40})',
    ]
    candidates: list[str] = []
    for pattern in patterns:
        candidates.extend(re.findall(pattern, source))
    cleaned_candidates: list[str] = []
    for candidate in candidates:
        cleaned = str(candidate or '').strip('\uff1a:;\uff1b\uff0c,\u3002 ')
        cleaned = re.split(r'(\d{8,}|\u4eba\u6c11\u5e01|CNY|USD|HKD|EUR)', cleaned, maxsplit=1, flags=re.I)[0].strip()
        if cleaned:
            cleaned_candidates.append(cleaned)
    if not cleaned_candidates:
        return ''
    branch_candidates = [item for item in cleaned_candidates if re.search(r'(\u652f\u884c|\u5206\u884c|\u8425\u4e1a\u90e8|\u8425\u4e1a\u5ba4|\u5206\u7406\u5904)', item)]
    return max(branch_candidates or cleaned_candidates, key=len)


def _extract_currency_from_text(value: str) -> str:
    text = str(value or '')
    upper_text = text.upper()
    if '\u4eba\u6c11\u5e01' in text or 'CNY' in upper_text or 'RMB' in upper_text:
        return '\u4eba\u6c11\u5e01'
    if 'USD' in upper_text:
        return 'USD'
    if 'HKD' in upper_text:
        return 'HKD'
    if 'EUR' in upper_text:
        return 'EUR'
    return ''


def _extract_date_range_from_text(text: str) -> tuple[str, str]:
    source = str(text or '')
    match = re.search(r'(\d{4}[-/\.]\d{2}[-/\.]\d{2}).{0,5}(\d{4}[-/\.]\d{2}[-/\.]\d{2})', source)
    if not match:
        return '', ''
    return _normalize_date(match.group(1)), _normalize_date(match.group(2))


def _extract_longest_company_name(text: str) -> str:
    source = str(text or '')
    patterns = [
        r'([\u4e00-\u9fffA-Za-z\uff08\uff09()\u00b7]{2,80}?\u6709\u9650\u516c\u53f8)',
        r'([\u4e00-\u9fffA-Za-z\uff08\uff09()\u00b7]{2,80}?\u6709\u9650\u8d23\u4efb\u516c\u53f8)',
        r'([\u4e00-\u9fffA-Za-z\uff08\uff09()\u00b7]{2,80}?\u80a1\u4efd\u6709\u9650\u516c\u53f8)',
    ]
    candidates: list[str] = []
    for pattern in patterns:
        candidates.extend(re.findall(pattern, source))
    cleaned = [_clean_account_name_candidate(candidate) for candidate in candidates]
    cleaned = [candidate for candidate in cleaned if candidate]
    return max(cleaned, key=len) if cleaned else ''


def _extract_longest_bank_name(text: str) -> str:
    source = str(text or '')
    lines = [str(line or '').strip() for line in source.splitlines() if str(line or '').strip()]
    candidates = [_extract_bank_name_from_line(line) for line in lines if '\u94f6\u884c' in line]
    candidates = [candidate for candidate in candidates if candidate]
    return max(candidates, key=len) if candidates else ''


def _find_longest_digits(text: str, min_length: int = 10) -> str:
    candidates = re.findall(r'\d{%d,}' % min_length, str(text or ''))
    return max(candidates, key=len) if candidates else ''


def _extract_full_amount_after_label(text: str, labels: tuple[str, ...]) -> str:
    source = str(text or '')
    lines = [str(line or '').strip() for line in source.splitlines() if str(line or '').strip()]
    escaped_labels = [re.escape(label) for label in labels if label]
    if not escaped_labels:
        return ''
    joined = '|'.join(escaped_labels)
    inline_pattern = re.compile(rf'(?:{joined})\s*[\uff1a: ]?\s*([+-]?\d[\d,]*(?:\.\d{{1,2}})?)')
    multiline_pattern = re.compile(rf'(?:{joined}).{{0,20}}?([+-]?\d[\d,]*(?:\.\d{{1,2}})?)')
    for line in lines:
        if not any(label in line for label in labels):
            continue
        match = inline_pattern.search(line) or multiline_pattern.search(line)
        if match:
            value = normalize_amount(match.group(1))
            if value:
                return value
    source_match = multiline_pattern.search(source)
    if source_match:
        value = normalize_amount(source_match.group(1))
        if value:
            return value
    return ''


def _extract_count_after_label(text: str, labels: tuple[str, ...]) -> str:
    source = str(text or '')
    lines = [str(line or '').strip() for line in source.splitlines() if str(line or '').strip()]
    escaped_labels = [re.escape(label) for label in labels if label]
    if not escaped_labels:
        return ''
    joined = '|'.join(escaped_labels)
    inline_pattern = re.compile(rf'(?:{joined})\s*[\uff1a: ]?\s*(\d{{1,8}})')
    multiline_pattern = re.compile(rf'(?:{joined}).{{0,12}}?(\d{{1,8}})')
    for line in lines:
        if not any(label in line for label in labels):
            continue
        match = inline_pattern.search(line) or multiline_pattern.search(line)
        if match:
            return only_digits(match.group(1))
    source_match = multiline_pattern.search(source)
    return only_digits(source_match.group(1)) if source_match else ''


def _months_between(start_date: str, end_date: str) -> int:
    try:
        start = datetime.strptime(_normalize_date(start_date), '%Y-%m-%d')
        end = datetime.strptime(_normalize_date(end_date), '%Y-%m-%d')
    except Exception:
        return 0
    months = (end.year - start.year) * 12 + (end.month - start.month) + 1
    return months if months > 0 else 0


def _format_decimal_string(value: Decimal) -> str:
    quantized = value.quantize(Decimal('0.01'))
    return format(quantized, 'f')


def _safe_decimal(value: str) -> Decimal | None:
    try:
        text = normalize_amount(value)
        return Decimal(text) if text else None
    except (InvalidOperation, ValueError):
        return None


def _split_header_line(line: str) -> dict[str, str]:
    source = str(line or '').strip()
    if not source:
        return {'account_number': '', 'account_name': '', 'bank_name': '', 'currency': ''}

    digit_matches = re.findall(r'\d{8,}', source)
    account_number = digit_matches[0] if digit_matches else ''
    currency = _extract_currency_from_text(source)

    bank_name = ''
    for label in ('\u5f00\u6237\u94f6\u884c', '\u5f00\u6237\u884c'):
        if label in source:
            after = source.split(label, 1)[-1]
            bank_name = _extract_bank_name_from_line(after)
            if bank_name:
                break
    bank_name = bank_name or _extract_bank_name_from_line(source)

    account_name = ''
    for label in ('\u6237\u540d', '\u8d26\u6237\u540d\u79f0', '\u8d26\u6237\u540d'):
        if label in source:
            after = source.split(label, 1)[-1]
            account_name = _extract_company_name_from_line(after) or _clean_account_name_candidate(after)
            if account_name:
                break
    account_name = account_name or _extract_company_name_from_line(source)

    return {
        'account_number': account_number,
        'account_name': account_name,
        'bank_name': bank_name,
        'currency': currency,
    }


def extract_bank_statement_pdf_fields(text: str) -> dict[str, Any]:
    source = str(text or '')
    lines = [str(line or '').strip() for line in source.splitlines() if str(line or '').strip()]
    header_lines = _header_search_region(lines)
    header_text = '\n'.join(header_lines)

    def _safe_field(field_name: str, extractor: Callable[[], str]) -> str:
        try:
            value = str(extractor() or '').strip()
            if not value:
                logger.warning('bank_statement pdf field missing: %s', field_name)
            return value
        except Exception as exc:
            logger.warning('bank_statement pdf field parse fallback: %s (%s)', field_name, exc)
            return ''

    def _find_header_line(labels: list[str]) -> str:
        for line in header_lines:
            if any(label in line for label in labels):
                return line
        return ''

    header_candidates = [
        _find_header_line(['\u9009\u62e9\u8d26\u53f7', '\u8d26\u53f7']),
        _find_header_line(['\u6237\u540d', '\u8d26\u6237\u540d\u79f0', '\u8d26\u6237\u540d']),
        _find_header_line(['\u5f00\u6237\u884c', '\u5f00\u6237\u94f6\u884c']),
        _find_header_line(['\u5e01\u79cd']),
    ]
    header_candidates = [line for line in header_candidates if line]
    merged_header = ' '.join(dict.fromkeys(header_candidates)) if header_candidates else header_text or source[:500]
    split_result = _split_header_line(merged_header)

    start_date, end_date = _extract_date_range_from_text(header_text or source)

    account_number = _safe_field('account_number', lambda: split_result['account_number'] or _find_longest_digits(merged_header or header_text or source, min_length=10))
    account_name = _safe_field('account_name', lambda: split_result['account_name'] or _extract_longest_company_name(header_text or merged_header or source[:500]))
    bank_name = _safe_field('bank_name', lambda: split_result['bank_name'] or _extract_longest_bank_name(header_text or merged_header or source[:500]))
    currency = _safe_field('currency', lambda: split_result['currency'] or _extract_currency_from_text(header_text or merged_header or source[:300]))

    opening_balance = _safe_field(
        'opening_balance',
        lambda: _extract_full_amount_after_label(header_text or source, ('\u671f\u521d\u4f59\u989d', '\u4e0a\u671f\u4f59\u989d', '\u8d77\u59cb\u4f59\u989d'))
        or _extract_full_amount_after_label(source, ('\u671f\u521d\u4f59\u989d', '\u4e0a\u671f\u4f59\u989d', '\u8d77\u59cb\u4f59\u989d')),
    )
    closing_balance = _safe_field(
        'closing_balance',
        lambda: _extract_full_amount_after_label(header_text or source, ('\u671f\u672b\u4f59\u989d', '\u5f53\u524d\u4f59\u989d', '\u8d26\u6237\u4f59\u989d'))
        or _extract_full_amount_after_label(source, ('\u671f\u672b\u4f59\u989d', '\u5f53\u524d\u4f59\u989d', '\u8d26\u6237\u4f59\u989d')),
    )
    total_income = _safe_field(
        'total_income',
        lambda: _extract_full_amount_after_label(header_text or source, ('\u8d37\u65b9\u603b\u91d1\u989d', '\u6536\u5165\u5408\u8ba1', '\u603b\u6536\u5165'))
        or _extract_full_amount_after_label(source, ('\u8d37\u65b9\u603b\u91d1\u989d', '\u6536\u5165\u5408\u8ba1', '\u603b\u6536\u5165')),
    )
    total_expense = _safe_field(
        'total_expense',
        lambda: _extract_full_amount_after_label(header_text or source, ('\u501f\u65b9\u603b\u91d1\u989d', '\u652f\u51fa\u5408\u8ba1', '\u603b\u652f\u51fa'))
        or _extract_full_amount_after_label(source, ('\u501f\u65b9\u603b\u91d1\u989d', '\u652f\u51fa\u5408\u8ba1', '\u603b\u652f\u51fa')),
    )
    transaction_count = _safe_field(
        'transaction_count',
        lambda: _extract_count_after_label(header_text or source, ('\u603b\u7b14\u6570', '\u4ea4\u6613\u7b14\u6570', '\u660e\u7ec6\u7b14\u6570', '\u53d1\u751f\u7b14\u6570'))
        or only_digits(_find_line_value_by_labels(lines, ['\u603b\u7b14\u6570', '\u4ea4\u6613\u7b14\u6570', '\u660e\u7ec6\u7b14\u6570'])),
    )

    monthly_avg_income = ''
    monthly_avg_expense = ''
    month_count = _months_between(start_date, end_date)
    total_income_decimal = _safe_decimal(total_income)
    total_expense_decimal = _safe_decimal(total_expense)
    if month_count and total_income_decimal is not None:
        monthly_avg_income = _format_decimal_string(total_income_decimal / Decimal(month_count))
    if month_count and total_expense_decimal is not None:
        monthly_avg_expense = _format_decimal_string(total_expense_decimal / Decimal(month_count))

    return {
        'account_name': account_name,
        'account_number': account_number,
        'bank_name': bank_name,
        'currency': currency,
        'start_date': start_date or _safe_field('start_date', lambda: _find_first_date(source)),
        'end_date': end_date or _safe_field('end_date', lambda: _find_last_date(source)),
        'opening_balance': opening_balance,
        'closing_balance': closing_balance,
        'total_income': total_income,
        'total_expense': total_expense,
        'monthly_avg_income': monthly_avg_income,
        'monthly_avg_expense': monthly_avg_expense,
        'transaction_count': transaction_count,
        'top_inflows': [],
        'top_outflows': [],
    }
