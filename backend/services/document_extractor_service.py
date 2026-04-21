"""Structured extraction helpers for upload/chat document parsing."""

from __future__ import annotations

import json
import re
from typing import Any

from backend.document_types import (
    get_document_display_name,
    get_document_storage_label,
    normalize_document_type_code,
)
from prompts import get_prompt_for_type, load_prompts
from utils.json_parser import parse_json


_KEYWORD_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("business_license", ("营业执照", "统一社会信用代码", "法定代表人")),
    ("id_card", ("居民身份证", "公民身份号码", "住址")),
    ("marriage_cert", ("结婚证", "婚姻登记", "持证人")),
    ("hukou", ("户口簿", "户主", "与户主关系")),
    ("property_report", ("不动产", "房屋坐落", "权利人")),
    ("account_license", ("开户许可证", "开户银行", "核准号")),
    ("special_license", ("许可证", "有效期", "发证机关")),
    ("company_articles", ("公司章程", "股东", "注册资本")),
    ("bank_statement_detail", ("交易明细", "对方户名", "借方发生额", "贷方发生额")),
    ("bank_statement", ("对账单", "账户余额", "期末余额")),
    ("contract", ("合同", "甲方", "乙方", "金额", "期限")),
    ("enterprise_credit", ("企业征信", "企业信用", "授信")),
    ("personal_credit", ("个人征信", "个人信用报告", "信用卡")),
    ("enterprise_flow", ("对公", "企业流水", "单位名称")),
    ("personal_flow", ("个人流水", "账户名称", "卡号")),
    ("financial_data", ("资产负债表", "利润表", "现金流量表")),
    ("collateral", ("抵押物", "不动产权证", "权属人")),
    ("jellyfish_report", ("水母报告", "开票", "税务")),
    ("personal_tax", ("个税", "公积金", "纳税")),
)

_MONEY_PATTERN = re.compile(r"(?:人民币|金额|总金额|合同金额|价税合计|¥|￥)?\s*([0-9][0-9,]*(?:\.\d{1,2})?)")
_ID_CARD_PATTERN = re.compile(r"([1-9]\d{5}(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx])")
_UNIFIED_CODE_PATTERN = re.compile(r"([0-9A-Z]{18})")


def detect_document_type_code(
    text_content: str,
    explicit_type: str | None = None,
    ai_service: Any | None = None,
) -> str:
    normalized_explicit = normalize_document_type_code(explicit_type)
    if normalized_explicit:
        return normalized_explicit

    lower_text = (text_content or "").lower()
    for code, keywords in _KEYWORD_RULES:
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
    ai_service: Any | None = None,
) -> dict[str, Any]:
    normalized_code = normalize_document_type_code(document_type_code) or document_type_code
    extractor = _TYPE_EXTRACTORS.get(normalized_code)
    if extractor is not None:
        content = extractor(text_content)
    else:
        content = _extract_with_ai(text_content, normalized_code, ai_service)

    if "document_type_code" not in content:
        content["document_type_code"] = normalized_code
    if "document_type_name" not in content:
        content["document_type_name"] = get_document_display_name(normalized_code)
    if "storage_label" not in content:
        content["storage_label"] = get_document_storage_label(normalized_code)
    return content


def _extract_with_ai(text_content: str, document_type_code: str, ai_service: Any | None) -> dict[str, Any]:
    if ai_service is None:
        return {
            "摘要": _clean_line(text_content[:400]) or "暂无可解析内容",
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


def _first(pattern: re.Pattern[str], text: str) -> str:
    match = pattern.search(text or "")
    return match.group(1).strip() if match else ""


def _find_after_labels(text: str, labels: tuple[str, ...]) -> str:
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}[:：]?\s*([^\n\r；;，,]+)")
        match = pattern.search(text or "")
        if match:
            return match.group(1).strip()
    return ""


def _clean_line(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _amount_from_lines(text: str, keywords: tuple[str, ...]) -> float:
    total = 0.0
    for line in (text or "").splitlines():
        if not any(keyword in line for keyword in keywords):
            continue
        for match in _MONEY_PATTERN.findall(line):
            try:
                total += float(match.replace(",", ""))
            except ValueError:
                continue
    return round(total, 2)


def _extract_contract(text: str) -> dict[str, Any]:
    return {
        "对方主体": _find_after_labels(text, ("乙方", "相对方", "合作方", "客户名称")),
        "金额": _first(_MONEY_PATTERN, text),
        "期限": _find_after_labels(text, ("合同期限", "服务期限", "履行期限", "租赁期限")),
        "摘要": _clean_line(text[:200]),
    }


def _extract_id_card(text: str) -> dict[str, Any]:
    return {
        "姓名": _find_after_labels(text, ("姓名",)),
        "身份证号": _first(_ID_CARD_PATTERN, text),
        "地址": _find_after_labels(text, ("住址", "地址")),
    }


def _extract_business_license(text: str) -> dict[str, Any]:
    return {
        "公司名称": _find_after_labels(text, ("名称", "企业名称", "公司名称")),
        "统一社会信用代码": _first(_UNIFIED_CODE_PATTERN, text),
        "注册资本": _find_after_labels(text, ("注册资本",)),
        "法人": _find_after_labels(text, ("法定代表人", "法人")),
    }


def _extract_bank_statement(text: str) -> dict[str, Any]:
    income = _amount_from_lines(text, ("收入", "贷方", "入账"))
    expense = _amount_from_lines(text, ("支出", "借方", "出账"))
    trend = "平稳"
    if income and expense and expense > income * 1.2:
        trend = "支出高于收入"
    elif income and expense and income > expense * 1.2:
        trend = "收入高于支出"
    elif "下降" in text or "减少" in text:
        trend = "余额下降"
    elif "增长" in text or "增加" in text:
        trend = "余额上升"
    return {
        "收入": income,
        "支出": expense,
        "余额趋势": trend,
        "摘要": _clean_line(text[:240]),
    }


def _extract_bank_statement_detail(text: str) -> dict[str, Any]:
    income = _amount_from_lines(text, ("贷", "收入", "入账"))
    expense = _amount_from_lines(text, ("借", "支出", "出账"))
    return {
        "收入": income,
        "支出": expense,
        "余额趋势": "详见对账明细",
        "交易摘要": _clean_line(text[:240]),
    }


def _extract_marriage_cert(text: str) -> dict[str, Any]:
    return {
        "持证人一": _find_after_labels(text, ("姓名", "持证人")),
        "持证人二": _find_after_labels(text, ("配偶姓名", "另一方")),
        "登记日期": _find_after_labels(text, ("登记日期", "领证日期", "结婚登记日期")),
    }


def _extract_hukou(text: str) -> dict[str, Any]:
    return {
        "户主": _find_after_labels(text, ("户主姓名", "户主")),
        "地址": _find_after_labels(text, ("住址", "地址")),
        "关系": _find_after_labels(text, ("与户主关系", "关系")),
    }


def _extract_property_report(text: str) -> dict[str, Any]:
    return {
        "房屋坐落": _find_after_labels(text, ("房屋坐落", "坐落")),
        "权利人": _find_after_labels(text, ("权利人", "所有权人")),
        "建筑面积": _find_after_labels(text, ("建筑面积", "面积")),
    }


def _extract_account_license(text: str) -> dict[str, Any]:
    return {
        "账户名称": _find_after_labels(text, ("账户名称", "户名")),
        "开户行": _find_after_labels(text, ("开户银行", "开户行")),
        "账号": _find_after_labels(text, ("账号", "银行账号")),
    }


def _extract_special_license(text: str) -> dict[str, Any]:
    return {
        "许可证名称": _find_after_labels(text, ("许可证名称", "许可项目", "证书名称")),
        "许可证编号": _find_after_labels(text, ("许可证编号", "证书编号", "编号")),
        "有效期": _find_after_labels(text, ("有效期", "有效期限")),
    }


def _extract_company_articles(text: str) -> dict[str, Any]:
    return {
        "公司名称": _find_after_labels(text, ("公司名称", "名称")),
        "注册资本": _find_after_labels(text, ("注册资本",)),
        "股东结构摘要": _clean_line(text[:260]),
    }


_TYPE_EXTRACTORS: dict[str, Any] = {
    "contract": _extract_contract,
    "id_card": _extract_id_card,
    "marriage_cert": _extract_marriage_cert,
    "hukou": _extract_hukou,
    "property_report": _extract_property_report,
    "business_license": _extract_business_license,
    "account_license": _extract_account_license,
    "special_license": _extract_special_license,
    "company_articles": _extract_company_articles,
    "bank_statement": _extract_bank_statement,
    "bank_statement_detail": _extract_bank_statement_detail,
}

