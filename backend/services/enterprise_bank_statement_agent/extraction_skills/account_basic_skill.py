from __future__ import annotations

import re
from typing import Any

from ..evidence import make_evidence
from ..normalizer import normalize_account_number, normalize_amount, normalize_currency, normalize_date


LABELS = {
    "company_name": ("客户名称", "客户名", "户名", "账户名称", "单位名称", "企业名称"),
    "bank_name": ("银行名称", "所属银行", "开户银行"),
    "branch_name": ("开户行", "开户网点", "开户机构", "开户支行"),
    "account_number": ("账号", "账户", "银行账号", "账户号码", "账号/卡号"),
    "currency": ("币种", "货币"),
    "opening_balance": ("期初余额", "上期余额"),
    "closing_balance": ("期末余额", "本期余额"),
}


def _line_value(line: str, label: str) -> str:
    pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*([^\n\r|]+)")
    match = pattern.search(line)
    if not match:
        return ""
    value = match.group(1).strip(" :：\t")
    for known in {item for labels in LABELS.values() for item in labels}:
        value = re.split(rf"\s+{re.escape(known)}\s*[:：]?", value)[0].strip()
    return value


def extract_account_basic_info(segments: dict[str, Any]) -> tuple[dict[str, Any], list[str], list[dict[str, Any]]]:
    lines = segments.get("lines") or []
    full_text = segments.get("text") or ""
    result = {
        "company_name": "",
        "bank_name": "",
        "branch_name": "",
        "account_number": "",
        "currency": "人民币",
        "statement_period_start": "",
        "statement_period_end": "",
        "opening_balance": None,
        "closing_balance": None,
    }
    warnings: list[str] = []
    evidence: list[dict[str, Any]] = []

    for item in lines[:80]:
        line = str(item.get("text") or "")
        page = item.get("page")
        for field, labels in LABELS.items():
            if result.get(field) not in ("", None):
                continue
            for label in labels:
                value = _line_value(line, label)
                if not value:
                    continue
                if field == "company_name" and any(bank_word in value for bank_word in ("银行", "支行", "分行", "网点")):
                    warnings.append(f"疑似把开户行识别为客户名称，已留空：{value}")
                    continue
                if field == "bank_name" and any(word in value for word in ("有限公司", "公司", "集团")):
                    continue
                if field == "account_number":
                    value = normalize_account_number(value)
                elif field == "currency":
                    value = normalize_currency(value)
                elif field in {"opening_balance", "closing_balance"}:
                    value = normalize_amount(value)
                result[field] = value
                evidence.append(make_evidence(f"account_basic_info.{field}", value, line, page))
                break

    date_matches = re.findall(r"((?:19|20)\d{2}[年./-]\d{1,2}[月./-]\d{1,2}日?)", full_text)
    if len(date_matches) >= 2:
        dates = [normalize_date(item) for item in date_matches if normalize_date(item)]
        if dates:
            result["statement_period_start"] = min(dates)
            result["statement_period_end"] = max(dates)
    period_match = re.search(
        r"((?:19|20)\d{2}[年./-]\d{1,2}[月./-]\d{1,2}日?)\s*(?:至|到|-|—|~)\s*((?:19|20)\d{2}[年./-]\d{1,2}[月./-]\d{1,2}日?)",
        full_text,
    )
    if period_match:
        result["statement_period_start"] = normalize_date(period_match.group(1))
        result["statement_period_end"] = normalize_date(period_match.group(2))

    for field in ("company_name", "account_number"):
        if not result.get(field):
            warnings.append(f"未能确定账户基础信息字段：{field}")
    return result, warnings, evidence
