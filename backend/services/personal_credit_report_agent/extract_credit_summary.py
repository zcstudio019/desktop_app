from __future__ import annotations

import re
import unicodedata
from typing import Any

from .schema import default_credit_summary


FIELD_LABELS: dict[str, tuple[str, ...]] = {
    "credit_card_account_count": ("信用卡账户数", "贷记卡账户数", "信用卡账户", "贷记卡账户"),
    "active_credit_card_account_count": ("当前有效信用卡账户数", "有效信用卡账户数", "未销户信用卡账户数", "贷记卡未销户"),
    "loan_account_count": ("贷款账户数", "贷款账户"),
    "outstanding_loan_account_count": ("未结清贷款账户数", "未结清贷款账户", "当前有效贷款账户数"),
    "credit_card_overdue_account_count": ("信用卡逾期账户数", "贷记卡逾期账户数", "信用卡发生过逾期"),
    "credit_card_90d_overdue_account_count": (
        "信用卡90天以上逾期账户数",
        "信用卡 90 天以上逾期账户数",
        "贷记卡90天以上逾期账户数",
        "贷记卡发生过90天以上逾期",
    ),
    "loan_overdue_account_count": ("贷款逾期账户数", "贷款发生过逾期"),
    "loan_90d_overdue_account_count": (
        "贷款90天以上逾期账户数",
        "贷款 90 天以上逾期账户数",
        "贷款发生过90天以上逾期",
    ),
    "personal_related_repayment_responsibility_account_count": ("为个人相关还款责任账户数", "个人相关还款责任账户数"),
    "enterprise_related_repayment_responsibility_account_count": ("为企业相关还款责任账户数", "企业相关还款责任账户数"),
}

LEGACY_LABELS: dict[str, tuple[str, ...]] = {
    "housing_loan_account_count": ("购房贷款账户数", "住房贷款账户数", "购房贷款账户", "住房贷款账户"),
    "housing_loan_outstanding_count": ("未结清购房贷款账户数", "购房贷款未结清", "住房贷款未结清"),
    "housing_loan_overdue_count": ("购房贷款逾期账户数", "购房贷款发生过逾期", "住房贷款发生过逾期"),
    "other_loan_account_count": ("其他贷款账户数", "其他贷款账户"),
    "other_loan_outstanding_count": ("未结清其他贷款账户数", "其他贷款未结清"),
    "other_loan_overdue_count": ("其他贷款逾期账户数", "其他贷款发生过逾期"),
}

VALUE_STOP_LABELS = tuple(sorted(
    {label for labels in (*FIELD_LABELS.values(), *LEGACY_LABELS.values()) for label in labels},
    key=len,
    reverse=True,
))


def _normalize_text(text: str) -> str:
    source = unicodedata.normalize("NFKC", str(text or ""))
    source = source.replace("\r\n", "\n").replace("\r", "\n")
    source = re.sub(r"[ \t\u3000]+", " ", source)
    return source


def _clean_value(value: str) -> str:
    text = re.sub(r"[ \t\u3000]+", " ", str(value or "")).strip(" ：:|,，;；。")
    for label in VALUE_STOP_LABELS:
        index = text.find(label)
        if index > 0:
            text = text[:index].strip(" ：:|,，;；。")
    match = re.match(r"(\d+\s*(?:/\s*[^|,，;；。\n\r]{1,40})?)", text)
    if match:
        return re.sub(r"\s*/\s*", " / ", match.group(1)).strip()
    return text[:60]


def _extract_value(text: str, labels: tuple[str, ...]) -> str:
    for label in labels:
        pattern = rf"{re.escape(label)}\s*(?:[:：|]|\s)\s*([^\n\r|]{{1,80}})"
        match = re.search(pattern, text)
        if match:
            value = _clean_value(match.group(1))
            if value:
                return value

        # Table/OCR serial form: 项目 | 数量/状态, or 项目    5
        pattern = rf"{re.escape(label)}[^\d\n\r]{{0,20}}(\d+\s*(?:/\s*[^|,，;；。\n\r]{{1,40}})?)"
        match = re.search(pattern, text)
        if match:
            return _clean_value(match.group(1))
    return ""


def _to_int(value: Any) -> int | None:
    match = re.search(r"\d+", str(value or ""))
    return int(match.group(0)) if match else None


def _sum_values(*values: Any) -> str:
    numbers = [_to_int(value) for value in values]
    numbers = [number for number in numbers if number is not None]
    return str(sum(numbers)) if numbers else ""


def extract_credit_summary(sections: dict[str, Any]) -> dict[str, Any]:
    try:
        text = _normalize_text("\n".join(
            str(sections.get(key) or "")
            for key in ("credit_summary", "full_text")
        ))
        result = default_credit_summary()
        for field, labels in FIELD_LABELS.items():
            result[field] = _extract_value(text, labels) or None

        legacy = {field: _extract_value(text, labels) for field, labels in LEGACY_LABELS.items()}
        if not result.get("loan_account_count"):
            result["loan_account_count"] = _sum_values(
                legacy.get("housing_loan_account_count"),
                legacy.get("other_loan_account_count"),
            ) or None
        if not result.get("outstanding_loan_account_count"):
            result["outstanding_loan_account_count"] = _sum_values(
                legacy.get("housing_loan_outstanding_count"),
                legacy.get("other_loan_outstanding_count"),
            ) or None
        if not result.get("loan_overdue_account_count"):
            result["loan_overdue_account_count"] = _sum_values(
                legacy.get("housing_loan_overdue_count"),
                legacy.get("other_loan_overdue_count"),
            ) or None
        return result
    except Exception:
        return default_credit_summary()
