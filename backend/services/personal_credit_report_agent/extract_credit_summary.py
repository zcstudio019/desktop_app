from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any

from .schema import default_credit_summary


FIELD_LABELS: dict[str, tuple[str, ...]] = {
    "credit_card_90d_overdue_account_count": (
        "信用卡 90 天以上逾期账户数",
        "信用卡90天以上逾期账户数",
        "贷记卡 90 天以上逾期账户数",
        "贷记卡90天以上逾期账户数",
        "贷记卡发生过90天以上逾期",
    ),
    "credit_card_overdue_account_count": ("信用卡逾期账户数", "贷记卡逾期账户数", "信用卡发生过逾期"),
    "active_credit_card_account_count": ("当前有效信用卡账户数", "有效信用卡账户数", "未销户信用卡账户数", "贷记卡未销户"),
    "credit_card_account_count": ("信用卡账户数", "贷记卡账户数"),
    "loan_90d_overdue_account_count": (
        "贷款 90 天以上逾期账户数",
        "贷款90天以上逾期账户数",
        "贷款发生过90天以上逾期",
    ),
    "loan_overdue_account_count": ("贷款逾期账户数", "贷款发生过逾期"),
    "outstanding_loan_account_count": ("未结清贷款账户数", "未结清贷款账户", "当前有效贷款账户数"),
    "loan_account_count": ("贷款账户数",),
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


@dataclass(frozen=True)
class LabelMatch:
    start: int
    end: int
    field: str
    label: str


def canonical_label(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = re.sub(r"\s+", "", text)
    text = text.replace("九十天以上", "90天以上").replace("90以上", "90天以上")
    text = text.replace("90日以上", "90天以上")
    return text


def _normalize_text(text: str) -> str:
    source = unicodedata.normalize("NFKC", str(text or ""))
    source = source.replace("\r\n", "\n").replace("\r", "\n")
    source = re.sub(r"[ \t\u3000]+", " ", source)
    return source


def _flex_label_pattern(label: str) -> re.Pattern[str]:
    pieces = [re.escape(char) for char in unicodedata.normalize("NFKC", label) if not char.isspace()]
    return re.compile(r"\s*".join(pieces))


def _label_specs() -> list[tuple[str, str, re.Pattern[str]]]:
    specs: list[tuple[str, str, re.Pattern[str]]] = []
    for labels_by_field in (FIELD_LABELS, LEGACY_LABELS):
        for field, labels in labels_by_field.items():
            for label in labels:
                specs.append((field, label, _flex_label_pattern(label)))
    return sorted(specs, key=lambda item: len(canonical_label(item[1])), reverse=True)


LABEL_SPECS = _label_specs()


def _find_label_matches(line: str) -> list[LabelMatch]:
    matches: list[LabelMatch] = []
    occupied: list[tuple[int, int]] = []
    for field, label, pattern in LABEL_SPECS:
        for match in pattern.finditer(line):
            start, end = match.span()
            if any(not (end <= used_start or start >= used_end) for used_start, used_end in occupied):
                continue
            matches.append(LabelMatch(start=start, end=end, field=field, label=label))
            occupied.append((start, end))
    return sorted(matches, key=lambda item: item.start)


def _clean_value(value: str) -> str:
    text = re.sub(r"[ \t\u3000]+", " ", str(value or "")).strip()
    text = text.strip(" ：:|,，;；。")
    text = re.sub(r"^(?:数量\s*/\s*状态|数量|状态)\s*[:：|]?\s*", "", text)
    text = text.strip(" ：:|,，;；。")
    match = re.match(r"(\d+\s*(?:/\s*[^|,，;；。\n\r]{1,40})?)", text)
    if match:
        return re.sub(r"\s*/\s*", " / ", match.group(1)).strip()
    return ""


def _extract_from_segment(segment: str) -> str:
    return _clean_value(segment)


def _extract_all_values(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        matches = _find_label_matches(line)
        if not matches:
            continue
        for index, match in enumerate(matches):
            next_start = matches[index + 1].start if index + 1 < len(matches) else len(line)
            segment = line[match.end:next_start]
            value = _extract_from_segment(segment)
            if value and not values.get(match.field):
                values[match.field] = value
    return values


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
        extracted = _extract_all_values(text)
        result = default_credit_summary()
        for field in FIELD_LABELS:
            result[field] = extracted.get(field) or None

        if not result.get("loan_account_count"):
            result["loan_account_count"] = _sum_values(
                extracted.get("housing_loan_account_count"),
                extracted.get("other_loan_account_count"),
            ) or None
        if not result.get("outstanding_loan_account_count"):
            result["outstanding_loan_account_count"] = _sum_values(
                extracted.get("housing_loan_outstanding_count"),
                extracted.get("other_loan_outstanding_count"),
            ) or None
        if not result.get("loan_overdue_account_count"):
            result["loan_overdue_account_count"] = _sum_values(
                extracted.get("housing_loan_overdue_count"),
                extracted.get("other_loan_overdue_count"),
            ) or None
        return result
    except Exception:
        return default_credit_summary()
