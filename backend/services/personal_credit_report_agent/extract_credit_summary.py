from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Any

from .schema import default_credit_summary

logger = logging.getLogger(__name__)


FIELD_LABELS: dict[str, tuple[str, ...]] = {
    "credit_card_90d_overdue_account_count": (
        "信用卡90天以上逾期账户数",
        "信用卡九十天以上逾期账户数",
        "信用卡 90 天以上逾期账户数",
        "贷记卡90天以上逾期账户数",
        "贷记卡九十天以上逾期账户数",
        "贷记卡 90 天以上逾期账户数",
        "贷记卡发生过90天以上逾期",
    ),
    "credit_card_overdue_account_count": ("信用卡逾期账户数", "贷记卡逾期账户数", "信用卡发生过逾期"),
    "active_credit_card_account_count": ("当前有效信用卡账户数", "有效信用卡账户数", "未销户信用卡账户数", "贷记卡未销户"),
    "credit_card_account_count": ("信用卡账户数", "贷记卡账户数"),
    "loan_90d_overdue_account_count": (
        "贷款90天以上逾期账户数",
        "贷款九十天以上逾期账户数",
        "贷款 90 天以上逾期账户数",
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

WINDOW_ANCHORS = ("信贷记录概要", "信息概要", "信贷概要", "信用卡账户数")


@dataclass(frozen=True)
class LabelMatch:
    start: int
    end: int
    field: str
    label: str


def normalize_label_text(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = re.sub(r"[\s\u3000:：|]+", "", text)
    text = text.replace("九十天以上", "90天以上")
    text = text.replace("90天以上", "90天以上")
    text = text.replace("90以上", "90天以上")
    text = text.replace("90日以上", "90天以上")
    return text


def _normalize_text(text: str) -> str:
    source = unicodedata.normalize("NFKC", str(text or ""))
    source = source.replace("\r\n", "\n").replace("\r", "\n")
    source = re.sub(r"[ \t\u3000]+", " ", source)
    return source


def _label_pattern(label: str) -> re.Pattern[str]:
    normalized = unicodedata.normalize("NFKC", label)
    pieces = [re.escape(char) for char in normalized if not char.isspace()]
    return re.compile(r"\s*".join(pieces))


def build_label_patterns() -> list[tuple[str, str, re.Pattern[str]]]:
    specs: list[tuple[str, str, re.Pattern[str]]] = []
    for group in (FIELD_LABELS, LEGACY_LABELS):
        for field, labels in group.items():
            for label in labels:
                specs.append((field, label, _label_pattern(label)))
    return sorted(specs, key=lambda item: len(normalize_label_text(item[1])), reverse=True)


LABEL_PATTERNS = build_label_patterns()


def _find_label_matches(text: str) -> list[LabelMatch]:
    matches: list[LabelMatch] = []
    occupied: list[tuple[int, int]] = []
    for field, label, pattern in LABEL_PATTERNS:
        for match in pattern.finditer(text):
            start, end = match.span()
            if any(not (end <= used_start or start >= used_end) for used_start, used_end in occupied):
                continue
            matches.append(LabelMatch(start=start, end=end, field=field, label=label))
            occupied.append((start, end))
    return sorted(matches, key=lambda item: item.start)


def _source_windows(sections: dict[str, Any]) -> list[tuple[str, str]]:
    full_text = _normalize_text(str(sections.get("full_text") or ""))
    section_text = _normalize_text("\n".join(
        str(sections.get(key) or "")
        for key in ("credit_summary", "information_summary", "credit_overview")
        if sections.get(key)
    ))
    sources: list[tuple[str, str]] = []
    if section_text.strip():
        sources.append(("section", section_text))

    for anchor in WINDOW_ANCHORS:
        for match in re.finditer(_label_pattern(anchor), full_text):
            start = max(0, match.start() - 100)
            end = min(len(full_text), match.start() + 2500)
            window = full_text[start:end]
            if window.strip():
                sources.append(("window", window))
            break

    if full_text.strip():
        sources.append(("full_text", full_text[:4000]))
    return sources


def parse_summary_value(value_region: str) -> str:
    text = _normalize_text(value_region)
    text = re.sub(r"^(?:\s|[:：|,，;；。])+", "", text)
    text = re.sub(r"^(?:数量\s*/\s*状态|数量|状态)\s*[:：|]?\s*", "", text)
    text = text.strip(" \n\r\t:：|,，;；。")
    match = re.search(r"(?<!\d)(\d+\s*(?:/\s*[^|,，;；。\n\r]{1,40})?)(?!\d)", text)
    if not match:
        match = re.search(r"(未显示为有效|未显示|未识别)", text)
        return match.group(1).strip() if match else ""
    value = re.sub(r"\s*/\s*", " / ", match.group(1)).strip()
    if re.fullmatch(r"20[2-3]\d", value):
        return ""
    return value


def extract_value_after_label(raw_text: str, label_start: int, label_end: int, next_label_start: int | None) -> str:
    del label_start
    end = next_label_start if next_label_start is not None else len(raw_text)
    return parse_summary_value(raw_text[label_end:end])


def fallback_extract_from_numeric_sequence(window: str) -> dict[str, str]:
    values: dict[str, str] = {}
    matches = _find_label_matches(window)
    for index, match in enumerate(matches):
        next_start = matches[index + 1].start if index + 1 < len(matches) else None
        value = extract_value_after_label(window, match.start, match.end, next_start)
        if value and not values.get(match.field):
            values[match.field] = value
    return values


def _extract_all_values(window: str) -> dict[str, str]:
    values = fallback_extract_from_numeric_sequence(window)
    for field, value in values.items():
        logger.info("[PersonalCredit][Summary] matched label=%s value=%s", field, value)
    return values


def _to_int(value: Any) -> int | None:
    match = re.search(r"\d+", str(value or ""))
    return int(match.group(0)) if match else None


def _sum_values(*values: Any) -> str:
    numbers = [_to_int(value) for value in values]
    numbers = [number for number in numbers if number is not None]
    return str(sum(numbers)) if numbers else ""


def extract_credit_summary(sections: dict[str, Any]) -> dict[str, Any]:
    result = default_credit_summary()
    try:
        extracted: dict[str, str] = {}
        for source_name, source_text in _source_windows(sections):
            logger.info("[PersonalCredit][Summary] source=%s len=%s", source_name, len(source_text))
            source_values = _extract_all_values(source_text)
            for field, value in source_values.items():
                if value and not extracted.get(field):
                    extracted[field] = value
            if all(extracted.get(field) for field in FIELD_LABELS):
                break

        for field in FIELD_LABELS:
            result[field] = extracted.get(field) or None
            if not result[field]:
                logger.info("[PersonalCredit][Summary] missing label=%s", field)

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
    except Exception as exc:
        logger.info("[PersonalCredit][Summary] extraction failed error=%s", exc)
        return result
