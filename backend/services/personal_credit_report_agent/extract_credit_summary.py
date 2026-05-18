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

SUMMARY_FIELD_ORDER: tuple[str, ...] = (
    "credit_card_account_count",
    "active_credit_card_account_count",
    "loan_account_count",
    "housing_loan_account_count",
    "other_loan_account_count",
    "outstanding_loan_account_count",
    "housing_loan_outstanding_count",
    "other_loan_outstanding_count",
    "credit_card_overdue_account_count",
    "credit_card_90d_overdue_account_count",
    "loan_overdue_account_count",
    "loan_90d_overdue_account_count",
    "personal_related_repayment_responsibility_account_count",
    "enterprise_related_repayment_responsibility_account_count",
)

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


def _parse_value_sequence(value_region: str) -> list[str]:
    text = _normalize_text(value_region)
    text = re.sub(r"[|]+", " ", text)
    text = re.sub(r"(?i)\b(?:item|project|status|count)\b", " ", text)
    text = re.sub(r"(项目|数量|状态|数量\s*/\s*状态)", " ", text)
    values: list[str] = []
    for match in re.finditer(r"(?<!\d)(\d+)(?:\s*/\s*([^\d|,，；;。\n\r]{1,40}))?(?!\d)", text):
        number = match.group(1).strip()
        if re.fullmatch(r"20[2-3]\d", number):
            continue
        suffix = re.sub(r"\s+", "", (match.group(2) or "")).strip()
        if suffix:
            value = f"{number} / {suffix}"
        else:
            value = number
        values.append(value)
    if values:
        return values
    for match in re.finditer(r"(未显示为有效|未显示|未识别)", text):
        values.append(match.group(1).strip())
    return values


def parse_matrix_tokens(region: str, max_tokens: int) -> list[str]:
    text = _normalize_text(region)
    for marker in (
        "逾期记录可能影响对您的信用评价",
        "购房贷款,包括",
        "购房贷款，包括",
        "发生过逾期的信用卡账户",
        "指曾经",
        "透支超过",
    ):
        marker_index = text.find(marker)
        if marker_index >= 0:
            ignored_region = text[marker_index:]
            ignored_number = re.search(r"(?<!\d)(\d{1,3})(?!\d)", ignored_region)
            if ignored_number:
                logger.info(
                    "[PersonalCredit][Summary][IGNORE_EXPLANATION_NUMBER] value=%s reason=right_side_explanation",
                    ignored_number.group(1),
                )
            logger.info("[PersonalCredit][Summary][IGNORE_EXPLANATION_NUMBER] reason=right_side_explanation marker=%s", marker)
            text = text[:marker_index]
            break
    text = text[:80]
    tokens: list[str] = []
    for match in re.finditer(r"(--|——|-|未显示|0\s*/\s*未显示(?:为有效)?|(?<!\d)\d{1,3}(?!\d))", text):
        token = match.group(1).strip()
        if re.fullmatch(r"\d{4,}", token):
            continue
        tokens.append(re.sub(r"\s*/\s*", " / ", token))
        if len(tokens) >= max_tokens:
            break
    return tokens


def _matrix_dash(token: str, *, active: bool = False, responsibility: bool = False, overdue: bool = False) -> str:
    del active, responsibility, overdue
    text = str(token or "").strip()
    if text in {"--", "——", "-", "未显示", "0 / 未显示", "0 / 未显示为有效"}:
        logger.info("[PersonalCredit][Summary][NORMALIZE_DASH_TO_ZERO] raw=%s value=0", text)
        return "0"
    return text


def _token_int(value: Any) -> int | None:
    text = str(value or "").strip()
    if text in {"--", "——", "-", "未显示", "0 / 未显示", "0 / 未显示为有效"}:
        return 0
    match = re.search(r"\d+", text)
    return int(match.group(0)) if match else None


def _sum_tokens(*values: Any) -> str:
    numbers = [_token_int(value) for value in values]
    numbers = [number for number in numbers if number is not None]
    return str(sum(numbers)) if numbers else ""


MATRIX_ROW_PATTERNS: tuple[tuple[str, str, re.Pattern[str]], ...] = (
    ("overdue_90d", "发生过90天以上逾期的账户数", _label_pattern("发生过90天以上逾期的账户数")),
    ("overdue", "发生过逾期的账户数", _label_pattern("发生过逾期的账户数")),
    ("active", "未结清/未销户账户数", _label_pattern("未结清/未销户账户数")),
    ("responsibility", "相关还款责任账户数", _label_pattern("相关还款责任账户数")),
    ("account", "账户数", _label_pattern("账户数")),
)


def _find_matrix_rows(text: str) -> list[LabelMatch]:
    rows: list[LabelMatch] = []
    occupied: list[tuple[int, int]] = []
    for row_key, label, pattern in MATRIX_ROW_PATTERNS:
        for match in pattern.finditer(text):
            start, end = match.span()
            if any(not (end <= used_start or start >= used_end) for used_start, used_end in occupied):
                continue
            rows.append(LabelMatch(start=start, end=end, field=row_key, label=label))
            occupied.append((start, end))
    return sorted(rows, key=lambda item: item.start)


def _matrix_row_regions(text: str, rows: list[LabelMatch]) -> dict[str, str]:
    row_regions: dict[str, str] = {}
    for index, row in enumerate(rows):
        next_start = rows[index + 1].start if index + 1 < len(rows) else len(text)
        row_regions[row.field] = text[row.end:next_start]
    return row_regions


def _apply_matrix_row_values(result: dict[str, str], row_regions: dict[str, str]) -> None:
    account_tokens = parse_matrix_tokens(row_regions.get("account", ""), 4)
    if account_tokens:
        logger.info("[PersonalCredit][Summary][MATRIX_ROW] row=账户数 tokens=%s", account_tokens)
        if len(account_tokens) >= 1:
            result["credit_card_account_count"] = _matrix_dash(account_tokens[0])
        if len(account_tokens) >= 2:
            result["housing_loan_account_count"] = _matrix_dash(account_tokens[1])
        if len(account_tokens) >= 3:
            result["other_loan_account_count"] = _matrix_dash(account_tokens[2])
            result["loan_account_count"] = _sum_tokens(account_tokens[1], account_tokens[2]) or f"购房 {_matrix_dash(account_tokens[1])} / 其他 {_matrix_dash(account_tokens[2])}"

    active_tokens = parse_matrix_tokens(row_regions.get("active", ""), 4)
    if active_tokens:
        logger.info("[PersonalCredit][Summary][MATRIX_ROW] row=未结清/未销户账户数 tokens=%s", active_tokens)
        if len(active_tokens) >= 1:
            result["active_credit_card_account_count"] = _matrix_dash(active_tokens[0], active=True)
        if len(active_tokens) >= 2:
            result["housing_loan_outstanding_count"] = _matrix_dash(active_tokens[1])
        if len(active_tokens) >= 3:
            result["other_loan_outstanding_count"] = _matrix_dash(active_tokens[2])
            result["outstanding_loan_account_count"] = _sum_tokens(active_tokens[1], active_tokens[2]) or f"购房 {_matrix_dash(active_tokens[1])} / 其他 {_matrix_dash(active_tokens[2])}"

    overdue_tokens = parse_matrix_tokens(row_regions.get("overdue", ""), 4)
    if overdue_tokens:
        logger.info("[PersonalCredit][Summary][MATRIX_ROW] row=发生过逾期的账户数 tokens=%s", overdue_tokens)
        if len(overdue_tokens) >= 1:
            result["credit_card_overdue_account_count"] = _matrix_dash(overdue_tokens[0], overdue=True)
        if len(overdue_tokens) >= 3:
            result["loan_overdue_account_count"] = _matrix_dash(overdue_tokens[2], overdue=True)

    overdue_90d_tokens = parse_matrix_tokens(row_regions.get("overdue_90d", ""), 4)
    if overdue_90d_tokens:
        logger.info("[PersonalCredit][Summary][MATRIX_ROW] row=发生过90天以上逾期的账户数 tokens=%s", overdue_90d_tokens)
        if len(overdue_90d_tokens) >= 1:
            result["credit_card_90d_overdue_account_count"] = _matrix_dash(overdue_90d_tokens[0], overdue=True)
        if len(overdue_90d_tokens) >= 3:
            result["loan_90d_overdue_account_count"] = _matrix_dash(overdue_90d_tokens[2], overdue=True)

    responsibility_tokens = parse_matrix_tokens(row_regions.get("responsibility", ""), 2)
    if responsibility_tokens:
        logger.info("[PersonalCredit][Summary][MATRIX_ROW] row=相关还款责任账户数 tokens=%s", responsibility_tokens)
        if len(responsibility_tokens) >= 1:
            result["personal_related_repayment_responsibility_account_count"] = _matrix_dash(responsibility_tokens[0], responsibility=True)
        if len(responsibility_tokens) >= 2:
            result["enterprise_related_repayment_responsibility_account_count"] = _matrix_dash(responsibility_tokens[1], responsibility=True)
        logger.info(
            "[PersonalCredit][Summary][RELATED_RESPONSIBILITY] personal=%s enterprise=%s",
            result.get("personal_related_repayment_responsibility_account_count"),
            result.get("enterprise_related_repayment_responsibility_account_count"),
        )


def _extract_partial_matrix_rows(text: str) -> dict[str, str]:
    """Parse high-risk single matrix rows even when the full table is fragmented."""
    result: dict[str, str] = {}
    for row_key, _, pattern in MATRIX_ROW_PATTERNS:
        if row_key not in {"overdue_90d", "responsibility"}:
            continue
        match = pattern.search(text)
        if not match:
            continue
        next_start = len(text)
        for other_key, _, other_pattern in MATRIX_ROW_PATTERNS:
            for next_match in other_pattern.finditer(text, match.end()):
                if next_match.start() < next_start:
                    next_start = next_match.start()
        _apply_matrix_row_values(result, {row_key: text[match.end():next_start]})
    return result


def extract_summary_matrix_from_ocr_window(window: str) -> dict[str, str]:
    try:
        compact_text = re.sub(r"\s+", " ", _normalize_text(window)).strip()
        rows = _find_matrix_rows(compact_text)
        detected = len({row.field for row in rows}) >= 3
        logger.info("[PersonalCredit][Summary][MATRIX] detected=%s", detected)
        if not detected:
            return _extract_partial_matrix_rows(compact_text)

        row_regions = _matrix_row_regions(compact_text, rows)

        result: dict[str, str] = {}
        _apply_matrix_row_values(result, row_regions)

        logger.info(
            "[PersonalCredit][Summary][MATRIX_PARSED] credit_card_account_count=%s loan_account_count=%s outstanding_loan_account_count=%s enterprise_related=%s",
            result.get("credit_card_account_count"),
            result.get("loan_account_count"),
            result.get("outstanding_loan_account_count"),
            result.get("enterprise_related_repayment_responsibility_account_count"),
        )
        return {key: value for key, value in result.items() if value}
    except Exception as exc:
        logger.info("[PersonalCredit][Summary][MATRIX] failed error=%s", exc)
        return {}


def _strip_label_text(window: str, matches: list[LabelMatch]) -> str:
    if not matches:
        return window
    parts: list[str] = []
    cursor = 0
    for match in sorted(matches, key=lambda item: item.start):
        parts.append(window[cursor:match.start])
        cursor = max(cursor, match.end)
    parts.append(window[cursor:])
    return "".join(parts)


def extract_by_known_label_order_and_value_sequence(window: str) -> dict[str, str]:
    """Fallback for OCR tables where labels are grouped before the value row."""
    try:
        matches = [match for match in _find_label_matches(window) if match.field in FIELD_LABELS]
        found_fields = {match.field for match in matches}
        if len(found_fields) < 6:
            return {}

        ordered_matches = sorted(matches, key=lambda item: item.start)
        label_block_end = max(match.end for match in ordered_matches)
        trailing_text = window[label_block_end:]
        values = _parse_value_sequence(trailing_text)
        if len(values) < 2:
            values = _parse_value_sequence(_strip_label_text(window, ordered_matches))
        if not values:
            return {}

        fields_by_position = list(dict.fromkeys(match.field for match in ordered_matches if match.field in FIELD_LABELS))
        fixed_fields = [field for field in SUMMARY_FIELD_ORDER if field in found_fields]
        ordered_fields = fixed_fields if fields_by_position == fixed_fields else fields_by_position
        result: dict[str, str] = {}
        for field, value in zip(ordered_fields, values):
            if value:
                result[field] = value
                logger.info("[PersonalCredit][Summary][PARSED] key=%s value=%s", field, value)
        return result
    except Exception as exc:
        logger.info("[PersonalCredit][Summary] value sequence fallback failed error=%s", exc)
        return {}


def extract_value_after_label(raw_text: str, label_start: int, label_end: int, next_label_start: int | None) -> str:
    del label_start
    end = next_label_start if next_label_start is not None else len(raw_text)
    return parse_summary_value(raw_text[label_end:end])


def fallback_extract_from_numeric_sequence(window: str) -> dict[str, str]:
    values: dict[str, str] = {}
    matches = _find_label_matches(window)
    for index, match in enumerate(matches):
        next_start = matches[index + 1].start if index + 1 < len(matches) else None
        logger.info(
            "[PersonalCredit][Summary][LABEL_POS] key=%s label=%s start=%s end=%s",
            match.field,
            match.label,
            match.start,
            match.end,
        )
        logger.info(
            "[PersonalCredit][Summary][VALUE_REGION] key=%s region=%s",
            match.field,
            window[match.end:next_start if next_start is not None else len(window)][:300],
        )
        value = extract_value_after_label(window, match.start, match.end, next_start)
        if value and not values.get(match.field):
            values[match.field] = value
            logger.info("[PersonalCredit][Summary][PARSED] key=%s value=%s", match.field, value)
    return values


def _extract_all_values(window: str) -> dict[str, str]:
    values = fallback_extract_from_numeric_sequence(window)
    sequence_values = extract_by_known_label_order_and_value_sequence(window)
    should_override = len(sequence_values) >= 6
    for field, value in sequence_values.items():
        if value and (should_override or not values.get(field)):
            values[field] = value
    matrix_values = extract_summary_matrix_from_ocr_window(window)
    for field, value in matrix_values.items():
        if value:
            values[field] = value
    return values


def _to_int(value: Any) -> int | None:
    match = re.search(r"\d+", str(value or ""))
    return int(match.group(0)) if match else None


def _sum_values(*values: Any) -> str:
    numbers = [_to_int(value) for value in values]
    numbers = [number for number in numbers if number is not None]
    return str(sum(numbers)) if numbers else ""


def _safe_row_region(text: str, row_label: str) -> str:
    pattern = _label_pattern(row_label)
    match = pattern.search(text)
    if not match:
        return ""
    next_start = len(text)
    for _, other_label, other_pattern in MATRIX_ROW_PATTERNS:
        if other_label == row_label:
            continue
        for next_match in other_pattern.finditer(text, match.end()):
            if next_match.start() < next_start:
                next_start = next_match.start()
    return text[match.end():next_start]


def _force_matrix_corrections(summary: dict[str, Any], sections: dict[str, Any]) -> None:
    """Final guard for matrix rows where prose numbers can pollute key-value fallback."""
    full_text = _normalize_text("\n".join(
        str(sections.get(key) or "")
        for key in ("credit_summary", "information_summary", "credit_overview", "full_text")
        if sections.get(key)
    ))
    compact_text = re.sub(r"\s+", " ", full_text).strip()
    if not compact_text:
        return

    overdue_90d_region = _safe_row_region(compact_text, "发生过90天以上逾期的账户数")
    overdue_90d_tokens = parse_matrix_tokens(overdue_90d_region, 4)
    if overdue_90d_tokens:
        logger.info("[PersonalCredit][Summary][MATRIX_ROW] row=发生过90天以上逾期的账户数 tokens=%s", overdue_90d_tokens)
        summary["credit_card_90d_overdue_account_count"] = _matrix_dash(overdue_90d_tokens[0], overdue=True)
        if len(overdue_90d_tokens) >= 3:
            summary["loan_90d_overdue_account_count"] = _matrix_dash(overdue_90d_tokens[2], overdue=True)

    responsibility_region = _safe_row_region(compact_text, "相关还款责任账户数")
    responsibility_tokens = parse_matrix_tokens(responsibility_region, 2)
    if responsibility_tokens:
        logger.info("[PersonalCredit][Summary][MATRIX_ROW] row=相关还款责任账户数 tokens=%s", responsibility_tokens)
        summary["personal_related_repayment_responsibility_account_count"] = _matrix_dash(responsibility_tokens[0], responsibility=True)
        if len(responsibility_tokens) >= 2:
            summary["enterprise_related_repayment_responsibility_account_count"] = _matrix_dash(responsibility_tokens[1], responsibility=True)
        logger.info(
            "[PersonalCredit][Summary][RELATED_RESPONSIBILITY] personal=%s enterprise=%s",
            summary.get("personal_related_repayment_responsibility_account_count"),
            summary.get("enterprise_related_repayment_responsibility_account_count"),
        )


def extract_credit_summary(sections: dict[str, Any]) -> dict[str, Any]:
    result = default_credit_summary()
    try:
        extracted: dict[str, str] = {}
        for source_name, source_text in _source_windows(sections):
            logger.info("[PersonalCredit][Summary] source=%s len=%s", source_name, len(source_text))
            logger.info("[PersonalCredit][Summary][RAW_WINDOW_START]\n%s\n[PersonalCredit][Summary][RAW_WINDOW_END]", source_text[:2500])
            source_values = _extract_all_values(source_text)
            for field, value in source_values.items():
                if value and not extracted.get(field):
                    extracted[field] = value
            if all(extracted.get(field) for field in FIELD_LABELS):
                break

        for field in SUMMARY_FIELD_ORDER:
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
        _force_matrix_corrections(result, sections)
        return result
    except Exception as exc:
        logger.info("[PersonalCredit][Summary] extraction failed error=%s", exc)
        return result
