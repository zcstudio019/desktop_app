from __future__ import annotations

import re
from datetime import date
from typing import Any

from .evidence import clean_value
from .schema import QUERY_RECORD_FIELDS, ensure_record_fields

DATE_PATTERN = r"(?:19|20)\d{2}[-./年]\d{1,2}[-./月]\d{1,2}日?"
REASONS = ("贷款审批", "信用卡审批", "贷后管理", "担保资格审查", "本人查询", "异议查询")
COUNTED_REASON_PATTERNS = (
    "法人代表负责人高管等",
    "担保资格审查",
    "贷款审批",
)


def _normalize_line(line: str) -> str:
    return clean_value(re.sub(r"[ \t\u3000]+", " ", str(line or "")))


def _query_type(line: str, current_section: str) -> str:
    if "本人查询" in line or current_section == "本人查询记录明细":
        return "本人查询"
    if "机构查询" in line or current_section == "机构查询记录明细" or any(reason in line for reason in REASONS[:4]):
        return "机构查询"
    return ""


def _reason(line: str) -> str:
    for reason in REASONS:
        if reason in line:
            return reason
    return ""


def _institution(tail: str, reason: str, query_type: str) -> str:
    value = tail
    if reason:
        value = value.split(reason, 1)[0]
    value = re.sub(r"(机构查询|本人查询)$", "", value).strip()
    if query_type == "本人查询" and not value:
        return "本人"
    return clean_value(value)


def _empty_statistics() -> dict[str, dict[str, int]]:
    return {
        "institution_query": {"last_1_month": 0, "last_3_months": 0, "last_6_months": 0},
        "personal_query": {"last_1_month": 0, "last_3_months": 0, "last_6_months": 0},
    }


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"((?:19|20)\d{2})[-./年](\d{1,2})[-./月](\d{1,2})日?", text)
    if not match:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except Exception:
        return None


def _add_months(base: date, months: int) -> date:
    month = base.month + months
    year = base.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    month_days = (31, 29 if year % 400 == 0 or (year % 4 == 0 and year % 100 != 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    day = min(base.day, month_days[month - 1])
    return date(year, month, day)


def _is_counted_reason(value: Any) -> bool:
    text = re.sub(r"[\s、/／,，]+", "", str(value or ""))
    if "贷后管理" in text or "异议查询" in text or "本人查询信用报告" in text:
        return False
    return any(pattern in text for pattern in COUNTED_REASON_PATTERNS)


def _query_bucket(record: dict[str, Any]) -> str:
    query_type = str(record.get("query_type") or "")
    evidence = str(record.get("evidence") or record.get("evidence_text") or "")
    if any(keyword in query_type for keyword in ("个人查询", "本人查询", "个人")) or any(keyword in evidence for keyword in ("本人查询记录明细", "个人查询记录明细")):
        return "personal_query"
    if any(keyword in query_type for keyword in ("机构查询", "机构")) or "机构查询记录明细" in evidence:
        return "institution_query"
    return ""


def build_query_statistics(query_records: list[dict[str, Any]], report_time: str) -> dict[str, Any]:
    statistics: dict[str, Any] = _empty_statistics()
    warnings: list[str] = []
    try:
        reference = _parse_date(report_time)
        if not reference:
            warnings.append("查询记录统计缺少报告时间，无法计算近1/3/6个月查询次数")
            statistics["warnings"] = warnings
            return statistics
        thresholds = {
            "last_1_month": _add_months(reference, -1),
            "last_3_months": _add_months(reference, -3),
            "last_6_months": _add_months(reference, -6),
        }
        for record in query_records or []:
            if not isinstance(record, dict):
                continue
            reason = str(record.get("query_reason") or record.get("evidence") or record.get("evidence_text") or "")
            if not _is_counted_reason(reason):
                continue
            query_date = _parse_date(record.get("query_date"))
            if not query_date:
                if record.get("query_date"):
                    warnings.append(f"query_date_parse_failed: {record.get('query_date')}")
                continue
            if query_date > reference:
                continue
            bucket = _query_bucket(record)
            if not bucket:
                continue
            for key, threshold in thresholds.items():
                if query_date >= threshold:
                    statistics[bucket][key] += 1
        if warnings:
            statistics["warnings"] = warnings
        return statistics
    except Exception as exc:
        statistics["warnings"] = [f"query_statistics_failed: {exc}"]
        return statistics


def extract_query_records(sections: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        text = str(sections.get("query_records") or sections.get("full_text") or "")
        records: list[dict[str, Any]] = []
        current_section = ""
        for raw_line in text.splitlines():
            line = _normalize_line(raw_line)
            if not line:
                continue
            if "机构查询记录明细" in line:
                current_section = "机构查询记录明细"
                continue
            if "本人查询记录明细" in line:
                current_section = "本人查询记录明细"
                continue
            date_match = re.search(DATE_PATTERN, line)
            if not date_match:
                continue
            if not any(keyword in line for keyword in ("查询", "审批", "贷后管理", "担保资格审查", "本人", "机构")):
                continue
            query_date = clean_value(date_match.group(0))
            tail = _normalize_line(line[date_match.end():])
            reason = _reason(line)
            query_type = _query_type(line, current_section)
            record = {
                "query_date": query_date,
                "query_institution": _institution(tail, reason, query_type),
                "query_reason": reason or ("本人查询" if query_type == "本人查询" else ""),
                "query_type": query_type,
                "evidence": line,
                "evidence_text": line,
            }
            records.append(ensure_record_fields(record, QUERY_RECORD_FIELDS))
        return records
    except Exception:
        return []
