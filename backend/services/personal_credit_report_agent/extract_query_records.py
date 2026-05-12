from __future__ import annotations

import re
from typing import Any

from .evidence import clean_value
from .schema import QUERY_RECORD_FIELDS, ensure_record_fields

DATE_PATTERN = r"(?:19|20)\d{2}[-./年]\d{1,2}[-./月]\d{1,2}日?"
REASONS = ("贷款审批", "信用卡审批", "贷后管理", "担保资格审查", "本人查询", "异议查询")


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
