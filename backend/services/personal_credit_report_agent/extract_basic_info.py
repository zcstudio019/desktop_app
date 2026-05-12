from __future__ import annotations

import re
from typing import Any

from .evidence import clean_value, value_after_label
from .schema import default_basic_info

ID_CARD_PATTERN = re.compile(
    r"(?<!\d)([1-9]\d{5}(?:(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx]|\d{9}))(?!\d)"
)
MARRIAGE_VALUES = ("未婚", "已婚", "离异", "丧偶")
ID_TYPE_VALUES = ("身份证", "护照", "军官证", "港澳居民来往内地通行证", "台湾居民来往大陆通行证", "其他")
FORBIDDEN_ID_CONTEXT = ("中征码", "机构代码", "账号", "账户编号", "授信协议编号", "银行账号", "贷款编号", "查询机构代码")


def _section_text(sections: dict[str, Any]) -> str:
    return "\n".join(
        str(sections.get(key) or "")
        for key in ("report_basic_info", "personal_basic_info")
        if str(sections.get(key) or "").strip()
    )


def _all_text(sections: dict[str, Any]) -> str:
    return "\n".join(
        str(sections.get(key) or "")
        for key in ("report_basic_info", "personal_basic_info", "full_text")
        if str(sections.get(key) or "").strip()
    )


def _extract_allowed_id_type(text: str) -> str:
    raw = value_after_label(text, ("证件类型", "证件名称"), max_chars=80)
    if "中征码" in raw:
        raw = ""
    for item in ID_TYPE_VALUES:
        if item in raw or item in text:
            return item
    return ""


def _valid_id_number(value: str) -> str:
    text = re.sub(r"\s+", "", str(value or ""))
    match = ID_CARD_PATTERN.search(text)
    return match.group(1).upper() if match else ""


def _candidate_near_keyword(text: str) -> str:
    source = str(text or "")
    best: tuple[int, str] | None = None
    for keyword in ("证件号码", "身份证号码", "证件号"):
        for key_match in re.finditer(re.escape(keyword), source):
            window = source[key_match.start() : key_match.start() + 160]
            if any(forbidden in window[:30] for forbidden in FORBIDDEN_ID_CONTEXT):
                continue
            for match in ID_CARD_PATTERN.finditer(window):
                candidate = match.group(1).upper()
                score = match.start()
                if best is None or score < best[0]:
                    best = (score, candidate)
    return best[1] if best else ""


def _extract_id_number(primary_text: str, fallback_text: str) -> str:
    near = _candidate_near_keyword(primary_text) or _candidate_near_keyword(fallback_text)
    if near:
        return near
    for source in (primary_text, fallback_text):
        for match in ID_CARD_PATTERN.finditer(str(source or "")):
            start = max(0, match.start() - 30)
            context = source[start : match.end() + 30]
            if any(forbidden in context for forbidden in FORBIDDEN_ID_CONTEXT):
                continue
            return match.group(1).upper()
    return ""


def _extract_name(text: str) -> str:
    inline = re.search(r"姓名\s*[:：]?\s*([\u4e00-\u9fff·]{2,20})\s+证件类型", text)
    if inline:
        return clean_value(inline.group(1))
    raw = value_after_label(text, ("姓名", "被查询者姓名"), max_chars=80)
    match = re.match(r"([\u4e00-\u9fff·]{2,20})", raw)
    return clean_value(match.group(1)) if match else clean_value(raw)


def _extract_marital_status(text: str) -> str:
    raw = value_after_label(text, ("婚姻状况", "婚姻状态"), max_chars=80)
    for item in MARRIAGE_VALUES:
        if item in raw or item in text:
            return item
    return ""


def _split_report_number_time(report_number: str, report_time: str) -> tuple[str, str]:
    number = clean_value(report_number)
    time = clean_value(report_time)
    if "报告时间" in number:
        parts = re.split(r"报告时间\s*[:：]?", number, maxsplit=1)
        number = clean_value(re.sub(r"报告编号\s*[:：]?", "", parts[0]))
        if len(parts) > 1 and not time:
            time = clean_value(parts[1])
    number_match = re.search(r"([A-Za-z0-9\-]{6,80})", number)
    if number_match:
        number = number_match.group(1)
    time_match = re.search(r"((?:19|20)\d{2}[-/年.]\d{1,2}[-/月.]\d{1,2}(?:日)?(?:\s+\d{1,2}:\d{1,2}:\d{1,2})?)", time)
    if time_match:
        time = time_match.group(1)
    return number, time


def extract_basic_info(sections: dict[str, Any], source_file: str | None = None) -> dict[str, Any]:
    try:
        primary_text = _section_text(sections)
        text = _all_text(sections)
        scan_text = primary_text or text
        report_number, report_time = _split_report_number_time(
            value_after_label(scan_text, ("报告编号", "报告号码", "报告号"), max_chars=140),
            value_after_label(scan_text, ("报告时间", "报告日期", "生成时间", "查询时间"), max_chars=80),
        )
        result = default_basic_info()
        result.update(
            {
                "report_number": report_number,
                "report_time": report_time,
                "name": _extract_name(scan_text),
                "id_type": _extract_allowed_id_type(scan_text),
                "id_number": _extract_id_number(scan_text, text),
                "marital_status": _extract_marital_status(scan_text),
                "source_file": source_file or "",
            }
        )
        return result
    except Exception:
        result = default_basic_info()
        result["source_file"] = source_file or ""
        return result
