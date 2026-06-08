from __future__ import annotations

import re
from typing import Any

from .common import clean, compact_text, lines


FORBIDDEN_FIELDS = {"权利人", "坐落", "面积", "权利类型", "土地用途", "房屋用途"}


def _registration_date(text: str) -> tuple[str, list[str]]:
    warnings: list[str] = []
    compact = compact_text(text)
    match = re.search(r"((?:19|20)\d{2}年\d{1,2}月\d{1,2}日)", compact)
    if match:
        return match.group(1), warnings
    if "专用章" in compact or "登记" in compact:
        warnings.append("登记日期疑似存在但 OCR 未准确识别，请人工确认。")
    return "", warnings


def _registration_authority(text: str) -> str:
    for line in lines(text):
        if "国土资源部监制" in line:
            continue
        if "不动产登记专用章" in line:
            return clean(line)
    if "不动产登记专用章" in compact_text(text):
        return "不动产登记专用章"
    return ""


def _cover_number(text: str) -> str:
    compact = compact_text(text)
    match = re.search(r"编号(?:№|No\.?)?([A-Z]?\d{8,})", compact, re.IGNORECASE)
    return match.group(1).upper() if match else ""


def extract(payload: dict[str, Any]) -> dict[str, Any]:
    text = str(payload.get("text") or "")
    fields: dict[str, Any] = {}
    warnings: list[str] = []
    date, date_warnings = _registration_date(text)
    warnings.extend(date_warnings)
    if date:
        fields["登记日期"] = date
    authority = _registration_authority(text)
    if authority:
        fields["登记机构"] = authority
    cover_number = _cover_number(text)
    if cover_number:
        fields["封面编号"] = cover_number
    return {
        "fields": fields,
        "warnings": warnings,
        "page_role": "cover_page",
        "blocked_fields": sorted(FORBIDDEN_FIELDS),
    }
