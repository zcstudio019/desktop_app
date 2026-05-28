from __future__ import annotations

import re
from typing import Any


DATE_FIELDS = {
    "birth_date",
    "valid_from",
    "valid_to",
    "establishment_date",
    "issue_date",
    "registration_date",
    "inspection_valid_until",
}
AMOUNT_FIELDS = {"registered_capital"}
AREA_FIELDS = {"building_area", "land_area", "total_area"}


def normalize_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"(\d{4})[年./-]?\s*(\d{1,2})[月./-]?\s*(\d{1,2})日?", text)
    if not match:
        match = re.search(r"(\d{4})(\d{2})(\d{2})", text)
    if match:
        year, month, day = (int(part) for part in match.groups())
        try:
            return f"{year:04d}-{month:02d}-{day:02d}"
        except ValueError:
            return text
    if text in {"长期", "长久"}:
        return "长期"
    return text


def normalize_amount(value: Any) -> dict[str, Any] | str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"(?:人民币|￥|¥)?\s*([\d,.]+)\s*([万亿千百十]*元|万元|亿元)?", text)
    if not match:
        return text
    number = float(match.group(1).replace(",", "").strip())
    unit = match.group(2) or "元"
    return {"amount": number, "unit": unit}


def normalize_area(value: Any) -> dict[str, Any] | str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"([\d,.]+)\s*(平方米|㎡|m2|M2)?", text)
    if not match:
        return text
    return {"value": float(match.group(1).replace(",", "")), "unit": "平方米"}


def normalize_result(result: dict[str, Any]) -> dict[str, Any]:
    fields = result.get("fields") or {}
    for field, value in list(fields.items()):
        if field in DATE_FIELDS:
            fields[field] = normalize_date(value)
        elif field in AMOUNT_FIELDS:
            fields[field] = normalize_amount(value)
        elif field in AREA_FIELDS:
            fields[field] = normalize_area(value)
        elif isinstance(value, str):
            fields[field] = re.sub(r"\s+", " ", value).strip(" :：,，")
    result["fields"] = fields
    return result
