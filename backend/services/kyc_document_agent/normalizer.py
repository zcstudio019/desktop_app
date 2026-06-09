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


def normalize_id_number(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip().upper()


def normalize_text_field(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip(" :：,，;；")


def normalize_result(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("doc_type") == "marriage_cert":
        result["doc_type"] = "marriage_certificate"
        result["doc_type_name"] = "结婚证"
    fields = result.get("fields") or {}
    if result.get("doc_type") == "marriage_certificate":
        for holder_key in ("holder_1", "holder_2"):
            holder = fields.get(holder_key)
            if isinstance(holder, dict):
                if holder.get("id_number"):
                    holder["id_number"] = normalize_id_number(holder.get("id_number"))
                if holder.get("birth_date"):
                    holder["birth_date"] = normalize_date(holder.get("birth_date"))
                if holder.get("gender"):
                    gender = str(holder.get("gender") or "").strip()
                    holder["gender"] = gender if gender in {"男", "女"} else ""
                if holder.get("nationality"):
                    nationality = normalize_text_field(holder.get("nationality"))
                    holder["nationality"] = "中国" if nationality in {"中", "中国", "中华人民共和国"} else nationality
        for field in ("holder_id_number", "spouse_id_number"):
            if fields.get(field):
                fields[field] = normalize_id_number(fields.get(field))
        for field in ("marriage_date", "registration_date", "issue_date"):
            if fields.get(field):
                fields[field] = normalize_date(fields.get(field))
        if fields.get("certificate_no"):
            fields["certificate_no"] = re.sub(r"\s+", "", str(fields.get("certificate_no") or "")).strip()
        if fields.get("certificate_number"):
            fields["certificate_number"] = re.sub(r"\s+", "", str(fields.get("certificate_number") or "")).strip()
        fields["marital_status"] = "已婚"
    for field, value in list(fields.items()):
        if result.get("doc_type") == "id_card" and field == "id_number":
            fields[field] = normalize_id_number(value)
        elif result.get("doc_type") == "id_card" and field == "gender":
            gender = str(value or "").strip()
            fields[field] = gender if gender in {"男", "女"} else ""
        elif result.get("doc_type") == "id_card" and field == "ethnicity":
            fields[field] = normalize_text_field(value).replace("族", "")
        elif result.get("doc_type") == "id_card" and field in {"address", "issuing_authority"}:
            fields[field] = normalize_text_field(value)
        elif field in DATE_FIELDS:
            fields[field] = normalize_date(value)
        elif field in AMOUNT_FIELDS:
            fields[field] = normalize_amount(value)
        elif field in AREA_FIELDS:
            fields[field] = normalize_area(value)
        elif isinstance(value, str):
            fields[field] = re.sub(r"\s+", " ", value).strip(" :：,，")
    if result.get("doc_type") in {"property_cert", "real_estate_cert"} and fields.get("登记日期"):
        fields["registration_date"] = fields["登记日期"]
    result["fields"] = fields
    return result
