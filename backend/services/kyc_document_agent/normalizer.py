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
VALID_MARRIAGE_AUTHORITY_KEYWORDS = ("民政局", "婚姻登记处", "婚姻登记中心")
INVALID_MARRIAGE_AUTHORITY_FRAGMENTS = (
    "进行结婚登记",
    "符合本法",
    "申请结婚",
    "经审查符合",
    "准予登记",
    "发给此证",
    "国籍",
    "姓名",
    "性别",
    "出生日期",
    "身份证件号",
    "发证日期",
)


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


def normalize_marriage_authority(value: Any) -> str:
    text = normalize_text_field(value)
    if not text:
        return ""
    if any(fragment in text for fragment in INVALID_MARRIAGE_AUTHORITY_FRAGMENTS):
        return ""
    if not any(keyword in text for keyword in VALID_MARRIAGE_AUTHORITY_KEYWORDS):
        return ""
    return text


def normalize_result(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("doc_type") == "marriage_cert":
        result["doc_type"] = "marriage_certificate"
        result["doc_type_name"] = "结婚证"
    fields = result.get("fields") or {}
    if result.get("doc_type") == "business_license":
        for field in (
            "unified_social_credit_code",
            "license_number",
        ):
            if fields.get(field):
                fields[field] = re.sub(r"[\s:：]+", "", str(fields.get(field) or "")).upper()
        for field in (
            "company_name",
            "company_type",
            "legal_representative",
            "registered_address",
        ):
            if fields.get(field):
                fields[field] = normalize_text_field(fields.get(field))
        if fields.get("registered_capital"):
            fields["registered_capital"] = re.sub(r"\s+", "", str(fields.get("registered_capital") or "")).strip(" :：,，;；")
        if fields.get("business_scope"):
            fields["business_scope"] = re.sub(r"\s+", "", str(fields.get("business_scope") or "")).strip(" :：,，;；")
        if fields.get("business_term"):
            fields["business_term"] = re.sub(r"\s+", "", str(fields.get("business_term") or "")).strip(" :：,，;；")
        for field in ("establishment_date", "issue_date"):
            if fields.get(field):
                fields[field] = normalize_date(fields.get(field))
        if fields.get("registration_authority"):
            authority = normalize_text_field(fields.get("registration_authority"))
            authority_keywords = (
                "市场监督管理局",
                "行政审批局",
                "工商行政管理局",
                "工商行政管理部门",
                "市场监督管理部门",
            )
            if (
                authority in {"登记机关", "未识别", "发照日期"}
                or re.fullmatch(r"(?:19|20)\d{2}年\d{1,2}月\d{1,2}日", authority)
                or re.fullmatch(r"(?:19|20)\d{2}[-./]\d{1,2}[-./]\d{1,2}", authority)
                or not any(keyword in authority for keyword in authority_keywords)
                or len(re.findall(r"[\u4e00-\u9fff]", authority)) < 6
            ):
                fields["registration_authority"] = ""
            else:
                fields["registration_authority"] = authority
        result["fields"] = fields
        return result
    if result.get("doc_type") == "vehicle_license":
        for field in ("plate_number", "vin", "engine_number"):
            if fields.get(field):
                fields[field] = re.sub(r"[\s:：]+", "", str(fields.get(field) or "")).upper()
        for field in ("owner", "address", "vehicle_type", "use_character"):
            if fields.get(field):
                fields[field] = normalize_text_field(fields.get(field))
        if fields.get("brand_model"):
            value = normalize_text_field(fields.get("brand_model"))
            fields["brand_model"] = re.sub(r"[a-z]+", lambda item: item.group(0).upper(), value)
        for field in ("registration_date", "issue_date", "inspection_valid_until"):
            if fields.get(field):
                fields[field] = normalize_date(fields.get(field))
        for field in ("approved_passengers", "total_mass", "curb_weight"):
            if fields.get(field):
                fields[field] = re.sub(r"\s+", "", str(fields.get(field) or "")).strip(" :：,，;；")
        result["fields"] = fields
        return result
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
        for field in ("registration_authority", "issuing_authority"):
            if fields.get(field):
                fields[field] = normalize_marriage_authority(fields.get(field))
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
