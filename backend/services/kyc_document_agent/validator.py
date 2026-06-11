from __future__ import annotations

import re
from datetime import date
from typing import Any

from .schema import REQUIRED_FIELDS

ID_CARD_WEIGHTS = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
ID_CARD_CHECK_CODES = "10X98765432"


def _is_valid_date(date_text: str) -> bool:
    match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", date_text or "")
    if not match:
        return False
    try:
        date(*(int(part) for part in match.groups()))
    except ValueError:
        return False
    return True


def validate_id_number(id_number: str) -> bool:
    code = (id_number or "").strip().upper()
    if not re.fullmatch(r"\d{17}[\dX]", code):
        return False
    birth = f"{code[6:10]}-{code[10:12]}-{code[12:14]}"
    if not _is_valid_date(birth):
        return False
    total = sum(int(code[index]) * ID_CARD_WEIGHTS[index] for index in range(17))
    return ID_CARD_CHECK_CODES[total % 11] == code[-1]


def validate_credit_code(code: str) -> bool:
    return bool(re.fullmatch(r"[0-9A-HJ-NPQRTUWXY]{18}", (code or "").strip().upper()))


def validate_plate_number(plate_number: str) -> bool:
    return bool(re.fullmatch(r"[\u4e00-\u9fa5][A-Z][A-Z0-9挂学警港澳新能源]{5,6}", (plate_number or "").strip().upper()))


def validate_bank_account(account: str) -> bool:
    digits = re.sub(r"\s+", "", account or "")
    return bool(re.fullmatch(r"\d{8,30}", digits))


def _validate_id_card_result(result: dict[str, Any], warnings: list[str], errors: list[str]) -> None:
    fields = result.get("fields") or {}
    front_required = ["name", "gender", "ethnicity", "birth_date", "address", "id_number"]
    back_required = ["issuing_authority", "valid_from", "valid_to"]
    required = front_required + back_required
    missing = [field for field in required if not fields.get(field)]
    result["missing_fields"] = missing

    has_front = any(fields.get(field) for field in front_required)
    has_back = any(fields.get(field) for field in back_required)
    front_complete = all(fields.get(field) for field in front_required)
    back_complete = all(fields.get(field) for field in back_required)

    if fields.get("id_number") and not validate_id_number(str(fields["id_number"])):
        warnings.append("身份证号码格式或校验位不合法")
    if fields.get("id_number") and fields.get("birth_date"):
        code = str(fields["id_number"]).strip().upper()
        if re.fullmatch(r"\d{17}[\dX]", code):
            id_birth = f"{code[6:10]}-{code[10:12]}-{code[12:14]}"
            if _is_valid_date(id_birth) and str(fields["birth_date"]) != id_birth:
                warnings.append("身份证号码中的出生日期与证件出生日期不一致")
    if fields.get("valid_from") and fields.get("valid_to") and fields.get("valid_to") != "长期":
        if _is_valid_date(str(fields["valid_from"])) and _is_valid_date(str(fields["valid_to"])):
            if str(fields["valid_to"]) < str(fields["valid_from"]):
                warnings.append("身份证有效期限截止日期早于起始日期")

    if front_complete and back_complete:
        result["extraction_status"] = "success"
        return
    if has_front and not has_back:
        warnings.append("已识别身份证正面信息，缺少签发机关和有效期限，请补充身份证背面")
        result["extraction_status"] = "partial"
        return
    if has_back and not has_front:
        warnings.append("已识别身份证背面信息，缺少姓名、身份证号码等正面信息，请补充身份证正面")
        result["extraction_status"] = "partial"
        return
    if has_front or has_back:
        warnings.append("身份证字段识别不完整，请补充身份证正反面或人工确认")
        result["extraction_status"] = "partial"
        return
    warnings.append("未从 OCR 文本中识别到身份证字段，请检查图片清晰度或 OCR 原文")
    result["extraction_status"] = "failed"


def _holder(fields: dict[str, Any], key: str) -> dict[str, Any]:
    value = fields.get(key)
    return value if isinstance(value, dict) else {}


def _validate_marriage_certificate_result(result: dict[str, Any], warnings: list[str], errors: list[str]) -> None:
    fields = result.get("fields") or {}
    holder_1 = _holder(fields, "holder_1")
    holder_2 = _holder(fields, "holder_2")
    missing: list[str] = []
    if not fields.get("certificate_no") and not fields.get("certificate_number"):
        missing.append("结婚证字号")
    if not holder_1.get("name") and not fields.get("holder_name"):
        missing.append("配偶一姓名")
    if not holder_2.get("name") and not fields.get("spouse_name"):
        missing.append("配偶二姓名")
    id_1 = str(holder_1.get("id_number") or fields.get("holder_id_number") or "").strip().upper()
    id_2 = str(holder_2.get("id_number") or fields.get("spouse_id_number") or "").strip().upper()
    if not id_1:
        missing.append("配偶一身份证号")
    if not id_2:
        missing.append("配偶二身份证号")
    result["missing_fields"] = missing

    if id_1 and not validate_id_number(id_1):
        warnings.append("配偶一身份证号格式或校验位不合法")
    if id_2 and not validate_id_number(id_2):
        warnings.append("配偶二身份证号格式或校验位不合法")
    for index, (holder, id_number) in enumerate(((holder_1, id_1), (holder_2, id_2)), start=1):
        birth_date = str(holder.get("birth_date") or "").strip()
        if id_number and birth_date and re.fullmatch(r"\d{17}[\dX]", id_number):
            id_birth = f"{id_number[6:10]}-{id_number[10:12]}-{id_number[12:14]}"
            if _is_valid_date(id_birth) and birth_date != id_birth:
                warnings.append(f"配偶{index}身份证号中的出生日期与 OCR 出生日期不一致")
    if id_1 and id_2 and id_1 == id_2:
        errors.append("两位配偶身份证号相同")
    name_1 = str(holder_1.get("name") or fields.get("holder_name") or "").strip()
    name_2 = str(holder_2.get("name") or fields.get("spouse_name") or "").strip()
    if name_1 and name_2 and name_1 == name_2:
        warnings.append("两位配偶姓名相同，请人工复核")
    fields["marital_status"] = "已婚"
    cert_no = str(fields.get("certificate_no") or fields.get("certificate_number") or "").strip()
    holder_1_birth = str(holder_1.get("birth_date") or "").strip()
    holder_2_birth = str(holder_2.get("birth_date") or "").strip()
    issue_or_marriage_date = str(
        fields.get("marriage_date")
        or fields.get("issue_date")
        or fields.get("registration_date")
        or ""
    ).strip()
    raw_id_1 = str(
        holder_1.get("raw_id_number")
        or holder_1.get("suspected_id_number")
        or fields.get("holder_raw_id_number")
        or fields.get("holder_suspected_id_number")
        or ""
    ).strip()
    raw_id_2 = str(
        holder_2.get("raw_id_number")
        or holder_2.get("suspected_id_number")
        or fields.get("spouse_raw_id_number")
        or fields.get("spouse_suspected_id_number")
        or ""
    ).strip()

    core_values = [
        cert_no,
        name_1,
        name_2,
        id_1,
        id_2,
        raw_id_1,
        raw_id_2,
        fields.get("registration_authority"),
        fields.get("issuing_authority"),
        fields.get("issue_date"),
        fields.get("registration_date"),
    ]
    if not any(str(value or "").strip() for value in core_values):
        warning = "未获取到有效 OCR 文本或字段识别失败"
        if warning not in warnings:
            warnings.append(warning)
        result["extraction_status"] = "failed"
        return

    if errors:
        result["extraction_status"] = "partial" if fields else "failed"
    elif not missing:
        result["extraction_status"] = "success"
    elif cert_no and name_1 and name_2 and holder_1_birth and holder_2_birth and issue_or_marriage_date and (id_1 or raw_id_1) and (id_2 or raw_id_2):
        result["extraction_status"] = "success"
    else:
        result["extraction_status"] = "partial"


def _validate_business_license_result(result: dict[str, Any], warnings: list[str], errors: list[str]) -> None:
    fields = result.get("fields") or {}
    required = [
        "company_name",
        "legal_representative",
        "registered_address",
        "business_scope",
        "registration_authority",
    ]
    result["missing_fields"] = [field for field in required if not fields.get(field)]

    credit_code = str(fields.get("unified_social_credit_code") or "").strip().upper()
    if not credit_code:
        result["missing_fields"].append("unified_social_credit_code")
    elif len(credit_code) != 18 or not re.fullmatch(r"[0-9A-Z]+", credit_code):
        warnings.append("统一社会信用代码长度异常，请人工核对")

    for field, label in (("establishment_date", "成立日期"), ("issue_date", "发照日期")):
        value = str(fields.get(field) or "").strip()
        if value and not _is_valid_date(value):
            warnings.append(f"{label}日期格式需人工复核")

    core_fields = (
        "unified_social_credit_code",
        "company_name",
        "legal_representative",
        "registered_address",
        "business_scope",
    )
    present_count = sum(1 for field in core_fields if fields.get(field))
    if present_count == len(core_fields):
        result["extraction_status"] = "success"
    elif present_count >= 2:
        result["extraction_status"] = "partial"
    else:
        warnings.append("未从 OCR 文本中识别到足够的营业执照核心字段，请检查图片清晰度或 OCR 原文")
        result["extraction_status"] = "failed"


def _validate_vehicle_license_result(result: dict[str, Any], warnings: list[str], errors: list[str]) -> None:
    fields = result.get("fields") or {}
    required = [
        "plate_number",
        "vehicle_type",
        "owner",
        "address",
        "use_character",
        "brand_model",
        "vin",
        "engine_number",
        "registration_date",
        "issue_date",
    ]
    result["missing_fields"] = [field for field in required if not fields.get(field)]

    if fields.get("plate_number") and not validate_plate_number(str(fields.get("plate_number"))):
        warnings.append("号牌号码格式需人工复核")
    vin = str(fields.get("vin") or "").strip().upper()
    if vin and len(vin) != 17:
        warnings.append("车辆识别代号长度异常")
    for field, label in (("registration_date", "注册日期"), ("issue_date", "发证日期")):
        value = str(fields.get(field) or "").strip()
        if value and not _is_valid_date(value):
            warnings.append(f"{label}日期格式需人工复核")
    registration_date = str(fields.get("registration_date") or "").strip()
    issue_date = str(fields.get("issue_date") or "").strip()
    if _is_valid_date(registration_date) and _is_valid_date(issue_date) and issue_date < registration_date:
        warnings.append("发证日期早于注册日期，请核对")

    present_count = sum(1 for field in required if fields.get(field))
    if present_count == len(required):
        result["extraction_status"] = "success"
    elif present_count >= 2:
        result["extraction_status"] = "partial"
    else:
        warnings.append("未从 OCR 文本中识别到足够的行驶证核心字段，请检查图片清晰度或 OCR 原文")
        result["extraction_status"] = "failed"


def _validate_account_result(result: dict[str, Any], warnings: list[str], errors: list[str]) -> None:
    fields = result.get("fields") or {}
    required = ["company_name", "bank_account_name", "bank_account_number", "opening_bank", "legal_representative"]
    result["missing_fields"] = [field for field in required if not fields.get(field)]

    account_number = str(fields.get("bank_account_number") or "").strip()
    if account_number and not re.fullmatch(r"[0-9A-Za-z-]{6,40}", account_number):
        warnings.append("银行账号格式需人工复核")

    basic_account_number = str(fields.get("basic_account_number") or "").strip().upper()
    raw_text = str(result.get("raw_text_preview") or "")
    if basic_account_number and not re.fullmatch(r"[A-Z0-9]{8,30}", basic_account_number):
        warnings.append("基本存款账户编号格式需人工复核")
    if "基本存款账户编号" in raw_text and not basic_account_number:
        warnings.append("文本包含基本存款账户编号但未能提取，请人工核对")

    opening_bank = str(fields.get("opening_bank") or "").strip()
    if opening_bank and len(opening_bank) < 4:
        warnings.append("开户银行可能识别不完整")

    issue_date = str(fields.get("issue_date") or "").strip()
    if issue_date and not _is_valid_date(issue_date):
        warnings.append("日期格式无法识别")

    core_fields = ("opening_bank", "bank_account_number", "legal_representative", "basic_account_number")
    present_count = sum(1 for field in core_fields if fields.get(field))
    if present_count >= 3 and fields.get("bank_account_number") and fields.get("opening_bank"):
        result["extraction_status"] = "success"
    elif present_count >= 2:
        result["extraction_status"] = "partial"
    elif fields:
        result["extraction_status"] = "partial"
    else:
        warnings.append("未从 OCR 文本中识别到账户资料核心字段，请检查图片清晰度或 OCR 原文")
        result["extraction_status"] = "failed"


def validate_result(result: dict[str, Any]) -> dict[str, Any]:
    fields = result.get("fields") or {}
    doc_type = result.get("doc_type") or "unknown"
    warnings = list(result.get("validation", {}).get("warnings") or [])
    errors = list(result.get("validation", {}).get("errors") or [])

    if doc_type == "id_card":
        _validate_id_card_result(result, warnings, errors)
    elif doc_type == "business_license":
        _validate_business_license_result(result, warnings, errors)
    elif doc_type == "vehicle_license":
        _validate_vehicle_license_result(result, warnings, errors)
    elif doc_type in {"account_permit", "basic_account_info"}:
        _validate_account_result(result, warnings, errors)
    elif doc_type in {"marriage_certificate", "marriage_cert"}:
        result["doc_type"] = "marriage_certificate"
        _validate_marriage_certificate_result(result, warnings, errors)
    else:
        required = REQUIRED_FIELDS.get(doc_type, [])
        missing = [field for field in required if not fields.get(field)]
        result["missing_fields"] = missing
        if missing:
            warnings.append(f"必填字段缺失: {', '.join(missing)}")

    if fields.get("holder_id_number") and not validate_id_number(str(fields["holder_id_number"])):
        warnings.append("持证人身份证号码格式或校验位不合法")
    if fields.get("spouse_id_number") and not validate_id_number(str(fields["spouse_id_number"])):
        warnings.append("配偶身份证号码格式或校验位不合法")
    if doc_type != "business_license" and fields.get("unified_social_credit_code") and not validate_credit_code(str(fields["unified_social_credit_code"])):
        errors.append("统一社会信用代码格式不合法")
    if doc_type != "vehicle_license" and fields.get("plate_number") and not validate_plate_number(str(fields["plate_number"])):
        warnings.append("车牌号格式需人工复核")
    if doc_type not in {"account_permit", "basic_account_info"} and fields.get("bank_account_number") and not validate_bank_account(str(fields["bank_account_number"])):
        warnings.append("银行账号格式需人工复核")

    for field, value in fields.items():
        if field.endswith("_date") or field in {"valid_from", "valid_to", "inspection_valid_until"}:
            if value and value != "长期" and not _is_valid_date(str(value)):
                warnings.append(f"{field} 日期格式需人工复核")

    for field, confidence in (result.get("confidence", {}).get("fields") or {}).items():
        if fields.get(field) and confidence < 0.6:
            warnings.append(f"{field} 字段置信度较低")

    result["validation"] = {
        "is_valid": not errors,
        "warnings": list(dict.fromkeys(warnings)),
        "errors": list(dict.fromkeys(errors)),
    }
    if result.get("doc_type") not in {"id_card", "business_license", "vehicle_license", "account_permit", "basic_account_info", "marriage_certificate"}:
        if errors:
            result["extraction_status"] = "partial" if fields else "failed"
        elif result.get("missing_fields"):
            result["extraction_status"] = "partial"
        elif fields:
            result["extraction_status"] = "success"
        else:
            result["extraction_status"] = "failed"
    return result
