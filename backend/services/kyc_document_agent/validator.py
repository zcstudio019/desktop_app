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


def validate_result(result: dict[str, Any]) -> dict[str, Any]:
    fields = result.get("fields") or {}
    doc_type = result.get("doc_type") or "unknown"
    warnings = list(result.get("validation", {}).get("warnings") or [])
    errors = list(result.get("validation", {}).get("errors") or [])

    if doc_type == "id_card":
        _validate_id_card_result(result, warnings, errors)
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
    if fields.get("unified_social_credit_code") and not validate_credit_code(str(fields["unified_social_credit_code"])):
        errors.append("统一社会信用代码格式不合法")
    if fields.get("plate_number") and not validate_plate_number(str(fields["plate_number"])):
        warnings.append("车牌号格式需人工复核")
    if fields.get("bank_account_number") and not validate_bank_account(str(fields["bank_account_number"])):
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
    if doc_type != "id_card":
        if errors:
            result["extraction_status"] = "partial" if fields else "failed"
        elif result.get("missing_fields"):
            result["extraction_status"] = "partial"
        elif fields:
            result["extraction_status"] = "success"
        else:
            result["extraction_status"] = "failed"
    return result
