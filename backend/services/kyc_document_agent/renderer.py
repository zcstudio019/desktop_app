from __future__ import annotations

from typing import Any


PROPERTY_FIELD_ORDER = [
    "权利人",
    "共有情况",
    "权证编号",
    "封面编号",
    "坐落",
    "不动产单元号",
    "权利类型",
    "权利性质",
    "权属性质",
    "使用权取得方式",
    "土地用途",
    "房屋用途",
    "地号",
    "宗地号",
    "宗地面积",
    "使用期限",
    "土地使用期限",
    "室号或部位",
    "建筑面积",
    "建筑类型",
    "总层数",
    "竣工日期",
    "登记日期",
    "登记机构",
    "登记日",
    "填证单位",
]

ENGLISH_TO_CHINESE_FIELDS = {
    "owner": "权利人",
    "co_owners": "共有人",
    "certificate_number": "权证编号",
    "property_address": "房地坐落",
    "property_unit_number": "不动产单元号",
    "right_type": "权利类型",
    "right_nature": "权属性质",
    "co_ownership": "共有情况",
    "shared_status": "共有情况",
    "ownership_status": "共有情况",
    "real_estate_unit_no": "不动产单元号",
    "real_estate_unit_number": "不动产单元号",
    "acquisition_method": "使用权取得方式",
    "land_use": "土地用途",
    "use_type": "房屋用途",
    "house_use": "房屋用途",
    "building_use": "房屋用途",
    "parcel_number": "宗地号",
    "land_area": "宗地面积",
    "usage_area": "使用权面积",
    "total_area": "使用权面积",
    "land_use_term": "使用期限",
    "use_term": "使用期限",
    "cover_certificate_number": "封面编号",
    "location": "坐落",
    "address": "坐落",
    "room_number": "室号或部位",
    "building_area": "建筑面积",
    "building_type": "建筑类型",
    "total_floors": "总层数",
    "completion_date": "竣工日期",
    "registration_date": "登记日期",
    "registration_authority": "登记机构",
    "issue_date": "登记日",
    "issuing_unit": "填证单位",
    "土地用途": "土地用途",
    "用途": "土地用途",
    "共有情况": "共有情况",
    "坐落": "坐落",
    "不动产单元号": "不动产单元号",
    "权利类型": "权利类型",
    "权利性质": "权利性质",
    "地号": "地号",
    "使用期限": "使用期限",
    "封面编号": "封面编号",
    "登记日期": "登记日期",
    "登记机构": "登记机构",
}

OWNER_TYPE_LABELS = {
    "person": "个人资料",
    "enterprise": "企业资料",
    "asset": "资产资料",
    "unknown": "未知",
}

STATUS_LABELS = {
    "success": "成功",
    "partial": "部分成功",
    "failed": "失败",
}

AGENT_LABELS = {
    "kyc_document_agent": "KYC资料识别",
}

INVALID_DISPLAY_VALUES = {"", "对", "的合法权益，对", "无", "未识别", "null", "None", "none"}
INVALID_DISPLAY_KEYWORDS = ("合法权益", "房地产权利人", "本证是证明", "根据", "法律")
FORBIDDEN_DISPLAY_KEYS = {
    "historical_financial_reports",
    "financial_reports",
    "enterprise_credit_reports",
    "personal_credit_reports",
    "enterprise_flows",
    "bank_flows",
    "financial_statement_diagnostic",
    "financing_diagnostic_report",
    "comprehensive_financing_advice",
    "customer_profile_markdown",
    "customer_context",
    "customer_profile",
    "profile_context",
}

FIELD_LABELS = {
    "name": "姓名",
    "gender": "性别",
    "ethnicity": "民族",
    "birth_date": "出生日期",
    "address": "住址",
    "id_number": "身份证号码",
    "issuing_authority": "签发机关",
    "valid_from": "有效期限起",
    "valid_to": "有效期限止",
    "certificate_no": "结婚证字号",
    "certificate_number": "结婚证字号",
    "marital_status": "婚姻状态",
    "marriage_date": "登记日期",
    "registration_authority": "登记机关",
    "holder_name": "配偶一姓名",
    "spouse_name": "配偶二姓名",
    "holder_id_number": "配偶一身份证号",
    "spouse_id_number": "配偶二身份证号",
    "owner": "权利人",
    "co_owners": "共有人",
    "certificate_number": "权证编号",
    "property_unit_number": "不动产单元号",
    "property_address": "房地坐落",
    "right_type": "权利类型",
    "right_nature": "权属性质",
    "use_type": "房屋用途",
    "house_use": "房屋用途",
    "building_use": "房屋用途",
    "land_use": "土地用途",
    "building_area": "建筑面积",
    "land_area": "宗地面积",
    "total_area": "使用权面积",
    "mortgage_status": "抵押状态",
    "seizure_status": "查封状态",
    "issue_date": "发证日期",
    "doc_type": "资料类型编码",
    "doc_type_name": "资料名称",
    "owner_type": "归属类型",
    "fields": "关键字段",
    "validation": "校验结果",
    "confidence": "置信度",
    "missing_fields": "缺失字段",
    "raw_text_preview": "原文预览",
    "agent_type": "处理 Agent",
}


def field_label(field: str) -> str:
    return FIELD_LABELS.get(field, ENGLISH_TO_CHINESE_FIELDS.get(field, field))


def _is_empty_or_invalid(value: Any) -> bool:
    if value in (None, [], {}):
        return True
    if isinstance(value, list):
        return not any(not _is_empty_or_invalid(item) for item in value)
    text = _format_value(value).strip()
    if text in INVALID_DISPLAY_VALUES:
        return True
    return any(keyword in text for keyword in INVALID_DISPLAY_KEYWORDS)


def _merge_owner_fields(fields: dict[str, Any]) -> Any:
    owner = fields.get("权利人") or fields.get("owner")
    co_owners = fields.get("共有人") or fields.get("co_owners")
    owner_text = _format_value(owner).strip() if owner not in (None, "", [], {}) else ""
    co_owner_text = _format_value(co_owners).strip() if co_owners not in (None, "", [], {}) else ""
    parts: list[str] = []
    for text in (owner_text, co_owner_text):
        if not text or _is_empty_or_invalid(text):
            continue
        for part in [item.strip() for item in text.replace("，", "、").replace(",", "、").split("、")]:
            if part and part not in parts:
                parts.append(part)
    return "、".join(parts)


def _is_hidden_property_display_field(display_key: str, value: Any) -> bool:
    if display_key == "共有人":
        return True
    if display_key == "使用权面积":
        text = _format_value(value).strip()
        return not text or text == "独用" or not any(char.isdigit() for char in text)
    if display_key == "竣工日期":
        text = _format_value(value).strip()
        return not ("年" in text or (len(text) == 10 and text[4] == "-" and text[7] == "-"))
    return False


def _is_new_real_estate_cert(fields: dict[str, Any], display_fields: dict[str, Any]) -> bool:
    cert_number = _format_value(
        fields.get("权证编号")
        or fields.get("certificate_number")
        or display_fields.get("权证编号")
    )
    if "房地" in cert_number or "房权" in cert_number:
        return False
    return bool(
        fields.get("不动产单元号")
        or fields.get("real_estate_unit_no")
        or fields.get("real_estate_unit_number")
        or fields.get("共有情况")
        or fields.get("co_ownership")
        or fields.get("shared_status")
        or fields.get("ownership_status")
        or fields.get("权利类型")
        or fields.get("right_type")
        or fields.get("权利性质")
        or "不动产权" in cert_number
    )


def _more_complete_display_value(current: Any, candidate: Any) -> Any:
    current_text = _format_value(current).strip() if current not in (None, "", [], {}) else ""
    candidate_text = _format_value(candidate).strip() if candidate not in (None, "", [], {}) else ""
    if not current_text:
        return candidate
    if not candidate_text:
        return current
    if current_text == candidate_text:
        return current
    current_score = len(current_text) + (20 if "止" in current_text else 0) + current_text.count("年") * 4
    candidate_score = len(candidate_text) + (20 if "止" in candidate_text else 0) + candidate_text.count("年") * 4
    return candidate if candidate_score > current_score else current


def _collapse_property_synonym_fields(fields: dict[str, Any], display_fields: dict[str, Any]) -> dict[str, Any]:
    collapsed = dict(display_fields)
    is_new_version = _is_new_real_estate_cert(fields, collapsed)
    groups = [
        ("坐落", ("房地坐落",), ("property_address", "address", "location")) if is_new_version else ("房地坐落", ("坐落",), ("property_address", "address", "location")),
        ("权利性质", ("权属性质",), ("right_nature",)) if is_new_version else ("权属性质", ("权利性质",), ("right_nature",)),
        ("地号", ("宗地号",), ("parcel_number",)) if is_new_version else ("宗地号", ("地号",), ("parcel_number",)),
        ("使用期限", ("土地使用期限",), ("use_term", "land_use_term")) if is_new_version else ("土地使用期限", ("使用期限",), ("use_term", "land_use_term")),
        ("登记日期", ("登记日",), ("registration_date", "issue_date")),
    ]
    for preferred, aliases, raw_aliases in groups:
        values = [collapsed.get(preferred)]
        values.extend(collapsed.get(alias) for alias in aliases)
        values.extend(fields.get(alias) for alias in raw_aliases)
        for alias in aliases:
            collapsed.pop(alias, None)
        best: Any = ""
        for value in values:
            best = _more_complete_display_value(best, value)
        if best not in (None, "", [], {}):
            collapsed[preferred] = best

    ordered: dict[str, Any] = {}
    for label in PROPERTY_FIELD_ORDER:
        if label in collapsed:
            ordered[label] = collapsed[label]
    for label, value in collapsed.items():
        if label not in ordered:
            ordered[label] = value
    return ordered


def _format_value(value: Any) -> str:
    if isinstance(value, list):
        return "、".join(str(item) for item in value if item not in ("", None)) or "未识别"
    if isinstance(value, dict):
        if "value" in value and "unit" in value:
            return f"{value.get('value') or ''} {value.get('unit') or ''}".strip()
        if "amount" in value and "unit" in value:
            return f"{value.get('amount') or ''} {value.get('unit') or ''}".strip()
        return "，".join(f"{field_label(str(key))}: {_format_value(item)}" for key, item in value.items())
    text = str(value)
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        year, month, day = text.split("-")
        if year.isdigit() and month.isdigit() and day.isdigit():
            return f"{year}年{int(month)}月{int(day)}日"
    return text


def get_display_fields(result: dict[str, Any]) -> dict[str, Any]:
    fields = result.get("fields") or {}
    if not isinstance(fields, dict):
        return {}

    display_fields: dict[str, Any] = {}
    owner_display = _merge_owner_fields(fields)
    if owner_display:
        display_fields["权利人"] = owner_display
    source_keys = PROPERTY_FIELD_ORDER + list(ENGLISH_TO_CHINESE_FIELDS)
    for source_key in source_keys:
        if source_key not in fields:
            continue
        display_key = ENGLISH_TO_CHINESE_FIELDS.get(source_key, source_key)
        if display_key in display_fields:
            continue
        value = fields.get(source_key)
        if _is_empty_or_invalid(value) or _is_hidden_property_display_field(display_key, value):
            continue
        display_fields[display_key] = value

    for source_key, value in fields.items():
        if str(source_key) in FORBIDDEN_DISPLAY_KEYS:
            continue
        display_key = ENGLISH_TO_CHINESE_FIELDS.get(str(source_key), str(source_key))
        if result.get("doc_type") in {"property_cert", "real_estate_cert"} and display_key not in PROPERTY_FIELD_ORDER:
            continue
        if display_key in display_fields or source_key in ENGLISH_TO_CHINESE_FIELDS:
            continue
        if _is_empty_or_invalid(value) or _is_hidden_property_display_field(display_key, value):
            continue
        display_fields[display_key] = value
    if result.get("doc_type") in {"property_cert", "real_estate_cert"}:
        return _collapse_property_synonym_fields(fields, display_fields)
    return display_fields


def render_markdown(result: dict[str, Any]) -> str:
    if result.get("doc_type") == "id_card":
        fields = result.get("fields") or {}
        status = STATUS_LABELS.get(str(result.get("extraction_status") or ""), str(result.get("extraction_status") or ""))
        lines = [
            "## 居民身份证",
            f"- 资料类型：居民身份证",
            f"- 提取状态：{status}",
            "",
            "### 基础信息",
        ]
        for key in ("name", "gender", "ethnicity", "birth_date", "address", "id_number"):
            if fields.get(key):
                lines.append(f"- {field_label(key)}：{_format_value(fields.get(key))}")
        lines.extend(["", "### 签发信息"])
        for key in ("issuing_authority", "valid_from", "valid_to"):
            if fields.get(key):
                lines.append(f"- {field_label(key)}：{_format_value(fields.get(key))}")
        missing = result.get("missing_fields") or []
        if missing:
            lines.extend(["", "### 缺失字段"])
            lines.extend(f"- {field_label(str(item))}" for item in missing)
        validation = result.get("validation") if isinstance(result.get("validation"), dict) else {}
        notices = list(validation.get("warnings") or []) + list(validation.get("errors") or [])
        if notices:
            lines.extend(["", "### 校验提醒"])
            lines.extend(f"- {item}" for item in dict.fromkeys(str(item) for item in notices))
        return "\n".join(lines)

    if result.get("doc_type") in {"marriage_certificate", "marriage_cert"}:
        fields = result.get("fields") or {}
        holder_1 = fields.get("holder_1") if isinstance(fields.get("holder_1"), dict) else {}
        holder_2 = fields.get("holder_2") if isinstance(fields.get("holder_2"), dict) else {}
        lines = [
            "## 结婚证",
            f"- 资料类型：结婚证",
            f"- 婚姻状态：{_format_value(fields.get('marital_status') or '已婚')}",
        ]
        for label, value in (
            ("结婚证字号", fields.get("certificate_no") or fields.get("certificate_number")),
            ("登记机关", fields.get("registration_authority") or fields.get("issuing_authority")),
            ("发证日期", fields.get("issue_date")),
            ("登记日期", fields.get("marriage_date") or fields.get("registration_date")),
        ):
            if value:
                lines.append(f"- {label}：{_format_value(value)}")
        lines.extend(["", "### 配偶一"])
        for label, value in (
            ("姓名", holder_1.get("name") or fields.get("holder_name")),
            ("性别", holder_1.get("gender")),
            ("国籍", holder_1.get("nationality")),
            ("出生日期", holder_1.get("birth_date")),
            ("身份证号", holder_1.get("id_number") or fields.get("holder_id_number")),
        ):
            if value:
                lines.append(f"- {label}：{_format_value(value)}")
        lines.extend(["", "### 配偶二"])
        for label, value in (
            ("姓名", holder_2.get("name") or fields.get("spouse_name")),
            ("性别", holder_2.get("gender")),
            ("国籍", holder_2.get("nationality")),
            ("出生日期", holder_2.get("birth_date")),
            ("身份证号", holder_2.get("id_number") or fields.get("spouse_id_number")),
        ):
            if value:
                lines.append(f"- {label}：{_format_value(value)}")
        validation = result.get("validation") if isinstance(result.get("validation"), dict) else {}
        notices = list(validation.get("warnings") or []) + list(validation.get("errors") or [])
        if notices:
            lines.extend(["", "### 校验提醒"])
            lines.extend(f"- {item}" for item in dict.fromkeys(str(item) for item in notices))
        return "\n".join(lines)

    doc_type_name = result.get("doc_type_name") or "未知资料"
    lines = [
        f"## {doc_type_name}",
        "",
        "### 关键字段",
    ]
    fields = get_display_fields(result)
    if fields:
        for key, value in fields.items():
            lines.append(f"- {field_label(key)}: {_format_value(value)}")
    else:
        lines.append("- 暂无可展示字段")
    return "\n".join(lines)
