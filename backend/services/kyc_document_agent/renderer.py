from __future__ import annotations

from typing import Any


PROPERTY_FIELD_ORDER = [
    "权利人",
    "共有人",
    "权证编号",
    "房地坐落",
    "权属性质",
    "使用权取得方式",
    "土地用途",
    "宗地号",
    "宗地面积",
    "使用权面积",
    "土地使用期限",
    "室号或部位",
    "建筑面积",
    "建筑类型",
    "房屋用途",
    "总层数",
    "竣工日期",
    "登记日",
    "填证单位",
]

ENGLISH_TO_CHINESE_FIELDS = {
    "owner": "权利人",
    "co_owners": "共有人",
    "certificate_number": "权证编号",
    "property_address": "房地坐落",
    "right_type": "权利类型",
    "right_nature": "权属性质",
    "acquisition_method": "使用权取得方式",
    "land_use": "土地用途",
    "use_type": "房屋用途",
    "parcel_number": "宗地号",
    "land_area": "宗地面积",
    "usage_area": "使用权面积",
    "total_area": "使用权面积",
    "land_use_term": "土地使用期限",
    "room_number": "室号或部位",
    "building_area": "建筑面积",
    "building_type": "建筑类型",
    "total_floors": "总层数",
    "completion_date": "竣工日期",
    "registration_date": "登记日",
    "issue_date": "登记日",
    "issuing_unit": "填证单位",
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

FIELD_LABELS = {
    "owner": "权利人",
    "co_owners": "共有人",
    "certificate_number": "权证编号",
    "property_unit_number": "不动产单元号",
    "property_address": "房地坐落",
    "right_type": "权利类型",
    "right_nature": "权属性质",
    "use_type": "房屋用途",
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


def _is_invalid_for_display_field(field: str, value: Any) -> bool:
    text = _format_value(value).strip()
    if field == "竣工日期":
        return not ("年" in text or (len(text) == 10 and text[4] == "-" and text[7] == "-"))
    if field == "房屋用途":
        return text in {"住宅用地", "城镇住宅用地", "住宅用地/居住用地"}
    return False


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
    source_keys = PROPERTY_FIELD_ORDER + list(ENGLISH_TO_CHINESE_FIELDS)
    for source_key in source_keys:
        if source_key not in fields:
            continue
        display_key = ENGLISH_TO_CHINESE_FIELDS.get(source_key, source_key)
        if display_key in display_fields:
            continue
        value = fields.get(source_key)
        if _is_empty_or_invalid(value) or _is_invalid_for_display_field(display_key, value):
            continue
        display_fields[display_key] = value

    for source_key, value in fields.items():
        display_key = ENGLISH_TO_CHINESE_FIELDS.get(str(source_key), str(source_key))
        if display_key in display_fields or source_key in ENGLISH_TO_CHINESE_FIELDS:
            continue
        if _is_empty_or_invalid(value) or _is_invalid_for_display_field(display_key, value):
            continue
        display_fields[display_key] = value
    return display_fields


def render_markdown(result: dict[str, Any]) -> str:
    doc_type = result.get("doc_type") or "unknown"
    doc_type_name = result.get("doc_type_name") or "未知资料"
    metadata = result.get("metadata") or {}
    filename = metadata.get("filename") or metadata.get("source_file") or result.get("filename") or result.get("source_file") or ""
    agent_type = result.get("agent_type") or "kyc_document_agent"
    lines = [
        f"## {doc_type_name}",
        "",
        f"- 资料类型编码: {doc_type}",
        f"- 资料名称: {doc_type_name}",
        f"- 归属类型: {OWNER_TYPE_LABELS.get(str(result.get('owner_type') or 'unknown'), str(result.get('owner_type') or 'unknown'))}",
        f"- 来源文件: {filename or '未记录'}",
        "- 原件状态: 可查看",
        f"- 提取状态: {STATUS_LABELS.get(str(result.get('extraction_status') or 'failed'), str(result.get('extraction_status') or 'failed'))}",
        f"- 处理 Agent: {AGENT_LABELS.get(str(agent_type), str(agent_type))}",
        f"- 置信度: {result.get('confidence', {}).get('overall', 0):.2f}",
        "",
        "### 关键字段",
    ]
    fields = get_display_fields(result)
    if fields:
        for key, value in fields.items():
            lines.append(f"- {field_label(key)}: {_format_value(value)}")
    else:
        lines.append("- 无")

    lines.extend(["", "### 缺失字段"])
    missing = result.get("missing_fields") or []
    lines.append("- " + "、".join(field_label(str(item)) for item in missing) if missing else "- 无")

    validation = result.get("validation") or {}
    notices = (validation.get("errors") or []) + (validation.get("warnings") or [])
    lines.extend(["", "### 校验提醒"])
    if notices:
        lines.extend(f"- {notice}" for notice in notices)
    else:
        lines.append("- 无")

    lines.extend(["", "### 证据摘要"])
    evidence = result.get("evidence") or {}
    if evidence:
        seen_evidence: set[str] = set()
        for field, item in evidence.items():
            display_field = ENGLISH_TO_CHINESE_FIELDS.get(str(field), str(field))
            if display_field not in fields or display_field in seen_evidence or not isinstance(item, dict):
                continue
            seen_evidence.add(display_field)
            lines.append(f"- {field_label(display_field)}: {item.get('evidence_text', '')}")
    else:
        lines.append("- 无")
    return "\n".join(lines)
