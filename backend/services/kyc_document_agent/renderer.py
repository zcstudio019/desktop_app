from __future__ import annotations

from typing import Any


FIELD_LABELS = {
    "owner": "权利人",
    "co_owners": "共有人",
    "certificate_number": "权证编号",
    "property_unit_number": "不动产单元号",
    "property_address": "房地坐落",
    "right_type": "权利类型",
    "right_nature": "权属性质",
    "use_type": "用途",
    "building_area": "建筑面积",
    "land_area": "土地面积",
    "total_area": "总面积",
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
    return FIELD_LABELS.get(field, field)


def _format_value(value: Any) -> str:
    if isinstance(value, list):
        return "、".join(str(item) for item in value if item not in ("", None)) or "未识别"
    if isinstance(value, dict):
        return "，".join(f"{field_label(str(key))}: {_format_value(item)}" for key, item in value.items())
    return str(value)


def render_markdown(result: dict[str, Any]) -> str:
    lines = [
        f"## {result.get('doc_type_name') or '未知资料'}",
        "",
        f"- 资料类型编码: `{result.get('doc_type') or 'unknown'}`",
        f"- 资料名称: {result.get('doc_type_name') or '未知资料'}",
        f"- 归属类型: {result.get('owner_type') or 'unknown'}",
        f"- 提取状态: `{result.get('extraction_status') or 'failed'}`",
        f"- 置信度: {result.get('confidence', {}).get('overall', 0):.2f}",
        "",
        "### 关键字段",
    ]
    fields = result.get("fields") or {}
    if fields:
        for key, value in fields.items():
            if key in {"owner", "co_owners", "certificate_number", "property_address", "right_nature", "use_type", "building_area", "land_area", "total_area", "issue_date"} and field_label(key) in fields:
                continue
            if value not in ("", None, [], {}):
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
        for field, item in evidence.items():
            if field in {"owner", "co_owners", "certificate_number", "property_address", "right_nature", "use_type", "building_area", "land_area", "total_area", "issue_date"} and field_label(field) in evidence:
                continue
            lines.append(f"- {field_label(field)}: {item.get('evidence_text', '')}")
    else:
        lines.append("- 无")
    return "\n".join(lines)
