from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _is_old_property_fields(fields: dict[str, Any], result: dict[str, Any]) -> bool:
    if result.get("old_version") is True:
        return True
    cert_number = str(fields.get("权证编号") or "")
    return any(key in fields for key in ("房地坐落", "权属性质", "使用权取得方式", "宗地号", "土地使用期限")) or "沪房地" in cert_number


def _display_fields(fields: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    next_fields = dict(fields)
    old_version = _is_old_property_fields(next_fields, result)
    if old_version:
        address = next_fields.get("房地坐落") or next_fields.get("坐落")
        if address:
            next_fields["房地坐落"] = address
        next_fields.pop("坐落", None)
    else:
        address = next_fields.get("坐落") or next_fields.get("房地坐落")
        if address:
            next_fields["坐落"] = address
        next_fields.pop("房地坐落", None)
    return next_fields


def render_markdown(result: dict[str, Any]) -> str:
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    raw_fields = result.get("fields") if isinstance(result.get("fields"), dict) else {}
    fields = _display_fields(raw_fields, result)
    validation = result.get("validation") if isinstance(result.get("validation"), dict) else {}
    lines = [
        "## 房产证/不动产权证",
        f"- 资料类型: {result.get('doc_type_name') or '房产证/不动产权证'}",
        f"- 来源文件: {metadata.get('filename') or ''}",
    ]
    supplemental = result.get("supplemental_files") if isinstance(result.get("supplemental_files"), list) else []
    if supplemental:
        lines.append(f"- 补充文件: {'、'.join(str(item) for item in supplemental if item)}")
    lines.append("- 原件状态: 可查看")
    if fields:
        lines.extend(["", "### 结构化提取结果"])
        logger.info("[PropertyRenderer][ADDRESS] fields_房地坐落=%s", fields.get("房地坐落"))
        logger.info("[PropertyRenderer][ADDRESS] fields_坐落=%s", fields.get("坐落"))
        logger.info("[PropertyRenderer][ADDRESS] display_order=%s", list(fields.keys()))
        for key, value in fields.items():
            if value:
                lines.append(f"- {key}: {value}")
    warnings = list(validation.get("warnings") or [])
    risk_sections = result.get("risk_sections") if isinstance(result.get("risk_sections"), dict) else {}
    for title, sections in risk_sections.items():
        lines.extend(["", f"### {title}信息"])
        for section in sections:
            if isinstance(section, dict):
                for key, value in section.items():
                    if value:
                        lines.append(f"- {key}: {value}")
    if warnings:
        lines.extend(["", "### 校验提醒"])
        lines.extend(f"- {warning}" for warning in warnings)
    markdown = "\n".join(lines).strip()
    logger.info("[PropertyRenderer][ADDRESS] markdown_contains_address=%s", str(("房地坐落" in markdown) or ("坐落" in markdown)).lower())
    return markdown
