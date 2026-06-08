from __future__ import annotations

from typing import Any


def render_markdown(result: dict[str, Any]) -> str:
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    fields = result.get("fields") if isinstance(result.get("fields"), dict) else {}
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
    return "\n".join(lines).strip()
