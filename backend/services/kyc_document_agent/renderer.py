from __future__ import annotations

from typing import Any


def render_markdown(result: dict[str, Any]) -> str:
    lines = [
        f"## {result.get('doc_type_name') or '未知资料'}",
        "",
        f"- 资料类型: `{result.get('doc_type') or 'unknown'}`",
        f"- 抽取状态: `{result.get('extraction_status') or 'failed'}`",
        f"- 整体置信度: {result.get('confidence', {}).get('overall', 0):.2f}",
        "",
        "### 关键字段",
    ]
    fields = result.get("fields") or {}
    if fields:
        for key, value in fields.items():
            if value not in ("", None, [], {}):
                lines.append(f"- {key}: {value}")
    else:
        lines.append("- 无")

    lines.extend(["", "### 缺失字段"])
    missing = result.get("missing_fields") or []
    lines.append("- " + ", ".join(missing) if missing else "- 无")

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
            lines.append(f"- {field}: {item.get('evidence_text', '')}")
    else:
        lines.append("- 无")
    return "\n".join(lines)
