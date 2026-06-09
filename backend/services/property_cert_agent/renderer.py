from __future__ import annotations

import logging
import re
from typing import Any

from .normalizer import is_attachment_placeholder, normalize_property_cert_fields
from .skills.attachment_page_skill import is_valid_unit_number

logger = logging.getLogger(__name__)

OLD_DISPLAY_ORDER = [
    "权利人",
    "权证编号",
    "房地坐落",
    "坐落",
    "权属性质",
    "使用权取得方式",
    "土地用途",
    "房屋用途",
    "宗地号",
    "宗地面积",
    "土地使用期限",
    "室号或部位",
    "建筑面积",
    "建筑类型",
    "总层数",
    "竣工日期",
    "登记日期",
    "登记日",
    "填证单位",
]

NEW_DISPLAY_ORDER = [
    "权利人",
    "共有情况",
    "权证编号",
    "封面编号",
    "坐落",
    "房地坐落",
    "不动产单元号",
    "权利类型",
    "权利性质",
    "土地用途",
    "房屋用途",
    "地号",
    "宗地面积",
    "建筑面积",
    "使用期限",
    "室号或部位",
    "建筑类型",
    "总层数",
    "竣工日期",
    "登记日期",
    "登记日",
    "填证单位",
]

ADDRESS_ALIAS_KEYS = ("房地坐落", "坐落", "property_address", "address")
ADDRESS_LABELS = ("房地坐落", "坐落")
ADDRESS_FEATURE_PATTERN = re.compile(r"[区县市路街弄号镇村室]")
ADDRESS_STOP_LABELS = {
    "权利人",
    "权证编号",
    "权属性质",
    "建筑面积",
    "使用权取得方式",
    "建筑类型",
    "用途",
    "土地状况",
    "房屋状况",
    "宗地号",
    "宗地面积",
    "宗地丘面积",
    "使用期限",
    "土地使用期限",
    "室号或部位",
    "总层数",
    "竣工日期",
    "登记日期",
    "登记日",
    "填证单位",
}
ATTACHMENT_DETAIL_COLUMNS = ("不动产单元号", "室号或部位", "建筑面积", "房屋用途", "建筑类型", "总层数", "竣工日期")


def _normalize_label(text: str) -> str:
    return re.sub(r"[\s:：()（）/、]+", "", str(text or ""))


def _is_stop_label(text: str) -> bool:
    compact = _normalize_label(text)
    return compact in ADDRESS_STOP_LABELS


def _looks_like_address(text: str) -> bool:
    value = str(text or "").strip(" :：")
    return bool(value and ADDRESS_FEATURE_PATTERN.search(value) and not _is_stop_label(value))


def _field_address(fields: dict[str, Any]) -> str:
    for key in ADDRESS_ALIAS_KEYS:
        value = str(fields.get(key) or "").strip()
        if _looks_like_address(value):
            return value
    return ""


def _recover_address_from_raw_text(raw_text: str | None) -> str:
    lines = [line.strip() for line in str(raw_text or "").splitlines()]
    for index, line in enumerate(lines):
        if not line:
            continue
        normalized = _normalize_label(line)
        for label in ADDRESS_LABELS:
            normalized_label = _normalize_label(label)
            if normalized == normalized_label:
                for candidate in lines[index + 1 :]:
                    candidate = candidate.strip()
                    if not candidate:
                        continue
                    if _is_stop_label(candidate):
                        break
                    if _looks_like_address(candidate):
                        return candidate
            if normalized.startswith(normalized_label) and ("：" in line or ":" in line):
                candidate = re.split(r"[:：]", line, maxsplit=1)[-1].strip()
                if _looks_like_address(candidate):
                    return candidate
    return ""


def _is_old_property_fields(fields: dict[str, Any], result: dict[str, Any] | None = None, raw_text: str | None = None, doc_version: str | None = None) -> bool:
    result = result or {}
    if result.get("old_version") is True or doc_version in {"old_shanghai_property_cert", "old_property_detail_page"}:
        return True
    cert_number = str(fields.get("权证编号") or fields.get("certificate_number") or "")
    source_text = str(raw_text or "")
    return (
        any(key in fields for key in ("房地坐落", "权属性质", "使用权取得方式", "宗地号", "土地使用期限"))
        or "沪房地" in cert_number
        or "房地产权证" in source_text
    )


def _ordered_fields(fields: dict[str, Any], *, old_version: bool) -> dict[str, Any]:
    order = OLD_DISPLAY_ORDER if old_version else NEW_DISPLAY_ORDER
    ordered = {key: fields[key] for key in order if fields.get(key)}
    for key, value in fields.items():
        if key not in ordered and value:
            ordered[key] = value
    return ordered


def _ensure_property_address_for_render_with_info(
    fields: dict[str, Any],
    raw_text: str | None = None,
    doc_version: str | None = None,
    result: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    next_fields = dict(fields or {})
    old_version = _is_old_property_fields(next_fields, result, raw_text, doc_version)
    input_address = _field_address(next_fields)
    recovered_from_raw_text = False
    address = input_address
    if not address:
        address = _recover_address_from_raw_text(raw_text)
        recovered_from_raw_text = bool(address)
    final_label = "房地坐落" if old_version else "坐落"
    if address:
        next_fields[final_label] = address
        if final_label == "房地坐落":
            next_fields.pop("坐落", None)
        else:
            next_fields.pop("房地坐落", None)
        next_fields.pop("property_address", None)
        next_fields.pop("address", None)
    return _ordered_fields(next_fields, old_version=old_version), {
        "old_version": old_version,
        "recovered_from_raw_text": recovered_from_raw_text,
        "final_label": final_label if address else "",
        "final_value": address,
    }


def ensure_property_address_for_render(fields: dict[str, Any], raw_text: str | None = None, doc_version: str | None = None) -> dict[str, Any]:
    ensured, _info = _ensure_property_address_for_render_with_info(fields, raw_text, doc_version)
    return ensured


def _display_fields(fields: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    raw_text = str(result.get("_raw_text") or result.get("raw_text") or result.get("raw_text_preview") or "")
    ensured, _info = _ensure_property_address_for_render_with_info(fields, raw_text, str(result.get("doc_version") or ""), result)
    return ensured


def _attachment_detail_rows(risk_sections: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sections = risk_sections.get("附记") if isinstance(risk_sections, dict) else None
    if not isinstance(sections, list):
        return rows
    for section in sections:
        if not isinstance(section, dict):
            continue
        details = section.get("附记明细")
        if isinstance(details, list):
            rows.extend(row for row in details if isinstance(row, dict))
    return rows


def _attachment_summary(risk_sections: dict[str, Any]) -> dict[str, str]:
    summary: dict[str, str] = {}
    sections = risk_sections.get("附记") if isinstance(risk_sections, dict) else None
    if not isinstance(sections, list):
        return summary
    mapping = {
        "室号或部位": "室号或部位列表",
        "建筑面积": "建筑面积列表",
        "房屋用途": "房屋用途列表",
        "建筑类型": "建筑类型列表",
        "总层数": "总层数列表",
        "竣工日期": "竣工日期列表",
    }
    for label, key in mapping.items():
        values: list[str] = []
        for section in sections:
            if not isinstance(section, dict):
                continue
            raw = section.get(key)
            if not isinstance(raw, list):
                continue
            for item in raw:
                value = str(item or "").replace("\\n", " ").replace("\n", " ").strip()
                if value and value not in values:
                    values.append(value)
        if values:
            summary[label] = "、".join(values)
    return summary


def _append_attachment_detail_table(lines: list[str], rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    columns = [column for column in ATTACHMENT_DETAIL_COLUMNS if any(str(row.get(column) or "").strip() for row in rows)]
    if "不动产单元号" in columns and not any(is_valid_unit_number(row.get("不动产单元号")) for row in rows):
        columns = [column for column in columns if column != "不动产单元号"]
    if not columns:
        return
    lines.extend(["", "### 附记明细"])
    lines.append("| " + " | ".join(columns) + " |")
    lines.append("| " + " | ".join("---" for _ in columns) + " |")
    for row in rows:
        values: list[str] = []
        for column in columns:
            value = str(row.get(column) or "").replace("\\n", " ").replace("\n", " ").replace("|", "/").strip(" ;；")
            if column == "不动产单元号" and value and not is_valid_unit_number(value):
                value = ""
            values.append(value)
        lines.append("| " + " | ".join(values) + " |")


def _append_attachment_summary(lines: list[str], summary: dict[str, str]) -> None:
    if not summary:
        return
    lines.extend(["", "### 附记明细"])
    for key, value in summary.items():
        if value:
            lines.append(f"- {key}: {value}")


def render_markdown(result: dict[str, Any]) -> str:
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    raw_fields = result.get("fields") if isinstance(result.get("fields"), dict) else {}
    raw_text = str(result.get("_raw_text") or result.get("raw_text") or result.get("raw_text_preview") or "")
    page_roles = result.get("page_roles") if isinstance(result.get("page_roles"), list) else []
    page_role = str(page_roles[0] if page_roles else result.get("page_role") or "")
    fields = normalize_property_cert_fields(raw_fields, raw_text=raw_text, page_role=page_role, cert_version=str(result.get("doc_version") or ""))
    fields, address_info = _ensure_property_address_for_render_with_info(fields, raw_text, str(result.get("doc_version") or ""), result)
    logger.info("[PropertyRenderer] using_normalized_fields=true")
    logger.info("[PropertyRenderer][FINAL_ADDRESS] old_version=%s", str(address_info["old_version"]).lower())
    logger.info("[PropertyRenderer][FINAL_ADDRESS] input_房地坐落=%s", raw_fields.get("房地坐落"))
    logger.info("[PropertyRenderer][FINAL_ADDRESS] input_坐落=%s", raw_fields.get("坐落"))
    logger.info("[PropertyRenderer][FINAL_ADDRESS] recovered_from_raw_text=%s", str(address_info["recovered_from_raw_text"]).lower())
    logger.info("[PropertyRenderer][FINAL_ADDRESS] final_label=%s", address_info["final_label"])
    logger.info("[PropertyRenderer][FINAL_ADDRESS] final_value=%s", address_info["final_value"])
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
        logger.info("[PropertyRenderer] display_权利人=%s", fields.get("权利人") or "")
        logger.info("[PropertyRenderer] display_共有情况=%s", fields.get("共有情况") or "")
        logger.info("[PropertyRenderer] display_建筑类型=%s", fields.get("建筑类型") or "")
        logger.info("[PropertyRenderer][DISPLAY] 土地用途=%s", fields.get("土地用途") or "")
        logger.info("[PropertyRenderer][DISPLAY] 房屋用途=%s", fields.get("房屋用途") or "")
        for debug_key in ("房屋用途", "建筑类型", "室号或部位", "总层数", "竣工日期"):
            logger.info("[PropertyRenderer][DISPLAY] %s=%s", debug_key, fields.get(debug_key) or "")
        for key, value in fields.items():
            if value:
                lines.append(f"- {key}: {value}")
    warnings = list(validation.get("warnings") or [])
    risk_sections = result.get("risk_sections") if isinstance(result.get("risk_sections"), dict) else {}
    attachment_rows = _attachment_detail_rows(risk_sections)
    if attachment_rows:
        _append_attachment_detail_table(lines, attachment_rows)
    else:
        _append_attachment_summary(lines, _attachment_summary(risk_sections))
    for title, sections in risk_sections.items():
        if title == "附记" and attachment_rows:
            continue
        lines.extend(["", f"### {title}信息"])
        for section in sections:
            if isinstance(section, dict):
                for key, value in section.items():
                    if value and not is_attachment_placeholder(value):
                        lines.append(f"- {key}: {value}")
    if warnings:
        lines.extend(["", "### 校验提醒"])
        lines.extend(f"- {warning}" for warning in warnings)
    markdown = "\n".join(lines).strip()
    logger.info("[PropertyRenderer][ADDRESS] markdown_contains_address=%s", str(("房地坐落" in markdown) or ("坐落" in markdown)).lower())
    logger.info("[PropertyRenderer][FINAL_ADDRESS] markdown_contains=%s", str(bool(address_info["final_value"]) and address_info["final_value"] in markdown).lower())
    logger.info("[PropertyRenderer] markdown_contains_详见附记=%s", str("详见附记" in markdown).lower())
    logger.info("[PropertyRenderer][DISPLAY] markdown_contains_房屋用途=%s", str("房屋用途: 商业" in markdown or "房屋用途：商业" in markdown).lower())
    logger.info("[PropertyRenderer][DISPLAY] markdown_contains_总层数=%s", str("总层数: 6、4" in markdown or "总层数：6、4" in markdown).lower())
    logger.info("[PropertyRenderer][DISPLAY] markdown_contains_建筑类型=%s", str("建筑类型: 商场" in markdown or "建筑类型：商场" in markdown).lower())
    logger.info("[PropertyRenderer][DISPLAY] markdown_contains_附记明细=%s", str("### 附记明细" in markdown).lower())
    logger.info("[PropertyRenderer] markdown_contains_invalid_unit_number=%s", str("不动产单元号: 使用权" in markdown or "不动产单元号：使用权" in markdown).lower())
    logger.info("[PropertyRenderer] markdown_contains_建筑类型_国有=%s", str("建筑类型: 国有" in markdown or "建筑类型：国有" in markdown).lower())
    attachment_fields_present = all(item in markdown for item in ("房屋用途: 商业", "建筑类型: 商场", "总层数: 6、4", "竣工日期: 1990年、1979年"))
    logger.info("[PropertyRenderer] markdown_contains_attachment_fields=%s", str(attachment_fields_present).lower())
    logger.info("[PropertyRenderer] markdown_contains_dirty_newline=%s", str("\\n" in markdown).lower())
    return markdown
