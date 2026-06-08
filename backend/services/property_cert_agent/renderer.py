from __future__ import annotations

import logging
import re
from typing import Any

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


def render_markdown(result: dict[str, Any]) -> str:
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    raw_fields = result.get("fields") if isinstance(result.get("fields"), dict) else {}
    raw_text = str(result.get("_raw_text") or result.get("raw_text") or result.get("raw_text_preview") or "")
    fields, address_info = _ensure_property_address_for_render_with_info(raw_fields, raw_text, str(result.get("doc_version") or ""), result)
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
    logger.info("[PropertyRenderer][FINAL_ADDRESS] markdown_contains=%s", str(bool(address_info["final_value"]) and address_info["final_value"] in markdown).lower())
    return markdown
