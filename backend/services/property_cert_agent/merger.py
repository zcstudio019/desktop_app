from __future__ import annotations

import logging
from typing import Any

from .normalizer import is_invalid_property_owner, is_old_version, normalize_fields
from .skills.attachment_page_skill import is_valid_building_type, is_valid_unit_number, is_valid_unit_number_value

logger = logging.getLogger(__name__)


OLD_ADDRESS_KEYS = ("房地坐落", "坐落", "property_address", "address")
NEW_ADDRESS_KEYS = ("坐落", "房地坐落", "property_address", "address")
ATTACHMENT_PLACEHOLDERS = ("详见附记", "详见附页", "见附记", "详见附表", "详见附记页", "详见附件")
ATTACHMENT_FILL_MAP = {
    "不动产单元号": "不动产单元号列表",
    "房屋用途": "房屋用途列表",
    "建筑类型": "建筑类型列表",
    "室号或部位": "室号或部位列表",
    "总层数": "总层数列表",
    "竣工日期": "竣工日期列表",
    "土地用途": "土地用途列表",
    "权利性质": "权利性质列表",
    "使用期限": "使用期限列表",
}


def _first_non_empty(fields: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = str(fields.get(key) or "").strip()
        if value:
            return value
    return ""


def _apply_address_priority(fields: dict[str, Any], *, old_version: bool) -> None:
    if old_version:
        address = _first_non_empty(fields, OLD_ADDRESS_KEYS)
        if address:
            fields["房地坐落"] = address
    else:
        address = _first_non_empty(fields, NEW_ADDRESS_KEYS)
        if address:
            fields["坐落"] = address


def is_attachment_placeholder(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(text and any(placeholder in text for placeholder in ATTACHMENT_PLACEHOLDERS))


def _attachment_values(attachment_pages: list[dict[str, Any]], list_key: str) -> list[str]:
    values: list[str] = []
    for page_fields in attachment_pages:
        raw = page_fields.get(list_key)
        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, dict):
                    continue
                value = str(item or "").strip()
                if list_key == "不动产单元号列表" and not is_valid_unit_number(value):
                    logger.info("[PropertyMerger][AttachmentFill] skip_invalid_unit_number=%s", value)
                    continue
                if list_key == "建筑类型列表" and not is_valid_building_type(value):
                    continue
                if value and value not in values:
                    values.append(value)
        elif raw:
            value = str(raw).strip()
            if list_key == "不动产单元号列表" and not is_valid_unit_number(value):
                logger.info("[PropertyMerger][AttachmentFill] skip_invalid_unit_number=%s", value)
                continue
            if list_key == "建筑类型列表" and not is_valid_building_type(value):
                continue
            if value and value not in values:
                values.append(value)
    return values


def _display_attachment_section(page_fields: dict[str, Any]) -> dict[str, Any]:
    display: dict[str, Any] = {}
    if page_fields.get("附记明细"):
        display["附记明细"] = page_fields["附记明细"]
    for key in ("室号或部位列表", "建筑面积列表", "房屋用途列表", "建筑类型列表", "总层数列表", "竣工日期列表", "不动产单元号列表"):
        if page_fields.get(key):
            display[key] = page_fields[key]
    return display


def merge_detail_page_with_attachment_pages(
    detail_fields: dict[str, Any],
    attachment_pages: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[str]]:
    fields = dict(detail_fields or {})
    warnings: list[str] = []
    if not attachment_pages:
        return fields, warnings

    for target_key, list_key in ATTACHMENT_FILL_MAP.items():
        values = _attachment_values(attachment_pages, list_key)
        current = fields.get(target_key)
        current_invalid_unit = target_key == "不动产单元号" and bool(current) and not is_valid_unit_number_value(current)
        current_invalid_building_type = target_key == "建筑类型" and bool(current) and not is_valid_building_type(current)
        if current_invalid_unit:
            logger.info("[PropertyMerger][AttachmentFill] skip_invalid_unit_number=%s", current)
        if values and (not current or is_attachment_placeholder(current) or current_invalid_unit or current_invalid_building_type):
            new_value = "、".join(values)
            logger.info("[PropertyMerger][AttachmentFill] field=%s old=%s new=%s", target_key, current or "", new_value)
            fields[target_key] = new_value
        elif is_attachment_placeholder(current) or current_invalid_unit or current_invalid_building_type:
            fields.pop(target_key, None)
            if target_key == "不动产单元号":
                warnings.append("不动产单元号在附记页中，系统未能识别到合法编号，请人工确认。")
            else:
                warnings.append(f"{target_key} 需要附件页回填，但未解析到可回填内容")
    return fields, warnings


def score_page(page: dict[str, Any]) -> int:
    fields = page.get("fields") if isinstance(page.get("fields"), dict) else {}
    role = str(page.get("page_role") or "")
    score = 0
    if fields:
        score += 20
    else:
        score -= 50
    if fields.get("权利人"):
        score += 15
    if fields.get("权证编号"):
        score += 15
    if fields.get("坐落") or fields.get("房地坐落"):
        score += 15
    if fields.get("不动产单元号"):
        score += 10
    if fields.get("建筑面积"):
        score += 10
    if fields.get("使用期限") or fields.get("土地使用期限"):
        score += 10
    if role in {"detail_page", "new_real_estate_detail_page"}:
        score += 30
    if role == "old_property_detail_page":
        score += 30
    if role == "cover_page":
        score -= 30
    return score


def merge_pages(pages: list[dict[str, Any]]) -> dict[str, Any]:
    detail_pages = [page for page in pages if page.get("page_role") in {"detail_page", "new_real_estate_detail_page", "old_property_detail_page"}]
    main = max(detail_pages or pages, key=score_page, default={})
    old_version = is_old_version(str(main.get("page_role") or ""), main.get("fields") if isinstance(main.get("fields"), dict) else {})
    fields = dict(main.get("fields") or {})
    if is_invalid_property_owner(fields.get("权利人")):
        logger.info("[PropertyMerger][OWNER] cover_owner_ignored=true")
        fields.pop("权利人", None)
    logger.info("[PropertyMerger][ADDRESS] merged_before_房地坐落=%s", fields.get("房地坐落"))
    for debug_key in ("房屋用途", "建筑类型", "室号或部位", "总层数", "竣工日期"):
        logger.info("[PropertyMerger][BEFORE] %s=%s", debug_key, fields.get(debug_key))
    supplemental_files: list[str] = []
    risk_sections: dict[str, list[dict[str, Any]]] = {"附记": [], "抵押": []}
    attachment_pages: list[dict[str, Any]] = []
    warnings: list[str] = []
    for page in pages:
        role = str(page.get("page_role") or "")
        page_fields = page.get("fields") if isinstance(page.get("fields"), dict) else {}
        logger.info("[PropertyMerger][ADDRESS] page_fields_房地坐落=%s", page_fields.get("房地坐落"))
        if page is not main and page.get("filename"):
            supplemental_files.append(str(page.get("filename")))
        if role == "cover_page":
            if page_fields.get("权利人") or page_fields.get("共有情况"):
                logger.info("[PropertyMerger][OWNER] cover_owner_ignored=true")
            for key in ("登记日期", "登记机构", "封面编号"):
                if page_fields.get(key) and not fields.get(key):
                    fields[key] = page_fields[key]
            continue
        if role == "attachment_page":
            attachment_pages.append(page_fields)
            display_section = _display_attachment_section(page_fields)
            if display_section:
                risk_sections["附记"].append(display_section)
            continue
        if role == "mortgage_page":
            risk_sections["抵押"].append(page_fields)
            continue
        if page is main:
            continue
        for key, value in page_fields.items():
            if key in {"权利人", "共有情况"} and (role == "cover_page" or is_invalid_property_owner(value)):
                logger.info("[PropertyMerger][OWNER] cover_owner_ignored=true")
                continue
            if value and key not in fields and key != "封面编号":
                fields[key] = value
    fields, attachment_warnings = merge_detail_page_with_attachment_pages(fields, attachment_pages)
    warnings.extend(attachment_warnings)
    for debug_key in ("房屋用途", "建筑类型", "室号或部位", "总层数", "竣工日期"):
        logger.info("[PropertyMerger][AFTER] %s=%s", debug_key, fields.get(debug_key))
    _apply_address_priority(fields, old_version=old_version)
    normalized_fields = normalize_fields(fields, old_version=old_version)
    logger.info("[PropertyMerger][ADDRESS] merged_after_房地坐落=%s", normalized_fields.get("房地坐落"))
    logger.info("[PropertyMerger][ADDRESS] merged_keys=%s", list(normalized_fields.keys()))
    return {
        "fields": normalized_fields,
        "main_page": main,
        "supplemental_files": sorted(set(supplemental_files)),
        "risk_sections": {key: value for key, value in risk_sections.items() if value},
        "old_version": old_version,
        "warnings": warnings,
    }
