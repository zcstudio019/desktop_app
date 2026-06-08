from __future__ import annotations

import logging
from typing import Any

from .normalizer import is_invalid_property_owner, is_old_version, normalize_fields

logger = logging.getLogger(__name__)


OLD_ADDRESS_KEYS = ("房地坐落", "坐落", "property_address", "address")
NEW_ADDRESS_KEYS = ("坐落", "房地坐落", "property_address", "address")


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
    supplemental_files: list[str] = []
    risk_sections: dict[str, list[dict[str, Any]]] = {"附记": [], "抵押": []}
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
            risk_sections["附记"].append(page_fields)
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
    }
