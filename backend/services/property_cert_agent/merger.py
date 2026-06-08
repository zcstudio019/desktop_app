from __future__ import annotations

from typing import Any

from .normalizer import is_old_version, normalize_fields


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
    if role == "detail_page":
        score += 30
    if role == "old_property_detail_page":
        score += 30
    if role == "cover_page":
        score -= 30
    return score


def merge_pages(pages: list[dict[str, Any]]) -> dict[str, Any]:
    detail_pages = [page for page in pages if page.get("page_role") in {"detail_page", "old_property_detail_page"}]
    main = max(detail_pages or pages, key=score_page, default={})
    old_version = is_old_version(str(main.get("page_role") or ""), main.get("fields") if isinstance(main.get("fields"), dict) else {})
    fields = dict(main.get("fields") or {})
    supplemental_files: list[str] = []
    risk_sections: dict[str, list[dict[str, Any]]] = {"附记": [], "抵押": []}
    for page in pages:
        role = str(page.get("page_role") or "")
        page_fields = page.get("fields") if isinstance(page.get("fields"), dict) else {}
        if page is not main and page.get("filename"):
            supplemental_files.append(str(page.get("filename")))
        if role == "cover_page":
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
            if value and key not in fields and key != "封面编号":
                fields[key] = value
    return {
        "fields": normalize_fields(fields, old_version=old_version),
        "main_page": main,
        "supplemental_files": sorted(set(supplemental_files)),
        "risk_sections": {key: value for key, value in risk_sections.items() if value},
        "old_version": old_version,
    }
