from __future__ import annotations

from typing import Any


DETAIL_REQUIRED_FIELDS = ("权利人", "权证编号", "坐落", "房地坐落", "建筑面积", "不动产单元号")


def validate_property_cert(fields: dict[str, Any], page_roles: list[str]) -> tuple[dict[str, Any], list[str], str]:
    warnings: list[str] = []
    errors: list[str] = []
    has_detail = any(role in {"detail_page", "new_real_estate_detail_page", "old_property_detail_page"} for role in page_roles)
    if not has_detail and "cover_page" in page_roles:
        warnings.append("仅识别到封面页，未识别到字段页，请补充上传正面字段页或人工确认。")
    if not fields:
        errors.append("未提取到房产证/不动产权证业务字段")
    missing = [key for key in ("权利人", "权证编号") if not fields.get(key)]
    if not any(fields.get(key) for key in ("坐落", "房地坐落")):
        missing.append("坐落")
    status = "success" if fields and has_detail else "partial" if fields else "failed"
    return {
        "is_valid": not errors,
        "warnings": warnings,
        "errors": errors,
        "has_detail_page": has_detail,
    }, missing, status
