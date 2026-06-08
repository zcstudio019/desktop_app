from __future__ import annotations

import re
import logging
from typing import Any

logger = logging.getLogger(__name__)

NEW_FIELD_ORDER = [
    "权利人",
    "共有情况",
    "权证编号",
    "封面编号",
    "坐落",
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
    "登记机构",
]

OLD_FIELD_ORDER = [
    "权利人",
    "权证编号",
    "房地坐落",
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
    "登记日",
    "登记日期",
    "填证单位",
]

SYNONYM_GROUPS = (
    ("权利性质", "权属性质"),
    ("地号", "宗地号"),
    ("使用期限", "土地使用期限"),
    ("登记日期", "登记日"),
)

OLD_ADDRESS_KEYS = ("房地坐落", "坐落", "property_address", "address")
NEW_ADDRESS_KEYS = ("坐落", "房地坐落", "property_address", "address")
FIELD_ALIASES = {
    "building_type": "建筑类型",
    "house_type": "建筑类型",
}


def clean_value(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"\s+", " ", text).strip(" :：,，;；")
    return text


def is_old_version(page_role: str, fields: dict[str, Any]) -> bool:
    cert_number = str(fields.get("权证编号") or fields.get("certificate_number") or "")
    return (
        page_role == "old_property_detail_page"
        or any(key in fields for key in ("房地坐落", "权属性质", "使用权取得方式", "宗地号", "土地使用期限"))
        or "沪房地" in cert_number
    )


def _first_non_empty(fields: dict[str, str], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = fields.get(key, "")
        if value:
            return value
    return ""


def _normalize_address_aliases(fields: dict[str, str], *, old_version: bool) -> None:
    if old_version:
        address = _first_non_empty(fields, OLD_ADDRESS_KEYS)
        if address:
            fields["房地坐落"] = address
        fields.pop("坐落", None)
    else:
        address = _first_non_empty(fields, NEW_ADDRESS_KEYS)
        if address:
            fields["坐落"] = address
        fields.pop("房地坐落", None)
    fields.pop("property_address", None)
    fields.pop("address", None)


def normalize_fields(fields: dict[str, Any], *, old_version: bool = False) -> dict[str, Any]:
    old_version = old_version or is_old_version("", fields or {})
    cleaned = {key: clean_value(value) for key, value in (fields or {}).items() if clean_value(value)}
    for alias, target in FIELD_ALIASES.items():
        if cleaned.get(alias) and not cleaned.get(target):
            cleaned[target] = cleaned.pop(alias)
        else:
            cleaned.pop(alias, None)
    if old_version:
        logger.info("[PropertyNormalizer][ADDRESS] input_房地坐落=%s", cleaned.get("房地坐落"))
        logger.info("[PropertyNormalizer][ADDRESS] input_坐落=%s", cleaned.get("坐落"))
    _normalize_address_aliases(cleaned, old_version=old_version)
    for new_key, old_key in SYNONYM_GROUPS:
        if old_version:
            if new_key in cleaned and old_key in cleaned:
                cleaned.pop(new_key, None)
            elif new_key in cleaned and old_key not in cleaned:
                cleaned[old_key] = cleaned.pop(new_key)
        else:
            if old_key in cleaned and new_key in cleaned:
                cleaned.pop(old_key, None)
            elif old_key in cleaned and new_key not in cleaned:
                cleaned[new_key] = cleaned.pop(old_key)
    order = OLD_FIELD_ORDER if old_version else NEW_FIELD_ORDER
    ordered = {key: cleaned[key] for key in order if cleaned.get(key)}
    for key, value in cleaned.items():
        if key not in ordered:
            ordered[key] = value
    if old_version:
        logger.info("[PropertyNormalizer][ADDRESS] output_房地坐落=%s", ordered.get("房地坐落"))
        logger.info("[PropertyNormalizer][ADDRESS] output_坐落=%s", ordered.get("坐落"))
        logger.info("[PropertyNormalizer][ADDRESS] output_keys=%s", list(ordered.keys()))
    return ordered


def field_confidence(fields: dict[str, Any]) -> dict[str, float]:
    return {key: 0.86 for key, value in fields.items() if value}
