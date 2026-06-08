from __future__ import annotations

import re
from typing import Any

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
    "宗地号",
    "宗地面积",
    "土地使用期限",
    "室号或部位",
    "建筑面积",
    "建筑类型",
    "房屋用途",
    "总层数",
    "竣工日期",
    "登记日",
    "填证单位",
]

SYNONYM_GROUPS = (
    ("坐落", "房地坐落"),
    ("权利性质", "权属性质"),
    ("地号", "宗地号"),
    ("使用期限", "土地使用期限"),
    ("登记日期", "登记日"),
)


def clean_value(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"\s+", " ", text).strip(" :：,，;；")
    return text


def is_old_version(page_role: str, fields: dict[str, Any]) -> bool:
    return page_role == "old_property_detail_page" or any(key in fields for key in ("房地坐落", "权属性质", "宗地号", "土地使用期限"))


def normalize_fields(fields: dict[str, Any], *, old_version: bool = False) -> dict[str, Any]:
    cleaned = {key: clean_value(value) for key, value in (fields or {}).items() if clean_value(value)}
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
    return ordered


def field_confidence(fields: dict[str, Any]) -> dict[str, float]:
    return {key: 0.86 for key, value in fields.items() if value}
