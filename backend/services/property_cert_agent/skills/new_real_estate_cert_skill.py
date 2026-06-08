from __future__ import annotations

import re
from typing import Any

from .common import certificate_number, clean, label_value, lines, normalize_use_term, split_usage


def _right_nature(text: str) -> str:
    split_lines = lines(text)
    for index, line in enumerate(split_lines):
        if "权利性质" not in line:
            continue
        after = clean(line.split("权利性质", 1)[-1])
        if after:
            return after
        for candidate in split_lines[index + 1 : index + 3]:
            candidate = clean(candidate)
            if candidate:
                return candidate
    return ""


def _area(value: str) -> str:
    text = clean(value)
    if text and re.fullmatch(r"\d+(?:\.\d+)?", text):
        return f"{text} 平方米"
    return text


def extract(payload: dict[str, Any]) -> dict[str, Any]:
    text = str(payload.get("text") or "")
    fields: dict[str, Any] = {}
    cert_no = certificate_number(text)
    if cert_no:
        fields["权证编号"] = cert_no
    mappings = (
        ("权利人", ("权利人",)),
        ("共有情况", ("共有情况",)),
        ("坐落", ("坐落",)),
        ("不动产单元号", ("不动产单元号",)),
        ("权利类型", ("权利类型",)),
        ("宗地面积", ("宗地面积",)),
        ("建筑面积", ("建筑面积",)),
        ("地号", ("地号",)),
        ("室号或部位", ("室号或部位", "室号部位", "室号 部位")),
        ("建筑类型", ("建筑类型",)),
        ("总层数", ("总层数",)),
        ("竣工日期", ("竣工日期",)),
        ("登记日期", ("登记日期", "登记日")),
        ("封面编号", ("封面编号",)),
    )
    for output_key, labels in mappings:
        value = label_value(text, labels)
        if value:
            if output_key in {"宗地面积", "建筑面积"}:
                value = _area(value)
            fields[output_key] = value
    right_nature = _right_nature(text)
    if right_nature:
        fields["权利性质"] = right_nature
    land_use, house_use = split_usage(text)
    if land_use:
        fields["土地用途"] = land_use
    if house_use:
        fields["房屋用途"] = house_use
    use_term = normalize_use_term(text) or label_value(text, ("使用期限",))
    if use_term:
        fields["使用期限"] = use_term
    return {"fields": fields, "warnings": [], "page_role": "detail_page"}
