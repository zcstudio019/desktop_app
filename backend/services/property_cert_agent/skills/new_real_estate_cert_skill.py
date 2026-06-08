from __future__ import annotations

from typing import Any

from .common import certificate_number, label_value, normalize_use_term, split_usage


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
        ("权利性质", ("权利性质",)),
        ("宗地面积", ("宗地面积",)),
        ("建筑面积", ("建筑面积",)),
        ("地号", ("地号",)),
        ("室号或部位", ("室号或部位", "室号部位", "室号 部位")),
        ("建筑类型", ("建筑类型",)),
        ("总层数", ("总层数",)),
        ("竣工日期", ("竣工日期",)),
    )
    for output_key, labels in mappings:
        value = label_value(text, labels)
        if value:
            fields[output_key] = value
    land_use, house_use = split_usage(text)
    if land_use:
        fields["土地用途"] = land_use
    if house_use:
        fields["房屋用途"] = house_use
    use_term = normalize_use_term(text) or label_value(text, ("使用期限",))
    if use_term:
        fields["使用期限"] = use_term
    return {"fields": fields, "warnings": [], "page_role": "detail_page"}
