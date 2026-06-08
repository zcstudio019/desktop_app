from __future__ import annotations

import re
import logging
from typing import Any

from .common import certificate_number, clean, label_value, lines, normalize_use_term, split_usage

logger = logging.getLogger(__name__)

BUILDING_TYPE_VALUES = ("公寓", "办公楼", "住宅", "商业", "工业", "厂房", "车库", "仓库", "商铺", "别墅", "非居住", "居住")


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


def _clean_building_type(value: str) -> str:
    text = clean(value).strip("。；;")
    for candidate in BUILDING_TYPE_VALUES:
        if candidate in text:
            return candidate
    return text if text in BUILDING_TYPE_VALUES else ""


def _building_type(text: str) -> tuple[str, str]:
    split_lines = lines(text)
    in_house_section = False
    for index, line in enumerate(split_lines):
        if "权利类型" in line:
            continue
        if "房屋状况" in line:
            in_house_section = True
        if any(label in line for label in ("土地状况", "登记日期", "登记日", "封面编号")):
            in_house_section = False
        context_window = "".join(split_lines[max(0, index - 2) : index + 3])
        has_house_context = in_house_section or any(label in context_window for label in ("室号部位", "室号或部位", "总层数", "竣工日期", "建筑面积"))
        if not has_house_context:
            continue
        match = re.search(r"(?:房屋类型|建筑类型|类型)\s*[:：]?\s*([^；;\n]+)", line)
        if match:
            value = _clean_building_type(match.group(1))
            if value:
                return value, line
        if clean(line) == "类型":
            for candidate_line in split_lines[index + 1 : index + 3]:
                value = _clean_building_type(candidate_line)
                if value:
                    return value, line
    return "", ""


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
        ("建筑类型", ("建筑类型", "房屋类型")),
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
    building_type, raw_type_line = _building_type(text)
    if building_type:
        fields["建筑类型"] = building_type
    logger.info("[NewRealEstateSkill][BUILDING_TYPE] raw_type_line=%s", raw_type_line)
    logger.info("[NewRealEstateSkill][BUILDING_TYPE] extracted_建筑类型=%s", fields.get("建筑类型") or "")
    land_use, house_use = split_usage(text)
    if land_use:
        fields["土地用途"] = land_use
    if house_use:
        fields["房屋用途"] = house_use
    use_term = normalize_use_term(text) or label_value(text, ("使用期限",))
    if use_term:
        fields["使用期限"] = use_term
    return {"fields": fields, "warnings": [], "page_role": "detail_page"}
