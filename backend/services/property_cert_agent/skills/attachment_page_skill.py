from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

UNIT_NUMBER_RE = re.compile(r"\d{12}GB\d{5}F\d{8,}", re.IGNORECASE)
AREA_RE = re.compile(r"(?<![\dA-Z])(\d{1,8}(?:\.\d{1,2})?)\s*平方米")
YEAR_RE = re.compile(r"(?:19|20)\d{2}年")
USE_TERM_RE = re.compile(r"(?:19|20)\d{2}年\d{1,2}月\d{1,2}日(?:起|至).{0,24}?(?:19|20)\d{2}年\d{1,2}月\d{1,2}日止?")

LAND_USE_VALUES = ("其它商服用地", "其他商服用地", "城镇住宅用地", "住宅用地", "商业用地", "工业用地", "办公用地", "仓储用地", "住宅", "商业", "办公", "工业", "仓储")
HOUSE_USE_VALUES = ("办公", "居住", "住宅", "商业", "工业", "仓储", "车库", "公寓", "商铺", "非居住")
BUILDING_TYPE_VALUES = ("办公楼", "商场", "公寓", "住宅", "商业", "工业", "厂房", "车库", "仓库", "商铺", "别墅", "非居住", "居住")
RIGHT_NATURE_VALUES = ("出让", "划拨", "租赁", "作价出资", "授权经营")

LABELS = (
    "附记",
    "不动产单元号",
    "土地状况",
    "房屋状况",
    "室号或部位",
    "室号部位",
    "建筑面积",
    "房屋用途",
    "土地用途",
    "总层数",
    "竣工日期",
    "类型",
    "合计",
    "权利性质",
    "使用期限",
)


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip(" :：,，;；。")


def _lines(text: str) -> list[str]:
    return [_clean(line) for line in str(text or "").replace("\\n", "\n").splitlines() if _clean(line)]


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        value = _clean(value)
        if value and value not in result:
            result.append(value)
    return result


def _known_value(text: str, candidates: tuple[str, ...]) -> str:
    compact = re.sub(r"\s+", "", text)
    for candidate in candidates:
        if candidate in compact:
            return candidate
    return ""


def _extract_label_values(lines: list[str], labels: tuple[str, ...], candidates: tuple[str, ...] | None = None) -> list[str]:
    values: list[str] = []
    for index, line in enumerate(lines):
        compact = re.sub(r"\s+", "", line)
        for label in labels:
            if label not in compact:
                continue
            after = _clean(re.split(rf"{re.escape(label)}[:：]?", line, maxsplit=1)[-1])
            probes = [after, *lines[index + 1 : index + 4]]
            for probe in probes:
                if not probe or probe == line:
                    continue
                if any(stop == re.sub(r"\s+", "", probe) for stop in LABELS):
                    break
                value = _known_value(probe, candidates) if candidates else _clean(probe)
                if value:
                    values.append(value)
                    break
    return _dedupe(values)


def _nearby_value(lines: list[str], unit_line_index: int, candidates: tuple[str, ...]) -> str:
    window = " ".join(lines[max(0, unit_line_index - 1) : unit_line_index + 3])
    return _known_value(window, candidates)


def _extract_attachment_rows(lines: list[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for index, line in enumerate(lines):
        unit_match = UNIT_NUMBER_RE.search(line)
        if not unit_match:
            continue
        row_parts = [line]
        for next_line in lines[index + 1 : index + 3]:
            if UNIT_NUMBER_RE.search(next_line):
                break
            row_parts.append(next_line)
        row_text = " ".join(row_parts)
        row = {"不动产单元号": unit_match.group(0).upper()}
        house_use = _known_value(row_text, HOUSE_USE_VALUES) or _nearby_value(lines, index, HOUSE_USE_VALUES)
        building_type = _known_value(row_text, BUILDING_TYPE_VALUES) or _nearby_value(lines, index, BUILDING_TYPE_VALUES)
        right_nature = _known_value(row_text, RIGHT_NATURE_VALUES) or _nearby_value(lines, index, RIGHT_NATURE_VALUES)
        use_term = USE_TERM_RE.search(row_text)
        completion = YEAR_RE.search(row_text)
        areas = [match.group(1) for match in AREA_RE.finditer(row_text)]
        floors = re.search(r"(?:总层数[:：]?)?(\d{1,2})(?:层|$)", row_text)
        room = re.search(r"(?:室号或部位|室号部位|部位|室号)[:：]?\s*([A-Za-z0-9\-号幢室层单元]+)", row_text)
        if house_use:
            row["房屋用途"] = house_use
        if building_type:
            row["建筑类型"] = building_type
        if right_nature:
            row["权利性质"] = right_nature
        if use_term:
            row["使用期限"] = use_term.group(0)
        if areas:
            row["建筑面积"] = f"{areas[-1]} 平方米"
        if floors:
            row["总层数"] = floors.group(1)
        if completion:
            row["竣工日期"] = completion.group(0)
        if room:
            row["室号或部位"] = _clean(room.group(1))
        rows.append(row)
    return rows


def extract(payload: dict[str, Any]) -> dict[str, Any]:
    text = str(payload.get("text") or "").strip()
    lines = _lines(text)
    rows = _extract_attachment_rows(lines)
    fields: dict[str, Any] = {"附记": text[:1000]} if text else {}

    unit_numbers = _dedupe([row["不动产单元号"] for row in rows if row.get("不动产单元号")])
    house_usages = _dedupe([row["房屋用途"] for row in rows if row.get("房屋用途")] or _extract_label_values(lines, ("房屋用途",), HOUSE_USE_VALUES))
    building_types = _dedupe([row["建筑类型"] for row in rows if row.get("建筑类型")] or _extract_label_values(lines, ("建筑类型", "类型"), BUILDING_TYPE_VALUES))
    room_numbers = _dedupe([row["室号或部位"] for row in rows if row.get("室号或部位")])
    building_areas = _dedupe([row["建筑面积"] for row in rows if row.get("建筑面积")])
    total_floors = _dedupe([row["总层数"] for row in rows if row.get("总层数")])
    completion_dates = _dedupe([row["竣工日期"] for row in rows if row.get("竣工日期")])
    land_usages = _dedupe([row["土地用途"] for row in rows if row.get("土地用途")] or _extract_label_values(lines, ("土地用途",), LAND_USE_VALUES))
    right_natures = _dedupe([row["权利性质"] for row in rows if row.get("权利性质")] or _extract_label_values(lines, ("权利性质",), RIGHT_NATURE_VALUES))
    use_terms = _dedupe([row["使用期限"] for row in rows if row.get("使用期限")])

    for key, value in (
        ("不动产单元号列表", unit_numbers),
        ("房屋用途列表", house_usages),
        ("建筑类型列表", building_types),
        ("室号或部位列表", room_numbers),
        ("建筑面积列表", building_areas),
        ("总层数列表", total_floors),
        ("竣工日期列表", completion_dates),
        ("土地用途列表", land_usages),
        ("权利性质列表", right_natures),
        ("使用期限列表", use_terms),
    ):
        if value:
            fields[key] = value
    if rows:
        fields["附记明细"] = rows

    logger.info("[AttachmentSkill] rows_count=%s", len(rows))
    logger.info("[AttachmentSkill] unit_numbers=%s", unit_numbers)
    logger.info("[AttachmentSkill] house_usages=%s", house_usages)
    return {"fields": fields, "warnings": [], "page_role": "attachment_page", "supplemental": True}
