from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

UNIT_NUMBER_RE = re.compile(r"\d{6,}GB[A-Z0-9]+F[A-Z0-9]+", re.IGNORECASE)
AREA_RE = re.compile(r"(?<![\dA-Z])(\d{1,8}(?:\.\d{1,2})?)\s*平方米")
BARE_AREA_RE = re.compile(r"(?<![\dA-Z])(\d{2,8}\.\d{2})(?![\dA-Z])")
YEAR_RE = re.compile(r"(?:19|20)\d{2}年")
USE_TERM_RE = re.compile(r"(?:19|20)\d{2}年\d{1,2}月\d{1,2}日(?:起|至).{0,24}?(?:19|20)\d{2}年\d{1,2}月\d{1,2}日止?")

LAND_USE_VALUES = ("其它商服用地", "其他商服用地", "城镇住宅用地", "住宅用地", "商业用地", "工业用地", "办公用地", "仓储用地", "住宅", "商业", "办公", "工业", "仓储")
HOUSE_USE_VALUES = ("办公", "居住", "住宅", "商业", "工业", "仓储", "车库", "公寓", "商铺", "非居住")
BUILDING_TYPE_VALUES = ("办公楼", "商场", "公寓", "厂房", "车库", "仓库", "商铺", "别墅", "非居住")
RIGHT_NATURE_VALUES = ("出让", "划拨", "租赁", "作价出资", "授权经营")
INVALID_UNIT_NUMBER_VALUES = (
    "使用权",
    "使用权面积",
    "土地使用权",
    "权利性质",
    "土地用途",
    "房屋状况",
    "土地状况",
    "室号或部位",
    "建筑面积",
    "类型",
    "用途",
    "总层数",
    "竣工日期",
    "合计",
    "出让",
    "商业",
    "商场",
    "办公",
    "详见附记",
)
POLLUTION_LABELS = (
    "使用权面积",
    "独用面积",
    "分摊面积",
    "房屋状况",
    "土地状况",
    "权利其他状况",
    "室号部位",
    "类型",
    "用途",
    "总层数",
    "竣工日期",
    "合计",
)

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
    text = str(value or "").replace("\\n", " ")
    text = text.replace("详见附记", "").replace("合计", "")
    text = re.sub(r"\s+", " ", text).strip(" :：,，;；。")
    return text


def is_valid_unit_number(value: Any) -> bool:
    text = re.sub(r"\s+", "", str(value or "")).upper()
    if not text or text in INVALID_UNIT_NUMBER_VALUES:
        return False
    return len(text) >= 20 and "GB" in text and "F" in text and bool(UNIT_NUMBER_RE.fullmatch(text))


def is_valid_unit_number_value(value: Any) -> bool:
    parts = [part for part in re.split(r"[、,，;；\s]+", str(value or "")) if part]
    return bool(parts) and all(is_valid_unit_number(part) for part in parts)


def _valid_unit_numbers(text: str) -> list[str]:
    values: list[str] = []
    for match in UNIT_NUMBER_RE.finditer(str(text or "")):
        value = match.group(0).upper()
        if is_valid_unit_number(value) and value not in values:
            values.append(value)
    return values


def _log_invalid_unit_tokens(text: str) -> None:
    for token in INVALID_UNIT_NUMBER_VALUES:
        if token in str(text or "") and not is_valid_unit_number(token):
            logger.info("[AttachmentSkill] invalid_unit_number_removed=%s", token)


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


def _known_values(text: str, candidates: tuple[str, ...]) -> list[str]:
    compact = re.sub(r"\s+", "", text)
    return [candidate for candidate in candidates if candidate in compact]


def _clean_field_value(value: str, *, keep_labels: tuple[str, ...] = ()) -> str:
    text = _clean(value)
    for label in POLLUTION_LABELS:
        if label in keep_labels:
            continue
        idx = text.find(label)
        if idx > 0:
            text = text[:idx]
        elif idx == 0:
            text = text.replace(label, "")
    return _clean(text)


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
    text = "\n".join(lines)
    matches = list(UNIT_NUMBER_RE.finditer(text))
    for index, unit_match in enumerate(matches):
        unit_number = unit_match.group(0).upper()
        if not is_valid_unit_number(unit_number):
            logger.info("[AttachmentSkill] invalid_unit_number_removed=%s", unit_number)
            continue
        next_start = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        row_text = text[unit_match.end() : next_start]
        row = {"不动产单元号": unit_number}
        house_use = _known_value(row_text, HOUSE_USE_VALUES) or _nearby_value(lines, index, HOUSE_USE_VALUES)
        building_type = _known_value(row_text, BUILDING_TYPE_VALUES) or _nearby_value(lines, index, BUILDING_TYPE_VALUES)
        right_nature = _known_value(row_text, RIGHT_NATURE_VALUES) or _nearby_value(lines, index, RIGHT_NATURE_VALUES)
        use_term = USE_TERM_RE.search(row_text)
        completion = YEAR_RE.search(row_text)
        areas = [match.group(1) for match in AREA_RE.finditer(row_text)] or [match.group(1) for match in BARE_AREA_RE.finditer(row_text)]
        floors = re.search(r"(?:总层数[:：]?)?(\d{1,2})(?:层|$)", row_text)
        room = re.search(r"(\d+号\d+(?:-\d+)?层|\d+号\d+层|[0-9]+幢\d+(?:-\d+)?层|[0-9]+幢[0-9A-Za-z\-层东间]+)", row_text)
        if house_use:
            row["房屋用途"] = house_use
        if building_type:
            row["建筑类型"] = building_type
        if right_nature:
            row["权利性质"] = right_nature
        if use_term:
            row["使用期限"] = use_term.group(0)
        if areas:
            row["建筑面积"] = f"{float(areas[0]):.2f}平方米"
        if floors:
            row["总层数"] = floors.group(1)
        if completion:
            row["竣工日期"] = completion.group(0)
        if room:
            row["室号或部位"] = _clean_field_value(room.group(1))
        rows.append(row)
    return rows


def _extract_column_rows(lines: list[str]) -> list[dict[str, str]]:
    text = " ".join(lines)
    rooms = _dedupe(
        [
            _clean_field_value(match.group(0))
            for match in re.finditer(r"\d+号\d+(?:-\d+)?层|[0-9]+幢\d+(?:-\d+)?层|[0-9]+幢[0-9A-Za-z\-层东间]+", text)
        ]
    )
    areas = _dedupe([f"{float(match.group(1)):.2f}平方米" for match in AREA_RE.finditer(text)] or [f"{float(match.group(1)):.2f}平方米" for match in BARE_AREA_RE.finditer(text)])
    house_usages = _dedupe(_known_values(text, HOUSE_USE_VALUES))
    building_types = _dedupe(_known_values(text, BUILDING_TYPE_VALUES))
    floors = _dedupe([match.group(1) for match in re.finditer(r"总层数[:：]?\s*(\d{1,2})", text)])
    years = _dedupe([match.group(0) for match in YEAR_RE.finditer(text)])
    units = _valid_unit_numbers(text)
    row_count = max(len(units), len(rooms), len(areas), 0)
    rows: list[dict[str, str]] = []
    for index in range(row_count):
        row: dict[str, str] = {}
        if index < len(units):
            row["不动产单元号"] = units[index]
        if index < len(rooms):
            row["室号或部位"] = rooms[index]
        if index < len(areas):
            row["建筑面积"] = areas[index]
        if house_usages:
            row["房屋用途"] = house_usages[min(index, len(house_usages) - 1)]
        if building_types:
            row["建筑类型"] = building_types[min(index, len(building_types) - 1)]
        if floors:
            row["总层数"] = floors[min(index, len(floors) - 1)]
        if years:
            row["竣工日期"] = years[min(index, len(years) - 1)]
        if row:
            rows.append(row)
    return rows


def _merge_rows(primary: list[dict[str, str]], fallback: list[dict[str, str]]) -> list[dict[str, str]]:
    if not primary:
        return fallback
    if len(fallback) > len(primary):
        for index, row in enumerate(primary):
            for key, value in fallback[index].items():
                row.setdefault(key, value)
    return primary


def extract(payload: dict[str, Any]) -> dict[str, Any]:
    text = str(payload.get("text") or "").strip()
    lines = _lines(text)
    _log_invalid_unit_tokens(text)
    rows = _merge_rows(_extract_attachment_rows(lines), _extract_column_rows(lines))
    fields: dict[str, Any] = {"附记": text[:1000]} if text else {}

    unit_numbers = _dedupe([row["不动产单元号"] for row in rows if is_valid_unit_number(row.get("不动产单元号"))])
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

    logger.info("[AttachmentSkill] selected_ocr_variant=%s", payload.get("selected_ocr_variant") or payload.get("metadata", {}).get("selected_ocr_variant") or "")
    logger.info("[AttachmentSkill] valid_unit_numbers=%s", unit_numbers)
    logger.info("[AttachmentSkill] house_usages=%s", house_usages)
    logger.info("[AttachmentSkill] building_types=%s", building_types)
    logger.info("[AttachmentSkill] rows_count=%s", len(rows))
    warnings = []
    if "不动产单元号" in text and not unit_numbers:
        warnings.append("不动产单元号在附记页中，系统未能识别到合法编号，请人工确认。")
    return {"fields": fields, "warnings": warnings, "page_role": "attachment_page", "supplemental": True}
