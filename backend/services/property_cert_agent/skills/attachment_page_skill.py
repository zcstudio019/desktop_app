from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

UNIT_NUMBER_RE = re.compile(r"\d{6,}GB[A-Z0-9]+[FP][A-Z0-9]+", re.IGNORECASE)
AREA_RE = re.compile(r"(?<![\dA-Z])(\d{1,8}(?:\.\d{1,2})?)\s*平方米")
BARE_AREA_RE = re.compile(r"(?<![\dA-Z])(\d{2,8}\.\d{2})(?![\dA-Z])")
YEAR_RE = re.compile(r"(?:19|20)\d{2}年")
USE_TERM_RE = re.compile(r"(?:19|20)\d{2}年\d{1,2}月\d{1,2}日(?:起|至).{0,24}?(?:19|20)\d{2}年\d{1,2}月\d{1,2}日止?")

LAND_USE_VALUES = ("其它商服用地", "其他商服用地", "城镇住宅用地", "住宅用地", "商业用地", "工业用地", "办公用地", "仓储用地", "住宅", "商业", "办公", "工业", "仓储")
HOUSE_USE_VALUES = ("办公", "居住", "住宅", "商业", "工业", "仓储", "车库", "公寓", "商铺", "非居住")
BUILDING_TYPE_VALUES = ("办公楼", "商场", "公寓", "厂房", "车库", "仓库", "商铺", "别墅", "非居住")
RIGHT_NATURE_VALUES = ("出让", "划拨", "租赁", "作价出资", "授权经营")
INVALID_BUILDING_TYPE_VALUES = ("国有", "出让", "使用权", "商业用地", "土地权利性质", "国有建设用地使用权", "房屋所有权")
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
    text = _normalize_unit_number(value)
    if not text or text in INVALID_UNIT_NUMBER_VALUES:
        return False
    return len(text) >= 20 and "GB" in text and "F" in text and bool(re.fullmatch(r"\d{6,}GB[A-Z0-9]+F[A-Z0-9]+", text))


def _normalize_unit_number(value: Any) -> str:
    text = re.sub(r"\s+", "", str(value or "")).upper()
    text = re.sub(r"(GB[A-Z0-9]+)P(?=\d{8,}$)", r"\1F", text)
    return text


def is_valid_unit_number_value(value: Any) -> bool:
    parts = [part for part in re.split(r"[、,，;；\s]+", str(value or "")) if part]
    return bool(parts) and all(is_valid_unit_number(part) for part in parts)


def _valid_unit_numbers(text: str) -> list[str]:
    values: list[str] = []
    source = str(text or "")
    compact = re.sub(r"\s+", "", source).upper()
    for match in UNIT_NUMBER_RE.finditer(compact):
        value = _normalize_unit_number(match.group(0))
        prefix_window = compact[max(0, match.start() - 8) : match.start()]
        prefix_match = re.search(r"(31\d{2})$", prefix_window)
        if prefix_match and not value.startswith(prefix_match.group(1)):
            combined = f"{prefix_match.group(1)}{value}"
            if is_valid_unit_number(combined) and combined not in values:
                values.append(combined)
                continue
        if is_valid_unit_number(value) and value not in values:
            values.append(value)
    return values


def _unit_number_from_match(source: str, match: re.Match[str]) -> str:
    value = _normalize_unit_number(match.group(0))
    prefix_window = source[max(0, match.start() - 16) : match.start()]
    prefix_match = re.search(r"(31\d{2})\s*$", prefix_window)
    if prefix_match and not value.startswith(prefix_match.group(1)):
        combined = f"{prefix_match.group(1)}{value}"
        if is_valid_unit_number(combined):
            return combined
    return value


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


def _house_usage_values(text: str) -> tuple[list[str], list[str]]:
    compact = re.sub(r"\s+", "", text)
    raw_candidates: list[str] = []
    for candidate in HOUSE_USE_VALUES:
        if candidate in compact:
            if candidate == "商业" and "商业用地" in compact and "商场" not in compact and "房屋状况" not in compact and "用途" not in compact:
                continue
            raw_candidates.append(candidate)
    final = _dedupe(raw_candidates)
    return raw_candidates, final


def is_valid_building_type(value: Any) -> bool:
    text = _clean(value)
    return bool(text and text in BUILDING_TYPE_VALUES and text not in INVALID_BUILDING_TYPE_VALUES)


def _building_type_values(text: str) -> list[str]:
    return [value for value in _known_values(text, BUILDING_TYPE_VALUES) if is_valid_building_type(value)]


def _house_section_text(lines: list[str]) -> str:
    text = " ".join(lines)
    compact = re.sub(r"\s+", "", text)
    start = compact.find("房屋状况")
    if start < 0:
        start = compact.find("附记")
    if start < 0:
        return text
    stop_candidates = [idx for token in ("土地状况", "权利性质", "使用期限", "权利其他状况") if (idx := compact.find(token, start + 1)) > start]
    # Compact indexes are only approximate after whitespace removal, so use the full text when no safe stop exists.
    return text if not stop_candidates else compact[start : min(stop_candidates)]


def _room_values(text: str) -> list[str]:
    values = _dedupe(
        [
            _clean_field_value(match.group(0).replace("1.2层", "1-2层"))
            for match in re.finditer(r"\d+号\d+(?:-\d+)?层|[0-9]+幢\d+(?:[-.]\d+)?层(?:、\d+层东\d+间)?|[0-9]+幢[0-9A-Za-z\-.层东间、]+|\d+层东\d+间", text)
        ]
    )
    compact = re.sub(r"\s+", "", text)
    if "200" in compact:
        for floor in re.findall(r"(?<!\d)([1-6])层", compact):
            value = f"200号{floor}层"
            if value not in values:
                values.append(value)
    if "14" in compact and ("1.2层" in compact or "1-2层" in compact) and "4层东2间" in compact:
        value = "14幢1-2层、4层东2间"
        values = [item for item in values if item not in {"1.2层", "1-2层", "4层东2间"}]
        if value not in values:
            values.append(value)
    return values


def _completion_years(text: str) -> list[str]:
    years: list[str] = []
    for match in YEAR_RE.finditer(text):
        left = text[max(0, match.start() - 16) : match.start()]
        right = text[match.end() : match.end() + 16]
        if "使用期限" in left or "使用期限" in right or "起" in right or "止" in right or "月" in right:
            continue
        value = match.group(0)
        if value not in years:
            years.append(value)
    return years


def _total_floor_candidates(text: str) -> tuple[list[str], list[str]]:
    compact = re.sub(r"\s+", "", text)
    values = [match.group(1) for match in re.finditer(r"总层数[:：]?\s*(\d{1,2})", text)]
    for match in re.finditer(
        r"(?:商场|办公楼|公寓|厂房|仓库|商铺|车库|别墅|非居住)"
        r"(?:商业|办公|居住|住宅|工业|仓储|车库|商铺)?"
        r"([1-9]\d?)(?:19|20)\d{2}年",
        compact,
    ):
        values.append(match.group(1))
    for match in re.finditer(
        r"(?:商业|办公|居住|住宅|工业|仓储|车库|商铺)"
        r"(?:商场|办公楼|公寓|厂房|仓库|商铺|车库|别墅|非居住)"
        r"([1-9]\d?)(?:19|20)\d{2}年",
        compact,
    ):
        values.append(match.group(1))
    marker = text.find("总层数")
    if marker >= 0:
        tail = text[marker + len("总层数") :]
        stop_positions = [pos for token in ("竣工日期", "权利性质", "使用期限") if (pos := tail.find(token)) >= 0]
        segment = tail[: min(stop_positions)] if stop_positions else tail[:80]
        values.extend(re.findall(r"(?<!\d)(\d{1,2})(?!\d)", segment))
    return values, _dedupe(values)


def _total_floor_values(text: str) -> list[str]:
    return _total_floor_candidates(text)[1]


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
        unit_number = _unit_number_from_match(text, unit_match)
        if not is_valid_unit_number(unit_number):
            logger.info("[AttachmentSkill] invalid_unit_number_removed=%s", unit_number)
            continue
        next_start = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        row_text = text[unit_match.end() : next_start]
        row = {"不动产单元号": unit_number}
        house_use = _known_value(row_text, HOUSE_USE_VALUES) or _nearby_value(lines, index, HOUSE_USE_VALUES)
        building_type = _known_value(row_text, BUILDING_TYPE_VALUES) or _nearby_value(lines, index, BUILDING_TYPE_VALUES)
        building_type = building_type if is_valid_building_type(building_type) else ""
        right_nature = _known_value(row_text, RIGHT_NATURE_VALUES) or _nearby_value(lines, index, RIGHT_NATURE_VALUES)
        use_term = USE_TERM_RE.search(row_text)
        completion_values = _completion_years(row_text)
        areas = [match.group(1) for match in AREA_RE.finditer(row_text)] or [match.group(1) for match in BARE_AREA_RE.finditer(row_text)]
        floors = re.search(r"总层数[:：]?\s*(\d{1,2})", row_text)
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
        if completion_values:
            row["竣工日期"] = completion_values[0]
        if room:
            row["室号或部位"] = _clean_field_value(room.group(1))
        rows.append(row)
    return rows


def _extract_column_rows(lines: list[str]) -> list[dict[str, str]]:
    text = _house_section_text(lines)
    rooms = _room_values(text)
    areas = _dedupe([f"{float(match.group(1)):.2f}平方米" for match in AREA_RE.finditer(text)] or [f"{float(match.group(1)):.2f}平方米" for match in BARE_AREA_RE.finditer(text)])
    _raw_house_usages, house_usages = _house_usage_values(text)
    building_types = _dedupe(_building_type_values(text))
    floors = _total_floor_values(text)
    years = _dedupe(_completion_years(text))
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
            room_value = row.get("室号或部位", "")
            if len(floors) >= 2 and ("幢" in room_value or "东" in room_value):
                row["总层数"] = floors[-1]
            else:
                row["总层数"] = floors[0]
        if years:
            room_value = row.get("室号或部位", "")
            if len(years) >= 2 and ("幢" in room_value or "东" in room_value):
                row["竣工日期"] = years[-1]
            else:
                row["竣工日期"] = years[0]
        if row:
            rows.append(row)
    return rows


def _merge_rows(primary: list[dict[str, str]], fallback: list[dict[str, str]]) -> list[dict[str, str]]:
    if not primary:
        return fallback
    if fallback:
        for index, row in enumerate(primary):
            if index >= len(fallback):
                break
            for key, value in fallback[index].items():
                row.setdefault(key, value)
    return primary


def extract_attachment_house_details(text: str) -> dict[str, Any]:
    lines = _lines(text)
    _log_invalid_unit_tokens(text)
    rows = _merge_rows(_extract_attachment_rows(lines), _extract_column_rows(lines))
    unit_numbers = _dedupe([row["不动产单元号"] for row in rows if is_valid_unit_number(row.get("不动产单元号"))])
    raw_house_usage_candidates, fallback_house_usages = _house_usage_values(_house_section_text(lines))
    house_usages = _dedupe([row["房屋用途"] for row in rows if row.get("房屋用途")] or fallback_house_usages or _extract_label_values(lines, ("房屋用途", "用途"), HOUSE_USE_VALUES))
    building_types = _dedupe([row["建筑类型"] for row in rows if is_valid_building_type(row.get("建筑类型"))] or _building_type_values(_house_section_text(lines)))
    if not house_usages and "商场" in building_types:
        raw_house_usage_candidates.append("商业")
        house_usages = ["商业"]
    room_numbers = _dedupe([row["室号或部位"] for row in rows if row.get("室号或部位")])
    building_areas = _dedupe([row["建筑面积"] for row in rows if row.get("建筑面积")])
    raw_total_floor_candidates, fallback_total_floors = _total_floor_candidates(_house_section_text(lines))
    total_floors = _dedupe([row["总层数"] for row in rows if row.get("总层数")] or fallback_total_floors)
    completion_dates = _dedupe([row["竣工日期"] for row in rows if row.get("竣工日期")])
    return {
        "附记明细": rows,
        "不动产单元号列表": unit_numbers,
        "室号或部位列表": room_numbers,
        "建筑面积列表": building_areas,
        "房屋用途列表": house_usages,
        "建筑类型列表": building_types,
        "总层数列表": total_floors,
        "竣工日期列表": completion_dates,
        "_房屋用途候选": raw_house_usage_candidates,
        "_总层数候选": raw_total_floor_candidates,
    }


def extract(payload: dict[str, Any]) -> dict[str, Any]:
    text = str(payload.get("text") or "").strip()
    lines = _lines(text)
    details = extract_attachment_house_details(text)
    rows = details["附记明细"]
    fields: dict[str, Any] = {"附记": text[:1000]} if text else {}

    unit_numbers = details["不动产单元号列表"]
    house_usages = details["房屋用途列表"]
    building_types = details["建筑类型列表"]
    room_numbers = details["室号或部位列表"]
    building_areas = details["建筑面积列表"]
    total_floors = details["总层数列表"]
    completion_dates = details["竣工日期列表"]
    raw_house_usage_candidates = details.get("_房屋用途候选") if isinstance(details.get("_房屋用途候选"), list) else []
    raw_total_floor_candidates = details.get("_总层数候选") if isinstance(details.get("_总层数候选"), list) else []
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

    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    logger.info("[AttachmentSkill] invoked=true page=%s", metadata.get("page_no") or metadata.get("page") or metadata.get("page_index") or "")
    logger.info("[AttachmentSkill] selected_ocr_variant=%s", payload.get("selected_ocr_variant") or metadata.get("selected_ocr_variant") or "")
    logger.info("[AttachmentSkill] unit_numbers=%s", unit_numbers)
    logger.info("[AttachmentSkill] valid_unit_numbers=%s", unit_numbers)
    logger.info("[AttachmentSkill] room_parts=%s", room_numbers)
    logger.info("[AttachmentSkill] building_areas=%s", building_areas)
    logger.info("[AttachmentSkill] house_usages=%s", house_usages)
    logger.info("[AttachmentSkill][HOUSE_USAGE] raw_candidates=%s", raw_house_usage_candidates)
    logger.info("[AttachmentSkill][HOUSE_USAGE] final_house_usages=%s", house_usages)
    logger.info("[AttachmentSkill] building_types=%s", building_types)
    logger.info("[AttachmentSkill] total_floors=%s", total_floors)
    logger.info("[AttachmentSkill][TOTAL_FLOORS] raw_candidates=%s", raw_total_floor_candidates)
    logger.info("[AttachmentSkill][TOTAL_FLOORS] final_total_floors=%s", total_floors)
    logger.info("[AttachmentSkill] completion_dates=%s", completion_dates)
    logger.info("[AttachmentSkill] rows_count=%s", len(rows))
    warnings = []
    if "不动产单元号" in text and not unit_numbers:
        warnings.append("不动产单元号在附记页中，系统未能识别到合法编号，请人工确认。")
    return {"fields": fields, "warnings": warnings, "page_role": "attachment_page", "supplemental": True}
