from __future__ import annotations

import re
import logging
from typing import Any

from .common import certificate_number, clean, label_value, lines, normalize_use_term, split_usage

logger = logging.getLogger(__name__)

BUILDING_TYPE_VALUES = ("公寓", "办公楼", "住宅", "商业", "工业", "厂房", "车库", "仓库", "商铺", "别墅", "非居住", "居住")
CO_OWNER_VALUES = ("单独所有", "共同共有", "按份共有", "共有", "单独所有/共同共有")
FIELD_LABELS = (
    "权利人",
    "共有情况",
    "坐落",
    "不动产单元号",
    "权利类型",
    "权利性质",
    "用途",
    "面积",
    "使用期限",
    "土地状况",
    "房屋状况",
    "地号",
    "使用权面积",
    "独用面积",
    "分摊面积",
    "室号部位",
    "室号或部位",
    "权利其他状况",
    "类型",
    "总层数",
    "竣工日期",
    "登记机构",
    "编号",
    "附记",
)
INVALID_OWNER_KEYWORDS = (
    "合法权益",
    "申请登记",
    "经审查核实",
    "准予登记",
    "颁发此证",
    "不动产权利人申请",
    "根据《中华人民共和国物权法》",
    "登记机构",
    "国土资源部监制",
    "权利人合法权益",
    "对不动产权利人",
)


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


def _normalized_label(text: str) -> str:
    return re.sub(r"[\s:：,，;；。]+", "", str(text or ""))


def _is_field_label(text: str) -> bool:
    compact = _normalized_label(text)
    return any(compact == _normalized_label(label) for label in FIELD_LABELS)


def _invalid_owner(value: str) -> bool:
    text = clean(value)
    return not text or any(keyword in text for keyword in INVALID_OWNER_KEYWORDS)


def _multiline_label_value(text: str, label: str, stop_labels: tuple[str, ...]) -> tuple[str, list[str]]:
    split_lines = lines(text)
    for index, line in enumerate(split_lines):
        normalized = _normalized_label(line)
        normalized_label = _normalized_label(label)
        if normalized != normalized_label and not normalized.startswith(normalized_label):
            continue
        after = clean(line.split(label, 1)[-1]) if label in line else ""
        raw_values: list[str] = [after] if after else []
        for next_line in split_lines[index + 1 :]:
            candidate = clean(next_line)
            if not candidate:
                continue
            candidate_label = _normalized_label(candidate)
            if any(candidate_label == _normalized_label(stop) or candidate_label.startswith(_normalized_label(stop)) for stop in stop_labels):
                break
            raw_values.append(candidate)
        value = clean("".join(raw_values))
        return value, raw_values
    return "", []


def _owner_and_co_owner(text: str) -> tuple[str, str, list[str]]:
    split_lines = lines(text)
    candidates: list[str] = []
    owner = ""
    co_owner = ""
    for index, line in enumerate(split_lines):
        if _normalized_label(line) != "权利人":
            continue
        for candidate_line in split_lines[index + 1 : index + 4]:
            candidate = clean(candidate_line)
            if not candidate:
                continue
            if _normalized_label(candidate) == "共有情况":
                break
            if _is_field_label(candidate):
                break
            candidates.append(candidate)
            if _invalid_owner(candidate):
                continue
            owner = candidate
            break
        if owner:
            for offset, candidate_line in enumerate(split_lines[index + 1 : index + 8], start=index + 1):
                if _normalized_label(candidate_line) != "共有情况":
                    continue
                after = clean(candidate_line.replace("共有情况", ""))
                if after:
                    co_owner = after
                    break
                for next_line in split_lines[offset + 1 : offset + 3]:
                    value = clean(next_line)
                    if not value or _is_field_label(value):
                        break
                    co_owner = value
                    break
                break
            break
    if co_owner:
        for candidate in CO_OWNER_VALUES:
            if candidate in co_owner:
                co_owner = candidate
                break
    return owner, co_owner, candidates


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
    owner, co_owner, owner_candidates = _owner_and_co_owner(text)
    logger.info("[NewRealEstateSkill][OWNER] candidate_lines=%s", owner_candidates)
    if owner:
        fields["权利人"] = owner
    if co_owner:
        fields["共有情况"] = co_owner
    logger.info("[NewRealEstateSkill][OWNER] extracted_权利人=%s", fields.get("权利人") or "")
    logger.info("[NewRealEstateSkill][CO_OWNER] extracted_共有情况=%s", fields.get("共有情况") or "")
    for output_key, labels in mappings:
        if output_key == "坐落":
            value, raw_address_lines = _multiline_label_value(
                text,
                "坐落",
                (
                    "不动产单元号",
                    "权利类型",
                    "权利性质",
                    "用途",
                    "面积",
                    "使用期限",
                    "土地状况",
                    "房屋状况",
                    "权利其他状况",
                ),
            )
            logger.info("[NewRealEstateSkill][ADDRESS] raw_lines_after_label=%s", raw_address_lines)
            logger.info("[NewRealEstateSkill][ADDRESS] extracted_坐落=%s", value)
        else:
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
