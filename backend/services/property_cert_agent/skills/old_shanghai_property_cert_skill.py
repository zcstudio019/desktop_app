from __future__ import annotations

import re
import logging
from typing import Any

logger = logging.getLogger(__name__)


LAND_USE_VALUES = (
    "城镇住宅用地",
    "其它商服用地",
    "住宅用地",
    "商业用地",
    "工业用地",
    "办公用地",
    "仓储用地",
)
HOUSE_USE_VALUES = ("居住", "住宅", "办公", "商业", "工业", "仓储", "车库", "公寓")
POLLUTION_WORDS = (
    "土地状况",
    "房屋状况",
    "宗地号",
    "宗地",
    "总层数",
    "建筑面积",
    "建筑类型",
    "使用期限",
    "使用权面积",
    "独用面积",
    "分摊面积",
    "填证单位",
    "面积单位",
    "状况",
)
LABELS = (
    "权利人",
    "房地坐落",
    "权属性质",
    "使用权取得方式",
    "土地状况",
    "房屋状况",
    "用途",
    "宗地号",
    "宗地(丘)面积",
    "宗地面积",
    "使用权面积",
    "土地使用期限",
    "使用期限",
    "室号或部位",
    "建筑面积",
    "建筑类型",
    "房屋用途",
    "总层数",
    "竣工日期",
    "登记日",
    "填证单位",
)


def _clean_line(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip(" :：,，;；")


def _compact(value: str) -> str:
    return re.sub(r"\s+", "", str(value or ""))


def _lines(text: str) -> list[str]:
    return [_clean_line(line) for line in str(text or "").splitlines() if _clean_line(line)]


def _normalized_label(line: str) -> str:
    return re.sub(r"[\s:：,，;；()（）]", "", line)


def _is_label(line: str) -> bool:
    normalized = _normalized_label(line)
    return normalized in {_normalized_label(label) for label in LABELS} or normalized.endswith("状况")


def _address_debug(text: str) -> tuple[bool, list[str], list[str]]:
    split_lines = _lines(text)
    lines_with_address: list[str] = []
    next_lines: list[str] = []
    for index, line in enumerate(split_lines):
        if "房地坐落" in _normalized_label(line):
            lines_with_address.append(line)
            next_lines.extend(split_lines[index + 1 : index + 4])
    return "房地坐落" in _compact(text), lines_with_address, next_lines


def _strip_after_label(value: str) -> str:
    value = _clean_line(value)
    for label in LABELS:
        idx = value.find(label)
        if idx > 0:
            return _clean_line(value[:idx])
    return value


def _label_next_line(text: str, labels: tuple[str, ...], *, max_next_lines: int = 3) -> str:
    split_lines = _lines(text)
    normalized_labels = {_normalized_label(label) for label in labels}
    for index, line in enumerate(split_lines):
        normalized = _normalized_label(line)
        for label in labels:
            if normalized == _normalized_label(label):
                for next_line in split_lines[index + 1 : index + 1 + max_next_lines]:
                    if _is_label(next_line):
                        continue
                    return _strip_after_label(next_line)
            if normalized.startswith(_normalized_label(label)) and normalized != _normalized_label(label):
                value = line
                for item in labels:
                    value = re.sub(re.escape(item), "", value, count=1)
                value = _strip_after_label(value)
                if value and _normalized_label(value) not in normalized_labels:
                    return value
    return ""


def _old_certificate_number(text: str) -> str:
    compact = _compact(text)
    pattern = re.compile(r"沪房地([\u4e00-\u9fa5]{1,4})字[（(]?(\d{4})[）)]?第(\d{4,8})号")
    match = pattern.search(compact)
    if not match:
        spaced = re.compile(
            r"沪\s*房\s*地\s*([\u4e00-\u9fa5]{1,4})\s*字\s*[（(]?\s*(\d{4})\s*[）)]?\s*第\s*(\d{4,8})\s*号"
        )
        match = spaced.search(text or "")
    if not match:
        return ""
    district, year, number = match.groups()
    return f"沪房地{district}字({year})第{number}号"


def _owner(text: str) -> str:
    value = _label_next_line(text, ("权利人", "房屋所有权人"), max_next_lines=2)
    return value.replace("、 ", "、").strip()


def _clean_usage_value(value: str, allowed_values: tuple[str, ...]) -> str:
    compact = _compact(value)
    for allowed in allowed_values:
        if allowed in compact:
            return allowed
    for word in POLLUTION_WORDS:
        idx = compact.find(word)
        if idx > 0:
            compact = compact[:idx]
    return compact if compact in allowed_values else ""


def _extract_usages(text: str) -> tuple[str, str]:
    split_lines = _lines(text)
    land_use = ""
    house_use = ""
    current_section = ""
    for index, line in enumerate(split_lines):
        normalized = _normalized_label(line)
        if "土地状况" in normalized or normalized in {"宗地号", "宗地丘面积", "使用权面积", "土地使用期限"}:
            current_section = "land"
        elif "房屋状况" in normalized or normalized in {"室号或部位", "建筑面积", "建筑类型", "总层数", "竣工日期"}:
            current_section = "building"

        if normalized == "用途" or normalized.endswith("用途"):
            candidate = ""
            if normalized not in {"用途", "土地用途", "房屋用途"}:
                candidate = re.sub(r"^(土地|房屋)?用途", "", line).strip()
            if not candidate:
                for next_line in split_lines[index + 1 : index + 4]:
                    if _is_label(next_line):
                        continue
                    candidate = next_line
                    break
            land_candidate = _clean_usage_value(candidate, LAND_USE_VALUES)
            house_candidate = _clean_usage_value(candidate, HOUSE_USE_VALUES)
            if land_candidate and (current_section == "land" or "用地" in land_candidate):
                land_use = land_use or land_candidate
                continue
            if house_candidate and current_section == "building":
                house_use = house_use or house_candidate
                continue
            if land_candidate and not land_use:
                land_use = land_candidate
            elif house_candidate and not house_use:
                house_use = house_candidate

    compact = _compact(text)
    if not land_use:
        for value in LAND_USE_VALUES:
            if value in compact:
                land_use = value
                break
    if not house_use:
        for value in HOUSE_USE_VALUES:
            if re.search(rf"(?:用途|房屋状况).{{0,20}}{re.escape(value)}", compact):
                house_use = value
                break
    return land_use, house_use


def _area_value(text: str, labels: tuple[str, ...]) -> str:
    value = _label_next_line(text, labels, max_next_lines=4)
    match = re.search(r"(\d+(?:\.\d+)?)", value)
    if not match:
        compact = _compact(text)
        label_pattern = "|".join(re.escape(_compact(label)) for label in labels)
        match = re.search(rf"(?:{label_pattern})(\d+(?:\.\d+)?)", compact)
    return f"{match.group(1)} 平方米" if match else ""


def _land_use_term(text: str) -> str:
    compact = _compact(text)
    match = re.search(r"((?:19|20)\d{2}年\d{1,2}月\d{1,2}日至(?:19|20)\d{2}年\d{1,2}月\d{1,2})(日止|止)?", compact)
    if match:
        return f"{match.group(1)}日止"
    value = _label_next_line(text, ("土地使用期限", "使用期限"), max_next_lines=4)
    merged = _compact(value)
    match = re.search(r"((?:19|20)\d{2}年\d{1,2}月\d{1,2}日至(?:19|20)\d{2}年\d{1,2}月\d{1,2})(日止|止)?", merged)
    return f"{match.group(1)}日止" if match else value


def _completion_date(text: str) -> str:
    split_lines = _lines(text)
    for index, line in enumerate(split_lines):
        if "竣工日期" not in line:
            continue
        window = split_lines[max(0, index - 3) : index] + split_lines[index + 1 : index + 4]
        for candidate in window:
            if "登记" in candidate or "使用期限" in candidate:
                continue
            if re.search(r"(?:19|20)\d{2}年\d{1,2}月\d{1,2}日", candidate):
                continue
            match = re.search(r"((?:19|20)\d{2}年)", candidate)
            if match:
                return match.group(1)
    direct = _label_next_line(text, ("竣工日期",), max_next_lines=3)
    if not re.search(r"(?:19|20)\d{2}年\d{1,2}月\d{1,2}日", direct):
        match = re.search(r"((?:19|20)\d{2}年)", direct)
        if match:
            return match.group(1)
    return ""


def _registration_date(text: str) -> str:
    value = _label_next_line(text, ("登记日", "登记日期"), max_next_lines=2)
    match = re.search(r"((?:19|20)\d{2}年\d{1,2}月\d{1,2}日)", value)
    if match:
        return match.group(1)
    compact = _compact(text)
    match = re.search(r"登记日((?:19|20)\d{2}年\d{1,2}月\d{1,2}日)", compact)
    return match.group(1) if match else ""


def extract(payload: dict[str, Any]) -> dict[str, Any]:
    text = str(payload.get("text") or "")
    fields: dict[str, Any] = {}
    contains_address, address_lines, address_next_lines = _address_debug(text)
    logger.info("[OldPropertyCertSkill][ADDRESS_DEBUG] raw_text_contains_房地坐落=%s", contains_address)
    logger.info("[OldPropertyCertSkill][ADDRESS_DEBUG] lines_with_房地坐落=%s", address_lines)
    logger.info("[OldPropertyCertSkill][ADDRESS_DEBUG] next_lines_after_房地坐落=%s", address_next_lines)
    extracted_address = _label_next_line(text, ("房地坐落", "房屋坐落"), max_next_lines=3)
    logger.info("[OldPropertyCertSkill][ADDRESS_DEBUG] extracted_房地坐落=%s", extracted_address)

    simple_fields = (
        ("权利人", _owner(text)),
        ("权证编号", _old_certificate_number(text)),
        ("房地坐落", extracted_address),
        ("权属性质", _label_next_line(text, ("权属性质",), max_next_lines=3)),
        ("使用权取得方式", _label_next_line(text, ("使用权取得方式",), max_next_lines=3)),
        ("宗地号", _label_next_line(text, ("宗地号",), max_next_lines=3)),
        ("宗地面积", _area_value(text, ("宗地(丘)面积", "宗地面积"))),
        ("土地使用期限", _land_use_term(text)),
        ("室号或部位", _label_next_line(text, ("室号或部位", "室号部位", "室号 部位"), max_next_lines=3)),
        ("建筑面积", _area_value(text, ("建筑面积",))),
        ("建筑类型", _label_next_line(text, ("建筑类型",), max_next_lines=3)),
        ("总层数", _label_next_line(text, ("总层数",), max_next_lines=3)),
        ("竣工日期", _completion_date(text)),
        ("登记日", _registration_date(text)),
        ("填证单位", _label_next_line(text, ("填证单位",), max_next_lines=3)),
    )
    for key, value in simple_fields:
        if value:
            fields[key] = value

    land_use, house_use = _extract_usages(text)
    if land_use:
        fields["土地用途"] = land_use
    if house_use:
        fields["房屋用途"] = house_use

    logger.info("[OldPropertyCertSkill][ADDRESS_DEBUG] fields_房地坐落=%s", fields.get("房地坐落"))
    return {"fields": fields, "warnings": [], "page_role": "old_property_detail_page"}
