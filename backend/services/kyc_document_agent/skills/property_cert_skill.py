from __future__ import annotations

import re
from typing import Any

from backend.services.kyc_document_agent.evidence import raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


CHINESE_FIELDS = [
    "权利人",
    "权证编号",
    "房地坐落",
    "权属性质",
    "使用权取得方式",
    "用途",
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

PROPERTY_FIELD_ORDER = CHINESE_FIELDS[:]

ENGLISH_ALIASES = {
    "权利人": "owner",
    "权证编号": "certificate_number",
    "房地坐落": "property_address",
    "权属性质": "right_nature",
    "用途": "land_use",
    "建筑面积": "building_area",
    "宗地面积": "land_area",
    "登记日": "issue_date",
}

INVALID_VALUE_EXACT = {"", "对", "无", "未识别"}
INVALID_VALUE_KEYWORDS = ("合法权益", "房地产权利人", "本证是证明", "根据", "法律")
OWNER_INVALID_KEYWORDS = (
    "房地产权证",
    "合法权益",
    "房地产权利人",
    "权利人的合法权益",
    "本证是证明",
    "本证",
    "证明",
    "根据",
    "法律",
    "状况",
    "土地",
    "房屋",
    "坐落",
    "用途",
    "面积",
)
OWNER_LABEL_PATTERN = r"权\s*利\s*人|权利\s*人|房屋所有权人|所有权人"
OWNER_STOP_PATTERN = r"共\s*有\s*人|共有情况|权证编号|房地坐落|房屋坐落|坐落|权属性质|土地状况|房屋状况|用途|面积"


def _clean_text(text: str) -> str:
    return (
        (text or "")
        .replace("\u3000", " ")
        .replace("：", ":")
    )


def _compact_value(value: str) -> str:
    value = re.sub(r"\s+", " ", value or "").strip(" :：|,，;；")
    value = re.sub(r"^(?:为|是|:)\s*", "", value).strip()
    return value


def _normalize_cert_number(value: str) -> str:
    return _compact_value(value).replace("（", "(").replace("）", ")")


def _is_valid_property_value(value: Any) -> bool:
    if value in (None, [], {}):
        return False
    if isinstance(value, list):
        return any(_is_valid_property_value(item) for item in value)
    text = _compact_value(str(value))
    if text in INVALID_VALUE_EXACT:
        return False
    return not any(keyword in text for keyword in INVALID_VALUE_KEYWORDS)


def _clean_owner_candidate(value: str) -> str:
    value = re.sub(OWNER_LABEL_PATTERN, "", value or "", count=1)
    value = re.split(OWNER_STOP_PATTERN, value, maxsplit=1)[0]
    value = _compact_value(value)
    value = re.sub(r"\s+", "", value)
    return value.strip(" :：|,，;；。")


def _is_valid_owner_value(value: str) -> bool:
    text = _clean_owner_candidate(value)
    if not text or text in INVALID_VALUE_EXACT:
        return False
    if any(keyword in text for keyword in OWNER_INVALID_KEYWORDS):
        return False
    return bool(re.fullmatch(r"[\u4e00-\u9fa5、，,/]{2,30}", text))


def _lines(text: str) -> list[str]:
    return [_compact_value(line) for line in re.split(r"[\r\n]+", _clean_text(text)) if _compact_value(line)]


def _extract_after_label(text: str, labels: list[str], stop_labels: list[str] | None = None) -> tuple[str, str]:
    stop_labels = stop_labels or []
    source_lines = _lines(text)
    label_pattern = "|".join(re.escape(label) for label in labels)
    stop_pattern = "|".join(re.escape(label) for label in stop_labels + CHINESE_FIELDS)
    for index, line in enumerate(source_lines):
        match = re.search(label_pattern, line)
        if not match:
            continue
        after = _compact_value(line[match.end():])
        if after:
            if stop_pattern:
                after = re.split(stop_pattern, after, maxsplit=1)[0]
            after = _compact_value(after)
            if after:
                return after, line
        for next_line in source_lines[index + 1 : index + 4]:
            if stop_pattern and re.search(stop_pattern, next_line):
                continue
            if next_line:
                return _compact_value(next_line), f"{line}\n{next_line}"
    dense = re.sub(r"\s+", "", _clean_text(text))
    match = re.search(rf"(?:{label_pattern})[:：]?(.*?)(?:{stop_pattern}|$)", dense)
    if match:
        value = _compact_value(match.group(1))
        if value:
            return value, match.group(0)[:80]
    return "", ""


def _extract_cert_number(text: str) -> tuple[str, str]:
    patterns = [
        r"(沪房地[\u4e00-\u9fa5]{0,6}字[（(]?\d{4}[）)]?第?\d+号)",
        r"((?:房权|沪房地)[^\n]{0,30}?第?\d+号)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text or "")
        if match:
            return _normalize_cert_number(match.group(1)), match.group(0)
    value, evidence = _extract_after_label(text, ["权证编号", "证号", "编号"])
    normalized = _normalize_cert_number(value)
    if normalized and "坐落" not in normalized and re.search(r"(?:沪房地|房权|字.*号|第.*号)", normalized):
        return normalized, evidence
    return "", ""


def _extract_date_near(text: str, label: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(text, [label])
    if value:
        match = re.search(r"\d{4}年\d{1,2}月\d{1,2}日|\d{4}年", value)
        return (match.group(0), evidence) if match else ("", "")
    return "", ""


def _extract_area(text: str, labels: list[str]) -> tuple[str, str]:
    value, evidence = _extract_after_label(text, labels)
    if not value:
        return "", ""
    match = re.search(r"(\d+(?:\.\d+)?)\s*(?:平方米|㎡|m2|M2)?", value)
    if match:
        return f"{match.group(1)} 平方米", evidence
    return value, evidence


def _extract_usage_area(text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(text, ["使用权面积"], stop_labels=["使用期限", "土地使用期限"])
    if not value:
        return "", ""
    if "独用" in value or not re.search(r"\d", value):
        return "", ""
    return _extract_area(text, ["使用权面积"])


def _extract_owner(text: str) -> tuple[str, str]:
    source_lines = _lines(text)
    for index, line in enumerate(source_lines):
        match = re.search(OWNER_LABEL_PATTERN, line)
        if not match:
            continue
        if any(keyword in line for keyword in INVALID_VALUE_KEYWORDS):
            continue
        value = _clean_owner_candidate(line[match.end():])
        if _is_valid_owner_value(value):
            return value, line

        for next_line in source_lines[index + 1 : index + 3]:
            if re.search(OWNER_STOP_PATTERN, next_line):
                break
            value = _clean_owner_candidate(next_line)
            if _is_valid_owner_value(value):
                return value, f"{line}\n{next_line}"

    for pattern in [
        rf"(?:{OWNER_LABEL_PATTERN})[:：\s]+([\u4e00-\u9fa5、，,/]{{2,30}})",
        rf"(?:{OWNER_LABEL_PATTERN})\s*([\u4e00-\u9fa5、，,/]{{2,30}})",
    ]:
        match = re.search(pattern, text or "")
        if not match:
            continue
        value = _clean_owner_candidate(match.group(1))
        if _is_valid_owner_value(value):
            return value, match.group(0)

    value, evidence = _extract_after_label(text, ["权利人", "房屋所有权人", "所有权人"])
    value = _clean_owner_candidate(value)
    if _is_valid_owner_value(value):
        return value, evidence
    return "", ""


def _extract_contextual_use(text: str, section_label: str, explicit_labels: list[str]) -> tuple[str, str]:
    value, evidence = _extract_after_label(text, explicit_labels)
    if _is_valid_property_value(value):
        return value, evidence

    source_lines = _lines(text)
    for index, line in enumerate(source_lines):
        if section_label not in line:
            continue
        section_lines = source_lines[index : index + 10]
        for section_line in section_lines:
            if "用途" not in section_line:
                continue
            value = re.sub(r".*?用途[:：]?", "", section_line, count=1)
            value = re.split(r"(?:宗地号|宗地|使用期限|室号或部位|建筑面积|建筑类型|总层数|竣工日期)", value, maxsplit=1)[0]
            value = _compact_value(value)
            if _is_valid_property_value(value):
                return value, section_line
    return "", ""


def _split_owners(owner: str) -> list[str]:
    return [part.strip() for part in re.split(r"[、,，;；\s]+", owner or "") if part.strip()]


def _build_evidence(value_map: dict[str, tuple[Any, str, float]]) -> tuple[dict[str, Any], dict[str, Any], dict[str, float]]:
    fields: dict[str, Any] = {}
    evidence: dict[str, Any] = {}
    confidences: dict[str, float] = {}
    for field, (value, evidence_text, confidence) in value_map.items():
        if value in ("", None, [], {}):
            continue
        fields[field] = value
        confidences[field] = confidence
        evidence[field] = {
            "value": value,
            "evidence_text": evidence_text or str(value),
            "page": None,
            "confidence": confidence,
        }
    return fields, evidence, confidences


def _extract_property(payload: dict[str, Any] | str, doc_type: str) -> dict[str, Any]:
    data = normalize_input(payload)
    text = _clean_text(data["text"])

    owner, owner_evidence = _extract_owner(text)
    co_owner_value, co_owner_evidence = _extract_after_label(text, ["共有人", "共有情况"])
    cert_number, cert_evidence = _extract_cert_number(text)
    address, address_evidence = _extract_after_label(text, ["房地坐落", "房屋坐落", "坐落", "不动产坐落"])
    right_nature, right_nature_evidence = _extract_after_label(text, ["权属性质", "权利性质"])
    acquire_method, acquire_evidence = _extract_after_label(text, ["使用权取得方式", "取得方式"])
    land_use, land_use_evidence = _extract_contextual_use(text, "土地状况", ["土地用途", "土地状况用途"])
    parcel_number, parcel_evidence = _extract_after_label(text, ["宗地号", "地号"])
    land_area, land_area_evidence = _extract_area(text, ["宗地(丘)面积", "宗地面积", "土地面积"])
    usage_area, usage_area_evidence = _extract_usage_area(text)
    term, term_evidence = _extract_after_label(text, ["使用期限", "土地使用期限"])
    room, room_evidence = _extract_after_label(text, ["室号或部位", "室号", "部位"])
    building_area, building_area_evidence = _extract_area(text, ["建筑面积", "房屋建筑面积"])
    building_type, building_type_evidence = _extract_after_label(text, ["建筑类型"])
    house_use, house_use_evidence = _extract_contextual_use(text, "房屋状况", ["房屋用途", "房屋状况用途"])
    total_floors, total_floors_evidence = _extract_after_label(text, ["总层数"])
    completion_date, completion_evidence = _extract_date_near(text, "竣工日期")
    register_date, register_evidence = _extract_date_near(text, "登记日")
    issue_unit, issue_unit_evidence = _extract_after_label(text, ["填证单位"])

    explicit_co_owners = _split_owners(co_owner_value) if _is_valid_property_value(co_owner_value) else []
    owner_display = owner
    if explicit_co_owners:
        owner_display = "、".join(_split_owners(owner) + explicit_co_owners)

    value_map: dict[str, tuple[Any, str, float]] = {
        "权利人": (owner_display, owner_evidence or co_owner_evidence, 0.78),
        "权证编号": (cert_number, cert_evidence, 0.76),
        "房地坐落": (address, address_evidence, 0.78),
        "权属性质": (right_nature, right_nature_evidence, 0.74),
        "使用权取得方式": (acquire_method, acquire_evidence, 0.72),
        "用途": (land_use, land_use_evidence, 0.7),
        "宗地号": (parcel_number, parcel_evidence, 0.72),
        "宗地面积": (land_area, land_area_evidence, 0.72),
        "使用权面积": (usage_area, usage_area_evidence, 0.7),
        "土地使用期限": (term, term_evidence, 0.72),
        "室号或部位": (room, room_evidence, 0.72),
        "建筑面积": (building_area, building_area_evidence, 0.78),
        "建筑类型": (building_type, building_type_evidence, 0.7),
        "房屋用途": (house_use, house_use_evidence, 0.68),
        "总层数": (total_floors, total_floors_evidence, 0.68),
        "竣工日期": (completion_date, completion_evidence, 0.66),
        "登记日": (register_date, register_evidence, 0.68),
        "填证单位": (issue_unit, issue_unit_evidence, 0.62),
    }
    value_map = {
        field: (value, evidence_text, confidence)
        for field, (value, evidence_text, confidence) in value_map.items()
        if _is_valid_property_value(value)
    }
    fields, evidence, confidences = _build_evidence(value_map)

    # Compatibility aliases for profile sync and older integrations. UI/Markdown use Chinese labels.
    for zh_key, en_key in ENGLISH_ALIASES.items():
        if zh_key in fields and en_key not in fields:
            fields[en_key] = fields[zh_key]
            evidence[en_key] = {**evidence[zh_key], "value": fields[zh_key]}
            confidences[en_key] = max(0.55, confidences[zh_key] - 0.02)

    result = build_result(doc_type, fields, evidence)
    result["doc_type_name"] = "房产证/房地产权证" if doc_type == "property_cert" else result["doc_type_name"]
    result["confidence"]["fields"] = confidences
    result["confidence"]["overall"] = round(sum(confidences.values()) / len(confidences), 4) if confidences else 0.0
    result["raw_text_preview"] = raw_preview(text)
    if fields:
        result["validation"]["warnings"].append("部分字段来自扫描件识别，建议人工确认权利人、权证编号和房地坐落。")
    return result


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    return _extract_property(payload, "property_cert")
