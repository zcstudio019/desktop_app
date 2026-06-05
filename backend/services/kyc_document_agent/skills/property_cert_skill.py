from __future__ import annotations

import logging
import re
from typing import Any

from backend.services.kyc_document_agent.evidence import raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


logger = logging.getLogger(__name__)

CHINESE_FIELDS = [
    "权利人",
    "共有情况",
    "权证编号",
    "坐落",
    "房地坐落",
    "不动产单元号",
    "权利类型",
    "权属性质",
    "权利性质",
    "使用权取得方式",
    "土地用途",
    "宗地号",
    "地号",
    "宗地面积",
    "使用期限",
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
    "坐落": "property_address",
    "房地坐落": "property_address",
    "不动产单元号": "property_unit_number",
    "权利类型": "right_type",
    "权属性质": "right_nature",
    "权利性质": "right_nature",
    "土地用途": "land_use",
    "宗地号": "parcel_number",
    "地号": "parcel_number",
    "建筑面积": "building_area",
    "建筑类型": "building_type",
    "房屋用途": "house_use",
    "总层数": "total_floors",
    "竣工日期": "completion_date",
    "宗地面积": "land_area",
    "使用期限": "land_use_term",
    "登记日": "issue_date",
    "填证单位": "issuing_unit",
}

INVALID_VALUE_EXACT = {"", "对", "无", "未识别"}
INVALID_VALUE_KEYWORDS = ("合法权益", "房地产权利人", "本证是证明", "根据", "法律")
OWNER_INVALID_KEYWORDS = (
    "权利",
    "不动产权利",
    "权利人合法权益",
    "房地产权证",
    "合法权益",
    "房地产权利人",
    "权利人的合法权益",
    "本证是证明",
    "本证",
    "证明",
    "根据",
    "法律",
    "经审查核实",
    "准予登记",
    "颁发此证",
    "本证所列",
    "申请登记",
    "法律法规",
    "中华人民共和国",
    "国土资源部",
    "登记机构",
    "监制",
    "状况",
    "土地",
    "房屋",
    "坐落",
    "用途",
    "面积",
)
OWNER_LABEL_PATTERN = r"权\s*利\s*人|权利\s*人|房屋所有权人|所有权人"
OWNER_STOP_PATTERN = r"共\s*有\s*人|共有情况|权证编号|房地坐落|房屋坐落|坐落|不动产单元号|权利类型|权利性质|权属性质|土地状况|房屋状况|用途|面积"

COVER_PAGE_KEYWORDS = (
    "根据《中华人民共和国物权法》",
    "为保护不动产权利人合法权益",
    "经审查核实",
    "准予登记",
    "颁发此证",
    "登记机构",
    "国土资源部监制",
    "编号",
)

DETAIL_PAGE_KEYWORDS = (
    "权利人",
    "共有情况",
    "坐落",
    "不动产单元号",
    "权利类型",
    "权利性质",
    "用途",
    "面积",
    "使用期限",
    "室号或部位",
    "建筑面积",
    "总层数",
    "竣工日期",
)


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


def _keyword_score(text: str, keywords: tuple[str, ...]) -> int:
    compact = re.sub(r"\s+", "", text or "")
    return sum(1 for keyword in keywords if re.sub(r"\s+", "", keyword) in compact)


def page_role(text: str) -> str:
    detail_score = _keyword_score(text, DETAIL_PAGE_KEYWORDS)
    cover_score = _keyword_score(text, COVER_PAGE_KEYWORDS)
    if detail_score >= 3:
        return "detail_page"
    if cover_score >= 3 and detail_score < 3:
        return "cover_page"
    return "unknown"


def _select_detail_text(data: dict[str, Any]) -> tuple[str, str]:
    text = _clean_text(data.get("text") or "")
    pages = data.get("pages") if isinstance(data.get("pages"), list) else []
    page_texts = [
        _clean_text(str(page.get("text") or ""))
        for page in pages
        if isinstance(page, dict) and str(page.get("text") or "").strip()
    ]
    candidates = page_texts or [text]
    detail_pages = [item for item in candidates if page_role(item) == "detail_page"]
    if detail_pages:
        return "\n\n".join(detail_pages), "detail_page"
    if page_role(text) == "detail_page":
        return text, "detail_page"
    if candidates and all(page_role(item) == "cover_page" for item in candidates):
        return "", "cover_page"
    return text, page_role(text)


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
        r"(沪[（(]\d{4}[）)][\u4e00-\u9fa5]{1,6}字不动产权第\d+号)",
        r"([\u4e00-\u9fa5][（(]\d{4}[）)][\u4e00-\u9fa5]{1,8}不动产权第\d+号)",
        r"(沪房地[\u4e00-\u9fa5]{0,6}字[（(]?\d{4}[）)]?第?\d+号)",
        r"((?:房权|沪房地)[^\n]{0,30}?第?\d+号)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text or "")
        if match:
            return _normalize_cert_number(match.group(1)), match.group(0)
    value, evidence = _extract_after_label(text, ["权证编号", "证号", "编号"])
    normalized = _normalize_cert_number(value)
    if normalized and "坐落" not in normalized and re.search(r"(?:沪房地|房权|不动产权|字.*号|第.*号)", normalized):
        return normalized, evidence
    return "", ""


def _extract_date_near(text: str, label: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(text, [label])
    if value:
        match = re.search(r"\d{4}年\d{1,2}月\d{1,2}日|\d{4}年", value)
        return (match.group(0), evidence) if match else ("", "")
    return "", ""


def _extract_valid_year_or_date(value: str) -> str:
    match = re.search(r"\d{4}年\d{1,2}月\d{1,2}日|\d{4}-\d{1,2}-\d{1,2}|\d{4}年", value or "")
    return match.group(0) if match else ""


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


def _section_between(text: str, start_label: str, end_label: str | None = None, max_lines: int = 18) -> str:
    source_lines = _lines(text)
    start_index = next((index for index, line in enumerate(source_lines) if start_label in line), -1)
    if start_index < 0:
        return ""
    end_index = min(len(source_lines), start_index + max_lines)
    if end_label:
        for index in range(start_index + 1, min(len(source_lines), start_index + max_lines)):
            if end_label in source_lines[index]:
                end_index = index
                break
    return "\n".join(source_lines[start_index:end_index])


def _extract_land_use(text: str) -> tuple[str, str]:
    combined = re.search(r"土地用途[:：\s]*([\u4e00-\u9fa5]{1,12})\s*/\s*房屋用途[:：\s]*([\u4e00-\u9fa5]{1,12})", text or "")
    if combined:
        return _compact_value(combined.group(1)), combined.group(0)
    land_section = _section_between(text, "土地状况", "房屋状况")
    candidates = [land_section, _clean_text(text)]
    for section in candidates:
        if not section:
            continue
        for pattern in [
            r"用\s*途[:：\s]*([\u4e00-\u9fa5]{2,12}用地)",
            r"用\s*\n\s*途\s*\n\s*([\u4e00-\u9fa5]{2,12}用地)",
        ]:
            match = re.search(pattern, section)
            if match:
                return _compact_value(match.group(1)), match.group(0)
    if land_section and "住宅用地" in land_section and "国有建设用地使用权" in text and "宗地号" in text:
        return "住宅用地", "土地状况\n住宅用地"
    return "", ""


def _extract_house_and_land_use(text: str) -> tuple[str, str, str, str]:
    combined = re.search(r"土地用途[:：\s]*([\u4e00-\u9fa5]{1,12})\s*/\s*房屋用途[:：\s]*([\u4e00-\u9fa5]{1,12})", text or "")
    if combined:
        return _compact_value(combined.group(1)), combined.group(0), _compact_value(combined.group(2)), combined.group(0)
    return "", "", "", ""


def _extract_completion_date(text: str) -> tuple[str, str]:
    building_section = _section_between(text, "房屋状况", None)
    candidates = [building_section, _clean_text(text)]
    for section in candidates:
        if not section:
            continue
        for pattern in [
            r"竣\s*工\s*日\s*期[:：\s]*([0-9]{4}年(?:\d{1,2}月\d{1,2}日)?|[0-9]{4}-\d{1,2}-\d{1,2})",
            r"竣\s*工\s*日\s*期\s*\n\s*([0-9]{4}年(?:\d{1,2}月\d{1,2}日)?|[0-9]{4}-\d{1,2}-\d{1,2})",
        ]:
            match = re.search(pattern, section)
            if match:
                value = _extract_valid_year_or_date(match.group(1))
                if value:
                    return value, match.group(0)
    if "竣工日期" in text:
        start = text.find("竣工日期")
        value = _extract_valid_year_or_date(text[start:start + 80])
        if value:
            return value, "竣工日期"
    return "", ""


def _extract_house_use(text: str) -> tuple[str, str]:
    combined = re.search(r"土地用途[:：\s]*([\u4e00-\u9fa5]{1,12})\s*/\s*房屋用途[:：\s]*([\u4e00-\u9fa5]{1,12})", text or "")
    if combined:
        value = _compact_value(combined.group(2))
        if value and "用地" not in value:
            return value, combined.group(0)
    building_section = _section_between(text, "房屋状况", "填证单位")
    candidates = [building_section]
    for section in candidates:
        if not section:
            continue
        for pattern in [
            r"(?:房屋用途|用\s*途)[:：\s]*([\u4e00-\u9fa5]{2,8})(?=\s|$)",
            r"用\s*\n\s*途\s*\n\s*([\u4e00-\u9fa5]{2,8})(?=\s|$)",
        ]:
            match = re.search(pattern, section)
            if not match:
                continue
            value = _compact_value(match.group(1))
            if value and "用地" not in value and value not in {"住宅用地", "国有建设"}:
                return value, match.group(0)

    if building_section and "居住" in building_section and any(
        keyword in building_section for keyword in ("室号或部位", "建筑面积", "建筑类型", "总层数", "竣工日期")
    ):
        return "居住", "房屋状况\n居住"
    return "", ""


def _extract_right_nature(text: str) -> tuple[str, str]:
    for line in _lines(text):
        if "权属性质" in line:
            value = re.sub(r"^.*?权属性质[:：]?", "", line)
            value = re.split(r"(?:使用权取得方式|用途|面积|使用期限|宗地号|地号)", value, maxsplit=1)[0]
            value = _compact_value(value)
            if _is_valid_property_value(value):
                return value, line
        if "权利性质" in line:
            value = re.sub(r"^.*?权利性质[:：]?", "", line, count=1)
            value = re.split(r"(?:用途|面积|使用期限|权利其他状况|附记)", value, maxsplit=1)[0]
            value = _compact_value(value)
            if value == "土地":
                match = re.search(r"土地权利性质[:：]\s*([\u4e00-\u9fa5]+)", line)
                if match:
                    value = f"土地权利性质：{match.group(1)}"
            if _is_valid_property_value(value):
                return value, line
    return _extract_after_label(text, ["权属性质", "权利性质"], stop_labels=["用途", "面积", "使用期限"])


def _extract_new_version_area(text: str, label: str) -> tuple[str, str]:
    pattern = rf"{label}[:：\s]*(\d+(?:\.\d+)?)\s*(?:平方米|㎡|m2|M2)?"
    match = re.search(pattern, text or "")
    if match:
        return f"{match.group(1)} 平方米", match.group(0)
    return "", ""


def _extract_term(text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(text, ["使用期限", "土地使用期限"], stop_labels=["权利其他状况", "附记", "室号或部位"])
    if not value:
        return "", ""
    match = re.search(r"\d{4}年\d{1,2}月\d{1,2}日起?\s*\d{4}年\d{1,2}月\d{1,2}日止?", value)
    if match:
        return match.group(0), evidence
    return value, evidence


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


def _has_house_context_fields(fields: dict[str, Any]) -> bool:
    context_keys = ("室号或部位", "建筑面积", "建筑类型", "总层数", "竣工日期")
    return sum(1 for key in context_keys if _is_valid_property_value(fields.get(key))) >= 2


def _extract_property(payload: dict[str, Any] | str, doc_type: str) -> dict[str, Any]:
    data = normalize_input(payload)
    source_text = _clean_text(data["text"])
    text, selected_role = _select_detail_text(data)
    if not text and selected_role == "cover_page":
        result = build_result(doc_type, {}, {})
        result["doc_type_name"] = "房产证/房地产权证" if doc_type == "property_cert" else result["doc_type_name"]
        result["raw_text_preview"] = raw_preview(source_text)
        result["validation"]["warnings"].append(
            "仅识别到房产证/不动产权证封面或说明页，未识别到权利人、坐落、面积等字段页，请补充上传正面字段页或人工确认。"
        )
        result["page_role"] = "cover_page"
        return result

    owner, owner_evidence = _extract_owner(text)
    co_owner_value, co_owner_evidence = _extract_after_label(text, ["共有情况", "共有人"])
    cert_number, cert_evidence = _extract_cert_number(text)
    address, address_evidence = _extract_after_label(text, ["房地坐落", "房屋坐落", "不动产坐落", "坐落"], stop_labels=["不动产单元号", "权利类型"])
    property_unit_number, unit_evidence = _extract_after_label(text, ["不动产单元号"], stop_labels=["权利类型", "权利性质", "用途"])
    right_type, right_type_evidence = _extract_after_label(text, ["权利类型"], stop_labels=["权利性质", "用途", "面积"])
    right_nature, right_nature_evidence = _extract_right_nature(text)
    acquire_method, acquire_evidence = _extract_after_label(text, ["使用权取得方式", "取得方式"])
    combined_land_use, combined_land_evidence, combined_house_use, combined_house_evidence = _extract_house_and_land_use(text)
    land_use, land_use_evidence = combined_land_use, combined_land_evidence
    house_use, house_use_evidence = combined_house_use, combined_house_evidence
    if not land_use:
        land_use, land_use_evidence = _extract_land_use(text)
    if not land_use:
        land_use, land_use_evidence = _extract_contextual_use(text, "土地状况", ["土地用途", "土地状况用途"])
    parcel_number, parcel_evidence = _extract_after_label(text, ["宗地号", "地号"], stop_labels=["面积", "使用期限"])
    land_area, land_area_evidence = _extract_new_version_area(text, "宗地面积")
    if not land_area:
        land_area, land_area_evidence = _extract_new_version_area(text, "土地面积")
    if not land_area:
        land_area, land_area_evidence = _extract_area(text, ["宗地(丘)面积", "宗地面积", "土地面积"])
    usage_area, usage_area_evidence = _extract_usage_area(text)
    term, term_evidence = _extract_term(text)
    room, room_evidence = _extract_after_label(text, ["室号或部位", "室号", "部位"])
    building_area, building_area_evidence = _extract_new_version_area(text, "建筑面积")
    if not building_area:
        building_area, building_area_evidence = _extract_area(text, ["建筑面积", "房屋建筑面积"])
    building_type, building_type_evidence = _extract_after_label(
        text,
        ["建筑类型"],
        stop_labels=["用途", "房屋用途", "总层数", "竣工日期"],
    )
    if not house_use:
        house_use, house_use_evidence = _extract_house_use(text)
    if not house_use:
        house_use, house_use_evidence = _extract_contextual_use(text, "房屋状况", ["房屋用途", "房屋状况用途"])
    total_floors, total_floors_evidence = _extract_after_label(text, ["总层数"])
    completion_date, completion_evidence = _extract_completion_date(text)
    if not completion_date:
        completion_date, completion_evidence = _extract_date_near(text, "竣工日期")
    register_date, register_evidence = _extract_date_near(text, "登记日")
    issue_unit, issue_unit_evidence = _extract_after_label(text, ["填证单位"])

    owner_display = owner

    value_map: dict[str, tuple[Any, str, float]] = {
        "权利人": (owner_display, owner_evidence or co_owner_evidence, 0.78),
        "共有情况": (co_owner_value, co_owner_evidence, 0.72),
        "权证编号": (cert_number, cert_evidence, 0.76),
        "坐落": (address, address_evidence, 0.78),
        "房地坐落": (address, address_evidence, 0.78),
        "不动产单元号": (property_unit_number, unit_evidence, 0.76),
        "权利类型": (right_type, right_type_evidence, 0.74),
        "权属性质": (right_nature, right_nature_evidence, 0.74),
        "权利性质": (right_nature, right_nature_evidence, 0.74),
        "使用权取得方式": (acquire_method, acquire_evidence, 0.72),
        "土地用途": (land_use, land_use_evidence, 0.7),
        "宗地号": (parcel_number, parcel_evidence, 0.72),
        "地号": (parcel_number, parcel_evidence, 0.72),
        "宗地面积": (land_area, land_area_evidence, 0.72),
        "使用权面积": (usage_area, usage_area_evidence, 0.7),
        "使用期限": (term, term_evidence, 0.72),
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

    if (
        doc_type in {"property_cert", "real_estate_cert"}
        and "房屋用途" not in fields
        and "居住" in text
        and _has_house_context_fields(fields)
    ):
        fields["房屋用途"] = "居住"
        evidence["房屋用途"] = {
            "value": "居住",
            "evidence_text": "房屋状况\n居住",
            "page": None,
            "confidence": 0.62,
        }
        confidences["房屋用途"] = 0.62

    # Compatibility aliases for profile sync and older integrations. UI/Markdown use Chinese labels.
    for zh_key, en_key in ENGLISH_ALIASES.items():
        if zh_key in fields and en_key not in fields:
            fields[en_key] = fields[zh_key]
            evidence[en_key] = {**evidence[zh_key], "value": fields[zh_key]}
            confidences[en_key] = max(0.55, confidences[zh_key] - 0.02)
    if "房屋用途" in fields:
        for alias in ("use_type", "building_use"):
            if alias not in fields:
                fields[alias] = fields["房屋用途"]
                evidence[alias] = {**evidence["房屋用途"], "value": fields["房屋用途"]}
                confidences[alias] = max(0.55, confidences["房屋用途"] - 0.02)

    logger.info("[PropertyCertSkill][DEBUG] raw_text_contains_居住=%s", str("居住" in text).lower())
    logger.info("[PropertyCertSkill][DEBUG] fields_keys=%s", list(fields.keys()))
    logger.info("[PropertyCertSkill][DEBUG] 房屋用途=%s", fields.get("房屋用途"))
    logger.info("[PropertyCertSkill][DEBUG] house_use=%s", fields.get("house_use"))
    logger.info("[PropertyCertSkill][DEBUG] building_use=%s", fields.get("building_use"))
    logger.info("[PropertyCertSkill][DEBUG] use_type=%s", fields.get("use_type"))

    result = build_result(doc_type, fields, evidence)
    result["doc_type_name"] = "房产证/房地产权证" if doc_type == "property_cert" else result["doc_type_name"]
    result["page_role"] = selected_role
    result["confidence"]["fields"] = confidences
    result["confidence"]["overall"] = round(sum(confidences.values()) / len(confidences), 4) if confidences else 0.0
    result["raw_text_preview"] = raw_preview(text)
    if fields:
        result["validation"]["warnings"].append("部分字段来自扫描件识别，建议人工确认权利人、权证编号和房地坐落。")
    elif selected_role != "detail_page":
        result["validation"]["warnings"].append("仅识别到房产证/不动产权证封面或说明页，未识别到权利人、坐落、面积等字段页，请补充上传正面字段页或人工确认。")
    return result


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    return _extract_property(payload, "property_cert")
