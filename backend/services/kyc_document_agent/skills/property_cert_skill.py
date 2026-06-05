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
    "房屋用途",
    "宗地号",
    "地号",
    "宗地面积",
    "使用期限",
    "土地使用期限",
    "室号或部位",
    "建筑面积",
    "建筑类型",
    "总层数",
    "竣工日期",
    "登记日期",
    "登记机构",
    "封面编号",
    "登记日",
    "填证单位",
]

PROPERTY_FIELD_ORDER = CHINESE_FIELDS[:]

ENGLISH_ALIASES = {
    "权利人": "owner",
    "共有情况": "co_ownership",
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
    "使用期限": "use_term",
    "登记日期": "registration_date",
    "登记机构": "registration_authority",
    "封面编号": "cover_certificate_number",
    "登记日": "issue_date",
    "填证单位": "issuing_unit",
}

EXTRA_FIELD_ALIASES = {
    "共有情况": ("shared_status", "ownership_status"),
    "不动产单元号": ("real_estate_unit_no", "real_estate_unit_number"),
    "使用期限": ("land_use_term",),
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


def _dense_text(text: str) -> str:
    return re.sub(r"\s+", "", _clean_text(text or ""))


def _compact_value(value: str) -> str:
    value = re.sub(r"\s+", " ", value or "").strip(" :：|,，;；")
    value = re.sub(r"^(?:为|是|:)\s*", "", value).strip()
    value = value.replace("土地权利性质:", "土地权利性质：")
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


def _dense_between(dense: str, start: str, stops: tuple[str, ...], *, max_len: int = 120) -> tuple[str, str]:
    start_index = dense.find(start)
    if start_index < 0:
        return "", ""
    value_start = start_index + len(start)
    while value_start < len(dense) and dense[value_start] in ":：;；,，":
        value_start += 1
    value_end = min(len(dense), value_start + max_len)
    for stop in stops:
        stop_index = dense.find(stop, value_start)
        if value_start <= stop_index < value_end:
            value_end = stop_index
    value = _compact_value(dense[value_start:value_end])
    return value, dense[start_index:value_end]


def _extract_new_version_dense_fields(text: str) -> dict[str, tuple[str, str, float]]:
    dense = _dense_text(text)
    extracted: dict[str, tuple[str, str, float]] = {}

    def add(field: str, value: str, evidence: str, confidence: float = 0.72) -> None:
        value = _compact_value(value)
        if value and _is_valid_property_value(value) and field not in extracted:
            extracted[field] = (value, evidence or value, confidence)

    cert_match = re.search(r"([\u4e00-\u9fa5]?[（(]\d{4}[）)][\u4e00-\u9fa5]{1,8}字?不动产权第\d+号)", dense)
    if cert_match:
        add("权证编号", _normalize_cert_number(cert_match.group(1)).replace("字不动产权", "字不动产权"), cert_match.group(0), 0.78)

    owner, evidence = _dense_between(dense, "权利人", ("共有情况", "坐落", "不动产单元号"), max_len=30)
    if _is_valid_owner_value(owner):
        add("权利人", _clean_owner_candidate(owner), evidence, 0.78)

    shared, evidence = _dense_between(dense, "共有情况", ("坐落", "不动产单元号", "权利类型"), max_len=30)
    add("共有情况", shared, evidence, 0.72)

    address, evidence = _dense_between(dense, "坐落", ("不动产单元号", "权利类型", "权利性质"), max_len=80)
    add("坐落", address, evidence, 0.78)
    add("房地坐落", address, evidence, 0.78)

    unit_match = re.search(r"不动产单元号[:：]?([0-9A-Z]{20,40})", dense)
    if unit_match:
        add("不动产单元号", unit_match.group(1), unit_match.group(0), 0.78)

    right_type, evidence = _dense_between(dense, "权利类型", ("权利性质", "用途", "面积"), max_len=80)
    add("权利类型", right_type, evidence, 0.74)

    right_nature, evidence = _dense_between(dense, "权利性质", ("用途", "面积", "使用期限"), max_len=80)
    if right_nature == "土地":
        nature_match = re.search(r"权利性质土地权利性质[:：]?([\u4e00-\u9fa5]+)", dense)
        if nature_match:
            right_nature = f"土地权利性质:{nature_match.group(1)}"
    add("权利性质", right_nature, evidence, 0.74)
    add("权属性质", right_nature, evidence, 0.74)

    use_match = re.search(r"土地用途[:：]?([\u4e00-\u9fa5]{1,12})/?房屋用途[:：]?([\u4e00-\u9fa5]{1,12})", dense)
    if use_match:
        add("土地用途", use_match.group(1), use_match.group(0), 0.72)
        add("房屋用途", use_match.group(2), use_match.group(0), 0.72)

    area_match = re.search(r"宗地面积[:：]?(\d+(?:\.\d+)?)平方米/?建筑面积[:：]?(\d+(?:\.\d+)?)平方米", dense)
    if area_match:
        add("宗地面积", f"{area_match.group(1)} 平方米", area_match.group(0), 0.74)
        add("建筑面积", f"{area_match.group(2)} 平方米", area_match.group(0), 0.78)

    term_match = re.search(r"(\d{4}年\d{1,2}月\d{1,2}日?\s*起\s*\d{4}年\d{1,2}月\d{1,2}日?\s*止?)", dense)
    if term_match:
        term_value = _normalize_use_term(term_match.group(1))
        add("使用期限", term_value, term_match.group(0), 0.72)
        add("土地使用期限", term_value, term_match.group(0), 0.72)

    parcel, evidence = _dense_between(
        dense,
        "地号",
        ("使用权面积", "独用面积", "分摊面积", "房屋状况", "室号", "室号部位", "建筑面积", "建筑类型", "类型", "总层数"),
        max_len=80,
    )
    add("地号", parcel, evidence, 0.7)
    add("宗地号", parcel, evidence, 0.7)

    room_match = re.search(r"室号(?:或部位|部位)?[:：]?([0-9A-Za-z-]{1,20})", dense)
    if room_match:
        add("室号或部位", room_match.group(1), room_match.group(0), 0.72)

    building_type_match = re.search(r"(?:建筑类型|类型)[:：]?([\u4e00-\u9fa5]{1,8})[;；,，。:]?(?:总层数|竣工日期)", dense)
    if building_type_match:
        add("建筑类型", building_type_match.group(1), building_type_match.group(0), 0.7)

    floor_match = re.search(r"总层数[:：]?(\d+)", dense)
    if floor_match:
        add("总层数", floor_match.group(1), floor_match.group(0), 0.68)

    completion_match = re.search(r"竣工日期[:：]?(\d{4}年(?:\d{1,2}月\d{1,2}日)?)", dense)
    if completion_match:
        add("竣工日期", completion_match.group(1), completion_match.group(0), 0.68)

    return extracted


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


def _normalize_chinese_date(value: str) -> str:
    text = re.sub(r"\s+", "", value or "")
    match = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日?", text)
    if match:
        return f"{match.group(1)}年{int(match.group(2))}月{int(match.group(3))}日"
    return ""


def _extract_cover_registration_date(text: str) -> tuple[str, str]:
    candidates: list[tuple[int, str]] = []
    for keyword in (
        "cover_registration_date_region",
        "cover_registration_date_line",
        "cover_seal_date_ocr",
        "seal_region_ocr",
        "Seal Region OCR",
        "登记机构",
        "登记专用章",
        "不动产登记专用章",
        "颁发此证",
        "专用章",
        "Seal",
        "OCR",
    ):
        index = text.find(keyword)
        if index >= 0:
            priority = 0 if keyword in {"cover_registration_date_region", "cover_registration_date_line", "cover_seal_date_ocr", "seal_region_ocr", "Seal Region OCR"} else 1
            candidates.append((priority, text[max(0, index - 180): index + 240]))
    candidates.append((9, text))
    pattern = r"(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日?(?:章|印|登记|（?\d{1,2}）?)?"
    matched_dates: list[tuple[int, str, str]] = []
    for priority, candidate in candidates:
        for match in re.finditer(pattern, candidate or ""):
            year, month, day = (int(part) for part in match.groups())
            if not (2010 <= year <= 2035 and 1 <= month <= 12 and 1 <= day <= 31):
                continue
            value = f"{year}年{month}月{day}日"
            matched_dates.append((priority, value, match.group(0)))
    selected = sorted(matched_dates, key=lambda item: (item[0], item[1]))[0] if matched_dates else None
    logger.info("[CoverDateOCR] raw_text_contains_2018=%s", str("2018" in (text or "")).lower())
    logger.info("[CoverDateOCR] seal_region_text=%s", "\n---\n".join(candidate for _, candidate in candidates[:4]))
    logger.info("[CoverDateOCR] matched_dates=%s", matched_dates)
    logger.info("[CoverDateOCR] selected_registration_date=%s", selected[1] if selected else "")
    if selected:
        return selected[1], selected[2]
    return "", ""


def _extract_cover_certificate_number(text: str) -> tuple[str, str]:
    patterns = [
        r"编号\s*[№NnOo\.\s]*([A-Z]?\d{8,20})",
        r"(?:NO|No|no|№)\s*([A-Z]?\d{8,20})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text or "")
        if match:
            return match.group(1), match.group(0)
    dense = _dense_text(text)
    match = re.search(r"编号[№NnOo\.]*([A-Z]?\d{8,20})", dense)
    if match:
        return match.group(1), match.group(0)
    return "", ""


def _extract_cover_registration_authority(text: str) -> tuple[str, str]:
    dense = _dense_text(text)
    if "中华人民共和国国土资源部监制" in dense and "不动产登记专用章" not in dense:
        return "", ""
    match = re.search(r"上海市\s*不动产\s*登记\s*专用章", text or "")
    if match:
        return "上海市不动产登记专用章", match.group(0)
    if "上海市不动产登记专用章" in dense:
        return "上海市不动产登记专用章", "上海市不动产登记专用章"
    match = re.search(r"[\u4e00-\u9fa5]{2,12}市\s*不动产\s*登记\s*专用章", text or "")
    if match:
        return re.sub(r"\s+", "", match.group(0)), match.group(0)
    if "不动产登记专用章" in dense:
        return "不动产登记专用章", "不动产登记专用章"
    if "登记专用章" in dense and "国土资源部监制" not in dense:
        return "登记专用章", "登记专用章"
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


def _normalize_use_term_result(value: str) -> str:
    normalized = _normalize_use_term(value)
    if re.fullmatch(r"\d{4}年\d{1,2}月\d{1,2}日?起\d{4}年\d{1,2}月\d{1,2}日?", normalized or ""):
        return f"{normalized}止"
    return normalized


def _use_term_debug_lines(text: str) -> str:
    lines = _lines(text)
    snippets: list[str] = []
    for index, line in enumerate(lines):
        if "使用期限" in line or "使用" in line and "期限" in line:
            snippets.append("\n".join(lines[index:index + 3]))
    return "\n---\n".join(snippets[:5])


def _use_term_candidate_debug(text: str) -> dict[str, Any]:
    lines = _lines(text)
    lines_with_term = [line for line in lines if "期限" in line]
    lines_with_2015 = [line for line in lines if "2015" in line or "２０１５" in line]
    lines_with_2076 = [line for line in lines if "2076" in line or "２０７６" in line]
    snippets: list[str] = []
    for index, line in enumerate(lines):
        dense_line = _dense_text(line)
        if (
            "使用期限" in dense_line
            or "期限" in dense_line
            or "2015年10月16" in dense_line
            or "2076" in dense_line
        ):
            start = max(0, index - 1)
            end = min(len(lines), index + 4)
            snippets.append("\n".join(lines[start:end]))
    return {
        "lines_count": len(lines),
        "lines_with_期限": lines_with_term,
        "lines_with_2015": lines_with_2015,
        "lines_with_2076": lines_with_2076,
        "candidate_lines_around_use_term": "\n---\n".join(snippets[:8]),
    }


def extract_real_estate_use_term(text: str) -> str | None:
    """Extract complete real-estate land-use term from noisy OCR text."""
    lines = _lines(text)
    term_pattern = r"\d{4}年\d{1,2}月\d{1,2}日?起\d{4}年\d{1,2}月\d{1,2}日?止?"
    term_without_stop_pattern = r"\d{4}年\d{1,2}月\d{1,2}日?起\d{4}年\d{1,2}月\d{1,2}日?"
    split_term_pattern = (
        r"(\d{4}年\d{1,2}月\d{1,2}日?起\d{4})"
        r"(?:使用期限|土地使用期限|期限|土地状况|建筑面积|平方米|[:：\s]*){0,8}"
        r"(年\d{1,2}月\d{1,2}日?止?)"
    )
    candidates: list[str] = []

    for index, line in enumerate(lines):
        dense_line = _dense_text(line)
        if not (
            "使用期限" in dense_line
            or "期限" in dense_line
            or "2015年10月16" in dense_line
            or "2076" in dense_line
        ):
            continue
        start = max(0, index - 3)
        end = min(len(lines), index + 4)
        candidates.append(_dense_text("".join(lines[start:end])))

    dense = _dense_text(text)
    for label in ("国有建设用地使用权使用期限", "使用期限", "土地使用期限"):
        label_index = dense.find(label)
        if label_index >= 0:
            candidates.append(dense[label_index: label_index + 180])
    candidates.append(dense)

    for candidate in candidates:
        candidate = (
            candidate.replace("：", "")
            .replace(":", "")
            .replace(" ", "")
            .replace("\n", "")
        )
        candidate = re.sub(
            r"(\d{4}年\d{1,2}月\d{1,2}日?起\d{4})使用期限(年\d{1,2}月\d{1,2}日?止?)",
            r"\1\2",
            candidate,
        )
        candidate = re.sub(
            r"(\d{4}年\d{1,2}月\d{1,2}日?起\d{4})(?:土地使用期限|期限)(年\d{1,2}月\d{1,2}日?止?)",
            r"\1\2",
            candidate,
        )
        match = re.search(term_pattern, candidate)
        if match:
            return _normalize_use_term_result(match.group(0))
        match = re.search(split_term_pattern, candidate)
        if match:
            return _normalize_use_term_result(f"{match.group(1)}{match.group(2)}")
        match = re.search(term_without_stop_pattern, candidate)
        if match:
            return _normalize_use_term_result(match.group(0))
    return None


def extract_use_term_from_property_cert_text(text: str) -> str | None:
    """Backward-compatible alias for the dedicated real-estate use-term extractor."""
    return extract_real_estate_use_term(text)


def _extract_complete_use_term_window(text: str) -> tuple[str, str]:
    value = extract_real_estate_use_term(text)
    return (value or "", value or "")


def _extract_term(text: str) -> tuple[str, str]:
    dense = _dense_text(text)
    match = re.search(r"\d{4}年\d{1,2}月\d{1,2}日?\s*起\s*\d{4}年\d{1,2}月\d{1,2}日?\s*止?", dense)
    if match:
        return _normalize_use_term_result(match.group(0)), match.group(0)
    value, evidence = _extract_after_label(text, ["使用期限", "土地使用期限"], stop_labels=["权利其他状况", "附记", "室号或部位"])
    if not value:
        return "", ""
    compact_value = _dense_text(value)
    match = re.search(r"\d{4}年\d{1,2}月\d{1,2}日?\s*起\s*\d{4}年\d{1,2}月\d{1,2}日?\s*止?", compact_value)
    if match:
        return _normalize_use_term_result(match.group(0)), evidence
    if re.fullmatch(r"\d{4}年\d{1,2}月\d{1,2}日?起\d{4}", compact_value or ""):
        return "", ""
    return _normalize_use_term_result(value), evidence


def _normalize_use_term(value: str) -> str:
    text = re.sub(r"\s+", "", value or "")
    text = re.sub(r"^.*?使用期限[:：]?", "", text)
    match = re.search(r"(\d{4}年\d{1,2}月\d{1,2}日?起\d{4}年\d{1,2}月\d{1,2}日?止?)", text)
    if match:
        return match.group(1)
    return _compact_value(text)


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
    metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    filename = str(metadata.get("filename") or metadata.get("source_file") or metadata.get("fileName") or "")
    source_text = _clean_text(data["text"])
    text, selected_role = _select_detail_text(data)
    if not text and selected_role == "cover_page":
        cover_cert_number, cover_cert_evidence = _extract_cover_certificate_number(source_text)
        cover_register_date, cover_register_evidence = _extract_cover_registration_date(source_text)
        cover_authority, cover_authority_evidence = _extract_cover_registration_authority(source_text)
        cover_value_map: dict[str, tuple[Any, str, float]] = {
            "封面编号": (cover_cert_number, cover_cert_evidence, 0.68),
            "登记日期": (cover_register_date, cover_register_evidence, 0.66),
            "登记日": (cover_register_date, cover_register_evidence, 0.62),
            "登记机构": (cover_authority, cover_authority_evidence, 0.64),
        }
        fields, evidence, confidences = _build_evidence(
            {
                field: value_tuple
                for field, value_tuple in cover_value_map.items()
                if _is_valid_property_value(value_tuple[0])
            }
        )
        for zh_key, en_key in ENGLISH_ALIASES.items():
            if zh_key in fields and en_key not in fields:
                fields[en_key] = fields[zh_key]
                evidence[en_key] = {**evidence[zh_key], "value": fields[zh_key]}
                confidences[en_key] = max(0.55, confidences[zh_key] - 0.02)
        if fields.get("登记机构") == "不动产登记专用章":
            result_warning = "登记机构城市需人工确认。"
        else:
            result_warning = ""
        date_warning = ""
        if not cover_register_date and re.search(r"20\d{2}年.*(?:专章|专用章|登记|印章)", source_text or ""):
            date_warning = "登记日期疑似存在但 OCR 未能准确识别，请人工确认。"
        result = build_result(doc_type, fields, evidence)
        result["doc_type_name"] = "房产证/房地产权证" if doc_type == "property_cert" else result["doc_type_name"]
        result["raw_text_preview"] = raw_preview(source_text)
        result["confidence"]["fields"] = confidences
        result["confidence"]["overall"] = round(sum(confidences.values()) / len(confidences), 4) if confidences else 0.0
        result["validation"]["warnings"].append(
            "仅识别到房产证/不动产权证封面或说明页，未识别到权利人、坐落、面积等字段页，请补充上传正面字段页或人工确认。"
        )
        if result_warning:
            result["validation"]["warnings"].append(result_warning)
        if date_warning:
            result["validation"]["warnings"].append(date_warning)
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
    parcel_number, parcel_evidence = _extract_after_label(
        text,
        ["宗地号", "地号"],
        stop_labels=["使用权面积", "独用面积", "分摊面积", "面积", "使用期限", "房屋状况"],
    )
    land_area, land_area_evidence = _extract_new_version_area(text, "宗地面积")
    if not land_area:
        land_area, land_area_evidence = _extract_new_version_area(text, "土地面积")
    if not land_area:
        land_area, land_area_evidence = _extract_area(text, ["宗地(丘)面积", "宗地面积", "土地面积"])
    usage_area, usage_area_evidence = _extract_usage_area(text)
    term, term_evidence = _extract_term(text)
    complete_term, complete_term_evidence = _extract_complete_use_term_window(text)
    if complete_term and (not term or len(complete_term) > len(term) or "止" in complete_term and "止" not in term):
        term, term_evidence = complete_term, complete_term_evidence
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
    dense_fields = _extract_new_version_dense_fields(text)
    dense_override_fields = {
        "权证编号",
        "宗地号",
        "地号",
        "使用期限",
        "土地使用期限",
        "室号或部位",
        "建筑类型",
        "总层数",
        "竣工日期",
    }
    for field, value_tuple in dense_fields.items():
        if field in dense_override_fields or field not in value_map:
            value_map[field] = value_tuple
    fields, evidence, confidences = _build_evidence(value_map)

    matched_use_term = extract_real_estate_use_term(text)
    if matched_use_term:
        for field in ("使用期限", "土地使用期限", "use_term", "land_use_term"):
            fields[field] = matched_use_term
            confidences[field] = max(confidences.get(field, 0), 0.72)
            evidence[field] = {
                "value": matched_use_term,
                "evidence_text": matched_use_term,
                "page": None,
                "confidence": confidences[field],
            }

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
    for zh_key, aliases in EXTRA_FIELD_ALIASES.items():
        if zh_key not in fields:
            continue
        for alias in aliases:
            if alias not in fields:
                fields[alias] = fields[zh_key]
                evidence[alias] = {**evidence[zh_key], "value": fields[zh_key]}
                confidences[alias] = max(0.55, confidences[zh_key] - 0.02)
    if "房屋用途" in fields:
        for alias in ("use_type", "building_use"):
            if alias not in fields:
                fields[alias] = fields["房屋用途"]
                evidence[alias] = {**evidence["房屋用途"], "value": fields["房屋用途"]}
                confidences[alias] = max(0.55, confidences["房屋用途"] - 0.02)

    if "房产正面" in filename:
        use_term_debug = _use_term_candidate_debug(text)
        logger.info("[USE_TERM_DEBUG] filename=%s", filename)
        logger.info("[USE_TERM_DEBUG] raw_text_length=%s", len(text or ""))
        logger.info("[USE_TERM_DEBUG] raw_text_full=%s", text)
        logger.info("[USE_TERM_DEBUG] lines_count=%s", use_term_debug["lines_count"])
        logger.info("[USE_TERM_DEBUG] lines_with_期限=%s", use_term_debug["lines_with_期限"])
        logger.info("[USE_TERM_DEBUG] lines_with_2015=%s", use_term_debug["lines_with_2015"])
        logger.info("[USE_TERM_DEBUG] lines_with_2076=%s", use_term_debug["lines_with_2076"])
        logger.info("[USE_TERM_DEBUG] candidate_lines_around_use_term=%s", use_term_debug["candidate_lines_around_use_term"])
        logger.info("[USE_TERM_DEBUG] matched_use_term=%s", matched_use_term)
        logger.info("[USE_TERM_DEBUG] final_fields_keys=%s", list(fields.keys()))
        logger.info("[USE_TERM_DEBUG] final_使用期限=%s", fields.get("使用期限"))
        logger.info("[USE_TERM_DEBUG] final_use_term=%s", fields.get("use_term"))
        logger.info("[USE_TERM_DEBUG] final_land_use_term=%s", fields.get("land_use_term"))
    logger.info("[PropertyCertSkill][DEBUG] raw_text_contains_居住=%s", str("居住" in text).lower())
    logger.info("[PropertyCertSkill][DEBUG] raw_text contains 使用期限 = %s", str("使用期限" in text).lower())
    logger.info("[PropertyCertSkill][DEBUG] raw_text contains 2015年10月16日 = %s", str("2015年10月16日" in text).lower())
    logger.info("[PropertyCertSkill][DEBUG] raw_text contains 2076年12月28日 = %s", str("2076年12月28日" in text).lower())
    logger.info("[PropertyCertSkill][USE_TERM_DEBUG] lines around 使用期限=%s", _use_term_debug_lines(text))
    logger.info("[PropertyCertSkill][USE_TERM_DEBUG] matched_use_term=%s", matched_use_term)
    logger.info("[PropertyCertSkill] raw_text_preview=%s", raw_preview(text))
    logger.info("[PropertyCertSkill] contains_权利人=%s", str("权利人" in _dense_text(text)).lower())
    logger.info("[PropertyCertSkill] contains_不动产单元号=%s", str("不动产单元号" in _dense_text(text)).lower())
    logger.info("[PropertyCertSkill] contains_沃志方=%s", str("沃志方" in _dense_text(text)).lower())
    logger.info("[PropertyCertSkill] extracted_fields=%s", fields)
    logger.info("[PropertyCertSkill][DEBUG] fields_keys=%s", list(fields.keys()))
    logger.info("[PropertyCertSkill][DEBUG] 房屋用途=%s", fields.get("房屋用途"))
    logger.info("[PropertyCertSkill][DEBUG] house_use=%s", fields.get("house_use"))
    logger.info("[PropertyCertSkill][DEBUG] building_use=%s", fields.get("building_use"))
    logger.info("[PropertyCertSkill][DEBUG] use_type=%s", fields.get("use_type"))
    logger.info("[PropertyCertSkill][DEBUG] fields[使用期限]=%s", fields.get("使用期限"))
    logger.info("[PropertyCertSkill][DEBUG] fields[土地使用期限]=%s", fields.get("土地使用期限"))
    logger.info("[PropertyCertSkill][DEBUG] fields[land_use_term]=%s", fields.get("land_use_term"))
    logger.info("[PropertyCertSkill][DEBUG] fields[use_term]=%s", fields.get("use_term"))
    logger.info("[PropertyCertSkill][USE_TERM_DEBUG] fields 使用期限=%s", fields.get("使用期限"))
    logger.info("[PropertyCertSkill][USE_TERM_DEBUG] fields 土地使用期限=%s", fields.get("土地使用期限"))
    logger.info("[PropertyCertSkill][USE_TERM_DEBUG] fields use_term=%s", fields.get("use_term"))
    logger.info("[PropertyCertSkill][USE_TERM_DEBUG] fields land_use_term=%s", fields.get("land_use_term"))

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
    else:
        result["validation"]["warnings"].append("已识别为房产证/不动产权证，但字段页 OCR 未能提取关键字段，请检查扫描清晰度或人工确认。")
    return result


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    return _extract_property(payload, "property_cert")
