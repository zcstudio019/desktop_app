from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any

from backend.services.kyc_document_agent.evidence import raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


logger = logging.getLogger(__name__)

BUSINESS_LICENSE_FIELDS = (
    "unified_social_credit_code",
    "license_number",
    "company_name",
    "company_type",
    "legal_representative",
    "registered_capital",
    "establishment_date",
    "business_term",
    "registered_address",
    "business_scope",
    "registration_authority",
    "issue_date",
)

AUTHORITY_KEYWORDS = (
    "市场监督管理局",
    "行政审批局",
    "工商行政管理局",
    "工商行政管理部门",
    "市场监督管理部门",
)
AUTHORITY_PARTIAL_KEYWORDS = (
    "监督管理局",
    "管理局",
    "行政审批局",
    "工商行政管理局",
    "工商行政管理部门",
    "市场监督管理部门",
)

FIELD_STOPS = (
    "统一社会信用代码",
    "社会信用代码",
    "证照编号",
    "名称",
    "名 称",
    "类型",
    "类 型",
    "法定代表人",
    "负责人",
    "经营者",
    "注册资本",
    "成立日期",
    "营业期限",
    "经营期限",
    "营业期限自",
    "住所",
    "住 所",
    "经营范围",
    "经 营 范 围",
    "登记机关",
    "发照日期",
)


def normalize_ocr_text(text: str) -> tuple[str, str, list[str]]:
    normalized = str(text or "").replace("\u3000", " ").replace("：", ":")
    lines: list[str] = []
    for raw_line in re.split(r"[\r\n]+", normalized):
        line = re.sub(r"[ \t]+", " ", raw_line).strip(" :;；")
        if line:
            lines.append(line)
    line_text = "\n".join(lines)
    compact_text = re.sub(r"\s+", "", line_text)
    return line_text, compact_text, lines


def _compact(value: str) -> str:
    return re.sub(r"\s+", "", str(value or ""))


def _label_pattern(labels: tuple[str, ...] | list[str]) -> str:
    variants: list[str] = []
    for label in labels:
        chars = [re.escape(char) for char in _compact(label)]
        if chars:
            variants.append(r"\s*".join(chars))
    return "|".join(variants)


def _clean_inline_value(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip(" :：,，;；")


def _clean_compact_value(value: str) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip(" :：,，;；")


def _split_at_stop(value: str, stops: tuple[str, ...]) -> str:
    if not value:
        return ""
    stop_pattern = _label_pattern(stops)
    if not stop_pattern:
        return value
    return re.split(stop_pattern, value, maxsplit=1)[0]


def _extract_label_value(
    line_text: str,
    compact_text: str,
    lines: list[str],
    labels: tuple[str, ...],
    stops: tuple[str, ...] = FIELD_STOPS,
    *,
    multiline: bool = False,
    max_chars: int = 180,
) -> tuple[str, str]:
    label_pattern = _label_pattern(labels)
    stop_pattern = _label_pattern(stops)

    for index, line in enumerate(lines):
        match = re.search(label_pattern, line)
        if not match:
            continue
        value = line[match.end() :]
        value = _split_at_stop(value, stops)
        parts: list[str] = []
        evidence_lines = [line]
        if _clean_inline_value(value):
            parts.append(value)
        if multiline:
            for next_line in lines[index + 1 :]:
                if stop_pattern and re.search(stop_pattern, next_line):
                    break
                if _is_bottom_noise(next_line):
                    break
                parts.append(next_line)
                evidence_lines.append(next_line)
        elif not parts:
            for next_line in lines[index + 1 : index + 4]:
                if stop_pattern and re.search(stop_pattern, next_line):
                    break
                if _clean_inline_value(next_line):
                    parts.append(next_line)
                    evidence_lines.append(next_line)
                    break
        value_text = _clean_inline_value("".join(parts) if multiline else " ".join(parts))
        if value_text:
            return value_text[:max_chars], "\n".join(evidence_lines)

    for label in tuple(_compact(item) for item in labels):
        start = compact_text.find(label)
        if start < 0:
            continue
        value_start = start + len(label)
        value_end = min(len(compact_text), value_start + max_chars)
        for stop in tuple(_compact(item) for item in stops):
            if stop == label:
                continue
            stop_index = compact_text.find(stop, value_start)
            if stop_index >= 0:
                value_end = min(value_end, stop_index)
        value = _clean_compact_value(compact_text[value_start:value_end])
        if value:
            return value, compact_text[start:value_end]
    return "", ""


def _is_bottom_noise(line: str) -> bool:
    compact = _compact(line)
    if not compact:
        return True
    noise_keywords = ("二维码", "国家企业信用信息公示系统", "扫描", "网址", "市场主体应当")
    if any(keyword in compact for keyword in noise_keywords):
        return True
    return bool(re.fullmatch(r"\d{4}年\d{1,2}月\d{1,2}日", compact))


def _is_date_text(value: str) -> bool:
    compact = _compact(value)
    return bool(
        re.fullmatch(r"(?:19|20)\d{2}年\d{1,2}月\d{1,2}日", compact)
        or re.fullmatch(r"(?:19|20)\d{2}[-./]\d{1,2}[-./]\d{1,2}", compact)
        or re.fullmatch(r"(?:19|20)\d{6}", compact)
    )


def _date_to_iso(value: str) -> str:
    text = str(value or "").strip()
    compact = _compact(text)
    patterns = (
        r"(\d{4})年(\d{1,2})月(\d{1,2})日",
        r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})",
        r"(\d{4})(\d{2})(\d{2})",
        r"(\d{4})\s+年\s*(\d{1,2})\s+月\s*(\d{1,2})\s+日",
    )
    for pattern in patterns:
        source = text if r"\s+" in pattern else compact
        match = re.search(pattern, source)
        if not match:
            continue
        try:
            year, month, day = (int(part) for part in match.groups())
            date(year, month, day)
        except ValueError:
            return ""
        return f"{year:04d}-{month:02d}-{day:02d}"
    return ""


def clean_registration_authority(value: str) -> str:
    keyword_pattern = "|".join(re.escape(keyword) for keyword in AUTHORITY_KEYWORDS)
    text = _compact(value)
    if not text:
        return ""
    text = text.replace("登记机关", "").replace("发照日期", "")
    text = re.sub(r"(?:19|20)\d{2}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日", "", text)
    text = re.sub(r"(?:19|20)\d{2}[-./]\d{1,2}[-./]\d{1,2}", "", text)
    text = re.sub(r"(?:19|20)\d{6}", "", text)
    text = re.sub(r"年\s*月\s*日", "", text)
    text = re.split(
        r"二维码|国家企业信用信息公示系统|扫码|扫描|网址|经营范围|住所|住 所|法定代表人|注册资本|成立日期|营业期限|营业执照",
        text,
        maxsplit=1,
    )[0]
    text = text.strip(" :：,，.。;；()（）[]【】")
    match = re.search(rf"([\u4e00-\u9fa5]{{2,50}}(?:{keyword_pattern}))", text)
    if not match:
        return ""
    authority = match.group(1).strip(" :：,，.。;；")
    forbidden = {"登记机关", "未识别", "发照日期", "经营范围", "住所", "营业执照", "法定代表人"}
    if authority in forbidden or _is_date_text(authority):
        return ""
    if len(re.findall(r"[\u4e00-\u9fff]", authority)) < 8:
        return ""
    return authority


def normalize_authority_from_stamp_ocr(text: str) -> str:
    _, compact_text, lines = normalize_ocr_text(text)
    candidates: list[str] = []
    for index, line in enumerate(lines):
        compact_line = _compact(line)
        if any(keyword in compact_line for keyword in AUTHORITY_KEYWORDS):
            candidates.append(line)
        if not any(keyword in compact_line for keyword in ("市场监督", "监督管理", "管理局", "行政审批局")):
            continue
        start = max(0, index - 2)
        end = min(len(lines), index + 3)
        for left in range(start, index + 1):
            for right in range(index + 1, end + 1):
                merged = "".join(lines[left:right])
                if any(fragment in _compact(merged) for fragment in ("市场监督管理局", "行政审批局", "工商行政管理局")):
                    candidates.append(merged)

    if compact_text:
        candidates.append(compact_text)

    cleaned = [clean_registration_authority(candidate) for candidate in candidates]
    cleaned = [candidate for candidate in cleaned if candidate]
    if not cleaned:
        return ""
    cleaned.sort(key=lambda value: (("上海" in value) + ("市" in value) + ("区" in value), len(value)), reverse=True)
    return cleaned[0]


def _extract_credit_code(line_text: str, compact_text: str, lines: list[str]) -> tuple[str, str]:
    value, evidence = _extract_label_value(
        line_text,
        compact_text,
        lines,
        ("统一社会信用代码", "社会信用代码"),
        ("证照编号", "名称", "类型", "法定代表人", "注册资本", "成立日期"),
        max_chars=36,
    )
    match = re.search(r"[0-9A-Z]{12,24}", _compact(value).upper())
    if match:
        return match.group(0), evidence
    match = re.search(r"(?:统一社会信用代码|社会信用代码)\s*[:：]?\s*([0-9A-Z\s]{12,30})", line_text)
    return (_compact(match.group(1)).upper(), match.group(0)) if match else ("", "")


def _extract_license_number(line_text: str, compact_text: str, lines: list[str]) -> tuple[str, str]:
    value, evidence = _extract_label_value(
        line_text,
        compact_text,
        lines,
        ("证照编号",),
        ("统一社会信用代码", "名称", "类型", "法定代表人", "注册资本", "成立日期"),
        max_chars=40,
    )
    match = re.search(r"[0-9A-Z-]{8,40}", _compact(value).upper())
    return (match.group(0), evidence) if match else ("", "")


def _extract_registered_address(line_text: str, compact_text: str, lines: list[str]) -> tuple[str, str]:
    value, evidence = _extract_label_value(
        line_text,
        compact_text,
        lines,
        ("住所", "住 所", "注册地址"),
        ("经营范围", "经 营 范 围", "营业期限", "经营期限", "登记机关", "发照日期"),
        multiline=True,
        max_chars=240,
    )
    return _clean_compact_value(value), evidence


def _extract_business_scope(line_text: str, compact_text: str, lines: list[str]) -> tuple[str, str]:
    value, evidence = _extract_label_value(
        line_text,
        compact_text,
        lines,
        ("经营范围", "经 营 范 围"),
        ("登记机关", "发照日期"),
        multiline=True,
        max_chars=1200,
    )
    value = re.sub(r"\s+", "", value).strip(" :：,，;；")
    value = re.split(r"(?:登记机关|发照日期|二维码|国家企业信用信息公示系统)", value, maxsplit=1)[0]
    return value, evidence


def _extract_registration_authority(line_text: str, compact_text: str, lines: list[str]) -> tuple[str, str]:
    candidates: list[dict[str, str]] = []

    def add_candidate(source: str, value: str, evidence_text: str) -> tuple[str, str] | None:
        cleaned = clean_registration_authority(value)
        candidates.append({
            "source": source,
            "raw": _clean_inline_value(value),
            "cleaned": cleaned,
        })
        logger.info("[BusinessLicenseSkill] authority candidate source=%s raw=%s cleaned=%s", source, value, cleaned)
        if cleaned:
            logger.info("[BusinessLicenseSkill] selected registration_authority: %s source=%s", cleaned, source)
            return cleaned, evidence_text or value
        return None

    # 优先级 1：登记机关同行。
    same_line_pattern = re.compile(
        r"登记\s*机\s*关\s*[:：]?\s*([^\n\r]*?(?:"
        + "|".join(re.escape(keyword) for keyword in AUTHORITY_KEYWORDS)
        + r"))"
    )
    for match in same_line_pattern.finditer(line_text):
        selected = add_candidate("label_same_line", match.group(1), match.group(0))
        if selected:
            logger.info("[BusinessLicenseSkill] authority candidates: %s", candidates)
            return selected

    # 优先级 2：登记机关下一行 1-3 行。
    for index, line in enumerate(lines):
        if "登记机关" not in _compact(line):
            continue
        for offset, next_line in enumerate(lines[index + 1 : index + 4], start=1):
            if _is_date_text(next_line):
                add_candidate(f"label_next_line_{offset}", next_line, "\n".join(lines[index : index + offset + 1]))
                continue
            if any(keyword in _compact(next_line) for keyword in AUTHORITY_KEYWORDS):
                selected = add_candidate(f"label_next_line_{offset}", next_line, "\n".join(lines[index : index + offset + 1]))
                if selected:
                    logger.info("[BusinessLicenseSkill] authority candidates: %s", candidates)
                    return selected

    # 优先级 3：全文扫描完整机关名称行。
    for index in range(len(lines) - 1, -1, -1):
        line = lines[index]
        if not any(keyword in _compact(line) for keyword in AUTHORITY_KEYWORDS):
            continue
        selected = add_candidate("full_text_line_scan", line, line)
        if selected:
            logger.info("[BusinessLicenseSkill] authority candidates: %s", candidates)
            return selected

    stamp_merged = normalize_authority_from_stamp_ocr(line_text)
    if stamp_merged:
        selected = add_candidate("stamp_ocr_multiline_merge", stamp_merged, stamp_merged)
        if selected:
            logger.info("[BusinessLicenseSkill] authority candidates: %s", candidates)
            return selected

    # 优先级 4：红章文字拆行合并，支持 “上海市长宁区/市场监督管理局” 和 “上海市长宁区市场/监督管理局”。
    for index, line in enumerate(lines):
        compact_line = _compact(line)
        if not any(keyword in compact_line for keyword in AUTHORITY_PARTIAL_KEYWORDS):
            continue
        neighbor_values: list[tuple[str, str]] = []
        if index > 0:
            neighbor_values.append(("previous_current", lines[index - 1] + line))
        if index + 1 < len(lines):
            neighbor_values.append(("current_next", line + lines[index + 1]))
        if index > 0 and index + 1 < len(lines):
            neighbor_values.append(("previous_current_next", lines[index - 1] + line + lines[index + 1]))
        for source, value in neighbor_values:
            selected = add_candidate(f"split_line_{source}", value, value)
            if selected:
                logger.info("[BusinessLicenseSkill] authority candidates: %s", candidates)
                return selected

    # 无换行或混排文本兜底。
    selected = add_candidate("compact_text_scan", compact_text, compact_text)
    logger.info("[BusinessLicenseSkill] authority candidates: %s", candidates)
    return selected if selected else ("", "")


def _extract_issue_date(line_text: str, compact_text: str, lines: list[str], establishment_date: str) -> tuple[str, str]:
    value, evidence = _extract_label_value(
        line_text,
        compact_text,
        lines,
        ("发照日期",),
        ("登记机关", "经营范围", "营业期限"),
        max_chars=40,
    )
    iso = _date_to_iso(value)
    if iso:
        return iso, evidence

    candidates: list[tuple[str, str]] = []
    for line in lines:
        for match in re.finditer(r"(?:19|20)\d{2}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日|(?:19|20)\d{2}[-./]\d{1,2}[-./]\d{1,2}", line):
            iso_value = _date_to_iso(match.group(0))
            if iso_value:
                candidates.append((iso_value, line))
    if not candidates:
        return "", ""
    non_establishment = [item for item in candidates if item[0] != establishment_date]
    return (non_establishment or candidates)[-1]


def _field(value: Any, evidence_text: str, confidence: float) -> tuple[Any, str, float]:
    return value, evidence_text, confidence


def _build_maps(value_map: dict[str, tuple[Any, str, float]]) -> tuple[dict[str, Any], dict[str, Any], dict[str, float]]:
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


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    data = normalize_input(payload)
    raw_text = str(data.get("text") or "")
    line_text, compact_text, lines = normalize_ocr_text(raw_text)
    logger.info("[BusinessLicenseSkill] raw_text preview=%s", raw_text[:3000])
    logger.info(
        "[business_license] full_text_has_exact_authority=%s full_text_has_authority_keyword=%s full_text_has_label=%s",
        str("上海市长宁区市场监督管理局" in compact_text).lower(),
        str(any(keyword in compact_text for keyword in AUTHORITY_KEYWORDS)).lower(),
        str("登记机关" in compact_text).lower(),
    )
    for keyword in ("市场监督管理局", "行政审批局", "工商行政管理局", "登记机关"):
        logger.info(
            "[BusinessLicenseSkill] raw_text contains %s: %s",
            keyword,
            str(keyword in compact_text).lower(),
        )

    credit_code, code_evidence = _extract_credit_code(line_text, compact_text, lines)
    license_number, license_evidence = _extract_license_number(line_text, compact_text, lines)
    company_name, company_evidence = _extract_label_value(
        line_text,
        compact_text,
        lines,
        ("名称", "名 称"),
        ("类型", "类 型", "法定代表人", "负责人", "经营者", "注册资本", "成立日期"),
        max_chars=100,
    )
    company_type, type_evidence = _extract_label_value(
        line_text,
        compact_text,
        lines,
        ("类型", "类 型"),
        ("法定代表人", "负责人", "经营者", "注册资本", "成立日期"),
        max_chars=100,
    )
    legal, legal_evidence = _extract_label_value(
        line_text,
        compact_text,
        lines,
        ("法定代表人", "负责人", "经营者"),
        ("注册资本", "成立日期", "营业期限", "住所", "经营范围"),
        max_chars=40,
    )
    registered_capital, capital_evidence = _extract_label_value(
        line_text,
        compact_text,
        lines,
        ("注册资本",),
        ("成立日期", "营业期限", "住所", "经营范围", "登记机关"),
        max_chars=80,
    )
    establishment_raw, establishment_evidence = _extract_label_value(
        line_text,
        compact_text,
        lines,
        ("成立日期",),
        ("营业期限", "经营期限", "住所", "经营范围", "登记机关"),
        max_chars=40,
    )
    establishment_date = _date_to_iso(establishment_raw) or establishment_raw
    business_term, term_evidence = _extract_label_value(
        line_text,
        compact_text,
        lines,
        ("营业期限", "经营期限", "营业期限自"),
        ("住所", "经营范围", "登记机关", "发照日期"),
        max_chars=120,
    )
    registered_address, address_evidence = _extract_registered_address(line_text, compact_text, lines)
    business_scope, scope_evidence = _extract_business_scope(line_text, compact_text, lines)
    registration_authority, authority_evidence = _extract_registration_authority(line_text, compact_text, lines)
    issue_date, issue_evidence = _extract_issue_date(line_text, compact_text, lines, establishment_date)

    fields, evidence, confidences = _build_maps(
        {
            "unified_social_credit_code": _field(credit_code, code_evidence, 0.9),
            "license_number": _field(license_number, license_evidence, 0.86),
            "company_name": _field(_clean_inline_value(company_name), company_evidence, 0.86),
            "company_type": _field(_clean_inline_value(company_type), type_evidence, 0.78),
            "legal_representative": _field(_clean_inline_value(legal), legal_evidence, 0.84),
            "registered_capital": _field(_clean_inline_value(registered_capital), capital_evidence, 0.8),
            "establishment_date": _field(establishment_date, establishment_evidence, 0.82),
            "business_term": _field(_clean_inline_value(business_term), term_evidence, 0.74),
            "registered_address": _field(registered_address, address_evidence, 0.78),
            "business_scope": _field(business_scope, scope_evidence, 0.72),
            "registration_authority": _field(registration_authority, authority_evidence, 0.76),
            "issue_date": _field(issue_date, issue_evidence, 0.78),
        }
    )
    result = build_result("business_license", fields, evidence)
    result["confidence"]["fields"] = confidences
    result["confidence"]["overall"] = round(sum(confidences.values()) / len(confidences), 4) if confidences else 0.0
    result["raw_text_preview"] = raw_preview(line_text)
    if not registration_authority and not any(keyword in compact_text for keyword in AUTHORITY_KEYWORDS):
        result["validation"]["warnings"].append("OCR 未识别到红章登记机关区域，请在人工审核中补录登记机关")
        logger.warning("[BusinessLicenseSkill] registration_authority empty and raw_text has no authority keyword; seal-region OCR fallback should be checked")
    logger.info("[BusinessLicenseSkill] final fields.registration_authority=%s", fields.get("registration_authority") or "")
    return result
