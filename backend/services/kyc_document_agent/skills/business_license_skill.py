from __future__ import annotations

import re
from datetime import date
from typing import Any

from backend.services.kyc_document_agent.evidence import raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


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
    keyword_pattern = "|".join(re.escape(keyword) for keyword in AUTHORITY_KEYWORDS)

    def clean_candidate(value: str) -> str:
        text = _compact(value)
        if not text:
            return ""
        text = text.replace("登记机关", "").replace("发照日期", "")
        text = re.sub(r"(?:19|20)\d{2}年\d{1,2}月\d{1,2}日", "", text)
        text = re.sub(r"(?:19|20)\d{2}[-./]\d{1,2}[-./]\d{1,2}", "", text)
        text = re.sub(r"(?:19|20)\d{6}", "", text)
        text = re.split(r"二维码|国家企业信用信息公示系统|经营范围|住所|住 所|法定代表人|注册资本|成立日期|营业期限", text, maxsplit=1)[0]
        text = text.strip(" :：,，.。;；()（）[]【】")
        match = re.search(rf"([\u4e00-\u9fa5]{{2,40}}(?:{keyword_pattern}))", text)
        if not match:
            return ""
        authority = match.group(1).strip(" :：,，.。;；")
        if authority in {"登记机关", "未识别", "发照日期"} or _is_date_text(authority):
            return ""
        if len(re.findall(r"[\u4e00-\u9fff]", authority)) < 6:
            return ""
        return authority

    # 1) 优先读取“登记机关”标签后方或后续相邻行。
    for index, line in enumerate(lines):
        if "登记机关" not in _compact(line):
            continue
        evidence_lines = [line]
        suffix = re.split(r"登记\s*机\s*关\s*:?", line, maxsplit=1)[-1]
        candidates = [suffix]
        for next_line in lines[index + 1 : index + 4]:
            compact_next = _compact(next_line)
            if not compact_next or _is_bottom_noise(next_line):
                evidence_lines.append(next_line)
                break
            if any(stop in compact_next for stop in ("经营范围", "住所", "住 所", "发照日期", "注册资本", "成立日期")):
                break
            evidence_lines.append(next_line)
            candidates.append(next_line)
            if any(keyword in compact_next for keyword in AUTHORITY_KEYWORDS):
                break
        for candidate in candidates + ["".join(candidates)]:
            authority = clean_candidate(candidate)
            if authority:
                return authority, "\n".join(evidence_lines)

    # 2) 扫描红章 OCR 行；如机构名被拆行，合并上一行地区名称。
    for index in range(len(lines) - 1, -1, -1):
        line = lines[index]
        compact_line = _compact(line)
        if not any(keyword in compact_line for keyword in AUTHORITY_KEYWORDS):
            continue
        evidence_lines = [line]
        candidates = [line]
        if index > 0:
            previous = lines[index - 1]
            compact_previous = _compact(previous)
            if (
                compact_previous
                and not _is_date_text(compact_previous)
                and not any(label in compact_previous for label in FIELD_STOPS)
                and re.fullmatch(r"[\u4e00-\u9fa5]{2,20}", compact_previous)
            ):
                evidence_lines.insert(0, previous)
                candidates.insert(0, previous + line)
        for candidate in candidates:
            authority = clean_candidate(candidate)
            if authority:
                return authority, "\n".join(evidence_lines)

    # 3) 极端 OCR 无换行时，从全文紧凑文本中兜底抓机构名。
    authority = clean_candidate(compact_text)
    return (authority, authority) if authority else ("", "")


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
    return result
