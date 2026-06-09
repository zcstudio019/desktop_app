from __future__ import annotations

import re
from typing import Any

from backend.services.kyc_document_agent.evidence import raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


ID_CARD_FIELDS = (
    "name",
    "gender",
    "ethnicity",
    "birth_date",
    "address",
    "id_number",
    "issuing_authority",
    "valid_from",
    "valid_to",
)


def _clean_text(text: str) -> str:
    return (text or "").replace("\u3000", " ")


def _compact_spaces(text: str) -> str:
    return re.sub(r"[ \t]+", " ", str(text or "")).strip(" :：,，;；")


def _lines(text: str) -> list[str]:
    return [_compact_spaces(line) for line in re.split(r"[\r\n]+", _clean_text(text)) if _compact_spaces(line)]


def _dense(text: str) -> str:
    return re.sub(r"\s+", "", _clean_text(text or ""))


def _date_to_iso(value: str) -> str:
    text = _dense(value)
    match = re.search(r"(\d{4})年?(\d{1,2})月?(\d{1,2})日?", text)
    if not match:
        match = re.search(r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})", text)
    if not match:
        match = re.search(r"(\d{4})(\d{2})(\d{2})", text)
    if not match:
        return _compact_spaces(value)
    year, month, day = (int(part) for part in match.groups())
    return f"{year:04d}-{month:02d}-{day:02d}"


def _extract_after_label(text: str, labels: tuple[str, ...], stop_labels: tuple[str, ...] = ()) -> tuple[str, str]:
    source_lines = _lines(text)
    label_pattern = "|".join(re.escape(label) for label in labels)
    stop_pattern = "|".join(re.escape(label) for label in stop_labels)
    for index, line in enumerate(source_lines):
        match = re.search(label_pattern, line)
        if not match:
            continue
        value = _compact_spaces(line[match.end():])
        if value:
            if stop_pattern:
                value = re.split(stop_pattern, value, maxsplit=1)[0]
            return _compact_spaces(value), line
        for next_line in source_lines[index + 1 : index + 4]:
            if stop_pattern and re.search(stop_pattern, next_line):
                break
            if next_line:
                return _compact_spaces(next_line), f"{line}\n{next_line}"
    dense_text = _dense(text)
    dense_labels = tuple(_dense(label) for label in labels)
    dense_stops = tuple(_dense(label) for label in stop_labels)
    for label in dense_labels:
        start = dense_text.find(label)
        if start < 0:
            continue
        value_start = start + len(label)
        value_end = min(len(dense_text), value_start + 120)
        for stop in dense_stops:
            stop_index = dense_text.find(stop, value_start)
            if stop_index >= 0:
                value_end = min(value_end, stop_index)
        value = dense_text[value_start:value_end]
        if value:
            return _compact_spaces(value), dense_text[start:value_end]
    return "", ""


def _extract_name(text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(text, ("姓名", "姓 名"), ("性别", "民族", "出生", "住址", "公民身份号码"))
    match = re.search(r"[\u4e00-\u9fa5·]{2,20}", value)
    return (match.group(0), evidence) if match else ("", "")


def _extract_gender(text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(text, ("性别", "性 别"), ("民族", "出生", "住址", "公民身份号码"))
    match = re.search(r"[男女]", value)
    return (match.group(0), evidence) if match else ("", "")


def _extract_ethnicity(text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(text, ("民族", "民 族"), ("出生", "住址", "公民身份号码"))
    match = re.search(r"[\u4e00-\u9fa5]{1,10}", value)
    if not match:
        return "", ""
    return match.group(0).replace("族", ""), evidence


def _extract_birth_date(text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(text, ("出生", "出生日期"), ("住址", "公民身份号码"))
    match = re.search(r"\d{4}\s*年?\s*\d{1,2}\s*月?\s*\d{1,2}\s*日?|\d{8}|\d{4}[./-]\d{1,2}[./-]\d{1,2}", value)
    return (_date_to_iso(match.group(0)), evidence) if match else ("", "")


def _extract_address(text: str) -> tuple[str, str]:
    source_lines = _lines(text)
    address_parts: list[str] = []
    evidence_lines: list[str] = []
    collecting = False
    for line in source_lines:
        if re.search(r"住\s*址", line):
            collecting = True
            value = re.sub(r"^.*?住\s*址[:：]?", "", line)
        elif collecting:
            value = line
        else:
            continue

        if re.search(r"公民\s*身份\s*号码|身份证号码|签发机关|有效期限", value):
            value = re.split(r"公民\s*身份\s*号码|身份证号码|签发机关|有效期限", value, maxsplit=1)[0]
            if value:
                address_parts.append(value)
                evidence_lines.append(line)
            break
        address_parts.append(value)
        evidence_lines.append(line)

    address = re.sub(r"\s+", "", "".join(address_parts))
    address = re.sub(r"[：:]+$", "", address)
    address = re.sub(r"[1-9]\d{16}[\dXx].*$", "", address)
    return address, "\n".join(evidence_lines)


def _extract_id_number(text: str) -> tuple[str, str]:
    patterns = [
        r"(?:公民\s*身份\s*号码|身份证号码|身份号码)\s*[:：]?\s*([1-9]\d[\d\s]{15,20}[\dXx])",
        r"\b([1-9]\d{16}[\dXx])\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text or "")
        if not match:
            continue
        value = re.sub(r"\s+", "", match.group(1)).upper()
        if re.fullmatch(r"[1-9]\d{16}[\dX]", value):
            return value, match.group(0)
    return "", ""


def _extract_issuing_authority(text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(text, ("签发机关", "签 发 机 关"), ("有效期限",))
    value = re.split(r"有效期限", value, maxsplit=1)[0]
    return _compact_spaces(value), evidence


def _extract_valid_period(text: str) -> tuple[str, str, str]:
    patterns = [
        r"有效期限\s*[:：]?\s*([0-9]{4}[./-][0-9]{1,2}[./-][0-9]{1,2}|[0-9]{8}|[0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日?)\s*(?:-|至|—|~)\s*(长期|[0-9]{4}[./-][0-9]{1,2}[./-][0-9]{1,2}|[0-9]{8}|[0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日?)",
        r"有效\s*期限\s*[:：]?\s*([0-9]{4}[./-][0-9]{1,2}[./-][0-9]{1,2}|[0-9]{8}|[0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日?)\s*(?:-|至|—|~)\s*(长期|[0-9]{4}[./-][0-9]{1,2}[./-][0-9]{1,2}|[0-9]{8}|[0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text or "")
        if match:
            valid_from = _date_to_iso(match.group(1))
            valid_to = "长期" if match.group(2) == "长期" else _date_to_iso(match.group(2))
            return valid_from, valid_to, match.group(0)
    value, evidence = _extract_after_label(text, ("有效期限", "有效 期限"))
    if "长期" in value:
        date_match = re.search(r"\d{4}[./-]\d{1,2}[./-]\d{1,2}|\d{8}|\d{4}年\d{1,2}月\d{1,2}日?", value)
        return (_date_to_iso(date_match.group(0)) if date_match else "", "长期", evidence)
    return "", "", ""


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
    text = _clean_text(data["text"])

    name, name_evidence = _extract_name(text)
    gender, gender_evidence = _extract_gender(text)
    ethnicity, ethnicity_evidence = _extract_ethnicity(text)
    birth_date, birth_evidence = _extract_birth_date(text)
    address, address_evidence = _extract_address(text)
    id_number, id_evidence = _extract_id_number(text)
    issuing_authority, authority_evidence = _extract_issuing_authority(text)
    valid_from, valid_to, valid_evidence = _extract_valid_period(text)

    fields, evidence, confidences = _build_maps(
        {
            "name": _field(name, name_evidence, 0.86),
            "gender": _field(gender, gender_evidence, 0.84),
            "ethnicity": _field(ethnicity, ethnicity_evidence, 0.8),
            "birth_date": _field(birth_date, birth_evidence, 0.84),
            "address": _field(address, address_evidence, 0.76),
            "id_number": _field(id_number, id_evidence, 0.9),
            "issuing_authority": _field(issuing_authority, authority_evidence, 0.78),
            "valid_from": _field(valid_from, valid_evidence, 0.78),
            "valid_to": _field(valid_to, valid_evidence, 0.78),
        }
    )
    result = build_result("id_card", fields, evidence)
    result["confidence"]["fields"] = confidences
    result["confidence"]["overall"] = round(sum(confidences.values()) / len(confidences), 4) if confidences else 0.0
    result["raw_text_preview"] = raw_preview(text)
    return result
