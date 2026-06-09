from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any

from backend.services.kyc_document_agent.evidence import raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


logger = logging.getLogger(__name__)

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

FIELD_STOPS = (
    "姓名",
    "性别",
    "民族",
    "出生",
    "住址",
    "公民身份号码",
    "身份号码",
    "身份证号码",
    "签发机关",
    "有效期限",
    "有效期",
    "居民身份证",
    "注意事项",
)


def _mask_id_numbers(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        value = re.sub(r"\s+", "", match.group(0)).upper()
        if len(value) != 18:
            return match.group(0)
        return f"{value[:6]}********{value[-4:]}"

    return re.sub(r"[1-9]\d[\d\s]{15,20}[\dXx]", repl, str(text or ""))


def normalize_ocr_text(text: str) -> tuple[str, str, list[str]]:
    normalized = str(text or "").replace("\u3000", " ").replace("：", ":")
    replacements = {
        "公民身份号": "公民身份号码",
        "身份证号": "公民身份号码",
        "身份证号码": "公民身份号码",
        "身份号码": "公民身份号码",
        "有效期": "有效期限",
        "签发机关注": "签发机关",
        "民 族": "民族",
        "姓 名": "姓名",
        "性 别": "性别",
        "出 生": "出生",
        "住 址": "住址",
        "签 发 机 关": "签发机关",
        "有 效 期 限": "有效期限",
        "公 民 身 份 号 码": "公民身份号码",
    }
    for old, new in replacements.items():
        normalized = normalized.replace(old, new)
    normalized = normalized.replace("公民公民身份号码码", "公民身份号码")
    normalized = normalized.replace("公民身份号码码", "公民身份号码")
    normalized = normalized.replace("公民公民身份号码", "公民身份号码")

    lines: list[str] = []
    for raw_line in re.split(r"[\r\n]+", normalized):
        line = re.sub(r"[ \t]+", " ", raw_line).strip(" :;；,，")
        if line:
            lines.append(line)
    line_text = "\n".join(lines)
    compact_text = re.sub(r"\s+", "", line_text)
    return line_text, compact_text, lines


def _date_to_iso(value: str) -> str:
    text = str(value or "").strip()
    compact = re.sub(r"\s+", "", text).replace("：", ":")
    patterns = [
        r"(\d{4})年(\d{1,2})月(\d{1,2})日?",
        r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})",
        r"(\d{4})(\d{2})(\d{2})",
        r"(\d{4})\s+(\d{1,2})\s+(\d{1,2})",
    ]
    for pattern in patterns:
        match = re.search(pattern, compact if r"\s+" not in pattern else text)
        if not match:
            continue
        try:
            year, month, day = (int(part) for part in match.groups())
            date(year, month, day)
        except ValueError:
            return ""
        return f"{year:04d}-{month:02d}-{day:02d}"
    return ""


def _clean_value(value: str) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip(" :;；,，")


def _strip_stop_labels(value: str, stop_labels: tuple[str, ...] = FIELD_STOPS) -> str:
    if not value:
        return ""
    stop_pattern = "|".join(re.escape(label) for label in stop_labels)
    return re.split(stop_pattern, value, maxsplit=1)[0]


def _extract_label_value(
    line_text: str,
    compact_text: str,
    labels: tuple[str, ...],
    stop_labels: tuple[str, ...] = FIELD_STOPS,
    max_chars: int = 80,
) -> tuple[str, str]:
    lines = line_text.splitlines()
    label_pattern = "|".join(re.escape(label) for label in labels)
    stop_pattern = "|".join(re.escape(label) for label in stop_labels)
    for index, line in enumerate(lines):
        match = re.search(label_pattern, line)
        if not match:
            continue
        value = line[match.end() :]
        if stop_pattern:
            value = re.split(stop_pattern, value, maxsplit=1)[0]
        if _clean_value(value):
            return _clean_value(value), line
        for next_line in lines[index + 1 : index + 4]:
            if stop_pattern and re.search(stop_pattern, next_line):
                break
            if _clean_value(next_line):
                return _clean_value(next_line), f"{line}\n{next_line}"

    dense_labels = tuple(re.sub(r"\s+", "", label) for label in labels)
    dense_stops = tuple(re.sub(r"\s+", "", label) for label in stop_labels)
    for label in dense_labels:
        start = compact_text.find(label)
        if start < 0:
            continue
        value_start = start + len(label)
        value_end = min(len(compact_text), value_start + max_chars)
        for stop in dense_stops:
            stop_index = compact_text.find(stop, value_start)
            if stop_index >= 0:
                value_end = min(value_end, stop_index)
        value = compact_text[value_start:value_end]
        if value:
            return _clean_value(value), compact_text[start:value_end]
    return "", ""


def _extract_name(line_text: str, compact_text: str) -> tuple[str, str]:
    value, evidence = _extract_label_value(
        line_text,
        compact_text,
        ("姓名", "姓 名"),
        ("性别", "民族", "出生", "住址", "公民身份号码", "签发机关", "有效期限"),
        max_chars=36,
    )
    value = _strip_stop_labels(value)
    match = re.search(r"[\u4e00-\u9fa5A-Za-z·]{2,30}", value)
    return (match.group(0), evidence) if match else ("", "")


def _extract_gender(line_text: str, compact_text: str) -> tuple[str, str]:
    value, evidence = _extract_label_value(
        line_text,
        compact_text,
        ("性别", "性 别"),
        ("民族", "出生", "住址", "公民身份号码", "签发机关", "有效期限"),
        max_chars=8,
    )
    match = re.search(r"[男女]", value)
    return (match.group(0), evidence) if match else ("", "")


def _extract_ethnicity(line_text: str, compact_text: str) -> tuple[str, str]:
    value, evidence = _extract_label_value(
        line_text,
        compact_text,
        ("民族", "民 族"),
        ("出生", "住址", "公民身份号码", "签发机关", "有效期限"),
        max_chars=20,
    )
    match = re.search(r"[\u4e00-\u9fa5]{1,10}", value)
    if not match:
        return "", ""
    return match.group(0).replace("族", ""), evidence


def _extract_birth_date(line_text: str, compact_text: str) -> tuple[str, str]:
    value, evidence = _extract_label_value(
        line_text,
        compact_text,
        ("出生", "出生日期", "出 生"),
        ("住址", "公民身份号码", "签发机关", "有效期限"),
        max_chars=40,
    )
    candidates = [
        r"\d{4}年\d{1,2}月\d{1,2}日?",
        r"\d{4}[./-]\d{1,2}[./-]\d{1,2}",
        r"\d{4}\s+\d{1,2}\s+\d{1,2}",
        r"\d{8}",
    ]
    for pattern in candidates:
        match = re.search(pattern, value)
        if match:
            return _date_to_iso(match.group(0)), evidence
    return "", ""


def _extract_address(line_text: str, compact_text: str) -> tuple[str, str]:
    lines = line_text.splitlines()
    parts: list[str] = []
    evidence_lines: list[str] = []
    collecting = False
    stop_pattern = r"公民身份号码|身份号码|身份证号码|签发机关|有效期限|有效期"
    for line in lines:
        if not collecting:
            match = re.search(r"住址|住\s*址", line)
            if not match:
                continue
            collecting = True
            value = line[match.end() :]
        else:
            value = line
        if re.search(stop_pattern, value):
            value = re.split(stop_pattern, value, maxsplit=1)[0]
            if value:
                parts.append(value)
                evidence_lines.append(line)
            break
        if value:
            parts.append(value)
            evidence_lines.append(line)

    if parts:
        address = _clean_value("".join(parts))
        evidence = "\n".join(evidence_lines)
    else:
        address, evidence = _extract_label_value(
            line_text,
            compact_text,
            ("住址", "住 址"),
            ("公民身份号码", "身份号码", "身份证号码", "签发机关", "有效期限", "有效期"),
            max_chars=160,
        )
    address = re.sub(r"[1-9]\d{16}[\dXx].*$", "", address)
    address = _strip_stop_labels(address, ("公民身份号码", "签发机关", "有效期限", "有效期"))
    return _clean_value(address), evidence


def _extract_id_number(line_text: str, compact_text: str) -> tuple[str, str]:
    labeled_patterns = [
        r"(?:公民身份号码|身份号码|身份证号码)[:\s]*([1-9]\d[\d\s]{15,20}[\dXx])",
        r"(?:公民身份号码|身份号码|身份证号码)([1-9]\d{16}[\dXx])",
    ]
    for source in (line_text, compact_text):
        for pattern in labeled_patterns:
            match = re.search(pattern, source)
            if not match:
                continue
            value = re.sub(r"\s+", "", match.group(1)).upper()
            if re.fullmatch(r"[1-9]\d{16}[\dX]", value):
                return value, match.group(0)

    match = re.search(r"(?<!\d)([1-9]\d{16}[\dXx])(?!\d)", compact_text)
    if match:
        return match.group(1).upper(), match.group(0)
    return "", ""


def _extract_issuing_authority(line_text: str, compact_text: str) -> tuple[str, str]:
    value, evidence = _extract_label_value(
        line_text,
        compact_text,
        ("签发机关", "签 发 机 关"),
        ("有效期限", "有效期", "居民身份证", "注意事项"),
        max_chars=80,
    )
    return _clean_value(value), evidence


def _extract_valid_period(line_text: str, compact_text: str) -> tuple[str, str, str]:
    candidates: list[tuple[str, str]] = []
    for line in line_text.splitlines():
        if "有效期限" in line or "有效期" in line:
            candidates.append((line, line))
    if not candidates:
        idx = compact_text.find("有效期限")
        if idx < 0:
            idx = compact_text.find("有效期")
        if idx >= 0:
            candidates.append((compact_text[idx : idx + 80], compact_text[idx : idx + 80]))
    candidates.append((compact_text, compact_text))

    date = r"(?:\d{4}[./-]\d{1,2}[./-]\d{1,2}|\d{8}|\d{4}年\d{1,2}月\d{1,2}日?)"
    sep = r"(?:-|至|到|~|—|－)"
    for candidate, evidence in candidates:
        dense = re.sub(r"\s+", "", candidate).replace("：", ":")
        match = re.search(rf"({date}){sep}(长期|{date})", dense)
        if not match:
            continue
        valid_from = _date_to_iso(match.group(1))
        valid_to = "长期" if match.group(2) == "长期" else _date_to_iso(match.group(2))
        if valid_from and valid_to:
            return valid_from, valid_to, evidence
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


def _log_match(field: str, value: Any) -> None:
    display = _mask_id_numbers(str(value or ""))
    logger.info("[IDCardSkill][MATCH] field=%s value=%s", field, display)


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    data = normalize_input(payload)
    raw_text = str(data.get("text") or "")
    line_text, compact_text, lines = normalize_ocr_text(raw_text)

    logger.info("[IDCardSkill][RAW_TEXT] %s", _mask_id_numbers(line_text[:1000]))
    logger.info("[IDCardSkill][LINES] %s", [_mask_id_numbers(line) for line in lines])

    name, name_evidence = _extract_name(line_text, compact_text)
    gender, gender_evidence = _extract_gender(line_text, compact_text)
    ethnicity, ethnicity_evidence = _extract_ethnicity(line_text, compact_text)
    birth_date, birth_evidence = _extract_birth_date(line_text, compact_text)
    address, address_evidence = _extract_address(line_text, compact_text)
    id_number, id_evidence = _extract_id_number(line_text, compact_text)
    issuing_authority, authority_evidence = _extract_issuing_authority(line_text, compact_text)
    valid_from, valid_to, valid_evidence = _extract_valid_period(line_text, compact_text)

    for field, value in {
        "name": name,
        "gender": gender,
        "ethnicity": ethnicity,
        "birth_date": birth_date,
        "address": address,
        "id_number": id_number,
        "issuing_authority": issuing_authority,
        "valid_from": valid_from,
        "valid_to": valid_to,
    }.items():
        _log_match(field, value)

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
    result["raw_text_preview"] = raw_preview(line_text)
    logger.info("[IDCardSkill][FIELDS] %s", {key: _mask_id_numbers(str(value)) for key, value in fields.items()})
    logger.info("[IDCardSkill][MISSING] %s", [field for field in ID_CARD_FIELDS if not fields.get(field)])
    return result
