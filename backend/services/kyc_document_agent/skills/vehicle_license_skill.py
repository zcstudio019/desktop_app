from __future__ import annotations

import re
from datetime import date
from typing import Any

from backend.services.kyc_document_agent.evidence import raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


VEHICLE_LICENSE_FIELDS = (
    "plate_number",
    "vehicle_type",
    "owner",
    "address",
    "use_character",
    "brand_model",
    "vin",
    "engine_number",
    "registration_date",
    "issue_date",
    "approved_passengers",
    "total_mass",
    "curb_weight",
    "inspection_valid_until",
)

FIELD_STOPS = (
    "号牌号码",
    "Plate No",
    "车辆类型",
    "Vehicle Type",
    "所有人",
    "Owner",
    "住址",
    "Address",
    "使用性质",
    "Use Character",
    "品牌型号",
    "Model",
    "车辆识别代号",
    "VIN",
    "发动机号码",
    "Engine No",
    "注册日期",
    "Register Date",
    "发证日期",
    "Issue Date",
    "核定载人数",
    "总质量",
    "整备质量",
    "检验有效期止",
    "检验有效期至",
    "Ower",
    "Owener",
    "Addrss",
    "Addresss",
)

ALL_LABELS = (
    "号牌号码",
    "Plate No.",
    "Plate No",
    "车辆类型",
    "Vehicle Type",
    "所有人",
    "Owner",
    "Ower",
    "Owener",
    "住址",
    "Address",
    "Addrss",
    "Addresss",
    "使用性质",
    "Use Character",
    "USECHARACTER",
    "品牌型号",
    "Model",
    "车辆识别代号",
    "VIN",
    "发动机号码",
    "Engine No.",
    "Engine No",
    "ENGINENO",
    "注册日期",
    "Register Date",
    "发证日期",
    "Issue Date",
    "核定载人数",
    "总质量",
    "整备质量",
    "检验有效期止",
    "检验有效期至",
)

VEHICLE_TYPES = ("小型轿车", "小型普通客车", "小型汽车", "轻型货车", "大型汽车", "普通二轮摩托车")
USE_CHARACTERS = ("非营运", "出租客运", "营运", "货运", "租赁", "教练")


def normalize_ocr_text(text: str) -> tuple[str, str, list[str]]:
    normalized = str(text or "").replace("\u3000", " ").replace("：", ":")
    replacements = (
        (r"Plate\s*No\.?", "Plate No."),
        (r"Vehicle\s*Type", "Vehicle Type"),
        (r"Use\s*Character", "Use Character"),
        (r"Engine\s*No\.?", "Engine No."),
        (r"Register\s*Date", "Register Date"),
        (r"Issue\s*Date", "Issue Date"),
        (r"\bVin\b", "VIN"),
        (r"\bVIN\b", "VIN"),
        (r"\bOwer\b|\bOwener\b", "Owner"),
        (r"\bAddrss\b|\bAddresss\b", "Address"),
        (r"号\s*牌\s*号\s*码", "号牌号码"),
        (r"车\s*辆\s*类\s*型", "车辆类型"),
        (r"所\s*有\s*人", "所有人"),
        (r"住\s*址", "住址"),
        (r"使\s*用\s*性\s*质", "使用性质"),
        (r"品\s*牌\s*型\s*号", "品牌型号"),
        (r"车\s*辆\s*识\s*别\s*代\s*号", "车辆识别代号"),
        (r"发\s*动\s*机\s*号\s*码", "发动机号码"),
        (r"注\s*册\s*日\s*期", "注册日期"),
        (r"发\s*证\s*日\s*期", "发证日期"),
    )
    for pattern, replacement in replacements:
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
    for label in sorted(ALL_LABELS, key=len, reverse=True):
        normalized = re.sub(re.escape(label), f" {label} ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    lines = [line.strip(" :;；,，") for line in re.split(r"[\r\n]+", normalized) if line.strip(" :;；,，")]
    line_text = normalized
    compact_text = re.sub(r"\s+", "", line_text)
    return line_text, compact_text, lines


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip(" :：,，;；")


def _clean_code(value: Any) -> str:
    return re.sub(r"[\s:：]+", "", str(value or "")).strip(" ,，;；").upper()


def clean_vehicle_field_value(value: str) -> str:
    text = str(value or "")
    for label in sorted(ALL_LABELS, key=len, reverse=True):
        text = re.sub(re.escape(label), " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" :：,，;；")
    return text


def _date_to_iso(value: Any) -> str:
    text = str(value or "").strip()
    compact = re.sub(r"\s+", "", text)
    patterns = (
        r"(\d{4})年(\d{1,2})月(\d{1,2})日?",
        r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})",
        r"(\d{4})(\d{2})(\d{2})",
    )
    for pattern in patterns:
        match = re.search(pattern, compact)
        if not match:
            continue
        try:
            year, month, day = (int(part) for part in match.groups())
            date(year, month, day)
        except ValueError:
            return ""
        return f"{year:04d}-{month:02d}-{day:02d}"
    return ""


def extract_between_labels(
    text: str,
    labels: tuple[str, ...],
    stop_labels: tuple[str, ...] = FIELD_STOPS,
    max_chars: int = 100,
) -> tuple[str, str]:
    label_pattern = "|".join(re.escape(label) for label in sorted(labels, key=len, reverse=True))
    start_matches = list(re.finditer(label_pattern, text, flags=re.IGNORECASE))
    if not start_matches:
        return "", ""
    start_match = start_matches[0]
    value_start = start_match.end()
    value_end = min(len(text), value_start + max_chars)
    stop_candidates = [label for label in stop_labels if label.lower() not in {item.lower() for item in labels}]
    if stop_candidates:
        stop_pattern = "|".join(re.escape(label) for label in sorted(stop_candidates, key=len, reverse=True))
        stop_match = re.search(stop_pattern, text[value_start:], flags=re.IGNORECASE)
        if stop_match:
            value_end = min(value_end, value_start + stop_match.start())
    evidence_end = min(len(text), value_end + 40)
    raw_value = text[value_start:value_end]
    evidence = text[start_match.start() : evidence_end].strip()
    return clean_vehicle_field_value(raw_value), evidence


def _extract_label_value(
    line_text: str,
    compact_text: str,
    labels: tuple[str, ...],
    stop_labels: tuple[str, ...] = FIELD_STOPS,
    max_chars: int = 100,
) -> tuple[str, str]:
    value, evidence = extract_between_labels(line_text, labels, stop_labels, max_chars=max_chars)
    if value:
        return _clean_text(value), evidence
    return "", ""


def _extract_plate_number(line_text: str, compact_text: str) -> tuple[str, str]:
    value, evidence = _extract_label_value(line_text, compact_text, ("号牌号码", "Plate No.", "Plate No"), max_chars=32)
    candidate = _clean_code(value)
    match = re.search(r"[\u4e00-\u9fa5][A-Z][A-Z0-9挂学警港澳]{5,6}", candidate)
    if not match:
        match = re.search(r"[\u4e00-\u9fa5][A-Z][A-Z0-9挂学警港澳]{5,6}", compact_text.upper())
    return (_clean_code(match.group(0)), evidence or match.group(0)) if match else ("", "")


def _extract_date(line_text: str, compact_text: str, labels: tuple[str, ...]) -> tuple[str, str]:
    value, evidence = _extract_label_value(line_text, compact_text, labels, max_chars=40)
    date_value = _date_to_iso(value)
    if date_value:
        return date_value, evidence
    return "", ""


def _extract_vin(line_text: str, compact_text: str) -> tuple[str, str]:
    value, evidence = _extract_label_value(line_text, compact_text, ("车辆识别代号", "VIN"), max_chars=40)
    candidate = _clean_code(value)
    match = re.search(r"[A-HJ-NPR-Z0-9]{17}", candidate) or re.search(r"[A-HJ-NPR-Z0-9]{8,20}", candidate)
    if not match:
        match = re.search(r"[A-HJ-NPR-Z0-9]{17}", compact_text.upper())
    return (match.group(0), evidence) if match else ("", "")


def _extract_engine_number(line_text: str, compact_text: str) -> tuple[str, str]:
    value, evidence = _extract_label_value(line_text, compact_text, ("发动机号码", "Engine No.", "Engine No"), max_chars=40)
    candidate = _clean_code(value)
    match = re.search(r"[A-Z0-9-]{4,30}", candidate)
    return (match.group(0), evidence) if match else ("", "")


def _pick_known_value(value: str, choices: tuple[str, ...]) -> str:
    text = clean_vehicle_field_value(value)
    for choice in choices:
        if choice in text:
            return choice
    return _clean_text(text)


def _extract_dates_from_text(text: str) -> list[str]:
    values: list[str] = []
    patterns = (
        r"\d{4}年\d{1,2}月\d{1,2}日?",
        r"\d{4}[./-]\d{1,2}[./-]\d{1,2}",
        r"\d{4}\s+\d{1,2}\s+\d{1,2}",
        r"\d{8}",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            value = _date_to_iso(match.group(0))
            if value and value not in values:
                values.append(value)
    return values


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
    line_text, compact_text, _lines = normalize_ocr_text(raw_text)

    plate_number, plate_evidence = _extract_plate_number(line_text, compact_text)
    vehicle_type_raw, vehicle_type_evidence = _extract_label_value(
        line_text,
        compact_text,
        ("车辆类型", "Vehicle Type"),
        ("所有人", "Owner", "住址", "Address", "使用性质", "Use Character", "品牌型号", "Model", "车辆识别代号", "VIN", "发动机号码", "Engine No.", "Engine No", "注册日期", "Register Date", "发证日期", "Issue Date"),
        max_chars=40,
    )
    vehicle_type = _pick_known_value(vehicle_type_raw, VEHICLE_TYPES) or next((item for item in VEHICLE_TYPES if item in line_text), "")
    owner, owner_evidence = _extract_label_value(
        line_text,
        compact_text,
        ("所有人", "Owner", "Ower", "Owener"),
        ("住址", "Address", "使用性质", "Use Character", "品牌型号", "Model", "车辆识别代号", "VIN", "发动机号码", "Engine No.", "Engine No", "注册日期", "Register Date", "发证日期", "Issue Date"),
        max_chars=80,
    )
    address, address_evidence = _extract_label_value(
        line_text,
        compact_text,
        ("住址", "Address", "Addrss", "Addresss"),
        ("使用性质", "Use Character", "品牌型号", "Model", "车辆识别代号", "VIN", "发动机号码", "Engine No.", "Engine No", "注册日期", "Register Date", "发证日期", "Issue Date"),
        max_chars=160,
    )
    use_raw, use_evidence = _extract_label_value(
        line_text,
        compact_text,
        ("使用性质", "Use Character", "USECHARACTER"),
        ("品牌型号", "Model", "车辆识别代号", "VIN", "发动机号码", "Engine No.", "Engine No", "注册日期", "Register Date", "发证日期", "Issue Date"),
        max_chars=40,
    )
    use_character = _pick_known_value(use_raw, USE_CHARACTERS) or next((item for item in USE_CHARACTERS if item in line_text), "")
    brand_model, brand_evidence = _extract_label_value(
        line_text,
        compact_text,
        ("品牌型号", "Model"),
        ("车辆识别代号", "VIN", "发动机号码", "Engine No.", "Engine No", "注册日期", "Register Date", "发证日期", "Issue Date", "使用性质", "Use Character", "USECHARACTER"),
        max_chars=80,
    )
    brand_model = re.sub(r"(?:非营运|营运|货运|出租客运|租赁|教练)$", "", brand_model).strip()
    vin, vin_evidence = _extract_vin(line_text, compact_text)
    engine_number, engine_evidence = _extract_engine_number(line_text, compact_text)
    registration_date, registration_evidence = _extract_date(line_text, compact_text, ("注册日期", "Register Date"))
    issue_date, issue_evidence = _extract_date(line_text, compact_text, ("发证日期", "Issue Date"))
    all_dates = _extract_dates_from_text(line_text)
    if not registration_date and all_dates:
        registration_date = all_dates[0]
        registration_evidence = all_dates[0]
    if not issue_date and len(all_dates) >= 2:
        issue_date = all_dates[1]
        issue_evidence = all_dates[1]
    approved_passengers, passengers_evidence = _extract_label_value(line_text, compact_text, ("核定载人数",), max_chars=20)
    total_mass, total_mass_evidence = _extract_label_value(line_text, compact_text, ("总质量",), max_chars=24)
    curb_weight, curb_weight_evidence = _extract_label_value(line_text, compact_text, ("整备质量",), max_chars=24)
    inspection_valid_until, inspection_evidence = _extract_date(line_text, compact_text, ("检验有效期止", "检验有效期至"))

    fields, evidence, confidences = _build_maps(
        {
            "plate_number": _field(plate_number, plate_evidence, 0.88),
            "vehicle_type": _field(vehicle_type, vehicle_type_evidence, 0.82),
            "owner": _field(owner, owner_evidence, 0.82),
            "address": _field(address, address_evidence, 0.76),
            "use_character": _field(use_character, use_evidence, 0.82),
            "brand_model": _field(brand_model, brand_evidence, 0.82),
            "vin": _field(vin, vin_evidence, 0.88),
            "engine_number": _field(engine_number, engine_evidence, 0.86),
            "registration_date": _field(registration_date, registration_evidence, 0.84),
            "issue_date": _field(issue_date, issue_evidence, 0.84),
            "approved_passengers": _field(approved_passengers, passengers_evidence, 0.72),
            "total_mass": _field(total_mass, total_mass_evidence, 0.72),
            "curb_weight": _field(curb_weight, curb_weight_evidence, 0.72),
            "inspection_valid_until": _field(inspection_valid_until, inspection_evidence, 0.72),
        }
    )
    result = build_result("vehicle_license", fields, evidence)
    result["doc_type_name"] = "行驶证"
    result["confidence"]["fields"] = confidences
    result["confidence"]["overall"] = round(sum(confidences.values()) / len(confidences), 4) if confidences else 0.0
    result["raw_text_preview"] = raw_preview(line_text)
    return result
