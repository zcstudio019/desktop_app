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
)


def normalize_ocr_text(text: str) -> tuple[str, str, list[str]]:
    normalized = str(text or "").replace("\u3000", " ").replace("：", ":")
    replacements = {
        "PlateNo.": "Plate No.",
        "PlateNo": "Plate No",
        "VehicleType": "Vehicle Type",
        "UseCharacter": "Use Character",
        "EngineNo.": "Engine No.",
        "EngineNo": "Engine No",
        "RegisterDate": "Register Date",
        "IssueDate": "Issue Date",
        "号 牌 号 码": "号牌号码",
        "车 辆 类 型": "车辆类型",
        "所 有 人": "所有人",
        "住 址": "住址",
        "使 用 性 质": "使用性质",
        "品 牌 型 号": "品牌型号",
        "车 辆 识 别 代 号": "车辆识别代号",
        "发 动 机 号 码": "发动机号码",
        "注 册 日 期": "注册日期",
        "发 证 日 期": "发证日期",
    }
    for old, new in replacements.items():
        normalized = normalized.replace(old, new)

    lines: list[str] = []
    for raw_line in re.split(r"[\r\n]+", normalized):
        line = re.sub(r"[ \t]+", " ", raw_line).strip(" :;；,，")
        if line:
            lines.append(line)
    line_text = "\n".join(lines)
    compact_text = re.sub(r"\s+", "", line_text)
    return line_text, compact_text, lines


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip(" :：,，;；")


def _clean_code(value: Any) -> str:
    return re.sub(r"[\s:：]+", "", str(value or "")).strip(" ,，;；").upper()


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


def _extract_label_value(
    line_text: str,
    compact_text: str,
    labels: tuple[str, ...],
    stop_labels: tuple[str, ...] = FIELD_STOPS,
    max_chars: int = 100,
) -> tuple[str, str]:
    label_pattern = "|".join(re.escape(label) for label in labels)
    stop_pattern = "|".join(re.escape(label) for label in stop_labels if label not in labels)
    lines = line_text.splitlines()
    for index, line in enumerate(lines):
        match = re.search(label_pattern, line, flags=re.IGNORECASE)
        if not match:
            continue
        value = line[match.end() :]
        if stop_pattern:
            value = re.split(stop_pattern, value, maxsplit=1, flags=re.IGNORECASE)[0]
        if _clean_text(value):
            return _clean_text(value), line
        parts: list[str] = []
        evidence = [line]
        for next_line in lines[index + 1 : index + 4]:
            if stop_pattern and re.search(stop_pattern, next_line, flags=re.IGNORECASE):
                break
            if _clean_text(next_line):
                parts.append(next_line)
                evidence.append(next_line)
        if parts:
            return _clean_text("".join(parts)), "\n".join(evidence)

    dense_labels = tuple(re.sub(r"\s+", "", label) for label in labels)
    dense_stops = tuple(re.sub(r"\s+", "", label) for label in stop_labels if label not in labels)
    dense_upper = compact_text.upper()
    for label in dense_labels:
        start = dense_upper.find(label.upper())
        if start < 0:
            continue
        value_start = start + len(label)
        value_end = min(len(compact_text), value_start + max_chars)
        for stop in dense_stops:
            stop_index = dense_upper.find(stop.upper(), value_start)
            if stop_index >= 0:
                value_end = min(value_end, stop_index)
        return _clean_text(compact_text[value_start:value_end]), compact_text[start:value_end]
    return "", ""


def _extract_plate_number(line_text: str, compact_text: str) -> tuple[str, str]:
    value, evidence = _extract_label_value(line_text, compact_text, ("号牌号码", "Plate No.", "Plate No"), max_chars=24)
    match = re.search(r"[\u4e00-\u9fa5][A-Za-z][A-Za-z0-9挂学警港澳]{5,6}", value or compact_text)
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
    match = re.search(r"[A-HJ-NPR-Z0-9]{8,20}", candidate)
    return (match.group(0), evidence) if match else ("", "")


def _extract_engine_number(line_text: str, compact_text: str) -> tuple[str, str]:
    value, evidence = _extract_label_value(line_text, compact_text, ("发动机号码", "Engine No.", "Engine No"), max_chars=40)
    candidate = _clean_code(value)
    match = re.search(r"[A-Z0-9-]{4,30}", candidate)
    return (match.group(0), evidence) if match else ("", "")


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
    vehicle_type, vehicle_type_evidence = _extract_label_value(line_text, compact_text, ("车辆类型", "Vehicle Type"), max_chars=40)
    owner, owner_evidence = _extract_label_value(line_text, compact_text, ("所有人", "Owner"), ("住址", "Address", *FIELD_STOPS), max_chars=80)
    address, address_evidence = _extract_label_value(
        line_text,
        compact_text,
        ("住址", "Address"),
        ("使用性质", "Use Character", "品牌型号", "Model", "车辆识别代号", "VIN", "发动机号码", "Engine No", "注册日期", "Register Date", "发证日期", "Issue Date"),
        max_chars=160,
    )
    use_character, use_evidence = _extract_label_value(line_text, compact_text, ("使用性质", "Use Character"), max_chars=40)
    brand_model, brand_evidence = _extract_label_value(line_text, compact_text, ("品牌型号", "Model"), ("车辆识别代号", "VIN", "发动机号码", "Engine No", "注册日期", "发证日期"), max_chars=60)
    vin, vin_evidence = _extract_vin(line_text, compact_text)
    engine_number, engine_evidence = _extract_engine_number(line_text, compact_text)
    registration_date, registration_evidence = _extract_date(line_text, compact_text, ("注册日期", "Register Date"))
    issue_date, issue_evidence = _extract_date(line_text, compact_text, ("发证日期", "Issue Date"))
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
