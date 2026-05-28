from __future__ import annotations

from typing import Any

from backend.services.kyc_document_agent.evidence import build_field_maps, first_match, raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    data = normalize_input(payload)
    text = data["text"]
    plate, plate_evidence = first_match(text, [r"号牌号码\s*[:：]?\s*([^\s\n]+)"])
    owner, owner_evidence = first_match(text, [r"所有人\s*[:：]?\s*([^\n]+)"])
    address, address_evidence = first_match(text, [r"住址\s*[:：]?\s*([^\n]+)"])
    vehicle_type, vehicle_type_evidence = first_match(text, [r"车辆类型\s*[:：]?\s*([^\n]+)"])
    use_character, use_evidence = first_match(text, [r"使用性质\s*[:：]?\s*([^\n]+)"])
    brand_model, brand_evidence = first_match(text, [r"品牌型号\s*[:：]?\s*([^\n]+)"])
    vin, vin_evidence = first_match(text, [r"(?:车辆识别代号|车架号)\s*[:：]?\s*([A-Z0-9]{8,25})"])
    engine, engine_evidence = first_match(text, [r"发动机号码\s*[:：]?\s*([A-Z0-9-]+)"])
    registration_date, registration_evidence = first_match(text, [r"注册日期\s*[:：]?\s*(\d{4}年\d{1,2}月\d{1,2}日|\d{4}[-./]\d{1,2}[-./]\d{1,2})"])
    issue_date, issue_evidence = first_match(text, [r"发证日期\s*[:：]?\s*(\d{4}年\d{1,2}月\d{1,2}日|\d{4}[-./]\d{1,2}[-./]\d{1,2})"])
    passengers, passengers_evidence = first_match(text, [r"核定载人数\s*[:：]?\s*([^\n]+)"])
    total_mass, total_mass_evidence = first_match(text, [r"总质量\s*[:：]?\s*([^\n]+)"])
    curb_weight, curb_evidence = first_match(text, [r"整备质量\s*[:：]?\s*([^\n]+)"])
    inspection, inspection_evidence = first_match(text, [r"检验有效期至\s*[:：]?\s*(\d{4}年\d{1,2}月|\d{4}[-./]\d{1,2})"])
    fields, evidence, confidences = build_field_maps(
        text,
        {
            "plate_number": (plate, plate_evidence, 0.86),
            "vehicle_owner": (owner, owner_evidence, 0.82),
            "address": (address, address_evidence, 0.74),
            "vehicle_type": (vehicle_type, vehicle_type_evidence, 0.78),
            "use_character": (use_character, use_evidence, 0.78),
            "brand_model": (brand_model, brand_evidence, 0.78),
            "vehicle_identification_number": (vin, vin_evidence, 0.88),
            "engine_number": (engine, engine_evidence, 0.8),
            "registration_date": (registration_date, registration_evidence, 0.78),
            "issue_date": (issue_date, issue_evidence, 0.78),
            "approved_passengers": (passengers, passengers_evidence, 0.7),
            "total_mass": (total_mass, total_mass_evidence, 0.68),
            "curb_weight": (curb_weight, curb_evidence, 0.68),
            "inspection_valid_until": (inspection, inspection_evidence, 0.66),
        },
    )
    result = build_result("vehicle_license", fields, evidence)
    result["confidence"]["fields"] = confidences
    result["confidence"]["overall"] = round(sum(confidences.values()) / len(confidences), 4)
    result["raw_text_preview"] = raw_preview(text)
    return result
