from __future__ import annotations

import re
from typing import Any

from backend.services.kyc_document_agent.evidence import build_field_maps, first_match, raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    data = normalize_input(payload)
    text = data["text"]
    id_number, id_evidence = first_match(text, [r"公民身份号码\s*[:：]?\s*([1-9]\d{16}[\dXx])", r"\b([1-9]\d{16}[\dXx])\b"])
    name, name_evidence = first_match(text, [r"姓名\s*[:：]?\s*([\u4e00-\u9fa5·]{2,20})"])
    gender, gender_evidence = first_match(text, [r"性别\s*[:：]?\s*([男女])"])
    ethnicity, ethnicity_evidence = first_match(text, [r"民族\s*[:：]?\s*([\u4e00-\u9fa5]{1,10})"])
    birth_date, birth_evidence = first_match(
        text,
        [r"出生\s*[:：]?\s*(\d{4}[年./-]?\d{1,2}[月./-]?\d{1,2}日?)", r"出生日期\s*[:：]?\s*(\d{8})"],
    )
    address, address_evidence = first_match(text, [r"住址\s*[:：]?\s*([^\n]+)"])
    issuing_authority, authority_evidence = first_match(text, [r"签发机关\s*[:：]?\s*([^\n]+)"])
    valid_from, valid_from_evidence = first_match(text, [r"有效期限\s*[:：]?\s*(\d{4}[年./-]?\d{1,2}[月./-]?\d{1,2}日?)"])
    valid_to = ""
    valid_to_evidence = ""
    valid_range = re.search(
        r"有效期限\s*[:：]?\s*\d{4}[年./-]?\d{1,2}[月./-]?\d{1,2}日?\s*[-至]\s*(长期|\d{4}[年./-]?\d{1,2}[月./-]?\d{1,2}日?)",
        text,
    )
    if valid_range:
        valid_to = valid_range.group(1)
        valid_to_evidence = valid_range.group(0)

    fields, evidence, confidences = build_field_maps(
        text,
        {
            "name": (name, name_evidence, 0.86),
            "gender": (gender, gender_evidence, 0.84),
            "ethnicity": (ethnicity, ethnicity_evidence, 0.78),
            "birth_date": (birth_date, birth_evidence, 0.83),
            "address": (address, address_evidence, 0.74),
            "id_number": (id_number.upper(), id_evidence, 0.9),
            "issuing_authority": (issuing_authority, authority_evidence, 0.78),
            "valid_from": (valid_from, valid_from_evidence, 0.76),
            "valid_to": (valid_to, valid_to_evidence, 0.76),
        },
    )
    result = build_result("id_card", fields, evidence)
    result["confidence"]["fields"] = confidences
    result["confidence"]["overall"] = round(sum(confidences.values()) / len(confidences), 4)
    result["raw_text_preview"] = raw_preview(text)
    return result
