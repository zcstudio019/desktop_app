from __future__ import annotations

from typing import Any

from backend.services.kyc_document_agent.evidence import build_field_maps, first_match, raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    data = normalize_input(payload)
    text = data["text"]
    holder_name, holder_evidence = first_match(text, [r"(?:持证人|姓名)\s*[:：]?\s*([\u4e00-\u9fa5·]{2,20})"])
    spouse_name, spouse_evidence = first_match(text, [r"(?:配偶|另一方|女方|男方)姓名\s*[:：]?\s*([\u4e00-\u9fa5·]{2,20})", r"配偶\s*[:：]?\s*([\u4e00-\u9fa5·]{2,20})"])
    ids = [match.group(1).upper() for match in __import__("re").finditer(r"([1-9]\d{16}[\dXx])", text)]
    holder_id = ids[0] if ids else ""
    spouse_id = ids[1] if len(ids) > 1 else ""
    holder_id_evidence = holder_id
    spouse_id_evidence = spouse_id
    registration_date, registration_evidence = first_match(text, [r"登记日期\s*[:：]?\s*(\d{4}年\d{1,2}月\d{1,2}日|\d{4}[-./]\d{1,2}[-./]\d{1,2})"])
    authority, authority_evidence = first_match(text, [r"(?:登记机关|发证机关)\s*[:：]?\s*([^\n]+)"])
    cert_number, cert_evidence = first_match(text, [r"(?:结婚证字号|证字号|证书编号)\s*[:：]?\s*([^\n]+)"])
    fields, evidence, confidences = build_field_maps(
        text,
        {
            "holder_name": (holder_name, holder_evidence, 0.78),
            "spouse_name": (spouse_name, spouse_evidence, 0.76),
            "holder_id_number": (holder_id, holder_id_evidence, 0.78),
            "spouse_id_number": (spouse_id, spouse_id_evidence, 0.78),
            "registration_date": (registration_date, registration_evidence, 0.82),
            "issuing_authority": (authority, authority_evidence, 0.76),
            "certificate_number": (cert_number, cert_evidence, 0.72),
        },
    )
    result = build_result("marriage_cert", fields, evidence)
    result["confidence"]["fields"] = confidences
    result["confidence"]["overall"] = round(sum(confidences.values()) / len(confidences), 4)
    result["raw_text_preview"] = raw_preview(text)
    return result
