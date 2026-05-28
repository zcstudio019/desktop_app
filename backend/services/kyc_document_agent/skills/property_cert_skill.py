from __future__ import annotations

from typing import Any

from backend.services.kyc_document_agent.evidence import build_field_maps, first_match, raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


def _extract_property(payload: dict[str, Any] | str, doc_type: str) -> dict[str, Any]:
    data = normalize_input(payload)
    text = data["text"]
    owner, owner_evidence = first_match(text, [r"(?:权利人|房屋所有权人|所有权人)\s*[:：]?\s*([^\n]+)"])
    co_owners, co_owner_evidence = first_match(text, [r"(?:共有情况|共有人)\s*[:：]?\s*([^\n]+)"])
    cert_number, cert_evidence = first_match(text, [r"(?:证号|证书编号|房权证字第)\s*[:：]?\s*([^\n]+)"])
    unit_number, unit_evidence = first_match(text, [r"不动产单元号\s*[:：]?\s*([A-Z0-9\u4e00-\u9fa5-]+)"])
    address, address_evidence = first_match(text, [r"(?:坐落|房屋坐落|不动产坐落)\s*[:：]?\s*([^\n]+)"])
    right_type, right_type_evidence = first_match(text, [r"权利类型\s*[:：]?\s*([^\n]+)"])
    right_nature, right_nature_evidence = first_match(text, [r"权利性质\s*[:：]?\s*([^\n]+)"])
    use_type, use_type_evidence = first_match(text, [r"(?:用途|规划用途)\s*[:：]?\s*([^\n]+)"])
    building_area, building_evidence = first_match(text, [r"(?:建筑面积|房屋建筑面积)\s*[:：]?\s*([\d.]+\s*(?:平方米|㎡)?)"])
    land_area, land_evidence = first_match(text, [r"(?:土地面积|宗地面积)\s*[:：]?\s*([\d.]+\s*(?:平方米|㎡)?)"])
    total_area, total_evidence = first_match(text, [r"(?:总面积|面积)\s*[:：]?\s*([\d.]+\s*(?:平方米|㎡)?)"])
    mortgage_status, mortgage_evidence = first_match(text, [r"(?:抵押情况|抵押状态)\s*[:：]?\s*([^\n]+)"])
    seizure_status, seizure_evidence = first_match(text, [r"(?:查封情况|查封状态)\s*[:：]?\s*([^\n]+)"])
    issue_date, issue_evidence = first_match(text, [r"(?:登记时间|填发日期|发证日期)\s*[:：]?\s*(\d{4}年\d{1,2}月\d{1,2}日|\d{4}[-./]\d{1,2}[-./]\d{1,2})"])
    fields, evidence, confidences = build_field_maps(
        text,
        {
            "owner": (owner, owner_evidence, 0.84),
            "co_owners": (co_owners, co_owner_evidence, 0.66),
            "certificate_number": (cert_number, cert_evidence, 0.78),
            "property_unit_number": (unit_number, unit_evidence, 0.82),
            "property_address": (address, address_evidence, 0.82),
            "right_type": (right_type, right_type_evidence, 0.74),
            "right_nature": (right_nature, right_nature_evidence, 0.74),
            "use_type": (use_type, use_type_evidence, 0.72),
            "building_area": (building_area, building_evidence, 0.8),
            "land_area": (land_area, land_evidence, 0.72),
            "total_area": (total_area, total_evidence, 0.7),
            "mortgage_status": (mortgage_status, mortgage_evidence, 0.62),
            "seizure_status": (seizure_status, seizure_evidence, 0.62),
            "issue_date": (issue_date, issue_evidence, 0.76),
        },
    )
    result = build_result(doc_type, fields, evidence)
    result["confidence"]["fields"] = confidences
    result["confidence"]["overall"] = round(sum(confidences.values()) / len(confidences), 4)
    result["raw_text_preview"] = raw_preview(text)
    return result


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    return _extract_property(payload, "property_cert")
