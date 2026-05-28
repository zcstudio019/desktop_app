from __future__ import annotations

from typing import Any

from backend.services.kyc_document_agent.evidence import build_field_maps, first_match, line_after_keyword, raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    data = normalize_input(payload)
    text = data["text"]
    company_name, company_evidence = line_after_keyword(text, ["名称", "企业名称"])
    credit_code, code_evidence = first_match(text, [r"统一社会信用代码\s*[:：]?\s*([0-9A-Z]{18})"])
    legal, legal_evidence = first_match(text, [r"法定代表人\s*[:：]?\s*([\u4e00-\u9fa5·]{2,20})", r"负责人\s*[:：]?\s*([\u4e00-\u9fa5·]{2,20})"])
    registered_capital, capital_evidence = first_match(text, [r"注册资本\s*[:：]?\s*([^\n]+)"])
    company_type, type_evidence = first_match(text, [r"类型\s*[:：]?\s*([^\n]+)"])
    establishment_date, establishment_evidence = first_match(text, [r"成立日期\s*[:：]?\s*(\d{4}年\d{1,2}月\d{1,2}日|\d{4}[-./]\d{1,2}[-./]\d{1,2})"])
    business_term, term_evidence = first_match(text, [r"营业期限\s*[:：]?\s*([^\n]+)"])
    registered_address, address_evidence = first_match(text, [r"住所\s*[:：]?\s*([^\n]+)", r"注册地址\s*[:：]?\s*([^\n]+)"])
    business_scope, scope_evidence = first_match(text, [r"经营范围\s*[:：]?\s*([\s\S]{1,260})"])
    registration_authority, authority_evidence = first_match(text, [r"登记机关\s*[:：]?\s*([^\n]+)"])
    issue_date, issue_evidence = first_match(text, [r"发照日期\s*[:：]?\s*(\d{4}年\d{1,2}月\d{1,2}日|\d{4}[-./]\d{1,2}[-./]\d{1,2})"])
    fields, evidence, confidences = build_field_maps(
        text,
        {
            "company_name": (company_name, company_evidence, 0.84),
            "unified_social_credit_code": (credit_code, code_evidence, 0.9),
            "legal_representative": (legal, legal_evidence, 0.84),
            "registered_capital": (registered_capital, capital_evidence, 0.78),
            "company_type": (company_type, type_evidence, 0.74),
            "establishment_date": (establishment_date, establishment_evidence, 0.8),
            "business_term": (business_term, term_evidence, 0.72),
            "registered_address": (registered_address, address_evidence, 0.76),
            "business_scope": (business_scope, scope_evidence, 0.66),
            "registration_authority": (registration_authority, authority_evidence, 0.72),
            "issue_date": (issue_date, issue_evidence, 0.78),
        },
    )
    result = build_result("business_license", fields, evidence)
    result["confidence"]["fields"] = confidences
    result["confidence"]["overall"] = round(sum(confidences.values()) / len(confidences), 4)
    result["raw_text_preview"] = raw_preview(text)
    return result
