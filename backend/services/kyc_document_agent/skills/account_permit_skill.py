from __future__ import annotations

from typing import Any

from backend.services.kyc_document_agent.evidence import build_field_maps, first_match, raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


def _extract_account(payload: dict[str, Any] | str, doc_type: str) -> dict[str, Any]:
    data = normalize_input(payload)
    text = data["text"]
    company_name, company_evidence = first_match(text, [r"(?:存款人名称|单位名称|企业名称)\s*[:：]?\s*([^\n]+)"])
    account_name, account_name_evidence = first_match(text, [r"(?:账户名称|户名)\s*[:：]?\s*([^\n]+)"])
    account_number, account_number_evidence = first_match(text, [r"(?:账号|银行账号|基本存款账户编号)\s*[:：]?\s*([0-9 ]{8,32})"])
    opening_bank, bank_evidence = first_match(text, [r"(?:开户银行|开户行|开户许可证开户行)\s*[:：]?\s*([^\n]+)"])
    account_type, type_evidence = first_match(text, [r"账户性质\s*[:：]?\s*([^\n]+)", r"账户类型\s*[:：]?\s*([^\n]+)"])
    approval_number, approval_evidence = first_match(text, [r"(?:核准号|开户许可证核准号)\s*[:：]?\s*([A-Z0-9-]+)"])
    legal, legal_evidence = first_match(text, [r"法定代表人\s*[:：]?\s*([\u4e00-\u9fa5·]{2,20})"])
    issue_date, issue_evidence = first_match(text, [r"(?:发证日期|打印日期|开户日期)\s*[:：]?\s*(\d{4}年\d{1,2}月\d{1,2}日|\d{4}[-./]\d{1,2}[-./]\d{1,2})"])
    account_status, status_evidence = first_match(text, [r"账户状态\s*[:：]?\s*([^\n]+)"])
    fields, evidence, confidences = build_field_maps(
        text,
        {
            "company_name": (company_name, company_evidence, 0.8),
            "bank_account_name": (account_name or company_name, account_name_evidence or company_evidence, 0.76),
            "bank_account_number": (account_number.replace(" ", ""), account_number_evidence, 0.86),
            "opening_bank": (opening_bank, bank_evidence, 0.82),
            "account_type": (account_type, type_evidence, 0.74),
            "approval_number": (approval_number, approval_evidence, 0.78),
            "legal_representative": (legal, legal_evidence, 0.72),
            "issue_date": (issue_date, issue_evidence, 0.72),
            "account_status": (account_status, status_evidence, 0.68),
        },
    )
    result = build_result(doc_type, fields, evidence)
    result["confidence"]["fields"] = confidences
    result["confidence"]["overall"] = round(sum(confidences.values()) / len(confidences), 4)
    result["raw_text_preview"] = raw_preview(text)
    return result


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    return _extract_account(payload, "account_permit")
