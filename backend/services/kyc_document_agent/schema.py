from __future__ import annotations

from typing import Any


DOC_TYPE_NAMES = {
    "id_card": "居民身份证",
    "business_license": "营业执照",
    "account_permit": "开户许可证",
    "basic_account_info": "基本存款账户信息",
    "vehicle_license": "机动车行驶证",
    "driving_license": "机动车驾驶证",
    "property_cert": "房产证/房地产权证",
    "real_estate_cert": "不动产权证",
    "lease_contract_keypage": "租赁合同关键页",
    "real_estate_query": "产调",
    "shareholder_id_card": "股东身份证",
    "articles_keypage": "公司章程关键页",
    "special_business_license": "特许经营许可证",
    "food_business_license": "食品经营许可证",
    "road_transport_license": "道路运输许可证",
    "account_receipt": "开户信息回单",
    "taxpayer_qualification": "纳税人资格证明",
    "marriage_cert": "结婚证",
    "marriage_certificate": "结婚证",
    "divorce_cert": "离婚证",
    "household_register": "户口本",
    "unknown": "未知资料",
}

OWNER_TYPES = {
    "id_card": "person",
    "shareholder_id_card": "person",
    "marriage_cert": "person",
    "marriage_certificate": "person",
    "divorce_cert": "person",
    "household_register": "person",
    "business_license": "enterprise",
    "account_permit": "enterprise",
    "basic_account_info": "enterprise",
    "special_business_license": "enterprise",
    "food_business_license": "enterprise",
    "road_transport_license": "enterprise",
    "account_receipt": "enterprise",
    "taxpayer_qualification": "enterprise",
    "property_cert": "asset",
    "real_estate_cert": "asset",
    "real_estate_query": "asset",
    "vehicle_license": "asset",
    "driving_license": "person",
    "lease_contract_keypage": "asset",
    "articles_keypage": "enterprise",
}

REQUIRED_FIELDS = {
    "id_card": ["name", "gender", "birth_date", "id_number"],
    "business_license": ["company_name", "unified_social_credit_code", "legal_representative"],
    "account_permit": ["company_name", "bank_account_number", "opening_bank"],
    "basic_account_info": ["company_name", "bank_account_number", "opening_bank"],
    "property_cert": ["权利人", "权证编号", "房地坐落"],
    "real_estate_cert": ["owner", "certificate_number", "property_unit_number", "property_address"],
    "vehicle_license": ["plate_number", "vehicle_owner", "vehicle_identification_number"],
    "marriage_cert": ["holder_name", "spouse_name", "registration_date"],
    "marriage_certificate": ["certificate_no", "holder_1", "holder_2"],
}


def build_result(doc_type: str, fields: dict[str, Any] | None = None, evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "doc_type": doc_type,
        "doc_type_name": DOC_TYPE_NAMES.get(doc_type, DOC_TYPE_NAMES["unknown"]),
        "owner_type": OWNER_TYPES.get(doc_type, "unknown"),
        "extraction_status": "failed",
        "fields": fields or {},
        "validation": {
            "is_valid": True,
            "warnings": [],
            "errors": [],
        },
        "confidence": {
            "overall": 0.0,
            "fields": {},
        },
        "evidence": evidence or {},
        "missing_fields": [],
        "raw_text_preview": "",
    }


def normalize_input(payload: dict[str, Any] | str) -> dict[str, Any]:
    if isinstance(payload, str):
        return {"text": payload, "pages": [], "metadata": {}}
    return {
        "text": str(payload.get("text") or ""),
        "pages": payload.get("pages") or [],
        "metadata": payload.get("metadata") or {},
    }
