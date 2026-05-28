from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


KYC_AGENT_TYPE = "kyc_document_agent"
PROPERTY_DOC_TYPES = {"property_cert", "real_estate_cert", "real_estate_query"}
VEHICLE_DOC_TYPES = {"vehicle_license"}
LICENSE_DOC_TYPES = {
    "special_business_license",
    "food_business_license",
    "road_transport_license",
    "taxpayer_qualification",
    "articles_keypage",
}


def _empty_profile(customer_id: str) -> dict[str, Any]:
    return {
        "customer_id": customer_id,
        "person_identity": {
            "name": "",
            "id_number": "",
            "gender": "",
            "birth_date": "",
            "address": "",
            "source_document_id": "",
        },
        "enterprise_identity": {
            "company_name": "",
            "unified_social_credit_code": "",
            "legal_representative": "",
            "registered_capital": "",
            "registered_address": "",
            "business_scope": "",
            "establishment_date": "",
            "source_document_id": "",
        },
        "bank_account": {
            "account_name": "",
            "account_number": "",
            "opening_bank": "",
            "account_type": "",
            "source_document_id": "",
        },
        "marriage": {
            "holder_name": "",
            "spouse_name": "",
            "registration_date": "",
            "source_document_id": "",
        },
        "assets": {
            "properties": [],
            "vehicles": [],
        },
        "licenses": [],
        "documents": [],
        "updated_at": "",
    }


def _fields(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("fields")
    return value if isinstance(value, dict) else {}


def _string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        if "amount" in value and "unit" in value:
            return f"{value.get('amount', '')}{value.get('unit', '')}".strip()
        if "value" in value and "unit" in value:
            return f"{value.get('value', '')}{value.get('unit', '')}".strip()
    return str(value).strip()


def _source_doc_id(extraction: dict[str, Any]) -> str:
    return str(extraction.get("doc_id") or extraction.get("document_id") or extraction.get("source_document_id") or "")


def _created_sort_key(extraction: dict[str, Any]) -> str:
    return str(extraction.get("created_at") or extraction.get("updated_at") or extraction.get("upload_time") or "")


def _is_kyc_extraction(extraction: dict[str, Any]) -> bool:
    data = extraction.get("extracted_data")
    return isinstance(data, dict) and data.get("agent_type") == KYC_AGENT_TYPE


def _document_summary(extraction: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
    return {
        "doc_id": _source_doc_id(extraction),
        "doc_type": data.get("doc_type") or extraction.get("extraction_type") or "",
        "doc_type_name": data.get("doc_type_name") or "",
        "extraction_status": data.get("extraction_status") or extraction.get("extraction_status") or "",
        "source_file": extraction.get("file_name") or extraction.get("source_file") or "",
        "created_at": extraction.get("created_at") or "",
    }


def _apply_latest_single(profile: dict[str, Any], section: str, values: dict[str, Any], source_document_id: str) -> None:
    target = profile[section]
    for key, value in values.items():
        target[key] = _string(value)
    target["source_document_id"] = source_document_id


def _property_item(fields: dict[str, Any], source_document_id: str, doc_type: str) -> dict[str, Any]:
    return {
        "doc_type": doc_type,
        "owner": _string(fields.get("owner")),
        "co_owners": fields.get("co_owners") or "",
        "certificate_number": _string(fields.get("certificate_number")),
        "property_unit_number": _string(fields.get("property_unit_number")),
        "property_address": _string(fields.get("property_address")),
        "right_type": _string(fields.get("right_type")),
        "right_nature": _string(fields.get("right_nature")),
        "use_type": _string(fields.get("use_type")),
        "building_area": _string(fields.get("building_area")),
        "land_area": _string(fields.get("land_area")),
        "total_area": _string(fields.get("total_area")),
        "mortgage_status": _string(fields.get("mortgage_status")),
        "seizure_status": _string(fields.get("seizure_status")),
        "issue_date": _string(fields.get("issue_date")),
        "source_document_id": source_document_id,
    }


def _vehicle_item(fields: dict[str, Any], source_document_id: str) -> dict[str, Any]:
    return {
        "plate_number": _string(fields.get("plate_number")),
        "vehicle_owner": _string(fields.get("vehicle_owner")),
        "vehicle_type": _string(fields.get("vehicle_type")),
        "brand_model": _string(fields.get("brand_model")),
        "vehicle_identification_number": _string(fields.get("vehicle_identification_number")),
        "engine_number": _string(fields.get("engine_number")),
        "registration_date": _string(fields.get("registration_date")),
        "issue_date": _string(fields.get("issue_date")),
        "source_document_id": source_document_id,
    }


def _license_item(fields: dict[str, Any], data: dict[str, Any], source_document_id: str) -> dict[str, Any]:
    return {
        "doc_type": data.get("doc_type") or "",
        "doc_type_name": data.get("doc_type_name") or "",
        "name": _string(fields.get("license_name") or fields.get("company_name") or data.get("doc_type_name")),
        "certificate_number": _string(fields.get("certificate_number") or fields.get("license_number")),
        "issuing_authority": _string(fields.get("issuing_authority") or fields.get("registration_authority")),
        "issue_date": _string(fields.get("issue_date")),
        "source_document_id": source_document_id,
    }


async def build_customer_kyc_profile(storage: Any, customer_id: str) -> dict[str, Any]:
    profile = _empty_profile(str(customer_id or ""))
    if not customer_id or storage is None:
        return profile

    try:
        extractions = await storage.get_extractions_by_customer(str(customer_id))
    except Exception:
        extractions = []
    if not isinstance(extractions, list):
        extractions = []

    if hasattr(storage, "list_documents"):
        try:
            await storage.list_documents(str(customer_id))
        except Exception:
            pass

    kyc_extractions = [item for item in extractions if isinstance(item, dict) and _is_kyc_extraction(item)]
    kyc_extractions.sort(key=_created_sort_key)
    if not kyc_extractions:
        return profile

    for extraction in kyc_extractions:
        data = extraction.get("extracted_data") or {}
        if not isinstance(data, dict):
            continue
        fields = _fields(data)
        doc_type = str(data.get("doc_type") or extraction.get("extraction_type") or "")
        source_document_id = _source_doc_id(extraction)
        profile["documents"].append(_document_summary(extraction, data))

        if doc_type in {"id_card", "shareholder_id_card"}:
            _apply_latest_single(
                profile,
                "person_identity",
                {
                    "name": fields.get("name"),
                    "id_number": fields.get("id_number"),
                    "gender": fields.get("gender"),
                    "birth_date": fields.get("birth_date"),
                    "address": fields.get("address"),
                },
                source_document_id,
            )
        elif doc_type == "business_license":
            _apply_latest_single(
                profile,
                "enterprise_identity",
                {
                    "company_name": fields.get("company_name"),
                    "unified_social_credit_code": fields.get("unified_social_credit_code"),
                    "legal_representative": fields.get("legal_representative"),
                    "registered_capital": fields.get("registered_capital"),
                    "registered_address": fields.get("registered_address"),
                    "business_scope": fields.get("business_scope"),
                    "establishment_date": fields.get("establishment_date"),
                },
                source_document_id,
            )
        elif doc_type in {"account_permit", "basic_account_info", "account_receipt"}:
            _apply_latest_single(
                profile,
                "bank_account",
                {
                    "account_name": fields.get("bank_account_name") or fields.get("company_name"),
                    "account_number": fields.get("bank_account_number"),
                    "opening_bank": fields.get("opening_bank"),
                    "account_type": fields.get("account_type"),
                },
                source_document_id,
            )
        elif doc_type == "marriage_cert":
            _apply_latest_single(
                profile,
                "marriage",
                {
                    "holder_name": fields.get("holder_name"),
                    "spouse_name": fields.get("spouse_name"),
                    "registration_date": fields.get("registration_date"),
                },
                source_document_id,
            )
        elif doc_type in PROPERTY_DOC_TYPES:
            profile["assets"]["properties"].append(_property_item(fields, source_document_id, doc_type))
        elif doc_type in VEHICLE_DOC_TYPES:
            profile["assets"]["vehicles"].append(_vehicle_item(fields, source_document_id))
        elif doc_type in LICENSE_DOC_TYPES:
            profile["licenses"].append(_license_item(fields, data, source_document_id))

    profile["updated_at"] = datetime.now(timezone.utc).isoformat()
    return profile
