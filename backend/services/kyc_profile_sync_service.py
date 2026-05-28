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


def _confirmed_fields(extraction: dict[str, Any]) -> dict[str, Any]:
    confirmed = extraction.get("confirmed_data")
    if not isinstance(confirmed, dict):
        return {}
    value = confirmed.get("confirmed_fields")
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


def _effective_field(
    fields: dict[str, Any],
    confirmed_fields: dict[str, Any],
    key: str,
    source_document_id: str,
) -> tuple[str, dict[str, Any]]:
    if key in confirmed_fields and confirmed_fields.get(key) not in (None, ""):
        value = _string(confirmed_fields.get(key))
        return value, {
            "value": value,
            "source": "confirmed_data",
            "source_document_id": source_document_id,
            "confirmed": True,
        }
    value = _string(fields.get(key))
    return value, {
        "value": value,
        "source": "extracted_data",
        "source_document_id": source_document_id,
        "confirmed": False,
    }


def _apply_latest_single(
    profile: dict[str, Any],
    section: str,
    fields: dict[str, Any],
    confirmed_fields: dict[str, Any],
    field_map: dict[str, str],
    source_document_id: str,
) -> None:
    target = profile[section]
    field_sources = target.setdefault("field_sources", {})
    for target_key, source_key in field_map.items():
        value, source = _effective_field(fields, confirmed_fields, source_key, source_document_id)
        target[target_key] = value
        field_sources[target_key] = source
    target["source_document_id"] = source_document_id


def _property_item(fields: dict[str, Any], source_document_id: str, doc_type: str, confirmed_fields: dict[str, Any] | None = None) -> dict[str, Any]:
    confirmed_fields = confirmed_fields or {}
    field_map = {
        "owner": "owner",
        "co_owners": "co_owners",
        "certificate_number": "certificate_number",
        "property_unit_number": "property_unit_number",
        "property_address": "property_address",
        "right_type": "right_type",
        "right_nature": "right_nature",
        "use_type": "use_type",
        "building_area": "building_area",
        "land_area": "land_area",
        "total_area": "total_area",
        "mortgage_status": "mortgage_status",
        "seizure_status": "seizure_status",
        "issue_date": "issue_date",
    }
    item = {
        "doc_type": doc_type,
        "field_sources": {},
        "source_document_id": source_document_id,
    }
    for target_key, source_key in field_map.items():
        value, source = _effective_field(fields, confirmed_fields, source_key, source_document_id)
        item[target_key] = value
        item["field_sources"][target_key] = source
    return item


def _vehicle_item(fields: dict[str, Any], source_document_id: str, confirmed_fields: dict[str, Any] | None = None) -> dict[str, Any]:
    confirmed_fields = confirmed_fields or {}
    field_map = {
        "plate_number": "plate_number",
        "vehicle_owner": "vehicle_owner",
        "vehicle_type": "vehicle_type",
        "brand_model": "brand_model",
        "vehicle_identification_number": "vehicle_identification_number",
        "engine_number": "engine_number",
        "registration_date": "registration_date",
        "issue_date": "issue_date",
    }
    item = {
        "field_sources": {},
        "source_document_id": source_document_id,
    }
    for target_key, source_key in field_map.items():
        value, source = _effective_field(fields, confirmed_fields, source_key, source_document_id)
        item[target_key] = value
        item["field_sources"][target_key] = source
    return item


def _license_item(fields: dict[str, Any], data: dict[str, Any], source_document_id: str, confirmed_fields: dict[str, Any] | None = None) -> dict[str, Any]:
    confirmed_fields = confirmed_fields or {}
    name, name_source = _effective_field(fields, confirmed_fields, "license_name", source_document_id)
    if not name:
        name = _string(confirmed_fields.get("company_name") or fields.get("company_name") or data.get("doc_type_name"))
    return {
        "doc_type": data.get("doc_type") or "",
        "doc_type_name": data.get("doc_type_name") or "",
        "name": name,
        "certificate_number": _effective_field(fields, confirmed_fields, "certificate_number", source_document_id)[0] or _string(confirmed_fields.get("license_number") or fields.get("license_number")),
        "issuing_authority": _effective_field(fields, confirmed_fields, "issuing_authority", source_document_id)[0] or _string(confirmed_fields.get("registration_authority") or fields.get("registration_authority")),
        "issue_date": _effective_field(fields, confirmed_fields, "issue_date", source_document_id)[0],
        "field_sources": {"name": name_source},
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
        confirmed_fields = _confirmed_fields(extraction)
        doc_type = str(data.get("doc_type") or extraction.get("extraction_type") or "")
        source_document_id = _source_doc_id(extraction)
        profile["documents"].append(_document_summary(extraction, data))

        if doc_type in {"id_card", "shareholder_id_card"}:
            _apply_latest_single(
                profile,
                "person_identity",
                fields,
                confirmed_fields,
                {
                    "name": "name",
                    "id_number": "id_number",
                    "gender": "gender",
                    "birth_date": "birth_date",
                    "address": "address",
                },
                source_document_id,
            )
        elif doc_type == "business_license":
            _apply_latest_single(
                profile,
                "enterprise_identity",
                fields,
                confirmed_fields,
                {
                    "company_name": "company_name",
                    "unified_social_credit_code": "unified_social_credit_code",
                    "legal_representative": "legal_representative",
                    "registered_capital": "registered_capital",
                    "registered_address": "registered_address",
                    "business_scope": "business_scope",
                    "establishment_date": "establishment_date",
                },
                source_document_id,
            )
        elif doc_type in {"account_permit", "basic_account_info", "account_receipt"}:
            _apply_latest_single(
                profile,
                "bank_account",
                fields,
                confirmed_fields,
                {
                    "account_name": "bank_account_name",
                    "account_number": "bank_account_number",
                    "opening_bank": "opening_bank",
                    "account_type": "account_type",
                },
                source_document_id,
            )
            if not profile["bank_account"].get("account_name"):
                value, source = _effective_field(fields, confirmed_fields, "company_name", source_document_id)
                profile["bank_account"]["account_name"] = value
                profile["bank_account"].setdefault("field_sources", {})["account_name"] = source
        elif doc_type == "marriage_cert":
            _apply_latest_single(
                profile,
                "marriage",
                fields,
                confirmed_fields,
                {
                    "holder_name": "holder_name",
                    "spouse_name": "spouse_name",
                    "registration_date": "registration_date",
                },
                source_document_id,
            )
        elif doc_type in PROPERTY_DOC_TYPES:
            profile["assets"]["properties"].append(_property_item(fields, source_document_id, doc_type, confirmed_fields))
        elif doc_type in VEHICLE_DOC_TYPES:
            profile["assets"]["vehicles"].append(_vehicle_item(fields, source_document_id, confirmed_fields))
        elif doc_type in LICENSE_DOC_TYPES:
            profile["licenses"].append(_license_item(fields, data, source_document_id, confirmed_fields))

    profile["updated_at"] = datetime.now(timezone.utc).isoformat()
    return profile
