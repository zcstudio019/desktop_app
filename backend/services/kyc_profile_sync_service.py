from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any


logger = logging.getLogger(__name__)

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
            "ethnicity": "",
            "birth_date": "",
            "address": "",
            "issuing_authority": "",
            "valid_from": "",
            "valid_to": "",
            "source_document_id": "",
            "source_file": "",
        },
        "enterprise_identity": {
            "company_name": "",
            "unified_social_credit_code": "",
            "legal_representative": "",
            "registered_capital": "",
            "company_type": "",
            "establishment_date": "",
            "registered_address": "",
            "business_scope": "",
            "registration_authority": "",
            "issue_date": "",
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
            "marital_status": "",
            "certificate_no": "",
            "holder_name": "",
            "holder_1_name": "",
            "holder_1_id_number": "",
            "spouse_name": "",
            "holder_2_name": "",
            "holder_2_id_number": "",
            "marriage_date": "",
            "registration_date": "",
            "registration_authority": "",
            "issue_date": "",
            "source_document_id": "",
            "confirmed": False,
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


def _payload_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _extraction_payloads(extraction: dict[str, Any]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for key in ("extracted_data", "extracted_json", "data", "agent_result"):
        payload = _payload_dict(extraction.get(key))
        if payload:
            payloads.append(payload)
    return payloads


def _effective_fields(extraction: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for payload in _extraction_payloads(extraction):
        for key, value in _fields(payload).items():
            if merged.get(key) in (None, "", [], {}) and value not in (None, "", [], {}):
                merged[key] = value
    direct_fields = _payload_dict(extraction.get("fields"))
    for key, value in direct_fields.items():
        if merged.get(key) in (None, "", [], {}) and value not in (None, "", [], {}):
            merged[key] = value
    return merged


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


def _is_invalid_effective_value(key: str, value: Any) -> bool:
    text = _string(value)
    if not text:
        return True
    if key != "registration_authority":
        return False
    compact = re.sub(r"\s+", "", text)
    if compact in {"未识别", "登记机关", "发照日期"}:
        return True
    if re.fullmatch(r"\d{4}年\d{1,2}月\d{1,2}日", compact):
        return True
    if re.fullmatch(r"\d{4}[-./]\d{1,2}[-./]\d{1,2}", compact):
        return True
    forbidden = ("经营范围", "住所", "营业执照", "法定代表人", "二维码")
    return any(item in compact for item in forbidden)


def _source_doc_id(extraction: dict[str, Any]) -> str:
    return str(extraction.get("doc_id") or extraction.get("document_id") or extraction.get("source_document_id") or "")


def _created_sort_key(extraction: dict[str, Any]) -> str:
    return str(extraction.get("created_at") or extraction.get("updated_at") or extraction.get("upload_time") or "")


def _mask_id_number(value: Any) -> str:
    return re.sub(r"(?<!\d)(\d{6})\d{8}(\d{3}[\dXx])(?!\d)", r"\1********\2", str(value or ""))


def _is_kyc_extraction(extraction: dict[str, Any]) -> bool:
    for data in _extraction_payloads(extraction):
        if data.get("agent_type") == KYC_AGENT_TYPE:
            return True
    return False


def _effective_fields_for_score(extraction: dict[str, Any]) -> dict[str, Any]:
    fields = _effective_fields(extraction)
    confirmed_fields = _confirmed_fields(extraction)
    if confirmed_fields:
        merged = dict(fields)
        merged.update({key: value for key, value in confirmed_fields.items() if value not in (None, "", [], {})})
        return merged
    return fields


def _id_card_field_score(fields: dict[str, Any], confirmed_fields: dict[str, Any]) -> int:
    merged = dict(fields)
    merged.update({key: value for key, value in confirmed_fields.items() if value not in (None, "", [], {})})
    important = ("name", "id_number", "gender", "birth_date", "address", "issuing_authority", "valid_from", "valid_to")
    return sum(1 for key in important if _string(merged.get(key)))


def score_kyc_property_cert_extraction(extraction: dict[str, Any]) -> int:
    data = extraction.get("extracted_data") if isinstance(extraction, dict) else {}
    if not isinstance(data, dict):
        return -100
    doc_type = str(data.get("doc_type") or extraction.get("extraction_type") or "")
    if doc_type not in {"property_cert", "real_estate_cert"}:
        return -100

    fields = _effective_fields_for_score(extraction)
    score = 10
    if fields:
        score += 20
    else:
        score -= 50

    def has_any(keys: tuple[str, ...]) -> bool:
        return any(_string(fields.get(key)) for key in keys)

    if has_any(("权利人", "owner")):
        score += 15
    if has_any(("权证编号", "certificate_number")):
        score += 15
    if has_any(("坐落", "房地坐落", "property_address")):
        score += 15
    if has_any(("不动产单元号", "property_unit_number")):
        score += 10
    if has_any(("建筑面积", "building_area")):
        score += 10
    if has_any(("土地用途", "land_use")) or has_any(("房屋用途", "house_use", "building_use", "use_type")):
        score += 10
    if has_any(("竣工日期", "completion_date")) or has_any(("总层数", "total_floors")):
        score += 5

    page_role = str(data.get("page_role") or "").strip()
    warnings = data.get("validation", {}).get("warnings") if isinstance(data.get("validation"), dict) else []
    warning_text = " ".join(str(item) for item in warnings or [])
    if page_role == "cover_page":
        score -= 30
    if "仅识别到" in warning_text and ("封面" in warning_text or "说明页" in warning_text):
        score -= 30
    return score


def _document_summary(extraction: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
    summary = {
        "doc_id": _source_doc_id(extraction),
        "doc_type": data.get("doc_type") or extraction.get("extraction_type") or "",
        "doc_type_name": data.get("doc_type_name") or "",
        "extraction_status": data.get("extraction_status") or extraction.get("extraction_status") or "",
        "source_file": extraction.get("file_name") or extraction.get("source_file") or "",
        "created_at": extraction.get("created_at") or "",
    }
    if summary["doc_type"] in {"property_cert", "real_estate_cert"}:
        score = score_kyc_property_cert_extraction(extraction)
        summary["quality_score"] = score
        summary["page_role"] = data.get("page_role") or ""
        summary["display_role"] = "主资料 / 字段完整" if score >= 40 else "封面页 / 补充页"
    return summary


def _effective_field(
    fields: dict[str, Any],
    confirmed_fields: dict[str, Any],
    key: str,
    source_document_id: str,
) -> tuple[str, dict[str, Any]]:
    if key in confirmed_fields and not _is_invalid_effective_value(key, confirmed_fields.get(key)):
        value = _string(confirmed_fields.get(key))
        return value, {
            "value": value,
            "source": "confirmed_data",
            "source_document_id": source_document_id,
            "confirmed": True,
        }
    value = "" if _is_invalid_effective_value(key, fields.get(key)) else _string(fields.get(key))
    return value, {
        "value": value,
        "source": "extracted_data",
        "source_document_id": source_document_id,
        "confirmed": False,
    }


def _effective_any_field(
    fields: dict[str, Any],
    confirmed_fields: dict[str, Any],
    keys: list[str],
    source_document_id: str,
) -> tuple[str, dict[str, Any]]:
    for key in keys:
        if key in confirmed_fields and not _is_invalid_effective_value(key, confirmed_fields.get(key)):
            value = _string(confirmed_fields.get(key))
            return value, {
                "value": value,
                "source": "confirmed_data",
                "source_document_id": source_document_id,
                "confirmed": True,
            }
    for key in keys:
        value = "" if _is_invalid_effective_value(key, fields.get(key)) else _string(fields.get(key))
        if value:
            return value, {
                "value": value,
                "source": "extracted_data",
                "source_document_id": source_document_id,
                "confirmed": False,
            }
    return "", {
        "value": "",
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
        "owner": ["owner", "权利人"],
        "co_owners": ["co_owners", "共有情况", "共有人"],
        "certificate_number": ["certificate_number", "权证编号"],
        "property_unit_number": ["property_unit_number", "不动产单元号"],
        "property_address": ["property_address", "坐落", "房地坐落"],
        "right_type": ["right_type", "权利类型"],
        "right_nature": ["right_nature", "权利性质", "权属性质"],
        "use_type": ["use_type", "house_use", "building_use", "房屋用途"],
        "land_use": ["land_use", "土地用途"],
        "building_area": ["building_area", "建筑面积"],
        "land_area": ["land_area", "宗地面积"],
        "total_area": ["total_area", "使用权面积"],
        "land_use_term": ["land_use_term", "使用期限", "土地使用期限"],
        "parcel_number": ["parcel_number", "地号", "宗地号"],
        "mortgage_status": ["mortgage_status"],
        "seizure_status": ["seizure_status"],
        "issue_date": ["登记日", "issue_date"],
        "registration_date": ["登记日期", "registration_date", "登记日", "issue_date"],
        "registration_authority": ["登记机构", "registration_authority"],
        "cover_certificate_number": ["封面编号", "cover_certificate_number"],
    }
    item = {
        "doc_type": doc_type,
        "field_sources": {},
        "source_document_id": source_document_id,
    }
    for target_key, source_keys in field_map.items():
        value, source = _effective_any_field(fields, confirmed_fields, source_keys, source_document_id)
        item[target_key] = value
        item["field_sources"][target_key] = source
    return item


def _property_item_from_extraction(
    extraction: dict[str, Any],
    fields: dict[str, Any],
    source_document_id: str,
    doc_type: str,
    confirmed_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = extraction.get("extracted_data") if isinstance(extraction.get("extracted_data"), dict) else {}
    item = _property_item(fields, source_document_id, doc_type, confirmed_fields)
    score = score_kyc_property_cert_extraction(extraction)
    item["quality_score"] = score
    item["page_role"] = data.get("page_role") or ""
    item["source_file"] = extraction.get("file_name") or extraction.get("source_file") or ""
    item["display_role"] = "主资料 / 字段完整" if score >= 40 else "封面页 / 补充页"
    return item


def _property_completeness_score(item: dict[str, Any]) -> int:
    keys = (
        "owner",
        "certificate_number",
        "property_unit_number",
        "property_address",
        "right_type",
        "right_nature",
        "land_use",
        "use_type",
        "building_area",
        "land_area",
        "land_use_term",
        "parcel_number",
    )
    return sum(1 for key in keys if _string(item.get(key)))


def _vehicle_item(fields: dict[str, Any], source_document_id: str, confirmed_fields: dict[str, Any] | None = None) -> dict[str, Any]:
    confirmed_fields = confirmed_fields or {}
    field_map = {
        "plate_number": ["plate_number"],
        "vehicle_type": ["vehicle_type"],
        "owner": ["owner", "vehicle_owner"],
        "vehicle_owner": ["owner", "vehicle_owner"],
        "address": ["address"],
        "use_character": ["use_character"],
        "brand_model": ["brand_model"],
        "vin": ["vin", "vehicle_identification_number"],
        "vehicle_identification_number": ["vin", "vehicle_identification_number"],
        "engine_number": ["engine_number"],
        "registration_date": ["registration_date"],
        "issue_date": ["issue_date"],
        "approved_passengers": ["approved_passengers"],
        "total_mass": ["total_mass"],
        "curb_weight": ["curb_weight"],
        "inspection_valid_until": ["inspection_valid_until"],
    }
    item = {
        "field_sources": {},
        "source_document_id": source_document_id,
    }
    for target_key, source_keys in field_map.items():
        value, source = _effective_any_field(fields, confirmed_fields, source_keys, source_document_id)
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

    id_card_candidates: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any], str, int]] = []
    for extraction in kyc_extractions:
        payloads = _extraction_payloads(extraction)
        data = payloads[0] if payloads else {}
        if not data:
            continue
        fields = _effective_fields(extraction)
        confirmed_fields = _confirmed_fields(extraction)
        doc_type = str(data.get("doc_type") or extraction.get("extraction_type") or "")
        source_document_id = _source_doc_id(extraction)
        profile["documents"].append(_document_summary(extraction, data))

        if doc_type in {"id_card", "shareholder_id_card"}:
            score = _id_card_field_score(fields, confirmed_fields)
            if score > 0:
                id_card_candidates.append((extraction, fields, confirmed_fields, source_document_id, score))
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
                    "company_type": "company_type",
                    "establishment_date": "establishment_date",
                    "registered_address": "registered_address",
                    "business_scope": "business_scope",
                    "registration_authority": "registration_authority",
                    "issue_date": "issue_date",
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
        elif doc_type in {"marriage_certificate", "marriage_cert"}:
            holder_1 = confirmed_fields.get("holder_1") if isinstance(confirmed_fields.get("holder_1"), dict) else fields.get("holder_1")
            holder_2 = confirmed_fields.get("holder_2") if isinstance(confirmed_fields.get("holder_2"), dict) else fields.get("holder_2")
            holder_1 = holder_1 if isinstance(holder_1, dict) else {}
            holder_2 = holder_2 if isinstance(holder_2, dict) else {}
            marriage = profile["marriage"]
            marriage["marital_status"] = _string(confirmed_fields.get("marital_status") or fields.get("marital_status") or "已婚")
            marriage["certificate_no"] = _string(confirmed_fields.get("certificate_no") or fields.get("certificate_no") or confirmed_fields.get("certificate_number") or fields.get("certificate_number"))
            marriage["holder_name"] = _string(holder_1.get("name") or confirmed_fields.get("holder_name") or fields.get("holder_name"))
            marriage["holder_1_name"] = marriage["holder_name"]
            marriage["holder_1_id_number"] = _string(holder_1.get("id_number") or confirmed_fields.get("holder_id_number") or fields.get("holder_id_number"))
            marriage["spouse_name"] = _string(holder_2.get("name") or confirmed_fields.get("spouse_name") or fields.get("spouse_name"))
            marriage["holder_2_name"] = marriage["spouse_name"]
            marriage["holder_2_id_number"] = _string(holder_2.get("id_number") or confirmed_fields.get("spouse_id_number") or fields.get("spouse_id_number"))
            marriage["marriage_date"] = _string(confirmed_fields.get("marriage_date") or fields.get("marriage_date") or confirmed_fields.get("registration_date") or fields.get("registration_date"))
            marriage["registration_date"] = marriage["marriage_date"]
            marriage["registration_authority"] = _string(confirmed_fields.get("registration_authority") or fields.get("registration_authority") or confirmed_fields.get("issuing_authority") or fields.get("issuing_authority"))
            marriage["issue_date"] = _string(confirmed_fields.get("issue_date") or fields.get("issue_date"))
            marriage["source_document_id"] = source_document_id
            marriage["confirmed"] = bool(confirmed_fields)
        elif doc_type in PROPERTY_DOC_TYPES:
            profile["assets"]["properties"].append(_property_item_from_extraction(extraction, fields, source_document_id, doc_type, confirmed_fields))
        elif doc_type in VEHICLE_DOC_TYPES:
            profile["assets"]["vehicles"].append(_vehicle_item(fields, source_document_id, confirmed_fields))
        elif doc_type in LICENSE_DOC_TYPES:
            profile["licenses"].append(_license_item(fields, data, source_document_id, confirmed_fields))

    if id_card_candidates:
        id_card_candidates.sort(
            key=lambda item: (item[4], _created_sort_key(item[0]), str(item[0].get("extraction_id") or "")),
            reverse=True,
        )
        selected_extraction, selected_fields, selected_confirmed_fields, selected_source_document_id, selected_score = id_card_candidates[0]
        logger.info(
            "[KYCDisplay][ID_CARD_LATEST] document_id=%s filename=%s created_at=%s has_fields=%s fields_keys=%s name=%s id_number=%s score=%s",
            selected_source_document_id,
            selected_extraction.get("file_name") or selected_extraction.get("source_file") or "",
            selected_extraction.get("created_at") or "",
            bool(selected_fields),
            list(selected_fields.keys()),
            bool(_string(selected_confirmed_fields.get("name") or selected_fields.get("name"))),
            _mask_id_number(selected_confirmed_fields.get("id_number") or selected_fields.get("id_number")),
            selected_score,
        )
        _apply_latest_single(
            profile,
            "person_identity",
            selected_fields,
            selected_confirmed_fields,
            {
                "name": "name",
                "id_number": "id_number",
                "gender": "gender",
                "ethnicity": "ethnicity",
                "birth_date": "birth_date",
                "address": "address",
                "issuing_authority": "issuing_authority",
                "valid_from": "valid_from",
                "valid_to": "valid_to",
            },
            selected_source_document_id,
        )
        profile["person_identity"]["source_file"] = (
            selected_extraction.get("file_name")
            or selected_extraction.get("source_file")
            or selected_extraction.get("filename")
            or ""
        )

    profile["assets"]["properties"].sort(
        key=lambda item: (int(item.get("quality_score") or 0), _property_completeness_score(item), _string(item.get("source_document_id"))),
        reverse=True,
    )
    if profile["assets"]["properties"]:
        main_property = profile["assets"]["properties"][0]
        for supplement in profile["assets"]["properties"][1:]:
            if not _string(main_property.get("issue_date")) and _string(supplement.get("issue_date")):
                main_property["issue_date"] = supplement.get("issue_date")
                main_property.setdefault("field_sources", {})["issue_date"] = {
                    **(supplement.get("field_sources", {}) or {}).get("issue_date", {}),
                    "source_document_id": supplement.get("source_document_id") or "",
                }
            if not _string(main_property.get("certificate_number")) and _string(supplement.get("certificate_number")):
                main_property["certificate_number"] = supplement.get("certificate_number")
                main_property.setdefault("field_sources", {})["certificate_number"] = {
                    **(supplement.get("field_sources", {}) or {}).get("certificate_number", {}),
                    "source_document_id": supplement.get("source_document_id") or "",
                }
            for field_name in ("registration_date", "registration_authority", "cover_certificate_number"):
                if not _string(main_property.get(field_name)) and _string(supplement.get(field_name)):
                    main_property[field_name] = supplement.get(field_name)
                    main_property.setdefault("field_sources", {})[field_name] = {
                        **(supplement.get("field_sources", {}) or {}).get(field_name, {}),
                        "source_document_id": supplement.get("source_document_id") or "",
                    }
            if _string(supplement.get("registration_date")):
                logger.info("[PropertyMerge] cover_fields 登记日期=%s", supplement.get("registration_date"))
                logger.info("[PropertyMerge] merged 登记日期=%s", main_property.get("registration_date"))
    profile["updated_at"] = datetime.now(timezone.utc).isoformat()
    return profile
