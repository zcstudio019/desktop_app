from __future__ import annotations

import importlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from .classifier import classify as classify_doc_type, classify_with_reason, normalize_declared_doc_type
from .normalizer import normalize_result
from .renderer import render_markdown
from .schema import build_result, normalize_input
from .validator import validate_result


logger = logging.getLogger(__name__)

ALLOWED_METADATA_KEYS = {
    "filename",
    "source_file",
    "customer_id",
    "customer_name",
    "source",
    "declared_doc_type",
    "original_document_type",
    "document_type",
    "documentType",
}

SKILL_MODULES = {
    "id_card": "backend.services.kyc_document_agent.skills.id_card_skill",
    "business_license": "backend.services.kyc_document_agent.skills.business_license_skill",
    "account_permit": "backend.services.kyc_document_agent.skills.account_permit_skill",
    "basic_account_info": "backend.services.kyc_document_agent.skills.basic_account_info_skill",
    "vehicle_license": "backend.services.kyc_document_agent.skills.vehicle_license_skill",
    "driving_license": "backend.services.kyc_document_agent.skills.driving_license_skill",
    "property_cert": "backend.services.kyc_document_agent.skills.property_cert_skill",
    "real_estate_cert": "backend.services.kyc_document_agent.skills.real_estate_cert_skill",
    "lease_contract_keypage": "backend.services.kyc_document_agent.skills.lease_contract_keypage_skill",
    "real_estate_query": "backend.services.kyc_document_agent.skills.real_estate_query_skill",
    "shareholder_id_card": "backend.services.kyc_document_agent.skills.shareholder_id_card_skill",
    "articles_keypage": "backend.services.kyc_document_agent.skills.articles_keypage_skill",
    "special_business_license": "backend.services.kyc_document_agent.skills.special_business_license_skill",
    "food_business_license": "backend.services.kyc_document_agent.skills.food_business_license_skill",
    "road_transport_license": "backend.services.kyc_document_agent.skills.road_transport_license_skill",
    "account_receipt": "backend.services.kyc_document_agent.skills.account_receipt_skill",
    "taxpayer_qualification": "backend.services.kyc_document_agent.skills.taxpayer_qualification_skill",
    "marriage_cert": "backend.services.kyc_document_agent.skills.marriage_cert_skill",
    "divorce_cert": "backend.services.kyc_document_agent.skills.divorce_cert_skill",
    "household_register": "backend.services.kyc_document_agent.skills.household_register_skill",
}


class KycDocumentAgent:
    def __init__(self, save_results: bool = False, save_dir: str | Path | None = None) -> None:
        self.save_results = save_results
        self.save_dir = Path(save_dir or "data/kyc_document_results")

    def classify(self, text: str, filename: str = "", declared_doc_type: str | None = None) -> str:
        return classify_doc_type(text, filename=filename, declared_doc_type=declared_doc_type)

    @staticmethod
    def _sanitize_metadata(metadata: dict[str, Any] | None, filename: str = "") -> dict[str, Any]:
        metadata = metadata or {}
        sanitized = {key: metadata.get(key) for key in ALLOWED_METADATA_KEYS if key in metadata}
        declared = (
            metadata.get("declared_doc_type")
            or metadata.get("document_type")
            or metadata.get("documentType")
        )
        normalized_declared = normalize_declared_doc_type(str(declared or ""), filename=filename)
        if normalized_declared:
            sanitized["declared_doc_type"] = normalized_declared
            if declared and str(declared).strip() != normalized_declared:
                sanitized["original_document_type"] = str(declared).strip()
        return sanitized

    def extract(self, payload: dict[str, Any] | str) -> dict[str, Any]:
        data = normalize_input(payload)
        raw_metadata = data.get("metadata") or {}
        raw_filename = str(raw_metadata.get("filename") or raw_metadata.get("source_file") or "")
        metadata = self._sanitize_metadata(raw_metadata, filename=raw_filename)
        filename = str(metadata.get("filename") or metadata.get("source_file") or "")
        declared_doc_type = str(metadata.get("declared_doc_type") or "")
        classification = classify_with_reason(data["text"], filename=filename, declared_doc_type=declared_doc_type)
        doc_type = classification["doc_type"]
        if doc_type == "unknown":
            result = build_result("unknown")
            result["raw_text_preview"] = data["text"][:240]
            result["metadata"] = metadata
            result["agent_type"] = "kyc_document_agent"
            result["classification_reason"] = classification.get("reason") or "未命中支持的KYC资料关键词"
            result["validation"]["warnings"].append("未识别到支持的KYC资料类型，请检查扫描件清晰度或人工选择资料类型")
            result["markdown"] = render_markdown(result)
            return result

        skill_module = importlib.import_module(SKILL_MODULES[doc_type])
        result = skill_module.extract(data)
        result["metadata"] = metadata
        result["agent_type"] = "kyc_document_agent"
        result["classification_reason"] = classification.get("reason") or ""
        result = normalize_result(result)
        result = validate_result(result)
        if result.get("doc_type") in {"property_cert", "real_estate_cert"}:
            fields = result.get("fields") or {}
            logger.info("[KycDocumentAgent] final_doc_type=%s", result.get("doc_type"))
            logger.info("[KycDocumentAgent] final_fields_keys=%s", list(fields.keys()))
            logger.info("[KycDocumentAgent] final_fields=%s", fields)
            logger.info("[KycDocumentAgent][DEBUG] final_fields_keys=%s", list(fields.keys()))
            logger.info("[KycDocumentAgent][DEBUG] final_房屋用途=%s", fields.get("房屋用途"))
            logger.info("[KycDocumentAgent][DEBUG] final_house_use=%s", fields.get("house_use"))
            logger.info("[KycDocumentAgent][DEBUG] final_building_use=%s", fields.get("building_use"))
            logger.info("[KycDocumentAgent][DEBUG] final_use_type=%s", fields.get("use_type"))
        result["markdown"] = render_markdown(result)
        if self.save_results:
            result["saved_path"] = str(self.save_structured_result(result, data.get("metadata") or {}))
        return result

    def save_structured_result(self, result: dict[str, Any], metadata: dict[str, Any] | None = None) -> Path:
        metadata = metadata or {}
        self.save_dir.mkdir(parents=True, exist_ok=True)
        customer_id = str(metadata.get("customer_id") or "unknown").replace("/", "_").replace("\\", "_")
        doc_type = result.get("doc_type") or "unknown"
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        path = self.save_dir / customer_id / f"{doc_type}_{timestamp}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return path


def run_kyc_document_agent(payload: dict[str, Any] | str, save_results: bool = False) -> dict[str, Any]:
    return KycDocumentAgent(save_results=save_results).extract(payload)
