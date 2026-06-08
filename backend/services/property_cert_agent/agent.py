from __future__ import annotations

import logging
import re
from typing import Any

from .evidence import build_evidence
from .merger import merge_pages
from .normalizer import normalize_property_cert_fields
from .ocr import full_page_ocr
from .page_role import detect_page_role
from .result import PropertyCertAgentResult
from .segmenter import segment_pages
from .validator import validate_property_cert
from .renderer import render_markdown
from .skills.attachment_page_skill import extract as extract_attachment_page
from .skills.cover_page_skill import extract as extract_cover_page
from .skills.mortgage_page_skill import extract as extract_mortgage_page
from .skills.new_real_estate_cert_skill import extract as extract_new_detail_page
from .skills.old_shanghai_property_cert_skill import extract as extract_old_detail_page

logger = logging.getLogger(__name__)


def _compact(text: str) -> str:
    return re.sub(r"\s+", "", str(text or ""))


def _has_new_real_estate_strong_feature(text: str) -> bool:
    compact = _compact(text)
    return "不动产权第" in compact or "不动产单元号" in compact


def _has_old_shanghai_strong_feature(text: str) -> bool:
    compact = _compact(text)
    return "沪房地" in compact or "房地产权证" in compact or "房地坐落" in compact


class PropertyCertAgent:
    schema_version = "property_cert_agent.v1"

    def extract(
        self,
        *,
        file_bytes: bytes,
        filename: str,
        customer_id: str | None = None,
        customer_name: str | None = None,
        declared_doc_type: str | None = None,
        metadata: dict | None = None,
    ) -> PropertyCertAgentResult:
        metadata = metadata or {}
        raw_text = str(metadata.get("raw_text") or metadata.get("text") or "")
        if not raw_text:
            raw_text = full_page_ocr(file_bytes or b"", metadata)
        raw_pages = metadata.get("raw_pages") if isinstance(metadata.get("raw_pages"), list) else metadata.get("pages")
        pages = []
        warnings: list[str] = []
        for page in segment_pages(raw_text, raw_pages if isinstance(raw_pages, list) else [], filename):
            page_text = str(page.get("text") or "")
            role = detect_page_role(page_text, page.get("metadata") if isinstance(page.get("metadata"), dict) else None)
            skill_payload = {"text": page_text, "metadata": page.get("metadata") or {}}
            if _has_new_real_estate_strong_feature(page_text):
                role = "new_real_estate_detail_page"
                logger.info("[PropertySkillRouter] selected_skill=new_real_estate_cert_skill role=%s", role)
                extracted = extract_new_detail_page(skill_payload)
            elif role == "cover_page":
                logger.info("[PropertySkillRouter] selected_skill=cover_page_skill role=%s", role)
                extracted = extract_cover_page(skill_payload)
            elif role == "old_property_detail_page" or _has_old_shanghai_strong_feature(page_text):
                role = "old_property_detail_page"
                logger.info("[PropertySkillRouter] selected_skill=old_shanghai_property_cert_skill role=%s", role)
                extracted = extract_old_detail_page(skill_payload)
            elif role == "attachment_page":
                logger.info("[PropertySkillRouter] selected_skill=attachment_page_skill role=%s", role)
                extracted = extract_attachment_page(skill_payload)
            elif role == "mortgage_page":
                logger.info("[PropertySkillRouter] selected_skill=mortgage_page_skill role=%s", role)
                extracted = extract_mortgage_page(skill_payload)
            else:
                role = "new_real_estate_detail_page" if role in {"unknown", "detail_page"} else role
                logger.info("[PropertySkillRouter] selected_skill=new_real_estate_cert_skill role=%s", role)
                extracted = extract_new_detail_page(skill_payload)
            fields = extracted.get("fields") if isinstance(extracted.get("fields"), dict) else {}
            if role == "new_real_estate_detail_page":
                logger.info("[NewRealEstateSkill] fields_keys=%s", list(fields.keys()))
            warnings.extend(str(item) for item in extracted.get("warnings") or [])
            pages.append(
                {
                    **page,
                    "page_role": role,
                    "fields": fields,
                    "confidence": 0.86 if fields else 0.35,
                    "warnings": list(extracted.get("warnings") or []),
                }
            )

        merged = merge_pages(pages)
        page_roles = [str(page.get("page_role") or "unknown") for page in pages]
        fields = normalize_property_cert_fields(
            merged["fields"],
            raw_text=raw_text,
            page_role=page_roles[0] if page_roles else "",
            cert_version="old_shanghai_property_cert" if merged.get("old_version") else "",
        )
        validation, missing_fields, status = validate_property_cert(fields, page_roles)
        validation["warnings"] = list(dict.fromkeys([*validation.get("warnings", []), *warnings]))
        result_metadata = {
            "filename": filename,
            "customer_id": customer_id or str(metadata.get("customer_id") or ""),
            "customer_name": customer_name or str(metadata.get("customer_name") or ""),
            "declared_doc_type": declared_doc_type or str(metadata.get("declared_doc_type") or "property_cert"),
            "source": str(metadata.get("source") or "upload"),
        }
        result = PropertyCertAgentResult(
            extraction_status=status,
            fields=fields,
            pages=pages,
            merged_fields=fields,
            page_roles=page_roles,
            validation=validation,
            confidence={
                "overall": 0.88 if status == "success" else 0.65 if status == "partial" else 0.2,
                "fields": {key: 0.86 for key in fields},
            },
            evidence=build_evidence(fields, raw_text, page_role="merged"),
            missing_fields=missing_fields,
            raw_text_preview=raw_text[:500],
            metadata=result_metadata,
            supplemental_files=merged.get("supplemental_files") or [],
            risk_sections=merged.get("risk_sections") or {},
        )
        output = result.to_dict()
        output["_raw_text"] = raw_text
        result.markdown = render_markdown(output)
        return result


def run_property_cert_agent(payload: dict[str, Any] | str) -> dict[str, Any]:
    if isinstance(payload, str):
        payload = {"text": payload, "metadata": {}}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    result = PropertyCertAgent().extract(
        file_bytes=payload.get("file_bytes") if isinstance(payload.get("file_bytes"), bytes) else b"",
        filename=str(metadata.get("filename") or payload.get("filename") or ""),
        customer_id=str(metadata.get("customer_id") or ""),
        customer_name=str(metadata.get("customer_name") or ""),
        declared_doc_type=str(metadata.get("declared_doc_type") or "property_cert"),
        metadata={**metadata, "raw_text": str(payload.get("text") or "")},
    )
    return result.to_dict()
