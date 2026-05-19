from __future__ import annotations

import logging
from typing import Any

from backend.document_types import get_document_display_name, get_document_storage_label, normalize_document_type_code
from backend.services.personal_credit_report_agent import run_personal_credit_report_agent

from .base import BaseExtractionSkill, ExtractionInput

logger = logging.getLogger(__name__)


class PersonalCreditSkill(BaseExtractionSkill):
    document_type = "personal_credit_report"
    skill_name = "personal_credit_report"
    supported_document_types = ["personal_credit_report", "personal_credit", "个人征信"]
    supported_extensions = {".pdf", ".png", ".jpg", ".jpeg"}
    schema_version = "personal_credit_report.agent.v1"
    skill_version = "v1"

    def extract(
        self,
        input_data: ExtractionInput | None = None,
        *,
        raw_text: str | None = None,
        filename: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if input_data is not None:
            raw_text = input_data.raw_text or raw_text or ""
            filename = input_data.file_name or filename
            merged_metadata = dict(input_data.metadata or {})
            if metadata:
                merged_metadata.update(metadata)
            metadata = merged_metadata
            requested_document_type = input_data.document_type
        else:
            requested_document_type = self.document_type

        document_type_code = normalize_document_type_code(requested_document_type) or self.document_type
        logger.info(
            "[PersonalCreditSkill] extract called document_type=%s filename=%s",
            document_type_code,
            filename,
        )

        result = run_personal_credit_report_agent(str(raw_text or ""), source_file=filename, debug=True)
        report_json = result.get("report_json") if isinstance(result.get("report_json"), dict) else {}
        markdown = str(result.get("report_markdown") or "")
        if isinstance(report_json, dict):
            report_json["report_markdown"] = markdown
            report_json["markdown_summary"] = markdown
            report_json["markdown"] = markdown

        basic = report_json.get("basic_info") if isinstance(report_json.get("basic_info"), dict) else {}
        content = {
            "type": "personal_credit_report",
            "name": "个人征信报告",
            "title": "个人征信报告",
            "document_type_code": "personal_credit_report",
            "document_type_name": get_document_display_name("personal_credit_report") or "个人征信",
            "storage_label": get_document_storage_label("personal_credit_report") or "个人征信提取",
            "skill_name": self.skill_name,
            "skill_version": self.skill_version,
            "schema_version": str(report_json.get("schema_version") or self.schema_version),
            "extraction_status": "success",
            "extraction_error": "",
            "confidence": 0.75,
            "warnings": list(result.get("warnings") or []),
            "errors": [],
            "report_markdown": markdown,
            "markdown": markdown,
            "markdown_summary": markdown,
            "summary": markdown,
            "extracted_json": report_json,
            "data": report_json,
            "evidence": {"sections": result.get("sections") or {}},
            "debug": result.get("debug") if isinstance(result.get("debug"), dict) else {},
            "customer_name": basic.get("name") or "",
            "id_number": basic.get("id_number") or "",
            "report_no": basic.get("report_number") or "",
            "report_date": basic.get("report_time") or "",
            "raw_text_preview": str(raw_text or "")[:3000],
        }
        logger.info(
            "[PersonalCreditSkill] extract success confidence=%s schema_version=%s",
            content["confidence"],
            content["schema_version"],
        )
        return content


def build_personal_credit_report_content(
    *,
    text: str,
    customer_id: str = "",
    customer_name: str = "",
    file_name: str = "",
    file_path: str = "",
    document_id: str = "",
    raw_pages: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    skill = PersonalCreditSkill()
    content = skill.extract(
        ExtractionInput(
            customer_id=customer_id,
            document_id=document_id,
            document_type=skill.document_type,
            file_name=file_name,
            file_path=file_path,
            raw_text=text,
            metadata={
                "customer_name": customer_name,
                "raw_pages": raw_pages or [],
            },
        )
    )
    if raw_pages:
        content["raw_pages"] = raw_pages
    if file_path:
        content["file_path"] = file_path
    return content
