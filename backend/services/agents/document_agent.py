from __future__ import annotations

from typing import Any

from .base import BaseAgent


class DocumentAgent(BaseAgent):
    agent_name = "DocumentAgent"

    async def _run(self, context: dict[str, Any]) -> dict[str, Any]:
        documents = context.get("documents") or []
        extractions = context.get("extractions") or []
        parsed_doc_ids = {str(item.get("doc_id") or "") for item in extractions}
        uploaded = [
            {
                "doc_id": item.get("doc_id") or item.get("id") or "",
                "file_name": item.get("file_name") or item.get("name") or "",
                "document_type": item.get("document_type") or item.get("file_type") or item.get("doc_type") or "",
                "upload_time": item.get("upload_time") or item.get("created_at") or "",
                "parsed": str(item.get("doc_id") or item.get("id") or "") in parsed_doc_ids,
            }
            for item in documents
            if isinstance(item, dict)
        ]
        parsed = [
            {
                "extraction_id": item.get("extraction_id") or item.get("id") or "",
                "doc_id": item.get("doc_id") or "",
                "extraction_type": item.get("extraction_type") or item.get("document_type") or "",
                "created_at": item.get("created_at") or "",
            }
            for item in extractions
            if isinstance(item, dict)
        ]
        required_types = {
            "business_license": "营业执照",
            "enterprise_credit": "企业征信",
            "id_card": "法人身份证",
            "bank_statement": "近12个月银行流水",
            "invoice": "近12个月开票",
            "tax": "近12个月纳税",
            "financial_report": "财务报表",
        }
        present_types = {
            (item.get("document_type") or item.get("file_type") or item.get("doc_type") or item.get("extraction_type") or "")
            for item in [*documents, *extractions]
            if isinstance(item, dict)
        }
        missing = [
            {"document_type": code, "name": label}
            for code, label in required_types.items()
            if code not in present_types
        ]
        return {
            "agent_name": self.agent_name,
            "status": "success",
            "uploaded_documents": uploaded,
            "parsed_documents": parsed,
            "missing_documents": missing,
            "warnings": [] if uploaded else ["当前客户暂无上传资料"],
        }
