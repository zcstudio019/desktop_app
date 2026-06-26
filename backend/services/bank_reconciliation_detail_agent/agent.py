"""Bank reconciliation detail Agent; parsing is delegated to bank_reconciliation_detail_skill."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from backend.extraction_skills import extract_with_skill
from backend.extraction_skills.base import ExtractionInput, ExtractionResult

logger = logging.getLogger(__name__)


class BankReconciliationDetailAgent:
    agent_name = "bank_reconciliation_detail_agent"
    schema_version = "bank_reconciliation_detail.agent.v1"

    def extract(
        self,
        *,
        raw_text: str,
        filename: str,
        customer_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ExtractionResult:
        metadata = metadata or {}
        files = metadata.get("files") if isinstance(metadata.get("files"), list) else []
        if not files and metadata.get("file_path"):
            files = [{"file_path": metadata.get("file_path"), "file_name": filename}]
        logger.info(
            "[BankReconciliationDetailAgent] start received_file_count=%s received_file_paths=%s source_file_names=%s filename=%s metadata_keys=%s",
            len(files),
            [str(item.get("file_path") or item.get("path") or item.get("filePath") or "") for item in files if isinstance(item, dict)],
            [str(item.get("file_name") or item.get("filename") or item.get("fileName") or "") for item in files if isinstance(item, dict)],
            filename,
            sorted(metadata.keys()),
        )
        for item in files:
            if not isinstance(item, dict):
                continue
            path = Path(str(item.get("file_path") or item.get("path") or item.get("filePath") or ""))
            logger.info(
                "[BankReconciliationDetailAgent] file_check path=%s exists=%s suffix=%s",
                path,
                path.exists() if str(path) else False,
                path.suffix.lower(),
            )
        result = extract_with_skill(
            ExtractionInput(
                customer_id=customer_id or "",
                document_id=str(metadata.get("document_id") or ""),
                document_type="bank_reconciliation_detail",
                file_name=filename,
                file_path=str(metadata.get("file_path") or ""),
                mime_type=str(metadata.get("mime_type") or ""),
                raw_text=raw_text,
                metadata=metadata,
            )
        )
        if not isinstance(result, ExtractionResult):
            raise RuntimeError("bank_reconciliation_detail_skill 未注册或未返回有效结果")
        return result
