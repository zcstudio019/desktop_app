from __future__ import annotations

import logging
from typing import Any

from .schema import SCHEMA_VERSION

logger = logging.getLogger(__name__)


def company_articles_payload_version(payload: dict[str, Any]) -> str:
    structured = payload.get("structured_data") if isinstance(payload.get("structured_data"), dict) else {}
    return str(
        payload.get("extraction_version")
        or payload.get("schema_version")
        or structured.get("extraction_version")
        or structured.get("schema_version")
        or ""
    ).strip()


def is_current_company_articles_payload(payload: dict[str, Any]) -> bool:
    return company_articles_payload_version(payload) == SCHEMA_VERSION


def refresh_stale_company_articles_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Re-extract stale saved payloads when their persisted OCR source is available."""
    if is_current_company_articles_payload(payload):
        return payload
    raw_pages = payload.get("raw_pages") if isinstance(payload.get("raw_pages"), list) else []
    raw_text = str(payload.get("raw_text") or payload.get("raw_text_full") or "")
    if not raw_text and raw_pages:
        raw_text = "\n\n".join(
            str(page.get("text") or "") for page in raw_pages if isinstance(page, dict)
        )
    old_version = company_articles_payload_version(payload) or "(missing)"
    if not raw_text.strip():
        logger.warning(
            "[CompanyArticles][VersionGate] stale_version=%s current_version=%s reextract=false reason=raw_source_missing",
            old_version,
            SCHEMA_VERSION,
        )
        return None

    from .agent import CompanyArticlesAgent

    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    logger.info(
        "[CompanyArticles][VersionGate] stale_version=%s current_version=%s reextract=true",
        old_version,
        SCHEMA_VERSION,
    )
    return CompanyArticlesAgent().run(
        {
            "text": raw_text,
            "raw_pages": raw_pages,
            "filename": str(
                payload.get("source_file")
                or payload.get("filename")
                or metadata.get("filename")
                or ""
            ),
            "source": "stale_version_reextract",
        }
    ).to_dict()
