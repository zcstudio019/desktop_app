"""Celery tasks for background customer index rebuilds."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from backend.celery_app import HEAVY_QUEUE_NAME, INDEX_REBUILD_TASK_NAME, celery_app
from backend.services import get_storage_service
from backend.services.index_rebuild_service import IndexRebuildService
from backend.services.markdown_profile_service import regenerate_customer_profile
from backend.services.profile_sync_service import ProfileSyncService

logger = logging.getLogger(__name__)


async def _run_index_rebuild(
    customer_id: str,
    reason: str,
    source_document_id: str = "",
) -> dict[str, Any]:
    storage_service = get_storage_service()
    logger.info(
        "[Index Rebuild Task] start customer_id=%s reason=%s source_document_id=%s",
        customer_id,
        reason,
        source_document_id,
    )
    await regenerate_customer_profile(storage_service, customer_id)
    result = await IndexRebuildService().rebuild_customer_index(storage_service, customer_id, reason)
    await ProfileSyncService().mark_customer_applications_stale(storage_service, customer_id)
    if not result.get("success"):
        raise RuntimeError(str(result.get("error") or "index rebuild failed"))
    logger.info(
        "[Index Rebuild Task] finish customer_id=%s reason=%s source_document_id=%s chunk_count=%s",
        customer_id,
        reason,
        source_document_id,
        result.get("chunk_count") or 0,
    )
    return result


@celery_app.task(name=INDEX_REBUILD_TASK_NAME, bind=True, max_retries=2, default_retry_delay=30, queue=HEAVY_QUEUE_NAME)
def rebuild_customer_index_task(
    self: Any,
    customer_id: str,
    reason: str = "document_saved",
    source_document_id: str | None = None,
) -> dict[str, Any]:
    """Rebuild customer RAG index without blocking document parsing jobs."""
    try:
        return asyncio.run(_run_index_rebuild(str(customer_id or ""), str(reason or "document_saved"), str(source_document_id or "")))
    except Exception as exc:
        logger.exception(
            "[Index Rebuild Task] failed customer_id=%s reason=%s source_document_id=%s error=%s",
            customer_id,
            reason,
            source_document_id or "",
            exc,
        )
        raise self.retry(exc=exc)
