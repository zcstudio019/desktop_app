"""Index rebuild orchestration helpers."""

from __future__ import annotations

import logging
from typing import Any

from .rag_service import RagService

logger = logging.getLogger(__name__)


class IndexRebuildService:
    """Centralized, non-blocking RAG index rebuild wrapper."""

    def __init__(self, rag_service: RagService | None = None) -> None:
        self.rag_service = rag_service or RagService()

    async def rebuild_customer_index(
        self,
        storage_service: Any,
        customer_id: str,
        operation_type: str,
    ) -> dict[str, Any]:
        """Rebuild chunks for a customer with structured logging."""
        logger.info(
            "index_rebuild start customer_id=%s operation_type=%s status=start",
            customer_id,
            operation_type,
        )
        try:
            chunk_count = await self.rag_service.rebuild_customer_index(storage_service, customer_id)
            logger.info(
                "index_rebuild finish customer_id=%s operation_type=%s status=success chunk_count=%s",
                customer_id,
                operation_type,
                chunk_count,
            )
            return {
                "success": True,
                "chunk_count": chunk_count,
            }
        except Exception as exc:
            logger.warning(
                "index_rebuild finish customer_id=%s operation_type=%s status=failed error=%s",
                customer_id,
                operation_type,
                exc,
            )
            return {
                "success": False,
                "chunk_count": 0,
                "error": str(exc),
            }
