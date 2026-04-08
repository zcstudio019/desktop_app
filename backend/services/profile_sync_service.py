"""Profile/application/scheme sync orchestration for customer-scoped flows."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from .index_rebuild_service import IndexRebuildService
from .markdown_profile_service import regenerate_customer_profile

logger = logging.getLogger(__name__)


class ProfileSyncService:
    """Coordinate customer profile refreshes and downstream index updates."""

    def __init__(self, index_rebuild_service: IndexRebuildService | None = None) -> None:
        self.index_rebuild_service = index_rebuild_service or IndexRebuildService()

    async def _get_profile_meta(self, storage_service: Any, customer_id: str) -> tuple[int, str]:
        try:
            profile = await storage_service.get_customer_profile(customer_id)
        except Exception as exc:
            logger.warning(
                "application_summary profile_meta_failed customer_id=%s operation_type=profile_meta_load status=failed error=%s",
                customer_id,
                exc,
            )
            return 1, ""
        if not profile:
            return 1, ""
        return profile.get("version") or 1, profile.get("updated_at") or ""

    async def mark_customer_applications_stale(
        self,
        storage_service: Any,
        customer_id: str,
        reason: str = "客户资料已更新，请重新生成申请表。",
    ) -> dict[str, Any]:
        profile_version, profile_updated_at = await self._get_profile_meta(storage_service, customer_id)
        stale_at = datetime.now(tz=timezone.utc).isoformat()
        updated = await storage_service.mark_customer_applications_stale(
            customer_id=customer_id,
            reason=reason,
            stale_at=stale_at,
            profile_version=profile_version,
            profile_updated_at=profile_updated_at,
        )

        if updated:
            logger.info(
                "application_summary stale_marked customer_id=%s operation_type=document_saved status=success updated=%s",
                customer_id,
                updated,
            )

        return {
            "updated": updated,
            "stale_at": stale_at if updated else "",
            "profile_version": profile_version,
            "profile_updated_at": profile_updated_at,
            "reason": reason,
        }

    async def refresh_profile_and_index(
        self,
        storage_service: Any,
        customer_id: str,
        operation_type: str,
        refresh_profile: bool = True,
    ) -> dict[str, Any]:
        profile_result = {"success": True}
        if refresh_profile:
            logger.info("profile_sync start customer_id=%s operation_type=%s status=start", customer_id, operation_type)
            try:
                await regenerate_customer_profile(storage_service, customer_id)
                logger.info("profile_sync finish customer_id=%s operation_type=%s status=success", customer_id, operation_type)
            except Exception as exc:
                profile_result = {"success": False, "error": str(exc)}
                logger.warning(
                    "profile_sync finish customer_id=%s operation_type=%s status=failed error=%s",
                    customer_id,
                    operation_type,
                    exc,
                )

        index_result = await self.index_rebuild_service.rebuild_customer_index(storage_service, customer_id, operation_type)
        return {"profile": profile_result, "index": index_result}

    async def handle_document_saved(self, storage_service: Any, customer_id: str) -> dict[str, Any]:
        sync_result = await self.refresh_profile_and_index(
            storage_service,
            customer_id,
            operation_type="document_saved",
            refresh_profile=True,
        )
        stale_result = await self.mark_customer_applications_stale(storage_service, customer_id)
        return {**sync_result, "application_stale": stale_result}

    async def handle_profile_markdown_saved(self, storage_service: Any, customer_id: str) -> dict[str, Any]:
        return await self.refresh_profile_and_index(
            storage_service,
            customer_id,
            operation_type="profile_markdown_saved",
            refresh_profile=False,
        )

    async def save_application_summary(
        self,
        customer_name: str,
        customer_id: str | None,
        loan_type: str,
        application_data: dict[str, Any],
        storage_service: Any | None = None,
        owner_username: str = "",
    ) -> dict[str, Any] | None:
        if not customer_id or storage_service is None:
            logger.warning(
                "application_summary save_skipped customer_id=%s operation_type=application_generated status=skipped reason=missing_customer_id",
                customer_id,
            )
            return None

        saved_at = datetime.now(tz=timezone.utc).isoformat()
        profile_version, profile_updated_at = await self._get_profile_meta(storage_service, customer_id)
        application_record = {
            "id": str(uuid.uuid4()),
            "customerName": customer_name,
            "customerId": customer_id,
            "loanType": loan_type,
            "applicationData": application_data,
            "savedAt": saved_at,
            "ownerUsername": owner_username,
            "source": "auto_sync",
            "stale": False,
            "stale_reason": "",
            "stale_at": "",
            "profile_version": profile_version,
            "profile_updated_at": profile_updated_at,
        }
        await storage_service.save_application_record(application_record)
        logger.info(
            "application_summary finish customer_id=%s operation_type=application_generated status=success application_id=%s",
            customer_id,
            application_record["id"],
        )
        return application_record

    async def handle_application_generated(
        self,
        storage_service: Any,
        customer_name: str,
        customer_id: str | None,
        loan_type: str,
        application_data: dict[str, Any],
        owner_username: str = "",
    ) -> dict[str, Any]:
        record = await self.save_application_summary(
            customer_name=customer_name,
            customer_id=customer_id,
            loan_type=loan_type,
            application_data=application_data,
            storage_service=storage_service,
            owner_username=owner_username,
        )
        if not customer_id:
            return {"application": record, "sync": None}
        sync_result = await self.refresh_profile_and_index(
            storage_service,
            customer_id,
            operation_type="application_generated",
            refresh_profile=True,
        )
        return {"application": record, "sync": sync_result}

    async def handle_scheme_matched(
        self,
        storage_service: Any,
        customer_id: str | None,
        customer_name: str,
        match_result: str,
    ) -> dict[str, Any]:
        if not customer_id:
            logger.warning(
                "scheme_snapshot save_skipped customer_id=%s operation_type=scheme_matched status=skipped reason=missing_customer_id",
                customer_id,
            )
            return {"snapshot": None, "sync": None}

        snapshot = await storage_service.save_scheme_snapshot(
            {
                "snapshot_id": str(uuid.uuid4()),
                "customer_id": customer_id,
                "customer_name": customer_name,
                "summary_markdown": match_result,
                "raw_result": match_result,
                "source": "scheme_match",
            }
        )
        logger.info(
            "scheme_snapshot finish customer_id=%s operation_type=scheme_matched status=success snapshot_id=%s",
            customer_id,
            "unknown" if not snapshot else snapshot.get("snapshot_id") or "",
        )
        sync_result = await self.refresh_profile_and_index(
            storage_service,
            customer_id,
            operation_type="scheme_matched",
            refresh_profile=True,
        )
        return {"snapshot": snapshot, "sync": sync_result}
