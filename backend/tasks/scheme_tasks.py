"""Celery tasks for scheme match jobs."""

from __future__ import annotations

import asyncio
import logging

from backend.celery_app import SCHEME_MATCH_TASK_NAME, celery_app

logger = logging.getLogger(__name__)


@celery_app.task(name=SCHEME_MATCH_TASK_NAME)
def run_scheme_match_job_task(job_id: str) -> dict[str, str]:
    """Run a scheme match job by job_id."""
    task_id = getattr(run_scheme_match_job_task.request, "id", "") or ""
    hostname = getattr(run_scheme_match_job_task.request, "hostname", "") or ""
    logger.info(
        "[Celery Scheme Job] received job_id=%s celery_task_id=%s worker=%s",
        job_id,
        task_id,
        hostname,
    )
    from backend.routers.scheme import execute_scheme_match_job_from_job

    try:
        logger.info(
            "[Celery Scheme Job] start job_id=%s celery_task_id=%s worker=%s",
            job_id,
            task_id,
            hostname,
        )
        payload_preview = asyncio.run(_load_scheme_job_payload_preview(job_id))
        logger.info(
            "[Celery Scheme Job] payload loaded job_id=%s celery_task_id=%s worker=%s payload_keys=%s customer_id=%s username=%s",
            job_id,
            task_id,
            hostname,
            payload_preview.get("payload_keys") or [],
            payload_preview.get("customer_id") or "",
            payload_preview.get("username") or "",
        )
        asyncio.run(execute_scheme_match_job_from_job(job_id))
        logger.info(
            "[Celery Scheme Job] success job_id=%s celery_task_id=%s worker=%s",
            job_id,
            task_id,
            hostname,
        )
        return {"job_id": job_id, "status": "submitted"}
    except Exception:
        logger.exception(
            "[Celery Scheme Job] failed job_id=%s celery_task_id=%s worker=%s",
            job_id,
            task_id,
            hostname,
        )
        raise


async def _load_scheme_job_payload_preview(job_id: str) -> dict[str, object]:
    from backend.services import get_storage_service

    storage_service = get_storage_service()
    payload = await storage_service.get_async_job_execution_payload(job_id)
    if not isinstance(payload, dict):
        return {
            "payload_keys": [],
            "customer_id": "",
            "username": "",
        }
    return {
        "payload_keys": sorted(payload.keys()),
        "customer_id": payload.get("customerId") or "",
        "username": payload.get("username") or "",
    }
