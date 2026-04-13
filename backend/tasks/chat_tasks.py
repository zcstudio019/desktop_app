"""Celery tasks for chat extraction jobs."""

from __future__ import annotations

import asyncio
import logging

from backend.celery_app import CHAT_EXTRACT_TASK_NAME, celery_app

logger = logging.getLogger(__name__)


@celery_app.task(name=CHAT_EXTRACT_TASK_NAME)
def run_chat_extract_job_task(job_id: str) -> dict[str, str]:
    """Run a chat extraction job by job_id."""
    task_id = getattr(run_chat_extract_job_task.request, "id", "") or ""
    hostname = getattr(run_chat_extract_job_task.request, "hostname", "") or ""
    logger.info(
        "[Celery Chat Job] received job_id=%s celery_task_id=%s worker=%s",
        job_id,
        task_id,
        hostname,
    )
    from backend.routers.chat import execute_chat_extract_job_from_job

    try:
        logger.info(
            "[Celery Chat Job] start job_id=%s celery_task_id=%s worker=%s",
            job_id,
            task_id,
            hostname,
        )
        asyncio.run(execute_chat_extract_job_from_job(job_id))
        logger.info(
            "[Celery Chat Job] success job_id=%s celery_task_id=%s worker=%s",
            job_id,
            task_id,
            hostname,
        )
        return {"job_id": job_id, "status": "submitted"}
    except Exception:
        logger.exception(
            "[Celery Chat Job] failed job_id=%s celery_task_id=%s worker=%s",
            job_id,
            task_id,
            hostname,
        )
        raise
