"""Celery tasks for chat extraction jobs."""

from __future__ import annotations

import asyncio
import logging

from backend.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(name="backend.tasks.chat.run_chat_extract_job")
def run_chat_extract_job_task(job_id: str) -> dict[str, str]:
    """Run a chat extraction job by job_id."""
    logger.info("[Celery Chat Job] received job_id=%s", job_id)
    from backend.routers.chat import execute_chat_extract_job_from_job

    asyncio.run(execute_chat_extract_job_from_job(job_id))
    return {"job_id": job_id, "status": "submitted"}
