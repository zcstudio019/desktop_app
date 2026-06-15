"""Celery tasks for URL based document extraction jobs."""

from __future__ import annotations

import logging

from backend.celery_app import URL_EXTRACT_TASK_NAME, celery_app
from backend.tasks.task_runtime import (
    AsyncJobTask,
    format_exception_message,
    run_async_job,
    sync_get_async_job,
    sync_update_async_job,
    utc_now_iso,
)

logger = logging.getLogger(__name__)


class UrlExtractTask(AsyncJobTask):
    task_label = "Celery URL Extract Job"
    task_display_name = "URL 提取任务"


@celery_app.task(name=URL_EXTRACT_TASK_NAME, bind=True, base=UrlExtractTask)
def run_url_extract_job_task(self: UrlExtractTask, job_id: str) -> dict[str, str]:
    """Run a URL document extraction job by job_id."""
    from backend.routers.customer import execute_url_extract_job_from_job

    try:
        sync_update_async_job(
            job_id,
            {
                "status": "running",
                "progress_message": "正在读取链接",
                "started_at": utc_now_iso(),
                "error_message": "",
            },
        )
        result = run_async_job(
            self,
            job_id=job_id,
            executor=execute_url_extract_job_from_job,
            label=self.task_label,
        )
        final_job = sync_get_async_job(job_id) or {}
        normalized = dict(result) if isinstance(result, dict) else {"job_id": job_id}
        normalized["job_id"] = job_id
        normalized["status"] = final_job.get("status") or "success"
        logger.info("[Celery URL Extract Job] finished job_id=%s status=%s", job_id, normalized["status"])
        return normalized
    except Exception as exc:
        sync_update_async_job(
            job_id,
            {
                "status": "failed",
                "progress_message": "提取失败",
                "error_message": format_exception_message(exc),
                "finished_at": utc_now_iso(),
            },
        )
        logger.exception("[Celery URL Extract Job] failed job_id=%s error=%s", job_id, format_exception_message(exc))
        raise
