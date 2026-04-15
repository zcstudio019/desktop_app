"""Celery tasks for chat extraction jobs."""

from __future__ import annotations

from backend.celery_app import CHAT_EXTRACT_TASK_NAME, celery_app
from backend.tasks.task_runtime import AsyncJobTask, run_async_job


class ChatExtractTask(AsyncJobTask):
    task_label = "Celery Chat Job"
    task_display_name = "资料提取任务"


@celery_app.task(name=CHAT_EXTRACT_TASK_NAME, bind=True, base=ChatExtractTask)
def run_chat_extract_job_task(self: ChatExtractTask, job_id: str) -> dict[str, str]:
    """Run a chat extraction job by job_id."""
    from backend.routers.chat import execute_chat_extract_job_from_job

    return run_async_job(
        self,
        job_id=job_id,
        executor=execute_chat_extract_job_from_job,
        label=self.task_label,
    )
