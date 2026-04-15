"""Celery tasks for application generation jobs."""

from __future__ import annotations

from backend.celery_app import APPLICATION_GENERATE_TASK_NAME, celery_app
from backend.tasks.task_runtime import AsyncJobTask, run_async_job


class ApplicationGenerateTask(AsyncJobTask):
    task_label = "Celery Application Job"
    task_display_name = "申请表生成任务"


@celery_app.task(name=APPLICATION_GENERATE_TASK_NAME, bind=True, base=ApplicationGenerateTask)
def run_application_generate_job_task(self: ApplicationGenerateTask, job_id: str) -> dict[str, str]:
    """Run an application generation job by job_id."""
    from backend.routers.application import execute_application_generate_job_from_job

    return run_async_job(
        self,
        job_id=job_id,
        executor=execute_application_generate_job_from_job,
        label=self.task_label,
    )
