"""Celery tasks for scheme match jobs."""

from __future__ import annotations

from backend.celery_app import SCHEME_MATCH_TASK_NAME, celery_app
from backend.tasks.task_runtime import AsyncJobTask, run_async_job


class SchemeMatchTask(AsyncJobTask):
    task_label = "Celery Scheme Job"
    task_display_name = "方案匹配任务"


@celery_app.task(name=SCHEME_MATCH_TASK_NAME, bind=True, base=SchemeMatchTask)
def run_scheme_match_job_task(self: SchemeMatchTask, job_id: str) -> dict[str, str]:
    """Run a scheme match job by job_id."""
    from backend.routers.scheme import execute_scheme_match_job_from_job

    return run_async_job(
        self,
        job_id=job_id,
        executor=execute_scheme_match_job_from_job,
        label=self.task_label,
    )
