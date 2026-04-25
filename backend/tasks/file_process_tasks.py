"""Celery tasks for upload page file processing jobs."""

from __future__ import annotations

from backend.celery_app import FILE_PROCESS_TASK_NAME, celery_app
from backend.tasks.task_runtime import AsyncJobTask, run_async_job


class FileProcessTask(AsyncJobTask):
    task_label = "Celery File Process Job"
    task_display_name = "上传处理任务"


@celery_app.task(name=FILE_PROCESS_TASK_NAME, bind=True, base=FileProcessTask)
def run_file_process_job_task(self: FileProcessTask, job_id: str) -> dict[str, str]:
    """Run an upload-page file processing job by job_id."""
    from backend.routers.file import execute_file_process_job_from_job

    return run_async_job(
        self,
        job_id=job_id,
        executor=execute_file_process_job_from_job,
        label=self.task_label,
    )
