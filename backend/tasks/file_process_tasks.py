"""Celery tasks for upload page file processing jobs."""

from __future__ import annotations

import logging

from backend.celery_app import FILE_PROCESS_TASK_NAME, celery_app
from backend.tasks.task_runtime import (
    AsyncJobTask,
    format_exception_message,
    run_async_job,
    sync_get_async_job,
    sync_update_async_job,
    utc_now_iso,
)

logger = logging.getLogger(__name__)


class FileProcessTask(AsyncJobTask):
    task_label = "Celery File Process Job"
    task_display_name = "上传处理任务"


@celery_app.task(name=FILE_PROCESS_TASK_NAME, bind=True, base=FileProcessTask)
def run_file_process_job_task(self: FileProcessTask, job_id: str) -> dict[str, str]:
    """Run an upload-page file processing job by job_id."""
    from backend.routers.file import execute_file_process_job_from_job

    try:
        payload = sync_get_async_job(job_id) or {}
        execution_payload = payload.get("execution_payload_json") if isinstance(payload.get("execution_payload_json"), dict) else {}
        logger.info(
            "[Celery File Process Job] start job_id=%s document_id=%s document_type=%s agent_type=%s status=running error_message=",
            job_id,
            "",
            execution_payload.get("documentType") or "",
            "",
        )
        sync_update_async_job(
            job_id,
            {
                "status": "running",
                "progress_message": "正在处理文件",
                "started_at": utc_now_iso(),
                "error_message": "",
            },
        )
        logger.info("[Celery File Process Job] job status updated to running job_id=%s", job_id)

        task_result = run_async_job(
            self,
            job_id=job_id,
            executor=execute_file_process_job_from_job,
            label=self.task_label,
        )

        final_job = sync_get_async_job(job_id) or {}
        final_result_json = final_job.get("result_json") if isinstance(final_job.get("result_json"), dict) else {}
        final_job_type = str(final_job.get("job_type") or "")
        final_progress_message = (
            final_job.get("progress_message")
            if final_job_type == "contract_extract" and final_job.get("progress_message")
            else "处理完成"
        )
        sync_update_async_job(
            job_id,
            {
                "status": "success",
                "progress_message": final_progress_message,
                "result_json": final_result_json,
                "error_message": "",
                "finished_at": final_job.get("finished_at") or utc_now_iso(),
            },
        )
        result_json = final_result_json
        content = result_json.get("content") if isinstance(result_json.get("content"), dict) else {}
        logger.info(
            "[Celery File Process Job] job status updated to success job_id=%s document_id=%s document_type=%s agent_type=%s status=success error_message=",
            job_id,
            result_json.get("documentId") or result_json.get("document_id") or "",
            result_json.get("documentType") or "",
            content.get("agent_type") or "",
        )

        normalized_result = dict(task_result) if isinstance(task_result, dict) else {"job_id": job_id}
        normalized_result["job_id"] = job_id
        normalized_result["status"] = "success"
        return normalized_result
    except Exception as exc:
        sync_update_async_job(
            job_id,
            {
                "status": "failed",
                "progress_message": "处理失败",
                "error_message": format_exception_message(exc),
                "finished_at": utc_now_iso(),
            },
        )
        logger.exception(
            "[Celery File Process Job] job status updated to failed job_id=%s document_id=%s document_type=%s agent_type=%s status=failed error_message=%s",
            job_id,
            "",
            "",
            "",
            format_exception_message(exc),
        )
        raise
