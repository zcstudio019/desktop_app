"""Celery tasks for customer risk report jobs."""

from __future__ import annotations

from backend.celery_app import RISK_REPORT_TASK_NAME, celery_app
from backend.tasks.task_runtime import AsyncJobTask, run_async_job


class RiskReportTask(AsyncJobTask):
    task_label = "Celery Risk Job"
    task_display_name = "风险报告任务"


@celery_app.task(name=RISK_REPORT_TASK_NAME, bind=True, base=RiskReportTask)
def run_risk_report_job_task(self: RiskReportTask, job_id: str) -> dict[str, str]:
    """Run a customer risk report job by job_id."""
    from backend.routers.customer import execute_customer_risk_report_job_from_job

    return run_async_job(
        self,
        job_id=job_id,
        executor=execute_customer_risk_report_job_from_job,
        label=self.task_label,
    )
