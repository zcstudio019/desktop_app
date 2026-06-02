from __future__ import annotations

from io import BytesIO
from typing import Any

import pytest
from fastapi import UploadFile

from backend.routers import file as file_router


class FakeJobStorage:
    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, Any]] = {}

    async def create_async_job(self, job_data: dict[str, Any]) -> dict[str, Any]:
        self.jobs[job_data["job_id"]] = dict(job_data)
        return self.jobs[job_data["job_id"]]

    async def get_async_job(self, job_id: str) -> dict[str, Any] | None:
        return self.jobs.get(job_id)

    async def update_async_job(self, job_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        job = self.jobs.setdefault(job_id, {"job_id": job_id, "job_type": file_router.FILE_PROCESS_JOB_TYPE, "username": "admin"})
        job.update(updates)
        return job

    async def get_async_job_execution_payload(self, job_id: str) -> dict[str, Any] | None:
        job = self.jobs.get(job_id) or {}
        payload = job.get("execution_payload_json")
        return payload if isinstance(payload, dict) else None

    async def mark_async_job_dispatched(self, job_id: str, task_id: str, worker_name: str = "") -> dict[str, Any] | None:
        return await self.update_async_job(job_id, {"celery_task_id": task_id, "worker_name": worker_name})


def _upload_file(filename: str = "林勇产证.pdf") -> UploadFile:
    return UploadFile(filename=filename, file=BytesIO(b"%PDF-1.4\n%%EOF"))


@pytest.mark.asyncio
async def test_process_jobs_returns_job_id_without_running_extraction(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_storage = FakeJobStorage()
    called = {"extract": False}

    async def fake_dispatch(job_id: str, current_user_payload: dict[str, Any], customer_id: str) -> tuple[bool, str, str]:
        return True, "", "celery-task-1"

    def forbidden_extract(*_: Any, **__: Any) -> None:
        called["extract"] = True
        raise AssertionError("upload job creation must not run extraction")

    monkeypatch.setattr(file_router, "HAS_DB_STORAGE", True)
    monkeypatch.setattr(file_router, "HAS_ASYNC_JOB_STORAGE", True)
    monkeypatch.setattr(file_router, "job_storage_service", fake_storage)
    monkeypatch.setattr(file_router, "_dispatch_file_process_job", fake_dispatch)
    monkeypatch.setattr("backend.services.kyc_document_agent.KycDocumentAgent.extract", forbidden_extract)

    response = await file_router.create_file_process_job(
        file=_upload_file(),
        documentType="property_cert",
        customerId="customer-1",
        customerName="林勇",
        current_user={"username": "admin", "role": "admin"},
    )

    body = response.body.decode("utf-8")
    assert "jobId" in body
    assert "文件已上传，正在后台处理" in body
    assert called["extract"] is False
    assert len(fake_storage.jobs) == 1


@pytest.mark.asyncio
async def test_process_jobs_collateral_returns_fast_job_id(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_storage = FakeJobStorage()

    async def fake_dispatch(job_id: str, current_user_payload: dict[str, Any], customer_id: str) -> tuple[bool, str, str]:
        return True, "", "celery-task-2"

    monkeypatch.setattr(file_router, "HAS_DB_STORAGE", True)
    monkeypatch.setattr(file_router, "HAS_ASYNC_JOB_STORAGE", True)
    monkeypatch.setattr(file_router, "job_storage_service", fake_storage)
    monkeypatch.setattr(file_router, "_dispatch_file_process_job", fake_dispatch)

    response = await file_router.create_file_process_job(
        file=_upload_file(),
        documentType="collateral",
        customerId="customer-1",
        customerName="林勇",
        current_user={"username": "operator", "role": "operator"},
    )

    assert "jobId" in response.body.decode("utf-8")
    job = next(iter(fake_storage.jobs.values()))
    assert job["execution_payload_json"]["documentType"] == "collateral"


@pytest.mark.asyncio
async def test_task_failure_marks_async_job_failed_and_polling_reads_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_storage = FakeJobStorage()
    job_id = "job-failed"
    await fake_storage.create_async_job({
        "job_id": job_id,
        "job_type": file_router.FILE_PROCESS_JOB_TYPE,
        "username": "admin",
        "status": "pending",
        "execution_payload_json": {"jobId": job_id, "documentType": "collateral"},
    })

    monkeypatch.setattr(file_router, "job_storage_service", fake_storage)

    with pytest.raises(ValueError):
        await file_router.execute_file_process_job_from_job(job_id)

    job = await fake_storage.get_async_job(job_id)
    assert job
    assert job["status"] == "failed"
    assert "missing tempFilePath" in job["error_message"]

    status = await file_router.get_file_process_job(job_id, current_user={"username": "admin", "role": "admin"})
    assert status.status == "failed"
    assert status.errorMessage and "missing tempFilePath" in status.errorMessage
