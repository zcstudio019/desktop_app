from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from fastapi import UploadFile

from backend.routers import file as file_router


class FakeJobStorage:
    def __init__(self, *, fail_create: bool = False) -> None:
        self.fail_create = fail_create
        self.jobs: dict[str, dict[str, Any]] = {}

    async def create_async_job(self, job_data: dict[str, Any]) -> dict[str, Any]:
        if self.fail_create:
            raise RuntimeError("async_jobs write failed")
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


def _response_json(response: Any) -> dict[str, Any]:
    return json.loads(response.body.decode("utf-8"))


@pytest.mark.asyncio
async def test_jobs_response_success_returns_job_id_status_message(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_storage = FakeJobStorage()

    async def fake_dispatch(job_id: str, current_user_payload: dict[str, Any], customer_id: str) -> tuple[bool, str, str]:
        return True, "", "celery-task-ok"

    monkeypatch.setattr(file_router, "HAS_DB_STORAGE", True)
    monkeypatch.setattr(file_router, "HAS_ASYNC_JOB_STORAGE", True)
    monkeypatch.setattr(file_router, "job_storage_service", fake_storage)
    monkeypatch.setattr(file_router, "_dispatch_file_process_job", fake_dispatch)

    response = await file_router.create_file_process_job(
        file=_upload_file(),
        documentType="property_cert",
        customerId="enterprise_上海耐吉电力集团有限公司",
        customerName="上海耐吉电力集团有限公司",
        current_user={"username": "admin", "role": "admin"},
    )
    body = _response_json(response)

    assert body["job_id"]
    assert body["success"] is True
    assert body["document_id"] is None
    assert body["status"] == "pending"
    assert body["message"] == "文件已上传，正在后台处理"
    assert "jobId" not in body


@pytest.mark.asyncio
async def test_jobs_response_accepts_collateral_document_type(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_storage = FakeJobStorage()

    async def fake_dispatch(job_id: str, current_user_payload: dict[str, Any], customer_id: str) -> tuple[bool, str, str]:
        return True, "", "celery-task-collateral"

    monkeypatch.setattr(file_router, "HAS_DB_STORAGE", True)
    monkeypatch.setattr(file_router, "HAS_ASYNC_JOB_STORAGE", True)
    monkeypatch.setattr(file_router, "job_storage_service", fake_storage)
    monkeypatch.setattr(file_router, "_dispatch_file_process_job", fake_dispatch)

    response = await file_router.create_file_process_job(
        file=_upload_file(),
        documentType="collateral",
        customerId="enterprise_上海耐吉电力集团有限公司",
        customerName="上海耐吉电力集团有限公司",
        current_user={"username": "operator", "role": "operator"},
    )
    body = _response_json(response)
    job = next(iter(fake_storage.jobs.values()))

    assert body["job_id"]
    assert job["execution_payload_json"]["documentType"] == "collateral"


@pytest.mark.asyncio
async def test_jobs_response_accepts_snake_case_form_names(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_storage = FakeJobStorage()

    async def fake_dispatch(job_id: str, current_user_payload: dict[str, Any], customer_id: str) -> tuple[bool, str, str]:
        return True, "", "celery-task-snake"

    monkeypatch.setattr(file_router, "HAS_DB_STORAGE", True)
    monkeypatch.setattr(file_router, "HAS_ASYNC_JOB_STORAGE", True)
    monkeypatch.setattr(file_router, "job_storage_service", fake_storage)
    monkeypatch.setattr(file_router, "_dispatch_file_process_job", fake_dispatch)

    response = await file_router.create_file_process_job(
        file=_upload_file(),
        document_type="collateral",
        customer_id="enterprise_上海耐吉电力集团有限公司",
        customer_name="上海耐吉电力集团有限公司",
        current_user={"username": "admin", "role": "admin"},
    )
    body = _response_json(response)
    job = next(iter(fake_storage.jobs.values()))

    assert body["job_id"]
    assert job["execution_payload_json"]["documentType"] == "collateral"
    assert job["execution_payload_json"]["customerId"] == "enterprise_上海耐吉电力集团有限公司"


@pytest.mark.asyncio
async def test_jobs_response_async_job_create_failure_returns_json_error(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_storage = FakeJobStorage(fail_create=True)

    monkeypatch.setattr(file_router, "HAS_DB_STORAGE", True)
    monkeypatch.setattr(file_router, "HAS_ASYNC_JOB_STORAGE", True)
    monkeypatch.setattr(file_router, "job_storage_service", fake_storage)

    response = await file_router.create_file_process_job(
        file=_upload_file(),
        documentType="collateral",
        customerId="enterprise_上海耐吉电力集团有限公司",
        customerName="上海耐吉电力集团有限公司",
        current_user={"username": "admin", "role": "admin"},
    )
    body = _response_json(response)

    assert response.status_code == 500
    assert body["detail"] == "上传成功，但解析任务创建失败，请联系管理员。错误阶段：create_async_job"
    assert body["job_id"]
    assert body["error_message"]


@pytest.mark.asyncio
async def test_jobs_response_celery_dispatch_failure_returns_json_error(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_storage = FakeJobStorage()

    async def failing_dispatch(job_id: str, current_user_payload: dict[str, Any], customer_id: str) -> tuple[bool, str, str]:
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(file_router, "HAS_DB_STORAGE", True)
    monkeypatch.setattr(file_router, "HAS_ASYNC_JOB_STORAGE", True)
    monkeypatch.setattr(file_router, "job_storage_service", fake_storage)
    monkeypatch.setattr(file_router, "_dispatch_file_process_job", failing_dispatch)

    response = await file_router.create_file_process_job(
        file=_upload_file(),
        documentType="collateral",
        customerId="enterprise_上海耐吉电力集团有限公司",
        customerName="上海耐吉电力集团有限公司",
        current_user={"username": "admin", "role": "admin"},
    )
    body = _response_json(response)
    job = await fake_storage.get_async_job(body["job_id"])

    assert response.status_code == 503
    assert body["detail"] == "上传成功，但解析任务投递失败，请检查 Celery heavy worker。错误阶段：enqueue_celery"
    assert body["error_message"] == "redis unavailable"
    assert job and job["status"] == "failed"


def test_frontend_normalize_job_id_supports_all_expected_shapes() -> None:
    source = Path("src/services/api.ts").read_text(encoding="utf-8")

    assert "export function normalizeJobId" in source
    assert "record.job_id" in source
    assert "record.jobId" in source
    assert "record.id" in source
    assert "nested.job_id" in source
    assert "nested.jobId" in source
