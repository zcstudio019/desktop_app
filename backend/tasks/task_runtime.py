from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from billiard.exceptions import SoftTimeLimitExceeded
from celery import Task
from fastapi import HTTPException

from backend.celery_app import (
    CELERY_MAX_RETRIES,
    CELERY_RETRY_BACKOFF,
    CELERY_RETRY_JITTER,
)
from services.ai_service import AIServiceError
from services.feishu_service import FeishuNetworkError, FeishuServiceError
from services.wiki_service import WikiServiceError

try:
    from kombu.exceptions import OperationalError as KombuOperationalError
except Exception:  # pragma: no cover - optional import safety
    KombuOperationalError = OSError  # type: ignore[misc,assignment]

logger = logging.getLogger(__name__)

RETRYABLE_EXCEPTIONS = (
    AIServiceError,
    FeishuServiceError,
    FeishuNetworkError,
    WikiServiceError,
    ConnectionError,
    TimeoutError,
    OSError,
    KombuOperationalError,
)
NON_RETRYABLE_EXCEPTIONS = (
    HTTPException,
    ValueError,
    KeyError,
    TypeError,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_timeout_message(label: str) -> str:
    return f"{label}执行超时，请稍后重试。"


def format_exception_message(exc: BaseException) -> str:
    if isinstance(exc, HTTPException):
        detail = getattr(exc, "detail", None)
        if isinstance(detail, str) and detail.strip():
            return detail.strip()
        return "请求处理失败"
    message = str(exc).strip()
    return message or exc.__class__.__name__


async def _update_async_job(job_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
    from backend.services import get_storage_service

    storage_service = get_storage_service()
    return await storage_service.update_async_job(job_id, updates)


async def _get_async_job(job_id: str) -> dict[str, Any] | None:
    from backend.services import get_storage_service

    storage_service = get_storage_service()
    return await storage_service.get_async_job(job_id)


def sync_update_async_job(job_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
    return asyncio.run(_update_async_job(job_id, updates))


def sync_get_async_job(job_id: str) -> dict[str, Any] | None:
    return asyncio.run(_get_async_job(job_id))


def mark_retrying(job_id: str, label: str, exc: BaseException, attempt: int) -> None:
    message = f"{format_exception_message(exc)}，准备第 {attempt} 次重试"
    sync_update_async_job(
        job_id,
        {
            "status": "retrying",
            "progress_message": message,
            "error_message": "",
        },
    )
    logger.warning("[%s] retry job_id=%s attempt=%s reason=%s", label, job_id, attempt, format_exception_message(exc))


def mark_failed(job_id: str, label: str, exc: BaseException, *, progress_message: str | None = None) -> None:
    current_job = sync_get_async_job(job_id)
    if current_job and current_job.get("status") == "success":
        return

    sync_update_async_job(
        job_id,
        {
            "status": "failed",
            "progress_message": progress_message or f"{label}执行失败",
            "error_message": format_exception_message(exc),
            "finished_at": utc_now_iso(),
        },
    )


def mark_timeout(job_id: str, label: str) -> None:
    sync_update_async_job(
        job_id,
        {
            "status": "failed",
            "progress_message": f"{label}执行超时",
            "error_message": build_timeout_message(label),
            "finished_at": utc_now_iso(),
        },
    )


async def load_job_payload_preview(job_id: str) -> dict[str, object]:
    from backend.services import get_storage_service

    storage_service = get_storage_service()
    payload = await storage_service.get_async_job_execution_payload(job_id)
    if not isinstance(payload, dict):
        return {"payload_keys": [], "customer_id": "", "username": ""}
    return {
        "payload_keys": sorted(payload.keys()),
        "customer_id": payload.get("customerId") or "",
        "username": payload.get("username") or "",
    }


class AsyncJobTask(Task):
    abstract = True
    autoretry_for = RETRYABLE_EXCEPTIONS
    dont_autoretry_for = NON_RETRYABLE_EXCEPTIONS + (SoftTimeLimitExceeded,)
    retry_backoff = CELERY_RETRY_BACKOFF
    retry_jitter = CELERY_RETRY_JITTER
    retry_kwargs = {"max_retries": CELERY_MAX_RETRIES}
    task_label = "Celery Job"
    task_display_name = "任务"

    def on_retry(self, exc: BaseException, task_id: str, args: tuple[Any, ...], kwargs: dict[str, Any], einfo: Any) -> None:
        job_id = str(args[0]) if args else ""
        attempt = int(getattr(self.request, "retries", 0))
        if job_id:
            mark_retrying(job_id, self.task_label, exc, attempt)
        super().on_retry(exc, task_id, args, kwargs, einfo)

    def on_failure(
        self,
        exc: BaseException,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        einfo: Any,
    ) -> None:
        job_id = str(args[0]) if args else ""
        if job_id:
            if isinstance(exc, SoftTimeLimitExceeded):
                mark_timeout(job_id, self.task_display_name)
                logger.exception("[%s] timeout job_id=%s celery_task_id=%s", self.task_label, job_id, task_id)
            else:
                mark_failed(job_id, self.task_display_name, exc)
                logger.exception(
                    "[%s] failed job_id=%s celery_task_id=%s error=%s",
                    self.task_label,
                    job_id,
                    task_id,
                    format_exception_message(exc),
                )
        super().on_failure(exc, task_id, args, kwargs, einfo)


def run_async_job(
    task: AsyncJobTask,
    *,
    job_id: str,
    executor: Callable[[str], Awaitable[None]],
    label: str,
) -> dict[str, str]:
    task_id = getattr(task.request, "id", "") or ""
    hostname = getattr(task.request, "hostname", "") or ""
    logger.info("[%s] received job_id=%s celery_task_id=%s worker=%s", label, job_id, task_id, hostname)
    try:
        logger.info("[%s] start job_id=%s celery_task_id=%s worker=%s", label, job_id, task_id, hostname)
        payload_preview = asyncio.run(load_job_payload_preview(job_id))
        logger.info(
            "[%s] payload loaded job_id=%s celery_task_id=%s worker=%s payload_keys=%s customer_id=%s username=%s",
            label,
            job_id,
            task_id,
            hostname,
            payload_preview.get("payload_keys") or [],
            payload_preview.get("customer_id") or "",
            payload_preview.get("username") or "",
        )
        asyncio.run(executor(job_id))
        final_job = sync_get_async_job(job_id)
        if final_job and final_job.get("status") == "failed":
            raise RuntimeError(final_job.get("error_message") or f"{label}内部执行失败")
        if final_job and final_job.get("status") not in {"success"}:
            raise RuntimeError(f"{label}未进入成功状态")
        logger.info("[%s] success job_id=%s celery_task_id=%s worker=%s", label, job_id, task_id, hostname)
        return {"job_id": job_id, "status": "submitted"}
    except SoftTimeLimitExceeded:
        mark_timeout(job_id, task.task_display_name)
        logger.exception("[%s] timeout job_id=%s celery_task_id=%s worker=%s", label, job_id, task_id, hostname)
        raise
    except Exception:
        raise
