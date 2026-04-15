"""Celery app bootstrap for async job execution."""

from __future__ import annotations

import os
import logging
import importlib

from celery import Celery
from celery.signals import worker_ready

from backend.services.worker_health_service import collect_worker_health

CELERY_TASK_MODULES = (
    "backend.tasks.chat_tasks",
    "backend.tasks.risk_tasks",
    "backend.tasks.scheme_tasks",
    "backend.tasks.application_tasks",
)
CHAT_EXTRACT_TASK_NAME = "backend.tasks.chat_tasks.run_chat_extract_job"
RISK_REPORT_TASK_NAME = "backend.tasks.risk_tasks.run_risk_report_job"
SCHEME_MATCH_TASK_NAME = "backend.tasks.scheme_tasks.run_scheme_match_job"
APPLICATION_GENERATE_TASK_NAME = "backend.tasks.application_tasks.run_application_generate_job"
EXPECTED_TASK_NAMES = (
    CHAT_EXTRACT_TASK_NAME,
    RISK_REPORT_TASK_NAME,
    SCHEME_MATCH_TASK_NAME,
    APPLICATION_GENERATE_TASK_NAME,
)
LEGACY_CHAT_EXTRACT_TASK_NAMES = (
    "backend.tasks.chat.run_chat_extract_job",
)

logger = logging.getLogger(__name__)


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _as_retry_backoff(value: str | None, default: int = 2) -> bool | int:
    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in {"true", "yes", "on"}:
        return True
    if normalized in {"false", "no", "off"}:
        return False
    try:
        parsed = int(normalized)
        return parsed if parsed > 0 else default
    except Exception:
        return default


REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", REDIS_URL)
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", CELERY_BROKER_URL)
TASK_QUEUE_ENABLED = _as_bool(os.getenv("TASK_QUEUE_ENABLED"), default=False)
CELERY_TASK_SOFT_TIME_LIMIT = int(os.getenv("CELERY_TASK_SOFT_TIME_LIMIT", "300"))
CELERY_TASK_TIME_LIMIT = int(os.getenv("CELERY_TASK_TIME_LIMIT", "360"))
CELERY_MAX_RETRIES = int(os.getenv("CELERY_MAX_RETRIES", "3"))
CELERY_RETRY_BACKOFF = _as_retry_backoff(os.getenv("CELERY_RETRY_BACKOFF"), default=2)
CELERY_RETRY_JITTER = _as_bool(os.getenv("CELERY_RETRY_JITTER"), default=True)
WORKER_HEALTHCHECK_ENABLED = _as_bool(os.getenv("WORKER_HEALTHCHECK_ENABLED"), default=True)
JOB_STALE_TIMEOUT_SECONDS = int(os.getenv("JOB_STALE_TIMEOUT_SECONDS", "900"))


celery_app = Celery(
    "loan_assistant",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=CELERY_TASK_MODULES,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Shanghai",
    enable_utc=False,
    task_track_started=True,
    task_ignore_result=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=CELERY_TASK_SOFT_TIME_LIMIT,
    task_time_limit=CELERY_TASK_TIME_LIMIT,
    imports=CELERY_TASK_MODULES,
)

# Keep autodiscovery as a helper, but rely on explicit imports/include for stable
# task registration in production and Windows worker startup.
celery_app.autodiscover_tasks(["backend.tasks"], force=True)

# Explicitly import task modules so worker registration does not depend on
# autodiscovery quirks across Windows / different startup commands.
for module_name in CELERY_TASK_MODULES:
    importlib.import_module(module_name)


def log_celery_bootstrap() -> None:
    registered_tasks = sorted(
        name
        for name in celery_app.tasks.keys()
        if not name.startswith("celery.")
    )
    worker_pid = os.getpid()
    legacy_registered_tasks = [
        name for name in registered_tasks if name in LEGACY_CHAT_EXTRACT_TASK_NAMES
    ]
    expected_task_registered = {
        task_name: task_name in registered_tasks for task_name in EXPECTED_TASK_NAMES
    }
    logger.info(
        "[Celery Bootstrap] pid=%s queue_enabled=%s broker=%s backend=%s expected_tasks=%s expected_registered=%s legacy_registered=%s registered_tasks=%s max_retries=%s retry_backoff=%s retry_jitter=%s soft_time_limit=%s time_limit=%s stale_timeout_seconds=%s",
        worker_pid,
        TASK_QUEUE_ENABLED,
        CELERY_BROKER_URL,
        CELERY_RESULT_BACKEND,
        EXPECTED_TASK_NAMES,
        expected_task_registered,
        legacy_registered_tasks,
        registered_tasks,
        CELERY_MAX_RETRIES,
        CELERY_RETRY_BACKOFF,
        CELERY_RETRY_JITTER,
        CELERY_TASK_SOFT_TIME_LIMIT,
        CELERY_TASK_TIME_LIMIT,
        JOB_STALE_TIMEOUT_SECONDS,
    )
    for task_name, is_registered in expected_task_registered.items():
        if not is_registered:
            logger.warning(
                "[Celery Bootstrap] expected task missing. This worker may be running old code or started from the wrong app module. expected_task=%s pid=%s",
                task_name,
                worker_pid,
            )
    if legacy_registered_tasks:
        logger.warning(
            "[Celery Bootstrap] legacy task names are still registered. Please stop old workers before retrying. legacy_registered=%s pid=%s",
            legacy_registered_tasks,
            worker_pid,
        )

    if WORKER_HEALTHCHECK_ENABLED:
        health = collect_worker_health(
            celery_app,
            broker_url=CELERY_BROKER_URL,
            queue_enabled=TASK_QUEUE_ENABLED,
            expected_tasks=EXPECTED_TASK_NAMES,
        )
        logger.info(
            "[Worker Health] ready queue_enabled=%s broker=%s ping_ok=%s missing_tasks=%s registered_tasks=%s",
            health.get("queue_enabled"),
            health.get("broker_url"),
            health.get("ping_ok"),
            health.get("missing_tasks"),
            health.get("registered_tasks"),
        )
        if health.get("ping_ok"):
            logger.info("[Worker Health] ping ok workers=%s", sorted((health.get("workers") or {}).keys()))
        for task_name in health.get("missing_tasks") or []:
            logger.warning("[Worker Health] missing task %s", task_name)


@worker_ready.connect
def _on_worker_ready(**_: object) -> None:
    log_celery_bootstrap()
