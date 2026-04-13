"""Celery app bootstrap for async job execution."""

from __future__ import annotations

import os
import logging

from celery import Celery
from celery.signals import worker_ready

CELERY_TASK_MODULES = (
    "backend.tasks.chat_tasks",
)

logger = logging.getLogger(__name__)


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", REDIS_URL)
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", CELERY_BROKER_URL)
TASK_QUEUE_ENABLED = _as_bool(os.getenv("TASK_QUEUE_ENABLED"), default=False)
CELERY_TASK_SOFT_TIME_LIMIT = int(os.getenv("CELERY_TASK_SOFT_TIME_LIMIT", "300"))
CELERY_TASK_TIME_LIMIT = int(os.getenv("CELERY_TASK_TIME_LIMIT", "360"))


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
    task_soft_time_limit=CELERY_TASK_SOFT_TIME_LIMIT,
    task_time_limit=CELERY_TASK_TIME_LIMIT,
    imports=CELERY_TASK_MODULES,
)

# Keep autodiscovery as a helper, but rely on explicit imports/include for stable
# task registration in production and Windows worker startup.
celery_app.autodiscover_tasks(["backend.tasks"], force=True)


def log_celery_bootstrap() -> None:
    registered_tasks = sorted(
        name
        for name in celery_app.tasks.keys()
        if not name.startswith("celery.")
    )
    logger.info(
        "[Celery Bootstrap] queue_enabled=%s broker=%s backend=%s registered_tasks=%s",
        TASK_QUEUE_ENABLED,
        CELERY_BROKER_URL,
        CELERY_RESULT_BACKEND,
        registered_tasks,
    )


@worker_ready.connect
def _on_worker_ready(**_: object) -> None:
    log_celery_bootstrap()
