from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlsplit, urlunsplit

logger = logging.getLogger(__name__)


def mask_broker_url(url: str | None) -> str:
    if not url:
        return ""
    try:
        parsed = urlsplit(url)
        if not parsed.netloc:
            return url
        hostname = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        username = parsed.username or ""
        password = parsed.password or ""
        auth = ""
        if username:
            auth = username
            if password:
                auth += ":***"
            auth += "@"
        netloc = f"{auth}{hostname}{port}"
        return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))
    except Exception:
        return url


def _normalize_registered_tasks(raw_registered: Any) -> dict[str, list[str]]:
    if not isinstance(raw_registered, dict):
        return {}
    normalized: dict[str, list[str]] = {}
    for worker_name, values in raw_registered.items():
        task_names: list[str] = []
        if isinstance(values, list):
            for item in values:
                if isinstance(item, str):
                    task_names.append(item.split(" ", 1)[0].strip())
        normalized[str(worker_name)] = sorted({name for name in task_names if name})
    return normalized


def collect_worker_health(
    celery_app: Any,
    *,
    broker_url: str,
    queue_enabled: bool,
    expected_tasks: tuple[str, ...] | list[str],
) -> dict[str, Any]:
    registered_tasks = sorted(
        name for name in celery_app.tasks.keys() if not str(name).startswith("celery.")
    )
    expected_task_list = [str(task_name) for task_name in expected_tasks]
    expected_registered = {
        task_name: task_name in registered_tasks for task_name in expected_task_list
    }

    ping_ok = False
    ping_response: dict[str, Any] = {}
    worker_registered: dict[str, list[str]] = {}
    missing_tasks = [
        task_name for task_name, is_registered in expected_registered.items() if not is_registered
    ]

    try:
        inspector = celery_app.control.inspect(timeout=2)
        ping_response = inspector.ping() or {}
        ping_ok = bool(ping_response)
        worker_registered = _normalize_registered_tasks(inspector.registered() or {})
        if ping_ok and worker_registered:
            worker_union = sorted(
                {
                    task_name
                    for values in worker_registered.values()
                    for task_name in values
                }
            )
            missing_tasks = [
                task_name for task_name in expected_task_list if task_name not in worker_union
            ]
    except Exception as exc:
        logger.warning("[Worker Health] inspect failed: %s", exc)

    return {
        "queue_enabled": bool(queue_enabled),
        "broker_url": mask_broker_url(broker_url),
        "registered_tasks": registered_tasks,
        "expected_tasks": expected_task_list,
        "missing_tasks": missing_tasks,
        "ping_ok": ping_ok,
        "ping_response": ping_response,
        "workers": worker_registered,
    }
