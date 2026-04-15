from __future__ import annotations

import json
import sys

from backend.celery_app import (
    CELERY_BROKER_URL,
    EXPECTED_TASK_NAMES,
    TASK_QUEUE_ENABLED,
    celery_app,
)
from backend.services.worker_health_service import collect_worker_health


def main() -> int:
    health = collect_worker_health(
        celery_app,
        broker_url=CELERY_BROKER_URL,
        queue_enabled=TASK_QUEUE_ENABLED,
        expected_tasks=EXPECTED_TASK_NAMES,
    )
    print(json.dumps(health, ensure_ascii=False, indent=2))

    if not health.get("queue_enabled"):
        return 0
    if not health.get("ping_ok"):
        return 1
    if health.get("missing_tasks"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
