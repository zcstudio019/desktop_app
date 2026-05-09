from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def log_step(run_id: str, agent_name: str, status: str, extra: dict[str, Any] | None = None) -> None:
    logger.info("[FinancingAgent] run_id=%s agent=%s status=%s extra=%s", run_id, agent_name, status, extra or {})
