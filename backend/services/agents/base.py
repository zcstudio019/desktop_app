from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from .schemas import AgentStepResult, utc_now

logger = logging.getLogger(__name__)


class BaseAgent:
    agent_name = "BaseAgent"

    async def run(self, context: dict[str, Any]) -> AgentStepResult:
        started = utc_now()
        started_dt = datetime.fromisoformat(started)
        try:
            output = await self._run(context)
            completed = utc_now()
            return AgentStepResult(
                agent_name=self.agent_name,
                status="success",
                output=output or {},
                warnings=list((output or {}).get("warnings") or []),
                started_at=started,
                completed_at=completed,
                duration_ms=_duration_ms(started_dt, completed),
            )
        except Exception as exc:
            logger.exception("[FinancingAgent] %s failed", self.agent_name)
            completed = utc_now()
            return AgentStepResult(
                agent_name=self.agent_name,
                status="failed",
                output={},
                warnings=[],
                error_message=str(exc),
                started_at=started,
                completed_at=completed,
                duration_ms=_duration_ms(started_dt, completed),
            )

    async def _run(self, context: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


def _duration_ms(started: datetime, completed: str) -> int:
    try:
        completed_dt = datetime.fromisoformat(completed)
        return max(0, int((completed_dt - started).total_seconds() * 1000))
    except Exception:
        return 0
