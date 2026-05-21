from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from backend.middleware.auth import get_current_user
from backend.services.agents.agent_memory import get_agent_run, get_latest_agent_run
from backend.services.agents.orchestrator import financing_agent_enabled, run_financing_agent_workflow

router = APIRouter(tags=["Financing Agent"])
logger = logging.getLogger(__name__)


class AgentRunRequest(BaseModel):
    task_type: str = "full"


@router.post("/customers/{customer_id}/agent/run")
async def run_customer_agent(
    customer_id: str,
    request: AgentRunRequest,
    _current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    if not financing_agent_enabled():
        return {
            "success": False,
            "status": "disabled",
            "error_message": "ENABLE_FINANCING_AGENT=false，融资 Agent 当前未开启",
        }
    return await run_financing_agent_workflow(customer_id=customer_id, task_type=request.task_type or "full")


@router.get("/customers/{customer_id}/agent/latest")
async def get_latest_customer_agent(
    customer_id: str,
    include_run: bool = Query(default=False),
    _current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    logger.info("[LatestExtraction] start customer_id=%s document_type=%s", customer_id, "agent_latest")
    latest = get_latest_agent_run(customer_id)
    if not latest:
        logger.info("[LatestExtraction] success customer_id=%s document_type=%s selected=false cost_ms=%s", customer_id, "agent_latest", int((time.perf_counter() - started_at) * 1000))
        return {"success": True, "report": None, "status": "not_found"}
    report = latest.get("output_report") or {}
    logger.info(
        "[LatestExtraction] selected document_id=%s extraction_id=%s",
        latest.get("customer_id") or customer_id,
        latest.get("run_id") or "",
    )
    logger.info("[LatestExtraction] success customer_id=%s document_type=%s cost_ms=%s", customer_id, "agent_latest", int((time.perf_counter() - started_at) * 1000))
    payload: dict[str, Any] = {"success": True, "report": report, "status": latest.get("status") or "success"}
    if include_run:
        payload["run"] = latest
    return payload


@router.get("/agent/runs/{run_id}")
async def get_agent_run_detail(
    run_id: str,
    _current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    run = get_agent_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Agent run not found")
    return {"success": True, "run": run, "report": run.get("output_report") or {}, "steps": run.get("steps") or []}
