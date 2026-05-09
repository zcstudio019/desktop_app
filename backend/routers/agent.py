from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from backend.middleware.auth import get_current_user
from backend.services.agents.agent_memory import get_agent_run, get_latest_agent_run
from backend.services.agents.orchestrator import financing_agent_enabled, run_financing_agent_workflow

router = APIRouter(tags=["Financing Agent"])


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
    _current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    latest = get_latest_agent_run(customer_id)
    if not latest:
        return {"success": True, "report": None, "status": "not_found"}
    return {"success": True, "report": latest.get("output_report") or {}, "run": latest}


@router.get("/agent/runs/{run_id}")
async def get_agent_run_detail(
    run_id: str,
    _current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    run = get_agent_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Agent run not found")
    return {"success": True, "run": run, "report": run.get("output_report") or {}, "steps": run.get("steps") or []}
