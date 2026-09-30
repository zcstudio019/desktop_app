"""Financing plan candidate, draft, version and confirmation endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from backend.middleware.auth import get_current_user
from backend.routers.financing_requirement import require_customer_access
from backend.services.financing_plan_service import (
    FinancingPlanDraftInput,
    FinancingPlanError,
    FinancingPlanService,
)
from backend.services.plan_combination_engine import CombinationStrategy
from backend.services.financing_plan_report_service import (
    FinancingPlanReportError, FinancingPlanReportService, render_financing_plan_pdf,
)


router = APIRouter(tags=["Financing Plans"])
plans = FinancingPlanService()
reports = FinancingPlanReportService()


class CandidateRequest(BaseModel):
    customer_id: str
    requirement_id: str
    match_snapshot_id: str


class CombinationRequest(CandidateRequest):
    strategy: CombinationStrategy | None = None


class CombinationDraftRequest(CandidateRequest):
    combination_id: str


class ManualOverrideRequest(CandidateRequest):
    product_match_item_id: int
    new_status: str = "conditional"
    reason: str


class StatusUpdateRequest(BaseModel):
    status: str


class SelectionRequest(BaseModel):
    customer_id: str
    requirement_id: str
    primary_plan_version_id: str | None = None
    backup_plan_version_ids: list[str] = Field(default_factory=list)
    conditional_plan_version_ids: list[str] = Field(default_factory=list)
    notes: str = ""


class CompareVersionsRequest(BaseModel):
    version_a: str
    version_b: str


class GenerateReportRequest(BaseModel):
    selection_id: str
    report_type: str


def _require_write(user: dict) -> None:
    if str(user.get("role") or "").lower() not in {"admin", "operator"}:
        raise HTTPException(status_code=403, detail="当前账号只有查看权限")


async def _call(customer_id: str, user: dict, action):
    await require_customer_access(customer_id, user)
    try:
        return action()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FinancingPlanError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/financing-plans/candidates")
async def build_candidates(request: CandidateRequest, user: dict = Depends(get_current_user)):
    return await _call(request.customer_id, user, lambda: plans.build_plan_candidates(
        request.customer_id, request.requirement_id, request.match_snapshot_id,
    ))


@router.post("/financing-plans/combinations")
async def generate_combinations(request: CombinationRequest, user: dict = Depends(get_current_user)):
    return await _call(request.customer_id, user, lambda: plans.generate_plan_combinations(
        request.customer_id, request.requirement_id, request.match_snapshot_id,
        strategy=request.strategy,
    ))


@router.post("/financing-plans/from-combination")
async def create_from_combination(request: CombinationDraftRequest, user: dict = Depends(get_current_user)):
    _require_write(user)
    return await _call(request.customer_id, user, lambda: plans.create_plan_from_combination(
        request.customer_id, request.requirement_id, request.match_snapshot_id,
        request.combination_id, created_by=str(user.get("username") or ""),
    ))


@router.post("/financing-plans/manual-overrides")
async def create_manual_override(request: ManualOverrideRequest, user: dict = Depends(get_current_user)):
    _require_write(user)
    return await _call(request.customer_id, user, lambda: plans.create_manual_override(
        customer_id=request.customer_id, requirement_id=request.requirement_id,
        match_snapshot_id=request.match_snapshot_id,
        product_match_item_id=request.product_match_item_id, new_status=request.new_status,
        operator_id=str(user.get("username") or ""),
        operator_name=str(user.get("display_name") or user.get("username") or ""), reason=request.reason,
    ))


@router.post("/financing-plans")
async def create_plan(request: FinancingPlanDraftInput, user: dict = Depends(get_current_user)):
    _require_write(user)
    return await _call(request.customer_id, user, lambda: plans.create_plan_draft(
        request, created_by=str(user.get("username") or ""),
    ))


@router.get("/financing-plans/{plan_id}")
async def get_plan(plan_id: str, user: dict = Depends(get_current_user)):
    try:
        result = plans.get_plan(plan_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await require_customer_access(result["customer_id"], user)
    return result


@router.patch("/financing-plans/{plan_id}")
async def update_plan(plan_id: str, request: FinancingPlanDraftInput, user: dict = Depends(get_current_user)):
    _require_write(user)
    return await _call(request.customer_id, user, lambda: plans.update_plan_draft(
        plan_id, request, updated_by=str(user.get("username") or ""),
    ))


@router.get("/financing-plans/{plan_id}/conditions")
async def get_conditions(plan_id: str, user: dict = Depends(get_current_user)):
    current = plans.get_plan(plan_id)
    return await _call(current["customer_id"], user, lambda: plans.get_conditions(plan_id))


@router.patch("/financing-plans/{plan_id}/conditions/{condition_id}")
async def update_condition(plan_id: str, condition_id: str, request: StatusUpdateRequest, user: dict = Depends(get_current_user)):
    _require_write(user)
    current = plans.get_plan(plan_id)
    return await _call(current["customer_id"], user, lambda: plans.update_condition(
        plan_id, condition_id, status=request.status, actor=str(user.get("username") or ""),
    ))


@router.get("/financing-plans/{plan_id}/materials")
async def get_materials(plan_id: str, user: dict = Depends(get_current_user)):
    current = plans.get_plan(plan_id)
    return await _call(current["customer_id"], user, lambda: plans.get_materials(plan_id))


@router.patch("/financing-plans/{plan_id}/materials/{material_id}")
async def update_material(plan_id: str, material_id: str, request: StatusUpdateRequest, user: dict = Depends(get_current_user)):
    _require_write(user)
    current = plans.get_plan(plan_id)
    return await _call(current["customer_id"], user, lambda: plans.update_material(
        plan_id, material_id, status=request.status, actor=str(user.get("username") or ""),
    ))


@router.post("/financing-plans/{plan_id}/validate")
async def validate_plan(plan_id: str, user: dict = Depends(get_current_user)):
    current = plans.get_plan(plan_id)
    return await _call(current["customer_id"], user, lambda: plans.validate_plan_for_confirmation(plan_id))


@router.get("/customers/{customer_id}/financing-plans/latest")
async def get_latest_plan(customer_id: str, user: dict = Depends(get_current_user)):
    await require_customer_access(customer_id, user)
    return {"plan": plans.get_latest(customer_id)}


@router.get("/customers/{customer_id}/financing-plans")
async def list_customer_plans(customer_id: str, requirement_id: str | None = None, user: dict = Depends(get_current_user)):
    await require_customer_access(customer_id, user)
    return {"plans": plans.list_customer_plans(customer_id, requirement_id)}


@router.post("/financing-plans/{plan_id}/confirm")
async def confirm_plan(plan_id: str, user: dict = Depends(get_current_user)):
    _require_write(user)
    try:
        current = plans.get_plan(plan_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return await _call(current["customer_id"], user, lambda: plans.confirm_plan(
        plan_id, confirmed_by=str(user.get("username") or ""),
    ))


@router.post("/financing-plans/{plan_id}/versions")
async def create_version(plan_id: str, request: FinancingPlanDraftInput, user: dict = Depends(get_current_user)):
    _require_write(user)
    return await _call(request.customer_id, user, lambda: plans.create_new_plan_version(
        plan_id, request, created_by=str(user.get("username") or ""),
    ))


@router.get("/financing-plans/{plan_id}/versions")
async def list_versions(plan_id: str, user: dict = Depends(get_current_user)):
    current = plans.get_plan(plan_id)
    await require_customer_access(current["customer_id"], user)
    return {"financing_plan_id": plan_id, "versions": current["versions"]}


@router.post("/financing-plans/selections")
async def create_selection(request: SelectionRequest, user: dict = Depends(get_current_user)):
    _require_write(user)
    await require_customer_access(request.customer_id, user)
    try:
        return reports.create_selection(
            customer_id=request.customer_id, requirement_id=request.requirement_id,
            primary_plan_version_id=request.primary_plan_version_id,
            backup_plan_version_ids=request.backup_plan_version_ids,
            conditional_plan_version_ids=request.conditional_plan_version_ids,
            selected_by=str(user.get("username") or ""), notes=request.notes,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FinancingPlanReportError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/financing-plans/selections/{selection_id}")
async def get_selection(selection_id: str, user: dict = Depends(get_current_user)):
    try:
        result = reports.get_selection(selection_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await require_customer_access(result["customer_id"], user)
    return result


@router.post("/financing-plans/selections/{selection_id}/finalize")
async def finalize_selection(selection_id: str, user: dict = Depends(get_current_user)):
    _require_write(user)
    current = reports.get_selection(selection_id)
    await require_customer_access(current["customer_id"], user)
    try:
        return reports.finalize_selection(selection_id, actor=str(user.get("username") or ""))
    except FinancingPlanReportError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/financing-plans/compare")
async def compare_versions(request: CompareVersionsRequest, user: dict = Depends(get_current_user)):
    try:
        result = reports.compare_plan_versions(request.version_a, request.version_b)
        await require_customer_access(result["customer_id"], user)
        return result
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FinancingPlanReportError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/financing-plan-reports/generate")
async def generate_report(request: GenerateReportRequest, user: dict = Depends(get_current_user)):
    _require_write(user)
    selection = reports.get_selection(request.selection_id)
    await require_customer_access(selection["customer_id"], user)
    try:
        return reports.generate_report(request.selection_id, request.report_type, generated_by=str(user.get("username") or ""))
    except FinancingPlanReportError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


async def _authorized_report(report_id: str, user: dict):
    report = reports.get_report(report_id)
    selection = reports.get_selection(report["plan_selection_id"])
    await require_customer_access(selection["customer_id"], user)
    return report


@router.get("/financing-plan-reports/{report_id}")
async def get_report(report_id: str, user: dict = Depends(get_current_user)):
    return await _authorized_report(report_id, user)


@router.get("/financing-plan-reports/{report_id}/html", response_class=HTMLResponse)
async def get_report_html(report_id: str, user: dict = Depends(get_current_user)):
    report = await _authorized_report(report_id, user)
    return HTMLResponse(report["rendered_html"])


@router.get("/financing-plan-reports/{report_id}/pdf")
async def get_report_pdf(report_id: str, user: dict = Depends(get_current_user)):
    report = await _authorized_report(report_id, user)
    try:
        content = await render_financing_plan_pdf(report["rendered_html"])
    except FinancingPlanReportError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return Response(content=content, media_type="application/pdf", headers={"Content-Disposition": f'attachment; filename="financing-plan-{report_id}.pdf"'})
