"""Customer-scoped financing requirement review endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from backend.middleware.auth import get_current_user
from backend.services import get_storage_service
from backend.services.financing_requirement_service import (
    RequirementPatch, cancel_requirement, confirm_requirement, create_requirement_draft, get_requirement,
)


router = APIRouter(prefix="/customers/{customer_id}/financing-requirements", tags=["Financing Requirements"])


async def require_customer_access(customer_id: str, user: dict):
    storage = get_storage_service()
    customer = await storage.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="客户不存在")
    if user.get("role") != "admin":
        username = str(user.get("username") or "")
        owner = str(customer.get("uploader") or "")
        checker = getattr(storage, "customer_has_document_uploader", None)
        document_access = bool(await checker(customer_id, username)) if callable(checker) and username else False
        if not username or (owner != username and not document_access):
            raise HTTPException(status_code=403, detail="无权访问该客户融资需求")
    return customer


@router.get("/current")
async def current_requirement(customer_id: str, user: dict = Depends(get_current_user)):
    await require_customer_access(customer_id, user)
    return {"requirement": get_requirement(customer_id)}


@router.get("/pending")
async def pending_requirement(customer_id: str, user: dict = Depends(get_current_user)):
    await require_customer_access(customer_id, user)
    return {"requirement": get_requirement(customer_id, status="needs_confirmation")}


@router.post("/draft")
async def draft_requirement(customer_id: str, patch: RequirementPatch, user: dict = Depends(get_current_user)):
    customer = await require_customer_access(customer_id, user)
    try:
        return {"requirement": create_requirement_draft(
            customer_id, patch, user["username"], borrower_name=str(customer.get("name") or ""),
        )}
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/{requirement_id}/confirm")
async def confirm_requirement_route(customer_id: str, requirement_id: str, user: dict = Depends(get_current_user)):
    await require_customer_access(customer_id, user)
    try:
        return {"requirement": confirm_requirement(customer_id, requirement_id, user["username"])}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/{requirement_id}/cancel")
async def cancel_requirement_route(customer_id: str, requirement_id: str, user: dict = Depends(get_current_user)):
    await require_customer_access(customer_id, user)
    try:
        return {"requirement": cancel_requirement(customer_id, requirement_id)}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
