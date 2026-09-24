"""Customer product matching snapshot endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from backend.middleware.auth import get_current_user
from backend.routers.financing_requirement import require_customer_access
from backend.services.product_matching_service import ProductMatchingError, ProductMatchingService


router = APIRouter(tags=["Product Matching"])
matching = ProductMatchingService()


class ProductMatchingRequest(BaseModel):
    customer_id: str
    requirement_id: str


@router.post("/product-matching/run")
async def run_matching(request: ProductMatchingRequest, user: dict = Depends(get_current_user)):
    await require_customer_access(request.customer_id, user)
    try:
        return await matching.run_product_matching(
            request.customer_id, request.requirement_id, generated_by=str(user.get("username") or ""),
        )
    except ProductMatchingError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/product-matching/{snapshot_id}")
async def get_matching_snapshot(snapshot_id: str, user: dict = Depends(get_current_user)):
    try:
        result = matching.get_snapshot(snapshot_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await require_customer_access(result["customer_id"], user)
    return result


@router.get("/customers/{customer_id}/product-matching/latest")
async def get_latest_matching(customer_id: str, user: dict = Depends(get_current_user)):
    await require_customer_access(customer_id, user)
    return {"snapshot": matching.get_latest(customer_id)}
