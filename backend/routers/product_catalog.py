"""Small administrator API for the Step 7A publishing layer."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.middleware.auth import require_admin
from backend.services.feishu_product_import_service import FeishuProductImportService
from backend.services.product_catalog_service import CatalogError, ProductCatalogService

router = APIRouter(prefix="/product-catalog", tags=["Product Catalog"])
catalog = ProductCatalogService()
importer = FeishuProductImportService()


class ImportRequest(BaseModel):
    product_category: str
    wiki_node_token: str


class DraftPatch(BaseModel):
    fields: dict[str, Any] = Field(default_factory=dict)


class RuleRequest(BaseModel):
    rule: dict[str, Any]


def _bad_request(exc: ValueError) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc))


@router.post("/import")
def import_drafts(request: ImportRequest, user: dict = Depends(require_admin)) -> dict[str, Any]:
    try:
        content, document_token, modified = importer.fetch(request.product_category, request.wiki_node_token)
        modified_at = None
        if isinstance(modified, datetime):
            modified_at = modified
        elif isinstance(modified, str):
            try:
                if modified.isdigit():
                    timestamp = int(modified)
                    modified_at = datetime.fromtimestamp(timestamp / (1000 if timestamp > 10_000_000_000 else 1))
                else:
                    modified_at = datetime.fromisoformat(modified.replace("Z", "+00:00")).replace(tzinfo=None)
            except ValueError:
                pass
        elif isinstance(modified, (int, float)):
            modified_at = datetime.fromtimestamp(modified / (1000 if modified > 10_000_000_000 else 1))
        items = catalog.import_document(request.product_category, request.wiki_node_token,
                                        document_token, content, user["username"], modified_at)
        return {"items": items, "total": len(items), "created": sum(bool(item["created"]) for item in items)}
    except ValueError as exc:
        raise _bad_request(exc) from exc


@router.get("/versions")
def list_versions(product_id: str | None = None, status: str | None = None,
                  _user: dict = Depends(require_admin)) -> dict[str, Any]:
    items = catalog.list_versions(product_id, status)
    return {"items": items, "total": len(items)}


@router.get("/versions/{version_id}")
def get_version(version_id: str, _user: dict = Depends(require_admin)) -> dict[str, Any]:
    try:
        return catalog.get_version(version_id)
    except CatalogError as exc:
        raise _bad_request(exc) from exc


@router.patch("/versions/{version_id}")
def update_draft(version_id: str, patch: DraftPatch, _user: dict = Depends(require_admin)) -> dict[str, Any]:
    try:
        return catalog.update_draft(version_id, patch.fields)
    except CatalogError as exc:
        raise _bad_request(exc) from exc


@router.post("/versions/{version_id}/rules")
def add_rule(version_id: str, request: RuleRequest, _user: dict = Depends(require_admin)) -> dict[str, Any]:
    try:
        return catalog.add_rule(version_id, request.rule)
    except CatalogError as exc:
        raise _bad_request(exc) from exc


@router.delete("/versions/{version_id}/rules/{rule_id}")
def delete_rule(version_id: str, rule_id: str, _user: dict = Depends(require_admin)) -> dict[str, bool]:
    try:
        catalog.delete_rule(version_id, rule_id)
        return {"deleted": True}
    except CatalogError as exc:
        raise _bad_request(exc) from exc


@router.post("/versions/{version_id}/publish")
def publish(version_id: str, user: dict = Depends(require_admin)) -> dict[str, Any]:
    try:
        return catalog.publish(version_id, user["username"])
    except CatalogError as exc:
        raise _bad_request(exc) from exc


@router.post("/versions/{version_id}/disable")
def disable(version_id: str, _user: dict = Depends(require_admin)) -> dict[str, Any]:
    try:
        return catalog.change_lifecycle(version_id, "disabled")
    except CatalogError as exc:
        raise _bad_request(exc) from exc


@router.get("/active")
def active(as_of_date: date | None = None, _user: dict = Depends(require_admin)) -> dict[str, Any]:
    items = catalog.get_active_products(as_of_date or date.today())
    return {"items": items, "total": len(items)}
