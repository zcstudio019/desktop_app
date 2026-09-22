"""Small administrator API for the Step 7A publishing layer."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.middleware.auth import require_admin
from backend.services.feishu_product_import_service import FeishuProductImportService
from backend.services.markdown_product_import_service import SOURCE_FILES, scan_sources
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
@router.post("/import/feishu")
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


@router.get("/sources")
def list_local_sources(_user: dict = Depends(require_admin)) -> dict[str, Any]:
    scan = scan_sources()
    summaries = scan["summaries"]
    database_status = "available"
    try:
        published = catalog.list_versions(status="published")
        counts = {name: 0 for name in SOURCE_FILES.values()}
        for item in published:
            source_file = item["version"].get("source_file")
            if source_file in counts:
                counts[source_file] += 1
        for summary in summaries:
            summary["published_count"] = counts[summary["source_file"]]
    except Exception:
        database_status = "unavailable"
        for summary in summaries:
            summary["published_count"] = None
    return {"sources": summaries, "database_status": database_status,
            "totals": {"parsed_count": scan["parsed_count"], "unique_count": scan["unique_count"],
                       "duplicate_code_count": scan["duplicate_code_count"],
                       "conflict_count": scan["conflict_count"],
                       "needs_review_count": sum(s["needs_review_count"] for s in summaries)}}


@router.get("/sources/conflicts")
def list_local_conflicts(_user: dict = Depends(require_admin)) -> dict[str, Any]:
    conflicts = scan_sources()["conflicts"]
    return {"items": conflicts, "total": len(conflicts)}


@router.get("/sources/{category}/products")
def list_local_products(category: str, _user: dict = Depends(require_admin)) -> dict[str, Any]:
    if category not in SOURCE_FILES:
        raise HTTPException(status_code=404, detail="产品库分类不存在")
    scan = scan_sources()
    source = next(s for s in scan["sources"] if s.category == category)
    items = [{"external_product_code": p.external_product_code, "product_name": p.product_name,
              "institution_name": p.institution_name, "source_file": p.source_file,
              "snapshot_hash": p.source_snapshot_hash, "needs_review": p.needs_review,
              "duplicate_conflict": p.external_product_code in scan["conflicting_codes"]} for p in source.products]
    return {"items": items, "total": len(items), "missing": source.missing}


@router.post("/sync-local")
def sync_local_all(user: dict = Depends(require_admin)) -> dict[str, Any]:
    try:
        return catalog.import_markdown_sources(user["username"])
    except CatalogError as exc:
        raise _bad_request(exc) from exc


@router.post("/sources/{category}/sync")
def sync_local_category(category: str, user: dict = Depends(require_admin)) -> dict[str, Any]:
    try:
        return catalog.import_markdown_sources(user["username"], category)
    except CatalogError as exc:
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
