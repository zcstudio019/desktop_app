"""Small administrator API for the Step 7A publishing layer."""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.middleware.auth import require_admin
from backend.services.feishu_product_import_service import FeishuProductImportService
from backend.services.markdown_product_import_service import SOURCE_DIR, SOURCE_FILES, scan_sources
from backend.services.product_catalog_service import FIELD_TYPES, OPERATORS, CatalogError, ProductCatalogService
from backend.services.product_conflict_service import ProductConflictService

router = APIRouter(prefix="/product-catalog", tags=["Product Catalog"])
catalog = ProductCatalogService()
conflict_manager = ProductConflictService(catalog)
importer = FeishuProductImportService()


class ImportRequest(BaseModel):
    product_category: str
    wiki_node_token: str


class DraftPatch(BaseModel):
    fields: dict[str, Any] = Field(default_factory=dict)


class RuleRequest(BaseModel):
    rule: dict[str, Any]


class ConflictDecisionRequest(BaseModel):
    strategy: str
    field_choices: dict[str, str] = Field(default_factory=dict)
    rename_side: str | None = None
    new_code: str | None = None


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
    current_conflicts = conflict_manager.list_conflicts()
    unresolved_codes = {row["external_product_code"] for row in current_conflicts["items"] if row["conflict"]}
    database_status = "available"
    try:
        versions = catalog.list_versions()
        counts = {name: {"draft": 0, "published": 0, "last_synced_at": None} for name in SOURCE_FILES.values()}
        for item in versions:
            source_file = item["version"].get("source_file")
            if source_file in counts:
                version = item["version"]
                if version["status"] in {"draft", "needs_review"}:
                    counts[source_file]["draft"] += 1
                if version["status"] == "published":
                    counts[source_file]["published"] += 1
                timestamp = version["source_imported_at"]
                if timestamp and (counts[source_file]["last_synced_at"] is None or timestamp > counts[source_file]["last_synced_at"]):
                    counts[source_file]["last_synced_at"] = timestamp
        for summary in summaries:
            summary["published_count"] = counts[summary["source_file"]]["published"]
            summary["draft_count"] = counts[summary["source_file"]]["draft"]
            summary["last_synced_at"] = counts[summary["source_file"]]["last_synced_at"]
    except Exception:
        database_status = "unavailable"
        for summary in summaries:
            summary["published_count"] = None
            summary["draft_count"] = None
            summary["last_synced_at"] = None
    for summary in summaries:
        source = next(s for s in scan["sources"] if s.category == summary["category"])
        summary["conflict_count"] = len({item.external_product_code for item in source.products} & unresolved_codes)
        summary["source_update_date"] = source.products[0].source_update_date.isoformat() if source.products and source.products[0].source_update_date else None
        path = SOURCE_DIR / summary["source_file"]
        summary["source_snapshot_hash"] = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
    return {"sources": summaries, "database_status": database_status,
            "totals": {"parsed_count": scan["parsed_count"], "unique_count": scan["unique_count"],
                       "duplicate_code_count": scan["duplicate_code_count"],
                       "conflict_count": len(unresolved_codes),
                       "needs_review_count": sum(s["needs_review_count"] for s in summaries)}}


@router.get("/sources/conflicts")
@router.get("/conflicts")
def list_local_conflicts(_user: dict = Depends(require_admin)) -> dict[str, Any]:
    return conflict_manager.list_conflicts()


@router.get("/conflicts/{code}")
def get_conflict(code: str, _user: dict = Depends(require_admin)) -> dict[str, Any]:
    try:
        return conflict_manager.get_conflict(code)
    except CatalogError as exc:
        raise _bad_request(exc) from exc


@router.post("/conflicts/{code}/resolve")
def resolve_conflict(code: str, request: ConflictDecisionRequest,
                     user: dict = Depends(require_admin)) -> dict[str, Any]:
    try:
        return conflict_manager.resolve(code, request.model_dump(), user["username"])
    except CatalogError as exc:
        raise _bad_request(exc) from exc


@router.get("/sources/{category}/products")
def list_local_products(category: str, _user: dict = Depends(require_admin)) -> dict[str, Any]:
    if category not in SOURCE_FILES:
        raise HTTPException(status_code=404, detail="产品库分类不存在")
    scan = scan_sources()
    unresolved_codes = {row["external_product_code"] for row in conflict_manager.list_conflicts()["items"] if row["conflict"]}
    source = next(s for s in scan["sources"] if s.category == category)
    items = [{"external_product_code": p.external_product_code, "product_name": p.product_name,
              "institution_name": p.institution_name, "source_file": p.source_file,
              "snapshot_hash": p.source_snapshot_hash, "needs_review": p.needs_review,
              "duplicate_conflict": p.external_product_code in unresolved_codes} for p in source.products]
    return {"items": items, "total": len(items), "missing": source.missing}


@router.get("/products")
def list_products(category: str | None = None, institution: str | None = None, status: str | None = None,
                  needs_review: bool | None = None, search: str | None = None,
                  _user: dict = Depends(require_admin)) -> dict[str, Any]:
    items = catalog.list_products(category=category, institution=institution, status=status,
                                  needs_review=needs_review, search=search)
    return {"items": items, "total": len(items)}


@router.get("/products/{product_id}")
def get_product(product_id: str, _user: dict = Depends(require_admin)) -> dict[str, Any]:
    try:
        return catalog.get_product(product_id)
    except CatalogError as exc:
        raise _bad_request(exc) from exc


@router.get("/products/{product_id}/versions")
def product_versions(product_id: str, _user: dict = Depends(require_admin)) -> dict[str, Any]:
    items = catalog.list_versions(product_id=product_id)
    return {"items": items, "total": len(items)}


@router.get("/rule-options")
def rule_options(_user: dict = Depends(require_admin)) -> dict[str, Any]:
    return {"fields": FIELD_TYPES, "operators": sorted(OPERATORS),
            "severities": ["hard", "soft", "info"], "failure_actions": ["exclude", "conditional", "review"]}


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


@router.get("/versions/{version_id}/rules")
def list_rules(version_id: str, _user: dict = Depends(require_admin)) -> dict[str, Any]:
    try:
        items = catalog.get_version(version_id)["rules"]
        return {"items": items, "total": len(items)}
    except CatalogError as exc:
        raise _bad_request(exc) from exc


@router.patch("/versions/{version_id}/rules/{rule_id}")
def update_rule(version_id: str, rule_id: str, request: RuleRequest,
                _user: dict = Depends(require_admin)) -> dict[str, Any]:
    try:
        return catalog.update_rule(version_id, rule_id, request.rule)
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
