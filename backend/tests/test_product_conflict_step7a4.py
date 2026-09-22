from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.database import Base
from backend.db_models import FinancingProduct, FinancingProductConflict, FinancingProductRule, FinancingProductVersion
from backend.services.markdown_product_import_service import SOURCE_FILES, scan_sources
from backend.services.product_catalog_service import CatalogError, ProductCatalogService
from backend.services.product_conflict_service import ProductConflictService
from backend.routers import product_catalog as routes
from backend.middleware.auth import create_access_token


def product(name: str, amount: int) -> str:
    return (f"## 1. 【NJB-001】 {name}\n- 机构：南京银行\n"
            "| 字段 | 内容 |\n| --- | --- |\n"
            f"| 最高额度 | 最高{amount}万元 |\n| 征信要求 | 征信良好 |\n")


@pytest.fixture
def setup(tmp_path, monkeypatch):
    for filename in SOURCE_FILES.values():
        (tmp_path / filename).write_text("# 空产品库\n产品数量：0款\n", encoding="utf-8")
    (tmp_path / SOURCE_FILES["technology_enterprise"]).write_text(product("产品 A", 500), encoding="utf-8")
    (tmp_path / SOURCE_FILES["enterprise_credit"]).write_text(product("产品 B", 800), encoding="utf-8")
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[FinancingProduct.__table__, FinancingProductVersion.__table__, FinancingProductRule.__table__, FinancingProductConflict.__table__])
    catalog = ProductCatalogService(sessionmaker(bind=engine), ensure_schema=False)
    manager = ProductConflictService(catalog, directory=tmp_path)
    monkeypatch.setattr("backend.services.product_catalog_service.scan_sources", lambda directory=tmp_path: scan_sources(directory))
    yield catalog, manager, tmp_path
    engine.dispose()


def reviewed(catalog, version_id):
    state = catalog.get_version(version_id)["version"]["field_review_json"]
    for key in ("external_product_code", "institution_name", "product_name", "product_category", "max_amount",
                "max_term_months", "region_scope", "guarantee_modes", "collateral_types", "materials", "company_age_rule"):
        state[key] = "confirmed" if state.get(key) == "extracted_review" else "acknowledged_unknown"
    catalog.update_draft(version_id, {"field_review_json": state, "review_status": "reviewed", "effective_from": "2026-09-01"})


def test_conflict_keep_a(setup):
    catalog, manager, _ = setup
    result = manager.resolve("NJB-001", {"strategy": "keep_a"}, "admin")
    assert result["status"] == "resolved_keep_a" and result["created_drafts"] == 1
    version = catalog.get_version(result["version_ids"][0])["version"]
    assert version["product_name"] == "产品 A"
    assert [r["disposition"] for r in version["source_refs_json"]] == ["selected", "duplicate_ignored"]
    assert manager.resolve("NJB-001", {"strategy": "keep_a"}, "admin")["created_drafts"] == 0


def test_conflict_keep_b(setup):
    catalog, manager, _ = setup
    result = manager.resolve("NJB-001", {"strategy": "keep_b"}, "admin")
    assert catalog.get_version(result["version_ids"][0])["version"]["product_name"] == "产品 B"
    assert manager.list_conflicts()["items"][0]["status"] == "resolved_keep_b"


def test_conflict_merge(setup):
    catalog, manager, _ = setup
    differences = manager.get_conflict("NJB-001")["differences"]
    choices = {row["field_name"]: "b" for row in differences}
    result = manager.resolve("NJB-001", {"strategy": "merge", "field_choices": choices}, "admin")
    version = catalog.get_version(result["version_ids"][0])["version"]
    assert version["source_type"] == "merged_markdown" and version["product_name"] == "产品 B"
    assert len(version["source_refs_json"]) == 2
    assert all(ref["source_snapshot_hash"] for ref in version["source_refs_json"])


def test_conflict_split_requires_new_unique_code(setup):
    catalog, manager, _ = setup
    with pytest.raises(CatalogError):
        manager.resolve("NJB-001", {"strategy": "split", "rename_side": "b", "new_code": "NJB-001"}, "admin")
    result = manager.resolve("NJB-001", {"strategy": "split", "rename_side": "b", "new_code": "NJB-999"}, "admin")
    assert result["created_drafts"] == 2
    assert {row["product"]["external_product_code"] for row in catalog.list_versions()} == {"NJB-001", "NJB-999"}


def test_unresolved_conflict_cannot_publish(setup):
    catalog, manager, directory = setup
    (directory / SOURCE_FILES["enterprise_credit"]).write_text("# 空\n", encoding="utf-8")
    catalog.import_markdown_sources("admin", directory=directory)
    version_id = catalog.list_versions()[0]["version"]["version_id"]
    reviewed(catalog, version_id)
    (directory / SOURCE_FILES["enterprise_credit"]).write_text(product("产品 B", 800), encoding="utf-8")
    with pytest.raises(CatalogError, match="冲突"):
        catalog.publish(version_id, "admin")


def test_resolved_conflict_can_create_draft(setup):
    catalog, manager, _ = setup
    result = manager.resolve("NJB-001", {"strategy": "keep_a"}, "admin")
    assert manager.list_conflicts()["items"][0]["conflict"] is False
    version_id = result["version_ids"][0]
    assert catalog.get_version(version_id)["version"]["status"] == "draft"
    reviewed(catalog, version_id)
    assert catalog.publish(version_id, "admin")["version"]["status"] == "published"


def test_merge_keeps_both_source_refs(setup):
    catalog, manager, _ = setup
    choices = {row["field_name"]: "a" for row in manager.get_conflict("NJB-001")["differences"]}
    version_id = manager.resolve("NJB-001", {"strategy": "merge", "field_choices": choices}, "admin")["version_ids"][0]
    refs = catalog.get_version(version_id)["version"]["source_refs_json"]
    assert {ref["side"] for ref in refs} == {"a", "b"}
    assert len({ref["source_file"] for ref in refs}) == 2


def test_resolution_does_not_modify_existing_published_version(setup):
    catalog, manager, directory = setup
    second = directory / SOURCE_FILES["enterprise_credit"]
    original = second.read_text(encoding="utf-8")
    second.write_text("# 空\n", encoding="utf-8")
    catalog.import_markdown_sources("admin", directory=directory)
    published_id = catalog.list_versions()[0]["version"]["version_id"]
    reviewed(catalog, published_id)
    catalog.publish(published_id, "admin")
    second.write_text(original, encoding="utf-8")
    result = manager.resolve("NJB-001", {"strategy": "keep_b"}, "admin")
    assert catalog.get_version(published_id)["version"]["status"] == "published"
    assert catalog.get_version(result["version_ids"][0])["version"]["status"] == "draft"


def test_changed_source_invalidates_resolution(setup):
    catalog, manager, directory = setup
    version_id = manager.resolve("NJB-001", {"strategy": "keep_a"}, "admin")["version_ids"][0]
    (directory / SOURCE_FILES["enterprise_credit"]).write_text(product("产品 B", 900), encoding="utf-8")
    assert manager.list_conflicts()["items"][0]["status"] == "unresolved"
    reviewed(catalog, version_id)
    with pytest.raises(CatalogError, match="来源已变化"):
        catalog.publish(version_id, "admin")


def test_same_code_same_hash_not_conflict(setup):
    _, manager, directory = setup
    same = product("产品 A", 500)
    (directory / SOURCE_FILES["enterprise_credit"]).write_text(same, encoding="utf-8")
    (directory / SOURCE_FILES["technology_enterprise"]).write_text(same, encoding="utf-8")
    assert scan_sources(directory)["conflict_count"] == 0
    assert manager.list_conflicts()["total"] == 0


def test_admin_conflict_detail_and_resolution_api(setup, monkeypatch):
    catalog, manager, _ = setup
    monkeypatch.setattr(routes, "catalog", catalog)
    monkeypatch.setattr(routes, "conflict_manager", manager)
    app = FastAPI()
    app.include_router(routes.router, prefix="/api")
    with TestClient(app) as client:
        admin = {"Authorization": "Bearer " + create_access_token("admin", "admin")}
        operator = {"Authorization": "Bearer " + create_access_token("operator", "operator")}
        assert client.get("/api/product-catalog/conflicts/NJB-001", headers=operator).status_code == 403
        detail = client.get("/api/product-catalog/conflicts/NJB-001", headers=admin)
        assert detail.status_code == 200
        assert len(detail.json()["sides"]) == 2
        assert detail.json()["source_line_changes"]["a_only"]
        assert client.post("/api/product-catalog/conflicts/NJB-001/resolve", headers=operator,
                           json={"strategy": "keep_a"}).status_code == 403
        result = client.post("/api/product-catalog/conflicts/NJB-001/resolve", headers=admin,
                             json={"strategy": "keep_a"})
        assert result.status_code == 200 and result.json()["created_drafts"] == 1
