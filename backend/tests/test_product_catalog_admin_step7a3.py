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
from backend.db_models import FinancingProduct, FinancingProductRule, FinancingProductVersion, FinancingProductConflict
from backend.middleware.auth import create_access_token
from backend.routers import product_catalog as routes
from backend.services.markdown_product_import_service import SOURCE_FILES, scan_sources
from backend.services.product_catalog_service import CatalogError, ProductCatalogService
from backend.services.product_conflict_service import ProductConflictService


def document(amount="最高1000万元"):
    return ("# 企业信用类产品库\n产品数量：1款\n更新日期：2026-09-20\n"
            "## 1. 【BOCOM-001】 普惠e贷1.0\n- 机构：交通银行\n"
            "| 字段 | 内容 |\n| --- | --- |\n"
            f"| 最高额度 | {amount} |\n| 征信要求 | 征信良好 |\n")


@pytest.fixture
def setup(tmp_path, monkeypatch):
    for filename in SOURCE_FILES.values():
        (tmp_path / filename).write_text("# 空产品库\n产品数量：0款\n", encoding="utf-8")
    (tmp_path / SOURCE_FILES["enterprise_credit"]).write_text(document(), encoding="utf-8")
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[FinancingProduct.__table__, FinancingProductVersion.__table__, FinancingProductRule.__table__, FinancingProductConflict.__table__])
    service = ProductCatalogService(sessionmaker(bind=engine), ensure_schema=False)
    monkeypatch.setattr(routes, "catalog", service)
    monkeypatch.setattr(routes, "conflict_manager", ProductConflictService(service, directory=tmp_path))
    monkeypatch.setattr(routes, "scan_sources", lambda: scan_sources(tmp_path))
    monkeypatch.setattr(routes, "SOURCE_DIR", tmp_path)
    app = FastAPI()
    app.include_router(routes.router, prefix="/api")
    client = TestClient(app)
    yield service, tmp_path, client
    client.close()
    engine.dispose()


def auth(role="admin"):
    return {"Authorization": "Bearer " + create_access_token("test-" + role, role)}


def sync(service, directory):
    return service.import_markdown_sources("admin", directory=directory)


def version(service):
    return service.list_versions()[0]["version"]["version_id"]


def reviewed(service, version_id):
    state = service.get_version(version_id)["version"]["field_review_json"]
    for key in ("external_product_code", "institution_name", "product_name", "product_category", "max_amount",
                "max_term_months", "region_scope", "guarantee_modes", "collateral_types", "materials", "company_age_rule"):
        state[key] = "reviewed" if state.get(key) == "extracted_review" else "insufficient_data"
    service.update_draft(version_id, {"field_review_json": state, "review_status": "reviewed", "effective_from": "2026-09-01"})


def ready_state(service, version_id):
    detail = service.get_version(version_id)
    version_data = detail["version"]
    state = version_data["field_review_json"]
    for key in ("external_product_code", "institution_name", "product_name", "product_category"):
        state[key] = "reviewed"
    values = {"max_amount": version_data["max_amount"], "max_term_months": version_data["max_term_months"],
              "region_scope": version_data["region_scope_json"], "guarantee_modes": version_data["guarantee_modes_json"],
              "collateral_types": version_data["collateral_types_json"], "materials": version_data["materials_json"],
              "company_age_rule": version_data["company_age_months"]}
    for key, value in values.items():
        state[key] = "reviewed" if value not in (None, "", []) else "insufficient_data"
    return state


def test_admin_can_view_product_sources(setup):
    _, _, client = setup
    response = client.get("/api/product-catalog/sources", headers=auth())
    assert response.status_code == 200
    assert len(response.json()["sources"]) == 6
    assert response.json()["sources"][-1]["source_snapshot_hash"]


def test_operator_cannot_sync_product_sources(setup):
    _, _, client = setup
    response = client.post("/api/product-catalog/sources/enterprise_credit/sync", headers=auth("operator"))
    assert response.status_code == 403


def test_sync_creates_drafts(setup):
    service, directory, _ = setup
    assert sync(service, directory)["created_drafts"] == 1
    assert service.get_version(version(service))["version"]["review_status"] == "unreviewed"


def test_repeat_sync_is_idempotent(setup):
    service, directory, _ = setup
    sync(service, directory)
    assert sync(service, directory)["created_drafts"] == 0
    assert len(service.list_versions()) == 1


def test_product_list_filters(setup):
    service, directory, client = setup
    sync(service, directory)
    assert client.get("/api/product-catalog/products?category=enterprise_credit&search=BOCOM-001", headers=auth()).json()["total"] == 1
    assert client.get("/api/product-catalog/products?status=published", headers=auth()).json()["total"] == 0
    assert service.list_products(institution="交通银行", needs_review=True)


def test_product_detail_contains_source_snapshot(setup):
    service, directory, client = setup
    sync(service, directory)
    response = client.get(f"/api/product-catalog/versions/{version(service)}", headers=auth())
    assert "【BOCOM-001】" in response.json()["version"]["source_snapshot"]
    assert response.json()["version"]["review_reasons_json"]


def test_draft_can_be_edited(setup):
    service, directory, _ = setup
    sync(service, directory)
    service.update_draft(version(service), {"max_amount": 8_000_000, "review_status": "reviewing"})
    assert service.get_version(version(service))["version"]["max_amount"] == "8000000.00"


def test_published_version_cannot_be_edited(setup):
    service, directory, _ = setup
    sync(service, directory)
    version_id = version(service)
    reviewed(service, version_id)
    service.publish(version_id, "admin")
    with pytest.raises(CatalogError):
        service.update_draft(version_id, {"max_amount": 1})


def test_rule_field_must_be_whitelisted(setup):
    service, directory, _ = setup
    sync(service, directory)
    with pytest.raises(CatalogError):
        service.add_rule(version(service), {"field_name": "customer.bad", "operator": "eq", "expected_value": 1, "source_text": "x"})


def test_publish_requires_valid_version(setup):
    service, directory, _ = setup
    sync(service, directory)
    with pytest.raises(CatalogError):
        service.publish(version(service), "admin")


def test_publish_enters_active_catalog(setup):
    service, directory, _ = setup
    sync(service, directory)
    reviewed(service, version(service))
    service.publish(version(service), "admin")
    assert len(service.get_active_products("2026-09-22")) == 1


def test_insufficient_data_does_not_block_publish(setup):
    service, directory, _ = setup
    sync(service, directory)
    version_id = version(service)
    state = ready_state(service, version_id)
    assert state["region_scope"] == "insufficient_data"
    assert state["collateral_types"] == "insufficient_data"
    service.update_draft(version_id, {"field_review_json": state, "review_status": "reviewed", "effective_from": "2026-09-01"})
    assert service.publish(version_id, "admin")["version"]["status"] == "published"


def test_needs_review_blocks_publish(setup):
    service, directory, _ = setup
    sync(service, directory)
    version_id = version(service)
    state = ready_state(service, version_id)
    state["guarantee_modes"] = "needs_review"
    service.update_draft(version_id, {"field_review_json": state, "review_status": "reviewed", "effective_from": "2026-09-01"})
    with pytest.raises(CatalogError, match="guarantee_modes"):
        service.publish(version_id, "admin")


def test_extracted_review_blocks_publish_until_confirmed(setup):
    service, directory, _ = setup
    sync(service, directory)
    version_id = version(service)
    state = ready_state(service, version_id)
    state["max_amount"] = "extracted_review"
    service.update_draft(version_id, {"field_review_json": state, "review_status": "reviewed", "effective_from": "2026-09-01"})
    with pytest.raises(CatalogError, match="max_amount"):
        service.publish(version_id, "admin")
    state["max_amount"] = "reviewed"
    service.update_draft(version_id, {"field_review_json": state})
    assert service.publish(version_id, "admin")["version"]["status"] == "published"


def test_required_core_field_missing_blocks_publish(setup):
    service, directory, _ = setup
    sync(service, directory)
    version_id = version(service)
    state = ready_state(service, version_id)
    service.update_draft(version_id, {"field_review_json": state, "review_status": "reviewed", "effective_from": "2026-09-01"})
    with service.session_factory.begin() as db:
        row = db.query(FinancingProduct).first()
        row.external_product_code = None
    with pytest.raises(CatalogError, match="稳定外部编号"):
        service.publish(version_id, "admin")


def test_reviewed_core_fields_allow_publish(setup):
    service, directory, _ = setup
    sync(service, directory)
    version_id = version(service)
    state = ready_state(service, version_id)
    assert all(state[key] == "reviewed" for key in ("external_product_code", "institution_name", "product_name", "product_category"))
    service.update_draft(version_id, {"field_review_json": state, "review_status": "reviewed", "effective_from": "2026-09-01"})
    assert service.publish(version_id, "admin")["version"]["status"] == "published"


def test_disable_removes_from_active_catalog(setup):
    service, directory, _ = setup
    sync(service, directory)
    reviewed(service, version(service))
    service.publish(version(service), "admin")
    service.change_lifecycle(version(service), "disabled")
    assert not service.get_active_products("2026-09-22")


def test_version_history_available(setup):
    service, directory, client = setup
    sync(service, directory)
    (directory / SOURCE_FILES["enterprise_credit"]).write_text(document("最高800万元"), encoding="utf-8")
    sync(service, directory)
    product_id = service.list_products()[0]["product"]["product_id"]
    response = client.get(f"/api/product-catalog/products/{product_id}/versions", headers=auth())
    assert response.json()["total"] == 2


def test_published_tab_retains_older_published_when_new_draft_exists(setup):
    service, directory, client = setup
    sync(service, directory)
    reviewed(service, version(service))
    service.publish(version(service), "admin")
    (directory / SOURCE_FILES["enterprise_credit"]).write_text(document("最高800万元"), encoding="utf-8")
    sync(service, directory)
    response = client.get("/api/product-catalog/products?status=published", headers=auth())
    assert response.json()["total"] == 1
    assert response.json()["items"][0]["version"]["status"] == "published"


def test_conflict_product_not_published(setup):
    service, directory, _ = setup
    text = document()
    (directory / SOURCE_FILES["enterprise_credit"]).write_text(text + text[text.index("## 1."):].replace("1000万元", "800万元"), encoding="utf-8")
    assert sync(service, directory)["created_drafts"] == 0
    assert service.get_active_products("2026-09-22") == []


def test_existing_draft_cannot_publish_after_source_code_conflict(setup, monkeypatch):
    service, directory, _ = setup
    sync(service, directory)
    version_id = version(service)
    reviewed(service, version_id)
    from backend.services import product_catalog_service as module
    monkeypatch.setattr(module, "scan_sources", lambda: {"conflicting_codes": {"BOCOM-001"}})
    with pytest.raises(CatalogError, match="冲突"):
        service.publish(version_id, "admin")


def test_duplicate_conflict_visible_in_admin(setup):
    _, directory, client = setup
    text = document()
    (directory / SOURCE_FILES["enterprise_credit"]).write_text(text + text[text.index("## 1."):].replace("1000万元", "800万元"), encoding="utf-8")
    response = client.get("/api/product-catalog/conflicts", headers=auth())
    assert response.json()["total"] == 1
    assert response.json()["items"][0]["conflict_fields"] == ["最高额度"]
