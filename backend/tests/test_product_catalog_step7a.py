from __future__ import annotations

import hashlib
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base
from backend.db_models import FinancingProduct, FinancingProductRule, FinancingProductVersion, ProductCacheEntry
from backend.services.feishu_product_import_service import FeishuProductImportService
from backend.services.product_catalog_service import CatalogError, ProductCatalogService, fact_state, validate_rule


DOCUMENT = """中国银行 - 惠担贷（批次）
利率：年化3%左右
额度：最高1000万元
还款方式：先息后本
成立时间：企业成立满2年
纳税要求：按银行政策审核
征信：具体以银行审核为准
材料准备清单
1. 营业执照
2. 企业流水
审批流程
提交申请后由银行审核
"""


@pytest.fixture
def catalog():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[FinancingProduct.__table__, FinancingProductVersion.__table__, FinancingProductRule.__table__])
    service = ProductCatalogService(sessionmaker(bind=engine), ensure_schema=False)
    yield service
    engine.dispose()


def imported(catalog, text=DOCUMENT):
    return catalog.import_document("enterprise_credit", "node-credit", "doc-credit", text, "admin")


def reviewed(catalog, version_id, **patch):
    review = catalog.get_version(version_id)["version"]["field_review_json"]
    for key in ("institution_name", "product_name", "product_category", "max_amount", "max_term_months",
                "region_scope", "guarantee_modes", "collateral_types", "materials", "company_age_rule"):
        review[key] = "confirmed" if review.get(key) == "extracted_review" else "acknowledged_unknown"
    catalog.update_draft(version_id, {"effective_from": "2026-09-01", "field_review_json": review, **patch})


def test_import_feishu_product_as_draft(catalog):
    class FakeWiki:
        PRODUCT_DOCS = {"enterprise_credit": "https://example.feishu.cn/wiki/node-credit"}

        def _extract_node_token(self, url):
            return url.rsplit("/", 1)[-1]

        def _get_node_info(self, token):
            return {"obj_type": "docx", "obj_token": "doc-credit"}

        def _get_document_raw_content(self, token):
            return DOCUMENT

    content, doc, _ = FeishuProductImportService(FakeWiki()).fetch("enterprise_credit", "node-credit")
    row = catalog.import_document("enterprise_credit", "node-credit", doc, content, "admin")[0]
    version = catalog.get_version(row["version_id"])
    assert row["status"] == "draft"
    assert version["product"]["institution_name"] == "中国银行"
    assert version["version"]["max_amount"] == "10000000.00"
    assert version["version"]["rate_text"] == "年化3%左右"
    assert version["version"]["field_review_json"]["effective_from"] == "insufficient_data"


def test_draft_does_not_enter_active_catalog(catalog):
    imported(catalog)
    assert catalog.get_active_products(date(2026, 9, 22)) == []


def test_publish_product_version(catalog):
    version_id = imported(catalog)[0]["version_id"]
    with pytest.raises(CatalogError):
        catalog.publish(version_id, "admin")
    reviewed(catalog, version_id)
    assert catalog.publish(version_id, "admin")["version"]["status"] == "published"
    active = catalog.get_active_products(date(2026, 9, 22))
    assert len(active) == 1
    assert active[0]["rules"][0]["field_name"] == "customer.company_age_months"


def test_published_version_is_immutable(catalog):
    version_id = imported(catalog)[0]["version_id"]
    reviewed(catalog, version_id)
    catalog.publish(version_id, "admin")
    with pytest.raises(CatalogError):
        catalog.update_draft(version_id, {"max_amount": 2})
    with pytest.raises(CatalogError):
        catalog.add_rule(version_id, {"field_name": "requirement.amount", "operator": "gte", "expected_value": 1,
                                      "source_text": "最低一元"})
    with catalog.session_factory() as db:
        row = db.scalar(select(FinancingProductVersion).where(FinancingProductVersion.version_id == version_id))
        row.max_amount = 2
        with pytest.raises(ValueError, match="不可修改"):
            db.commit()
        db.rollback()


def test_new_source_change_creates_new_draft_version(catalog):
    first = imported(catalog)[0]
    second = imported(catalog, DOCUMENT.replace("最高1000万元", "最高1200万元"))[0]
    assert second["version_id"] != first["version_id"]
    assert catalog.get_version(second["version_id"])["version"]["version_number"] == 2
    assert imported(catalog, DOCUMENT.replace("最高1000万元", "最高1200万元"))[0]["created"] is False


def test_old_published_version_can_be_superseded(catalog):
    first = imported(catalog)[0]["version_id"]
    reviewed(catalog, first)
    catalog.publish(first, "admin")
    second = imported(catalog, DOCUMENT.replace("最高1000万元", "最高1200万元"))[0]["version_id"]
    assert catalog.get_version(first)["version"]["status"] == "published"
    reviewed(catalog, second)
    catalog.publish(second, "admin")
    assert catalog.get_version(first)["version"]["status"] == "superseded"
    assert [x["version"]["version_id"] for x in catalog.get_active_products(date(2026, 9, 22))] == [second]


def test_effective_date_filter(catalog):
    version_id = imported(catalog)[0]["version_id"]
    reviewed(catalog, version_id, effective_from="2026-10-01", effective_to="2026-12-31")
    catalog.publish(version_id, "admin")
    assert catalog.get_active_products("2026-09-30") == []
    assert len(catalog.get_active_products("2026-10-01")) == 1
    assert len(catalog.get_active_products("2026-12-31")) == 1


def test_expired_product_not_active(catalog):
    version_id = imported(catalog)[0]["version_id"]
    reviewed(catalog, version_id, effective_to="2026-09-30")
    catalog.publish(version_id, "admin")
    assert catalog.get_active_products("2026-10-01") == []
    catalog.change_lifecycle(version_id, "expired")
    assert catalog.get_active_products("2026-09-22") == []


def test_rule_field_whitelist():
    with pytest.raises(CatalogError):
        validate_rule({"field_name": "customer.__class__", "operator": "eq", "expected_value": "x", "source_text": "x"})


def test_invalid_operator_rejected():
    with pytest.raises(CatalogError):
        validate_rule({"field_name": "requirement.amount", "operator": "eval", "expected_value": 1, "source_text": "x"})


def test_missing_fact_is_not_false():
    assert fact_state({}, "asset.has_real_estate") == {"status": "unknown", "reason": "insufficient_data", "value": None}
    assert fact_state({"requirement": {"purpose": ""}}, "requirement.purpose")["status"] == "unknown"
    assert fact_state({"requirement": {"amount": 0}}, "requirement.amount") == {"status": "known", "value": 0}
    assert fact_state({"asset": {"has_real_estate": False}}, "asset.has_real_estate") == {"status": "known", "value": False}


def test_cache_refresh_does_not_clear_unprovided_mortgage_cache(monkeypatch):
    from backend.services import product_cache_service as cache

    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[ProductCacheEntry.__table__])
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(cache, "SessionLocal", factory)
    monkeypatch.setattr(cache, "_TABLES_READY", True)
    cache.save_cache_map("credit-v1", "personal-v1", "mortgage-v1")
    cache.save_cache_map("credit-v2", "personal-v2")
    with factory() as db:
        row = db.scalar(select(ProductCacheEntry).where(ProductCacheEntry.cache_key == "enterprise_mortgage"))
        assert row.content == "mortgage-v1"
    engine.dispose()


def test_source_snapshot_hash_saved(catalog):
    version_id = imported(catalog)[0]["version_id"]
    version = catalog.get_version(version_id)["version"]
    assert version["source_snapshot_hash"] == hashlib.sha256(version["source_snapshot"].encode()).hexdigest()


def test_product_source_traceable(catalog):
    version_id = imported(catalog)[0]["version_id"]
    payload = catalog.get_version(version_id)
    assert payload["product"]["source_type"] == "feishu_wiki"
    assert payload["version"]["source_node_token"] == "node-credit"
    assert payload["version"]["source_document_token"] == "doc-credit"
    assert payload["version"]["source_imported_at"]
