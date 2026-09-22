from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.database import Base
from backend.db_models import FinancingProduct, FinancingProductRule, FinancingProductVersion
from backend.services.markdown_product_import_service import SOURCE_FILES, parse_markdown_source, scan_sources
from backend.services.product_catalog_service import CatalogError, ProductCatalogService
from backend.services.product_catalog_schema import ensure_markdown_catalog_schema


def document(code="BOCOM-001", name="普惠e贷1.0", age="企业成立满2年", amount="最高1000万元"):
    return ("# 企业信用类产品库\n产品数量：1款\n更新日期：2026-09-20\n"
            f"## 1. 【{code}】 {name}\n- 机构：交通银行\n"
            "| 字段 | 内容 |\n| --- | --- |\n"
            f"| 企业成立 | {age} |\n| 最高额度 | {amount} |\n"
            "| 特殊字段 | 保留原值 |\n")


@pytest.fixture
def catalog(tmp_path):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[FinancingProduct.__table__, FinancingProductVersion.__table__, FinancingProductRule.__table__])
    service = ProductCatalogService(sessionmaker(bind=engine), ensure_schema=False)
    yield service, tmp_path
    engine.dispose()


def write(directory, category="enterprise_credit", content=None):
    for filename in SOURCE_FILES.values():
        (directory / filename).write_text("# 空产品库\n产品数量：0款\n", encoding="utf-8")
    (directory / SOURCE_FILES[category]).write_text(content or document(), encoding="utf-8")


def first_version(service):
    return service.list_versions()[0]["version"]["version_id"]


def review(service, version_id):
    data = service.get_version(version_id)["version"]["field_review_json"]
    for key in ("external_product_code", "institution_name", "product_name", "product_category", "max_amount",
                "max_term_months", "region_scope", "guarantee_modes", "collateral_types", "materials", "company_age_rule"):
        data[key] = "confirmed" if data.get(key) == "extracted_review" else "acknowledged_unknown"
    service.update_draft(version_id, {"effective_from": "2026-09-01", "field_review_json": data})


def test_parse_markdown_product_header():
    parsed = parse_markdown_source("enterprise_credit", document())
    assert parsed.declared_count == 1
    assert parsed.products[0].product_name == "普惠e贷1.0"


def test_parse_product_code():
    assert parse_markdown_source("enterprise_credit", document(code="CZB-SGS-002")).products[0].external_product_code == "CZB-SGS-002"


def test_parse_product_table():
    product = parse_markdown_source("enterprise_credit", document()).products[0]
    assert product.raw_fields["最高额度"] == "最高1000万元"
    assert product.fields["max_amount"] == 10_000_000


def test_explicit_maximum_term_table_field():
    content = document().replace("| 特殊字段 |", "| 贷款授信最长期限 | 3年 |\n| 特殊字段 |")
    product = parse_markdown_source("enterprise_credit", content).products[0]
    assert product.fields["max_term_months"] == 36


def test_source_file_category_mapping():
    assert set(SOURCE_FILES) == {"guarantee_fund", "personal_mortgage", "personal_credit", "technology_enterprise", "enterprise_mortgage", "enterprise_credit"}
    assert len(set(SOURCE_FILES.values())) == 6


def test_duplicate_same_hash_is_deduplicated(tmp_path):
    content = document()
    write(tmp_path, content=content)
    # Repeated identical product blocks, including the heading, share the same hash.
    block = content[content.index("## 1."):]
    (tmp_path / SOURCE_FILES["enterprise_credit"]).write_text(content + block, encoding="utf-8")
    scan = scan_sources(tmp_path)
    assert scan["parsed_count"] == 2 and scan["unique_count"] == 1
    assert scan["deduplicated_count"] == 1 and scan["conflict_count"] == 0
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine, tables=[FinancingProduct.__table__, FinancingProductVersion.__table__, FinancingProductRule.__table__])
    service = ProductCatalogService(sessionmaker(bind=engine), ensure_schema=False)
    assert service.import_markdown_sources("admin", directory=tmp_path)["created_drafts"] == 1
    engine.dispose()


def test_duplicate_different_hash_requires_review(tmp_path):
    write(tmp_path, content=document() + document(amount="最高800万元")[document().index("## 1."):])
    scan = scan_sources(tmp_path)
    assert scan["conflict_count"] == 1
    assert scan["conflicts"][0]["status"] == "duplicate_conflict"
    assert len(scan["conflicts"][0]["sides"]) == 2


def test_reimport_same_file_does_not_create_version(catalog):
    service, directory = catalog
    write(directory)
    assert service.import_markdown_sources("admin", directory=directory)["created_drafts"] == 1
    second = service.import_markdown_sources("admin", directory=directory)
    assert second["created_drafts"] == 0 and second["unchanged"] == 1
    assert len(service.list_versions()) == 1


def test_changed_markdown_creates_new_draft(catalog):
    service, directory = catalog
    write(directory)
    service.import_markdown_sources("admin", directory=directory)
    write(directory, content=document(amount="最高800万元"))
    assert service.import_markdown_sources("admin", directory=directory)["created_drafts"] == 1
    assert sorted(v["version"]["version_number"] for v in service.list_versions()) == [1, 2]


def test_changed_markdown_does_not_overwrite_published(catalog):
    service, directory = catalog
    write(directory)
    service.import_markdown_sources("admin", directory=directory)
    version_id = first_version(service)
    review(service, version_id)
    service.publish(version_id, "admin")
    write(directory, content=document(amount="最高800万元"))
    service.import_markdown_sources("admin", directory=directory)
    assert service.get_version(version_id)["version"]["status"] == "published"
    assert service.get_version(version_id)["version"]["max_amount"] == "10000000.00"
    assert {v["version"]["status"] for v in service.list_versions()} == {"draft", "published"}


def test_unknown_field_kept_in_raw_fields():
    product = parse_markdown_source("enterprise_credit", document()).products[0]
    assert product.raw_fields["特殊字段"] == "保留原值"


def test_ambiguous_text_does_not_create_hard_rule():
    product = parse_markdown_source("enterprise_credit", document(age="经营稳定")).products[0]
    assert not product.rules


def test_explicit_numeric_rule_can_be_structured():
    product = parse_markdown_source("enterprise_credit", document()).products[0]
    assert product.rules[0]["field_name"] == "customer.company_age_months"
    assert product.rules[0]["operator"] == "gte"
    assert product.rules[0]["expected_value"] == 24
    ratio = parse_markdown_source("enterprise_credit", document().replace("企业成立满2年", "资产负债率≤70%")).products[0]
    assert ratio.rules[0]["field_name"] == "financial.debt_asset_ratio"
    assert ratio.rules[0]["expected_value"] == "0.7"


def test_draft_not_in_active_catalog(catalog):
    service, directory = catalog
    write(directory)
    service.import_markdown_sources("admin", directory=directory)
    assert service.get_active_products("2026-09-22") == []


def test_publish_markdown_product(catalog):
    service, directory = catalog
    write(directory)
    service.import_markdown_sources("admin", directory=directory)
    version_id = first_version(service)
    with pytest.raises(CatalogError):
        service.publish(version_id, "admin")
    review(service, version_id)
    service.publish(version_id, "admin")
    active = service.get_active_products("2026-09-22")
    assert active[0]["product"]["external_product_code"] == "BOCOM-001"
    assert active[0]["version"]["source_snapshot_hash"] == hashlib.sha256(active[0]["version"]["source_snapshot"].encode()).hexdigest()


def test_conflicting_code_is_not_imported(catalog):
    service, directory = catalog
    write(directory, content=document() + document(amount="最高800万元")[document().index("## 1."):])
    result = service.import_markdown_sources("admin", directory=directory)
    assert result["created_drafts"] == 0 and len(result["conflicts"]) == 1
    assert service.list_versions() == []


def test_existing_catalog_tables_gain_markdown_columns():
    engine = create_engine("sqlite://")
    with engine.begin() as db:
        db.execute(text("CREATE TABLE financing_products (id INTEGER PRIMARY KEY, product_id VARCHAR(64), identity_key VARCHAR(64), institution_name VARCHAR(255), product_name VARCHAR(255), product_category VARCHAR(32), region_key VARCHAR(255), source_type VARCHAR(32), source_ref VARCHAR(255), created_at DATETIME, updated_at DATETIME)"))
        db.execute(text("CREATE TABLE financing_product_versions (id INTEGER PRIMARY KEY, version_id VARCHAR(64), product_id VARCHAR(64), version_number INTEGER, status VARCHAR(32), effective_from DATE, effective_to DATE, source_snapshot TEXT, source_snapshot_hash VARCHAR(64), source_node_token VARCHAR(128), source_document_token VARCHAR(128), source_imported_at DATETIME, source_updated_at DATETIME, summary TEXT, region_scope_json TEXT, currency VARCHAR(8), min_amount NUMERIC, max_amount NUMERIC, min_term_months INTEGER, max_term_months INTEGER, repayment_methods_json TEXT, guarantee_modes_json TEXT, collateral_types_json TEXT, materials_json TEXT, rate_text VARCHAR(255), notes TEXT, field_review_json TEXT, created_by VARCHAR(128), published_by VARCHAR(128), created_at DATETIME, published_at DATETIME)"))
    ensure_markdown_catalog_schema(engine)
    assert "external_product_code" in {x["name"] for x in inspect(engine).get_columns("financing_products")}
    assert "raw_fields_json" in {x["name"] for x in inspect(engine).get_columns("financing_product_versions")}
    ensure_markdown_catalog_schema(engine)
    engine.dispose()
