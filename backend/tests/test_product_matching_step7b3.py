from __future__ import annotations

import asyncio
import sys
from copy import deepcopy
from datetime import date
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base
from backend.db_models import ProductMatchItem, ProductMatchSnapshot
from backend.services.customer_matching_facts_service import CustomerMatchingFacts, FactSource, FactValue, MatchingDataQuality
from backend.services.product_matching_service import ProductMatchingError, ProductMatchingService


def known(value, unit=None):
    return FactValue(value=value, status="known", unit=unit, source=FactSource(
        source_type="fixture", source_id="evidence-1", source_date="2026-03-31", evidence_refs=["fact:1"],
    ))


def unknown():
    return FactValue(status="unknown", reason="insufficient_data")


def matching_facts(requirement_id="req-1", version=1, amount=Decimal("8000000"), term=12):
    return CustomerMatchingFacts(
        customer_id="enterprise_上海意川建筑科技有限公司", requirement_id=requirement_id,
        requirement_version=version, generated_at="2026-09-24T10:00:00+00:00", as_of_date="2026-09-24",
        requirement={"amount": known(amount, "CNY"), "term_months": known(term, "month")},
        customer={}, credit={}, financial={}, cashflow={},
        asset={"has_confirmed_collateral": unknown()}, business={}, qualification={},
        data_quality=MatchingDataQuality(missing_domains=["asset", "business", "qualification"], preliminary_fields=["cashflow.operating_inflow"]),
    )


def explicit_rule(field="credit.personal.current_overdue_count", operator="eq", expected=0, *, action="exclude"):
    return {
        "rule_id": "rule-1", "version_id": "version-1", "field_name": field, "operator": operator,
        "expected_value": expected, "severity": "hard", "failure_action": action,
        "message": "", "source_text": "测试规则", "sort_order": 0,
    }


def entry(*, max_amount="10000000", max_term=12, rules=None, version_id="version-1", status="published"):
    normalized_rules = deepcopy(rules or [])
    for item in normalized_rules:
        item["version_id"] = version_id
    return {
        "product": {"product_id": f"product-{version_id}", "external_product_code": "TEST-001", "institution_name": "测试银行", "product_name": "测试产品", "product_category": "enterprise_credit"},
        "version": {"product_id": f"product-{version_id}", "version_id": version_id, "status": status, "institution_name": "测试银行", "product_name": "测试产品", "min_amount": None, "max_amount": max_amount, "max_term_months": max_term},
        "rules": normalized_rules,
    }


class Catalog:
    def __init__(self, values): self.values = values
    def get_active_products(self, _day): return deepcopy(self.values)


@pytest.fixture
def factory():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[ProductMatchSnapshot.__table__, ProductMatchItem.__table__])
    yield sessionmaker(bind=engine)
    engine.dispose()


def service(factory, catalog_values, facts_value=None, builder=None):
    async def default_builder(_customer_id, _requirement_id):
        return facts_value or matching_facts(requirement_id=_requirement_id)
    value = ProductMatchingService(
        session_factory=factory, catalog_service=Catalog(catalog_values), facts_builder=builder or default_builder,
        ensure_schema=False,
    )
    value._published_count = lambda: len(catalog_values)
    return value


def run(value, requirement_id="req-1"):
    return asyncio.run(value.run_product_matching("enterprise_上海意川建筑科技有限公司", requirement_id, generated_by="admin", as_of_date=date(2026, 9, 24)))


def test_matching_requires_confirmed_requirement(factory):
    async def rejected(_customer, _requirement): raise ValueError("产品匹配事实只能基于已确认的融资需求")
    with pytest.raises(ProductMatchingError, match="已确认"):
        run(service(factory, [], builder=rejected))


def test_matching_uses_active_catalog_only(factory):
    result = run(service(factory, [entry(rules=[])]))
    assert result["precheck"]["active_product_count"] == 1 and len(result["items"]) == 1


def test_empty_active_catalog_returns_no_match(factory):
    result = run(service(factory, []))
    assert result["status"] == "no_active_products" and result["snapshot_id"] is None and result["items"] == []


def test_product_without_rules_not_eligible(factory):
    result = run(service(factory, [entry(rules=[])]))
    assert result["items"][0]["overall_status"] == "manual_review"
    assert "尚未配置足够结构化准入规则" in result["items"][0]["review_reasons"][-1]


def test_builtin_amount_rule(factory):
    result = run(service(factory, [entry(max_amount="10000000", rules=[])]))
    builtins = [rule for rule in result["items"][0]["rule_results"] if rule["rule_source"] == "system_builtin"]
    assert any(rule["rule_id"].endswith("max_amount") and rule["result"] == "passed" for rule in builtins)


def test_builtin_term_rule(factory):
    result = run(service(factory, [entry(max_term=12, rules=[])]))
    assert any(rule["rule_id"].endswith("max_term_months") and rule["result"] == "passed" for rule in result["items"][0]["rule_results"])


def test_800w_requirement_excludes_500w_product(factory):
    result = run(service(factory, [entry(max_amount="5000000", rules=[])]))
    assert result["items"][0]["overall_status"] == "ineligible"


def test_unknown_asset_makes_conditional(factory):
    rules = [explicit_rule("asset.has_confirmed_collateral", "eq", True, action="conditional")]
    result = run(service(factory, [entry(rules=rules)]))
    assert result["items"][0]["overall_status"] == "conditional"


def test_manual_review_preserved(factory):
    rules = [explicit_rule("asset.has_confirmed_collateral", "eq", True, action="review")]
    result = run(service(factory, [entry(rules=rules)]))
    assert result["items"][0]["overall_status"] == "manual_review"


def test_configuration_error_preserved(factory):
    result = run(service(factory, [entry(rules=[explicit_rule("requirement.amount", "bad", 1)])]))
    assert result["items"][0]["overall_status"] == "product_configuration_error"
    assert result["summary"]["product_configuration_error"] == 1


def test_snapshot_persists_requirement_version_and_facts_hash(factory):
    result = run(service(factory, [entry(rules=[])]))
    with factory() as db:
        row = db.scalar(select(ProductMatchSnapshot))
        assert row.requirement_version == 1 and len(row.facts_hash) == 64 and "requirement" in row.facts_json
    assert result["requirement_version"] == 1


def test_snapshot_persists_product_version_and_rule_results(factory):
    result = run(service(factory, [entry(rules=[])]))
    with factory() as db:
        row = db.scalar(select(ProductMatchItem))
        assert row.version_id == "version-1" and "system_builtin" in row.rule_results_json
    assert result["items"][0]["version_id"] == "version-1"


def test_repeat_same_context_reuses_snapshot(factory):
    value = service(factory, [entry(rules=[])])
    first, second = run(value), run(value)
    assert first["snapshot_id"] == second["snapshot_id"] and second["reused"] is True
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(ProductMatchSnapshot)) == 1


def test_changed_requirement_creates_new_snapshot(factory):
    async def builder(_customer, requirement_id):
        return matching_facts(requirement_id=requirement_id, version=1 if requirement_id == "req-1" else 2)
    value = service(factory, [entry(rules=[])], builder=builder)
    assert run(value, "req-1")["snapshot_id"] != run(value, "req-2")["snapshot_id"]


def test_changed_catalog_creates_new_snapshot(factory):
    catalog = Catalog([entry(max_amount="10000000", rules=[])])
    value = service(factory, [], facts_value=matching_facts())
    value.catalog = catalog
    first = run(value)
    catalog.values = [entry(max_amount="9000000", rules=[])]
    second = run(value)
    assert first["snapshot_id"] != second["snapshot_id"]


def test_snapshot_data_quality_is_exposed(factory):
    result = run(service(factory, [entry(rules=[])]))
    assert "asset" in result["data_quality"]["missing_domains"]
    assert "cashflow.operating_inflow" in result["data_quality"]["preliminary_fields"]


def test_results_are_grouped_by_status_counts_without_ranking(factory):
    values = [entry(max_amount="5000000", rules=[], version_id="v-low"), entry(max_amount="10000000", rules=[], version_id="v-review")]
    result = run(service(factory, values))
    assert result["summary"]["ineligible"] == 1 and result["summary"]["manual_review"] == 1
    assert "score" not in result["summary"] and "ranking" not in result["summary"]
