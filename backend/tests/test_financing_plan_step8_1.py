from __future__ import annotations

import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base
from backend.db_models import (
    FinancingPlan,
    FinancingPlanCondition,
    FinancingPlanExplanation,
    FinancingPlanGap,
    FinancingPlanItem,
    FinancingPlanMaterial,
    FinancingPlanVersion,
    FinancingRequirement,
    ProductMatchItem,
    ProductMatchSnapshot,
    ManualCandidateOverride,
)
from backend.services.financing_plan_service import (
    FinancingPlanDraftInput,
    FinancingPlanError,
    FinancingPlanItemInput,
    FinancingPlanService,
    PlanRuleEngine,
)


CUSTOMER_ID = "enterprise_上海意川建筑科技有限公司"
REQUIREMENT_ID = "req-yichuan-v2"
SNAPSHOT_ID = "snapshot-yichuan"


@pytest.fixture
def factory():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[
        FinancingRequirement.__table__, ProductMatchSnapshot.__table__, ProductMatchItem.__table__,
        FinancingPlan.__table__, FinancingPlanVersion.__table__, FinancingPlanItem.__table__, FinancingPlanGap.__table__,
        ManualCandidateOverride.__table__, FinancingPlanCondition.__table__,
        FinancingPlanMaterial.__table__, FinancingPlanExplanation.__table__,
    ])
    yield sessionmaker(bind=engine), engine
    engine.dispose()


def facts(amount="8000000", term=12):
    return json.dumps({
        "requirement": {
            "amount": {"status": "known", "value": amount, "unit": "CNY"},
            "term_months": {"status": "known", "value": term, "unit": "month"},
            "currency": {"status": "known", "value": "CNY"},
        }
    }, ensure_ascii=False)


def seed(factory, products, *, requirement_status="confirmed", requirement_version=2):
    sessions, _ = factory
    with sessions.begin() as db:
        db.add(FinancingRequirement(
            requirement_id=REQUIREMENT_ID, customer_id=CUSTOMER_ID, version=requirement_version,
            status=requirement_status, borrower_entity="上海意川建筑科技有限公司",
            requested_amount=Decimal("8000000"), currency="CNY", financing_purpose="采购",
            purpose_detail="材料采购", term_value=12, term_unit="month",
        ))
        db.add(ProductMatchSnapshot(
            snapshot_id=SNAPSHOT_ID, context_hash="context", customer_id=CUSTOMER_ID,
            requirement_id=REQUIREMENT_ID, requirement_version=requirement_version,
            facts_hash="facts-hash", facts_json=facts(), catalog_as_of_date=date(2026, 9, 28),
            catalog_version_hash="catalog-hash", generated_at=datetime(2026, 9, 28, 7, 27, 51),
            generated_by="admin", summary_json="{}",
        ))
        db.flush()
        for index, product in enumerate(products, 1):
            db.add(ProductMatchItem(
                snapshot_id=SNAPSHOT_ID, product_id=f"product-{index}", version_id=f"version-{index}",
                external_product_code=product.get("code", f"TEST-{index:03d}"),
                institution_name="测试银行", product_name=product.get("name", f"产品{index}"),
                product_category="enterprise_credit", max_amount=product.get("max_amount"),
                max_term_months=product.get("max_term", 12), overall_status=product["status"],
                blocking_reasons_json=json.dumps(product.get("blocking", []), ensure_ascii=False),
                missing_information_json=json.dumps(product.get("missing", []), ensure_ascii=False),
                review_reasons_json=json.dumps(product.get("review", []), ensure_ascii=False),
                soft_gaps_json="[]", rule_results_json="[]",
            ))


def service(factory):
    return FinancingPlanService(session_factory=factory[0], ensure_schema=False)


def candidate_pool(value):
    return value.build_plan_candidates(CUSTOMER_ID, REQUIREMENT_ID, SNAPSHOT_ID)


def draft(item_id, *, amount="8000000", term=12, manual=False, plan_type="primary"):
    return FinancingPlanDraftInput(
        customer_id=CUSTOMER_ID, requirement_id=REQUIREMENT_ID, match_snapshot_id=SNAPSHOT_ID,
        plan_type=plan_type,
        items=[FinancingPlanItemInput(
            product_match_item_id=item_id, proposed_amount=Decimal(amount), proposed_term_months=term,
            manual_approved=manual,
        )],
    )


def first_item_id(factory):
    with factory[0]() as db:
        return db.scalar(select(ProductMatchItem.id).order_by(ProductMatchItem.id))


def test_create_financing_plan_models(factory):
    _, engine = factory
    tables = set(inspect(engine).get_table_names())
    assert {"financing_plans", "financing_plan_versions", "financing_plan_items", "financing_plan_gaps"} <= tables


def test_plan_versioning(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("10000000")}])
    value = service(factory)
    created = value.create_plan_draft(draft(first_item_id(factory)), created_by="admin")
    value.confirm_plan(created["financing_plan_id"], confirmed_by="admin")
    updated = value.create_new_plan_version(created["financing_plan_id"], draft(first_item_id(factory), amount="7000000"), created_by="admin")
    assert [row["version_no"] for row in updated["versions"]] == [1, 2]
    assert updated["versions"][0]["status"] == "superseded"


def test_confirmed_plan_immutable(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("10000000")}])
    value = service(factory)
    result = value.create_plan_draft(draft(first_item_id(factory)), created_by="admin")
    confirmed = value.confirm_plan(result["financing_plan_id"], confirmed_by="admin")
    version_id = confirmed["current_version_id"]
    with pytest.raises(ValueError, match="不可修改"):
        with factory[0].begin() as db:
            row = db.scalar(select(FinancingPlanVersion).where(FinancingPlanVersion.plan_version_id == version_id))
            row.summary = "试图原地修改"


@pytest.mark.parametrize("status", ["ineligible", "product_configuration_error"])
def test_excluded_product_cannot_enter_plan(factory, status):
    seed(factory, [{"status": status, "max_amount": Decimal("10000000")}])
    with pytest.raises(FinancingPlanError, match="不能进入"):
        service(factory).create_plan_draft(draft(first_item_id(factory)), created_by="admin")


def test_eligible_product_becomes_usable_candidate(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("10000000")}])
    assert candidate_pool(service(factory))["candidates"][0]["candidate_status"] == "usable"


def test_conditional_product_becomes_conditional_candidate(factory):
    seed(factory, [{"status": "conditional", "max_amount": Decimal("10000000"), "missing": ["补充材料"]}])
    result = candidate_pool(service(factory))["candidates"][0]
    assert result["candidate_status"] == "conditional" and result["missing_information"] == ["补充材料"]


def test_manual_review_product_requires_manual_action(factory):
    seed(factory, [{"status": "manual_review", "max_amount": Decimal("10000000"), "review": ["人工核验"]}])
    item_id = first_item_id(factory)
    with pytest.raises(FinancingPlanError, match="显式确认"):
        service(factory).create_plan_draft(draft(item_id), created_by="admin")
    result = service(factory).create_plan_draft(draft(item_id, manual=True, plan_type="conditional"), created_by="admin")
    assert result["versions"][0]["status"] == "needs_review"


def test_proposed_amount_cannot_exceed_product_max(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("5000000")}])
    with pytest.raises(FinancingPlanError, match="不能超过"):
        service(factory).create_plan_draft(draft(first_item_id(factory)), created_by="admin")


def test_unknown_max_amount_not_auto_usable(factory):
    seed(factory, [{"status": "eligible", "max_amount": None}])
    result = candidate_pool(service(factory))["candidates"][0]
    assert result["candidate_status"] == "manual_review" and result["eligible_amount_cap"] is None


def test_proposed_term_cannot_exceed_max_term(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("10000000"), "max_term": 6}])
    candidate = candidate_pool(service(factory))["candidates"][0]
    assert candidate["candidate_status"] == "excluded"
    assert any("期限" in reason for reason in candidate["blocking_reasons"])
    with pytest.raises(FinancingPlanError, match="不能进入"):
        service(factory).create_plan_draft(draft(first_item_id(factory), term=12), created_by="admin")


def test_unknown_term_not_auto_confirmed(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("10000000"), "max_term": None}])
    result = candidate_pool(service(factory))["candidates"][0]
    assert result["candidate_status"] == "manual_review"


def test_partial_plan_keeps_funding_gap(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("10000000")}])
    result = service(factory).create_plan_draft(draft(first_item_id(factory), amount="5000000"), created_by="admin")
    version = result["versions"][0]
    assert version["generation_status"] == "partial" and version["funding_gap"] == "3000000.00"
    assert version["gaps"][0]["gap_type"] == "amount_gap"


def test_complete_plan_requires_zero_gap():
    with pytest.raises(FinancingPlanError, match="零资金缺口"):
        PlanRuleEngine.validate_plan(Decimal("5000000"), Decimal("3000000"), "complete")


def test_requirement_version_frozen(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("10000000")}])
    result = service(factory).create_plan_draft(draft(first_item_id(factory)), created_by="admin")
    assert result["versions"][0]["source_requirement_version"] == 2


def test_match_snapshot_frozen(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("10000000")}])
    result = service(factory).create_plan_draft(draft(first_item_id(factory)), created_by="admin")
    assert result["versions"][0]["source_match_snapshot_id"] == SNAPSHOT_ID


def test_catalog_hash_frozen(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("10000000")}])
    result = service(factory).create_plan_draft(draft(first_item_id(factory)), created_by="admin")
    assert result["versions"][0]["source_catalog_hash"] == "catalog-hash"


def test_shanghai_yichuan_current_snapshot_no_fake_primary_plan(factory):
    seed(factory, [
        {"status": "ineligible", "max_amount": Decimal("5000000"), "code": "ABC-003"},
        {"status": "ineligible", "max_amount": Decimal("3500000"), "code": "BOC-003"},
        {"status": "ineligible", "max_amount": Decimal("5000000"), "code": "BOCOM-003", "max_term": None},
        {"status": "manual_review", "max_amount": None, "code": "BOCOM-005", "max_term": None},
        {"status": "manual_review", "max_amount": None, "code": "BOCOM-101", "max_term": None},
    ])
    result = candidate_pool(service(factory))
    assert result["counts"] == {"usable": 0, "conditional": 0, "manual_review": 2, "excluded": 3}
    assert result["generation_status"] == "manual_review_required"
    assert result["covered_amount"] == "0" and result["funding_gap"] == "8000000"
    with factory[0]() as db:
        assert db.scalar(select(FinancingPlan)) is None
