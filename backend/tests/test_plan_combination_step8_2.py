from __future__ import annotations

import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base
from backend.db_models import (
    FinancingPlan, FinancingPlanCondition, FinancingPlanExplanation, FinancingPlanGap,
    FinancingPlanItem, FinancingPlanMaterial, FinancingPlanVersion, ManualCandidateOverride,
    FinancingRequirement, ProductMatchItem, ProductMatchSnapshot,
)
from backend.services.financing_plan_service import FinancingPlanService
from backend.services.plan_combination_engine import CombinationStrategy, PlanCombinationEngine


CUSTOMER_ID = "enterprise_上海意川建筑科技有限公司"
REQUIREMENT_ID = "req-yichuan-v2"
SNAPSHOT_ID = "snapshot-yichuan"


def candidate(
    code: str,
    cap: str | None,
    *,
    status: str = "usable",
    minimum: str | None = None,
    term: int | None = 12,
    institution: str | None = None,
) -> dict:
    index = int("".join(value for value in code if value.isdigit()) or 1)
    return {
        "product_match_item_id": index,
        "product_id": f"product-{code}",
        "product_version_id": f"version-{code}",
        "external_product_code": code,
        "institution_name": institution or f"银行{code}",
        "product_name": f"产品{code}",
        "match_status": {
            "usable": "eligible", "conditional": "conditional",
            "manual_review": "manual_review", "excluded": "ineligible",
        }[status],
        "min_amount": minimum,
        "max_amount": cap,
        "max_term_months": term,
        "eligible_amount_cap": cap,
        "missing_information": ["补充资料"] if status == "conditional" else [],
        "review_reasons": ["人工核验"] if status == "manual_review" else [],
        "blocking_reasons": [],
        "candidate_status": status,
    }


def pool(*values: dict) -> dict:
    return {
        "customer_id": CUSTOMER_ID, "requirement_id": REQUIREMENT_ID,
        "requirement_version": 2, "match_snapshot_id": SNAPSHOT_ID,
        "currency": "CNY", "candidates": list(values),
    }


def generate(*values: dict, amount="8000000", term=12, strategy=None):
    return PlanCombinationEngine().generate_combinations(
        {"amount": amount, "term_months": term}, pool(*values), strategy,
    )


def test_single_product_full_coverage():
    result = generate(candidate("A1", "10000000"))
    assert len(result.formal_combinations) == 1 and result.formal_combinations[0].is_complete


def test_single_product_amount_is_target_not_max():
    item = generate(candidate("A1", "10000000")).formal_combinations[0].items[0]
    assert item.proposed_amount == Decimal("8000000")


def test_two_product_full_coverage():
    result = generate(candidate("A1", "5000000"), candidate("B2", "3000000"))
    assert any(value.is_complete and value.product_count == 2 for value in result.formal_combinations)


def test_three_product_combination():
    result = generate(candidate("A1", "3000000"), candidate("B2", "3000000"), candidate("C3", "2000000"))
    assert any(value.is_complete and value.product_count == 3 for value in result.formal_combinations)


def test_partial_coverage():
    result = generate(candidate("A1", "5000000"), candidate("B2", "2000000"))
    assert any(value.covered_amount == Decimal("7000000") and value.funding_gap == Decimal("1000000") for value in result.partial_combinations)


def test_conditional_combination():
    result = generate(candidate("A1", "5000000"), candidate("B2", "3000000", status="conditional"))
    combination = next(value for value in result.conditional_combinations if value.is_complete)
    assert combination.plan_type == "conditional" and combination.requires_manual_review


def test_manual_review_not_auto_used():
    result = generate(candidate("A1", "8000000", status="manual_review"))
    assert not result.formal_combinations and not result.conditional_combinations
    assert len(result.manual_review_candidates) == 1


@pytest.mark.parametrize("match_status", ["ineligible", "product_configuration_error"])
def test_excluded_status_never_used(match_status):
    value = candidate("A1", "8000000", status="excluded")
    value["match_status"] = match_status
    result = generate(value)
    assert not result.formal_combinations and len(result.excluded_candidates) == 1


def test_min_amount_validation():
    result = generate(candidate("A1", "5000000", minimum="3000000"), amount="2000000")
    assert not result.formal_combinations and not result.partial_combinations


def test_max_amount_validation():
    combination = generate(candidate("A1", "5000000"), amount="4000000").formal_combinations[0]
    assert combination.items[0].proposed_amount <= combination.items[0].allocation_cap


def test_term_validation():
    result = generate(candidate("A1", "8000000", term=12), term=24)
    assert not result.formal_combinations


def test_unknown_term_not_formal():
    result = generate(candidate("A1", "8000000", status="conditional", term=None))
    assert not result.formal_combinations
    assert result.conditional_combinations[0].gaps[0].gap_type == "term_gap"


def test_combination_dedup():
    result = generate(candidate("A1", "5000000"), candidate("B2", "3000000"))
    identities = [value.combination_id for value in result.formal_combinations + result.partial_combinations]
    assert len(identities) == len(set(identities))


def test_max_three_products():
    result = generate(*(candidate(f"A{i}", "2000000") for i in range(1, 5)))
    assert all(value.product_count <= 3 for value in result.formal_combinations + result.partial_combinations)


def test_max_three_results_per_group():
    result = generate(*(candidate(f"A{i}", "8000000") for i in range(1, 7)))
    assert len(result.formal_combinations) == 3


def test_zero_gap_complete():
    value = generate(candidate("A1", "8000000")).formal_combinations[0]
    assert value.funding_gap == 0 and value.generation_status == "complete"


def test_positive_gap_partial():
    value = next(row for row in generate(candidate("A1", "5000000")).partial_combinations if row.product_count == 1)
    assert value.funding_gap == Decimal("3000000") and value.generation_status == "partial"


def test_plan_gap_generated():
    value = generate(candidate("A1", "5000000")).partial_combinations[0]
    assert value.gaps[0].gap_type == "amount_gap"


def test_same_institution_is_marked_not_rejected():
    result = generate(
        candidate("A1", "5000000", institution="同一银行"),
        candidate("B2", "3000000", institution="同一银行"),
    )
    combination = next(value for value in result.formal_combinations if value.product_count == 2)
    assert combination.institution_concentration and any("同一机构" in risk for risk in combination.risks)


@pytest.fixture
def factory():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[
        FinancingRequirement.__table__, ProductMatchSnapshot.__table__, ProductMatchItem.__table__,
        FinancingPlan.__table__, FinancingPlanVersion.__table__, FinancingPlanItem.__table__, FinancingPlanGap.__table__,
        ManualCandidateOverride.__table__, FinancingPlanCondition.__table__,
        FinancingPlanMaterial.__table__, FinancingPlanExplanation.__table__,
    ])
    yield sessionmaker(bind=engine)
    engine.dispose()


def seed_db(factory, products: list[dict]):
    facts = json.dumps({"requirement": {
        "amount": {"status": "known", "value": "8000000", "unit": "CNY"},
        "term_months": {"status": "known", "value": 12, "unit": "month"},
        "currency": {"status": "known", "value": "CNY"},
    }}, ensure_ascii=False)
    with factory.begin() as db:
        db.add(FinancingRequirement(
            requirement_id=REQUIREMENT_ID, customer_id=CUSTOMER_ID, version=2, status="confirmed",
            borrower_entity="上海意川建筑科技有限公司", requested_amount=Decimal("8000000"),
            currency="CNY", financing_purpose="采购", purpose_detail="材料采购", term_value=12, term_unit="month",
        ))
        db.add(ProductMatchSnapshot(
            snapshot_id=SNAPSHOT_ID, context_hash="context", customer_id=CUSTOMER_ID,
            requirement_id=REQUIREMENT_ID, requirement_version=2, facts_hash="facts-hash",
            facts_json=facts, catalog_as_of_date=date(2026, 9, 28), catalog_version_hash="catalog-hash",
            generated_at=datetime(2026, 9, 28, 7, 27, 51), generated_by="admin", summary_json="{}",
        ))
        db.flush()
        for index, product in enumerate(products, 1):
            db.add(ProductMatchItem(
                snapshot_id=SNAPSHOT_ID, product_id=f"product-{index}", version_id=f"version-{index}",
                external_product_code=product.get("code", f"TEST-{index}"), institution_name="测试银行",
                product_name=f"产品{index}", product_category="enterprise_credit",
                max_amount=product.get("max_amount"), max_term_months=product.get("max_term", 12),
                overall_status=product["status"], blocking_reasons_json="[]",
                missing_information_json="[]", review_reasons_json="[]", soft_gaps_json="[]",
                rule_results_json="[]",
            ))


def test_create_draft_from_combination(factory):
    seed_db(factory, [{"status": "eligible", "max_amount": Decimal("10000000")}])
    service = FinancingPlanService(session_factory=factory, ensure_schema=False)
    generated = service.generate_plan_combinations(CUSTOMER_ID, REQUIREMENT_ID, SNAPSHOT_ID)
    combination_id = generated["formal_combinations"][0]["combination_id"]
    plan = service.create_plan_from_combination(
        CUSTOMER_ID, REQUIREMENT_ID, SNAPSHOT_ID, combination_id, created_by="admin",
    )
    assert len(plan["versions"][0]["items"]) == 1


def test_draft_not_auto_confirmed(factory):
    seed_db(factory, [{"status": "eligible", "max_amount": Decimal("10000000")}])
    service = FinancingPlanService(session_factory=factory, ensure_schema=False)
    generated = service.generate_plan_combinations(CUSTOMER_ID, REQUIREMENT_ID, SNAPSHOT_ID)
    plan = service.create_plan_from_combination(
        CUSTOMER_ID, REQUIREMENT_ID, SNAPSHOT_ID,
        generated["formal_combinations"][0]["combination_id"], created_by="admin",
    )
    assert plan["status"] == "draft" and plan["versions"][0]["status"] == "draft"


def test_shanghai_yichuan_generates_no_fake_combination():
    result = generate(
        candidate("ABC003", "5000000", status="excluded"),
        candidate("BOC003", "3500000", status="excluded"),
        candidate("BOCOM003", "5000000", status="excluded"),
        candidate("BOCOM005", None, status="manual_review", term=None),
        candidate("BOCOM101", None, status="manual_review", term=None),
    )
    assert len(result.formal_combinations) == 0
    assert len(result.conditional_combinations) == 0
    assert len(result.partial_combinations) == 0
    assert result.covered_amount == 0 and result.funding_gap == Decimal("8000000")
    assert result.generation_status == "manual_review_required"
    assert len(result.manual_review_candidates) == 2 and len(result.excluded_candidates) == 3
