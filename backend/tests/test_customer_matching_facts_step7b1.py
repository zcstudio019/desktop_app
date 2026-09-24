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
from backend.db_models import FinancingRequirement
from backend.services import customer_matching_facts_service as matching
from backend.services.customer_matching_facts_service import (
    FactValue, MATCHING_FACT_FIELD_TYPES, MatchingFactsAccessor, build_customer_matching_facts,
)
from backend.services.product_catalog_service import FIELD_TYPES, fact_state


class Context:
    def __init__(self, payload):
        self.payload = payload

    def model_dump(self):
        return deepcopy(self.payload)


class ReadOnlyStorage:
    writes = 0
    llm_calls = 0


@pytest.fixture
def factory():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[FinancingRequirement.__table__])
    factory = sessionmaker(bind=engine)
    yield factory
    engine.dispose()


def add_requirement(factory, *, status="confirmed"):
    requirement_id = "req-yichuan-v1"
    with factory() as db:
        row = FinancingRequirement(
            requirement_id=requirement_id, customer_id="enterprise_上海意川建筑科技有限公司",
            version=1, status=status, borrower_entity="上海意川建筑科技有限公司",
            requested_amount=Decimal("8000000.00"), currency="CNY", amount_confirmed=1,
            financing_purpose="采购", purpose_detail="材料采购", term_value=12, term_unit="month",
            term_confirmed=1, accept_mortgage=None, accept_additional_guarantee=None, accept_refinancing=None,
            preferred_banks_json="[]", excluded_banks_json="[]", existing_banks_json="[]",
            guarantee_preference_json="[]", collateral_available_json="[]", field_sources_json="{}",
        )
        db.add(row)
        db.commit()
    return requirement_id


def yichuan_context():
    return {
        "subject_profile": {
            "status": "available", "source": "kyc_profile", "as_of": "2026-03-31",
            "enterprise_name": "上海意川建筑科技有限公司", "unified_social_credit_code": "91310118MA1JP7UB2B",
            "legal_representative": "黎云", "actual_controller": "黎云", "established_date": "2019-06-28",
            "enterprise_type": "有限责任公司", "industry": "建筑业", "technology_enterprise_tags": [],
        },
        "enterprise_credit": {
            "status": "available", "source": "stable_credit_fact_adapter", "as_of": "2026-03-31",
            "outstanding_loan_balance": 1856.5, "outstanding_loan_balance_money": {"value": 1856.5, "unit": None},
            "loan_record_count": 2, "outstanding_loan_institution_count": 2,
            "overdue_summary": {"count": 0}, "nonperforming_summary": {"count": None},
        },
        "personal_credit": {
            "status": "available", "source": "stable_credit_fact_adapter", "as_of": "2026-03-31",
            "people": [{
                "name": "黎云", "source_report_date": "2026-03-31", "loan_balance": 3314569,
                "loan_balance_money": {"value": 3314569, "unit": "元"}, "loan_balance_unit": "元",
                "loan_account_count": 10, "credit_card_limit": 17000,
                "credit_card_limit_money": {"value": 17000, "unit": "元"}, "credit_card_used": 1000,
                "credit_card_used_money": {"value": 1000, "unit": "元"},
                "related_repayment_balance": 18739532,
                "related_repayment_balance_money": {"value": 18739532, "unit": "元"},
                "related_repayment_balance_unit": "元",
                "overdue_summary": {"loan_overdue_account_count": 0, "credit_card_overdue_account_count": 0,
                                    "overdue_90d_account_count": 0},
                "query_summary": {
                    "near_1_month": {"loan_approval": 0, "credit_card_approval": 0, "guarantee_review": 0, "legal_person_review": 0},
                    "near_3_months": {"loan_approval": 0, "credit_card_approval": 0, "guarantee_review": 0, "legal_person_review": 0},
                    "near_6_months": {"loan_approval": 2, "credit_card_approval": 0, "guarantee_review": 6, "legal_person_review": 0},
                    "near_1_year": {"loan_approval": 3, "credit_card_approval": 1, "guarantee_review": 7, "legal_person_review": 0},
                },
            }],
        },
        "financials": {
            "status": "available", "source": "financial_report_aggregator", "latest_period": "2026-03-31",
            "latest": {"period": "2026-03-31", "period_type": "monthly", "unit": "元",
                       "total_assets": 53789185.41, "total_liabilities": 53537100.71, "net_assets": 252084.70,
                       "revenue": 1000000, "operating_cost": 900000, "net_profit": -647521.89,
                       "debt_asset_ratio": 0.9953, "accounts_receivable": 12000000, "inventory": 3000000,
                       "short_term_borrowings": 15240000, "long_term_borrowings": 0,
                       "operating_cashflow": None},
        },
        "enterprise_cashflow": {
            "status": "available", "source": "enterprise_flow_aggregator", "as_of": "2026-03-31",
            "statement_period": {"start": "2025-04-01", "end": "2026-03-31", "months": 12},
            "total_inflow": 42499565.67, "operating_inflow": 19493700,
            "internal_transfer_inflow": 7331500, "non_operating_inflow": 15654365.67,
            "monthly_average_operating_inflow": 1624475, "account_count": 2,
        },
        "assets": {"status": "missing", "source": None, "as_of": None, "property": [], "vehicle": [], "equipment": []},
        "conflicts": [],
    }


def build(factory, monkeypatch, payload=None, *, status="confirmed"):
    requirement_id = add_requirement(factory, status=status)
    context = Context(payload or yichuan_context())

    async def read_context(_storage, _customer_id):
        return context

    monkeypatch.setattr(matching, "build_comprehensive_financing_report_context", read_context)
    return asyncio.run(build_customer_matching_facts(
        "enterprise_上海意川建筑科技有限公司", requirement_id,
        storage_service=ReadOnlyStorage(), session_factory=factory, as_of_date=date(2026, 9, 23),
    ))


def test_build_facts_requires_confirmed_requirement(factory, monkeypatch):
    with pytest.raises(ValueError, match="已确认"):
        build(factory, monkeypatch, status="needs_confirmation")


def test_missing_asset_is_unknown_not_false(factory, monkeypatch):
    fact = MatchingFactsAccessor(build(factory, monkeypatch)).get_fact("asset.has_real_estate")
    assert fact.status == "unknown" and fact.value is None


def test_missing_tax_is_unknown_not_zero(factory, monkeypatch):
    fact = MatchingFactsAccessor(build(factory, monkeypatch)).get_fact("business.tax_12m")
    assert fact.status == "unknown" and fact.value is None


def test_unfilled_optional_requirement_list_is_unknown(factory, monkeypatch):
    accessor = MatchingFactsAccessor(build(factory, monkeypatch))
    assert accessor.get_fact("requirement.preferred_banks").status == "unknown"
    assert accessor.get_fact("requirement.excluded_banks").status == "unknown"


def test_explicit_zero_is_known_zero(factory, monkeypatch):
    accessor = MatchingFactsAccessor(build(factory, monkeypatch))
    fact = accessor.get_fact("credit.personal.current_overdue_count")
    assert fact.status == "known" and fact.value == 0
    assert accessor.get_fact("credit.personal.overdue_90d_count").value == 0


def test_conflicted_fact_is_conflict(factory, monkeypatch):
    payload = yichuan_context()
    payload["personal_credit"]["people"].append({**payload["personal_credit"]["people"][0], "name": "另一主体"})
    fact = MatchingFactsAccessor(build(factory, monkeypatch, payload)).get_fact("credit.personal.outstanding_loan_balance")
    assert fact.status == "conflict" and len(fact.candidate_values) == 2


def test_enterprise_credit_unit_unknown_not_forced(factory, monkeypatch):
    fact = MatchingFactsAccessor(build(factory, monkeypatch)).get_fact("credit.enterprise.outstanding_loan_balance")
    assert fact.status == "unknown" and fact.value is None
    assert fact.reason == "amount_unit_unverified" and fact.candidate_values[0].value == Decimal("1856.5")


def test_personal_credit_balance_known(factory, monkeypatch):
    fact = MatchingFactsAccessor(build(factory, monkeypatch)).get_fact("credit.personal.outstanding_loan_balance")
    assert fact.status == "known" and fact.value == Decimal("3314569") and fact.unit == "CNY"


def test_financial_latest_period_used(factory, monkeypatch):
    accessor = MatchingFactsAccessor(build(factory, monkeypatch))
    assert accessor.get_fact("financial.period_end").value == "2026-03-31"
    assert accessor.get_fact("financial.total_assets").value == Decimal("53789185.41")


def test_cashflow_preliminary_classification_preserved(factory, monkeypatch):
    facts = build(factory, monkeypatch)
    accessor = MatchingFactsAccessor(facts)
    assert accessor.get_fact("cashflow.classification_status").value == "preliminary"
    assert "cashflow.operating_inflow" in facts.data_quality.preliminary_fields


def test_no_group_total_debt_is_invented(factory, monkeypatch):
    dumped = build(factory, monkeypatch).model_dump()
    assert "group_total_debt" not in str(dumped)


def test_technology_qualification_not_inferred_from_company_name(factory, monkeypatch):
    accessor = MatchingFactsAccessor(build(factory, monkeypatch))
    assert accessor.get_fact("qualification.is_high_tech_enterprise").status == "unknown"
    assert accessor.get_fact("qualification.is_technology_sme").status == "unknown"


def test_fact_accessor(factory, monkeypatch):
    accessor = MatchingFactsAccessor(build(factory, monkeypatch))
    fact = accessor.get_fact("requirement.amount")
    assert isinstance(fact, FactValue) and fact.value == Decimal("8000000.00") and fact.source.source_type == "confirmed_financing_requirement"
    with pytest.raises(KeyError):
        accessor.get_fact("customer.__class__")


def test_whitelist_fields_resolvable(factory, monkeypatch):
    facts = build(factory, monkeypatch)
    accessor = MatchingFactsAccessor(facts)
    assert FIELD_TYPES == MATCHING_FACT_FIELD_TYPES
    assert all(isinstance(accessor.get_fact(path), FactValue) for path in FIELD_TYPES)
    serialized = facts.model_dump()
    assert fact_state(serialized, "requirement.amount")["status"] == "known"
    assert fact_state(serialized, "asset.has_real_estate")["status"] == "unknown"


def test_build_facts_is_read_only(factory, monkeypatch):
    facts = build(factory, monkeypatch)
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(FinancingRequirement)) == 1
    assert facts.requirement_id == "req-yichuan-v1" and ReadOnlyStorage.writes == 0


def test_build_facts_does_not_call_llm(factory, monkeypatch):
    build(factory, monkeypatch)
    assert ReadOnlyStorage.llm_calls == 0


def test_yichuan_matching_facts_regression(factory, monkeypatch):
    accessor = MatchingFactsAccessor(build(factory, monkeypatch))
    expected = {
        "requirement.amount": Decimal("8000000.00"), "requirement.term_months": 12,
        "credit.personal.outstanding_loan_balance": Decimal("3314569"),
        "credit.personal.outstanding_loan_count": 10, "credit.personal.credit_card_limit": Decimal("17000"),
        "credit.personal.current_overdue_count": 0, "credit.personal.overdue_90d_count": 0,
        "financial.total_assets": Decimal("53789185.41"),
        "financial.total_liabilities": Decimal("53537100.71"), "financial.net_assets": Decimal("252084.7"),
        "financial.debt_asset_ratio": 0.9953, "financial.net_profit": Decimal("-647521.89"),
        "financial.short_term_borrowing": Decimal("15240000"), "cashflow.coverage_months": 12,
        "cashflow.operating_inflow": Decimal("19493700"),
        "cashflow.average_monthly_operating_inflow": Decimal("1624475"),
    }
    for path, value in expected.items():
        assert accessor.get_fact(path).value == value
    assert accessor.get_fact("asset.has_confirmed_collateral").status == "unknown"
