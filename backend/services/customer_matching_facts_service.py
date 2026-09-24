"""Read-only, source-traceable customer facts for product matching.

This module does not execute product rules, OCR, agents, or LLM calls.  It adapts
already persisted structured customer data and one confirmed financing
requirement into the only input protocol future matching code may consume.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select

from backend.database import SessionLocal
from backend.db_models import FinancingRequirement
from backend.services.comprehensive_financing_report_context_service import build_comprehensive_financing_report_context
from backend.services.financing_requirement_service import requirement_to_dict


FactStatus = Literal["known", "unknown", "not_applicable", "conflict"]
FACTS_VERSION = "customer_matching_facts.v1"


class FactSource(BaseModel):
    source_type: str
    source_id: str | None = None
    source_date: str | None = None
    evidence_refs: list[str] = Field(default_factory=list)


class FactCandidate(BaseModel):
    value: Any = None
    unit: str | None = None
    source: FactSource | None = None


class FactValue(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    value: Any = None
    status: FactStatus = "unknown"
    unit: str | None = None
    source: FactSource | None = None
    as_of_date: str | None = None
    candidate_values: list[FactCandidate] = Field(default_factory=list)
    reason: str | None = None

    @model_validator(mode="after")
    def validate_state(self) -> "FactValue":
        if self.status == "known" and self.value is None:
            raise ValueError("known fact must have a value")
        if self.status in {"unknown", "not_applicable", "conflict"} and self.value is not None:
            raise ValueError(f"{self.status} fact cannot expose an executable value")
        return self


class MatchingDataQuality(BaseModel):
    missing_domains: list[str] = Field(default_factory=list)
    conflicted_fields: list[str] = Field(default_factory=list)
    stale_fields: list[str] = Field(default_factory=list)
    preliminary_fields: list[str] = Field(default_factory=list)


class CustomerMatchingFacts(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    customer_id: str
    requirement_id: str
    requirement_version: int
    generated_at: str
    as_of_date: str
    facts_version: str = FACTS_VERSION
    requirement: dict[str, Any] = Field(default_factory=dict)
    customer: dict[str, Any] = Field(default_factory=dict)
    credit: dict[str, Any] = Field(default_factory=dict)
    financial: dict[str, Any] = Field(default_factory=dict)
    cashflow: dict[str, Any] = Field(default_factory=dict)
    asset: dict[str, Any] = Field(default_factory=dict)
    business: dict[str, Any] = Field(default_factory=dict)
    qualification: dict[str, Any] = Field(default_factory=dict)
    data_quality: MatchingDataQuality = Field(default_factory=MatchingDataQuality)


MATCHING_FACT_FIELD_TYPES: dict[str, str] = {
    # Confirmed financing requirement.
    "requirement.amount": "number", "requirement.currency": "string", "requirement.purpose": "string",
    "requirement.term_months": "number", "requirement.expected_funding_date": "date",
    "requirement.accept_mortgage": "boolean", "requirement.accept_additional_guarantee": "boolean",
    "requirement.accept_refinancing": "boolean", "requirement.registered_region": "string",
    "requirement.operating_region": "string", "requirement.preferred_banks": "list",
    "requirement.excluded_banks": "list",
    # Enterprise and responsible persons.
    "customer.company_name": "string", "customer.unified_social_credit_code": "string",
    "customer.legal_representative": "string", "customer.actual_controller": "string",
    "customer.company_established_date": "date", "customer.company_age_months": "number",
    "customer.registered_region": "string", "customer.operating_region": "string",
    "customer.enterprise_type": "string", "customer.industry": "string",
    "customer.legal_representative_age": "number", "customer.actual_controller_age": "number",
    "customer.legal_representative_shareholding": "number", "customer.actual_controller_shareholding": "number",
    # Enterprise credit.
    "credit.enterprise.report_date": "date", "credit.enterprise.current_overdue_count": "number",
    "credit.enterprise.overdue_90d_count": "number", "credit.enterprise.outstanding_loan_balance": "number",
    "credit.enterprise.outstanding_loan_count": "number", "credit.enterprise.loan_institution_count": "number",
    "credit.enterprise.external_guarantee_balance": "number",
    "credit.enterprise.abnormal_classification_count": "number", "credit.enterprise.has_current_overdue": "boolean",
    # Personal credit.
    "credit.personal.report_date": "date", "credit.personal.current_overdue_count": "number",
    "credit.personal.overdue_90d_count": "number", "credit.personal.outstanding_loan_balance": "number",
    "credit.personal.outstanding_loan_count": "number", "credit.personal.loan_institution_count": "number",
    "credit.personal.credit_card_limit": "number", "credit.personal.credit_card_used": "number",
    "credit.personal.related_repayment_balance": "number",
    # Financial statement latest period.
    "financial.period_type": "string", "financial.period_start": "date", "financial.period_end": "date",
    "financial.total_assets": "number", "financial.total_liabilities": "number", "financial.net_assets": "number",
    "financial.revenue": "number", "financial.operating_cost": "number", "financial.net_profit": "number",
    "financial.debt_asset_ratio": "number", "financial.accounts_receivable": "number",
    "financial.inventory": "number", "financial.short_term_borrowing": "number",
    "financial.long_term_borrowing": "number", "financial.operating_cash_flow": "number",
    # Enterprise cashflow.
    "cashflow.coverage_months": "number", "cashflow.period_start": "date", "cashflow.period_end": "date",
    "cashflow.total_inflow": "number", "cashflow.operating_inflow": "number",
    "cashflow.internal_transfer_inflow": "number",
    "cashflow.non_operating_or_unidentified_inflow": "number",
    "cashflow.average_monthly_operating_inflow": "number", "cashflow.account_count": "number",
    "cashflow.classification_status": "string",
    # Assets.
    "asset.has_real_estate": "boolean", "asset.real_estate_count": "number",
    "asset.has_vehicle": "boolean", "asset.vehicle_count": "number", "asset.has_equipment": "boolean",
    "asset.has_confirmed_collateral": "boolean", "asset.confirmed_collateral_value": "number",
    "asset.real_estate_regions": "list", "asset.real_estate_types": "list",
    # Formal qualifications.
    "qualification.is_high_tech_enterprise": "boolean",
    "qualification.is_specialized_innovative": "boolean", "qualification.is_technology_sme": "boolean",
    "qualification.is_little_giant": "boolean", "qualification.has_invention_patent": "boolean",
    "qualification.invention_patent_count": "number", "qualification.software_copyright_count": "number",
    "qualification.intellectual_property_count": "number", "qualification.qualification_expiry_date": "date",
    # Tax and invoice facts; these remain unknown until a stable structured source exists.
    "business.tax_grade": "string", "business.tax_12m": "number", "business.tax_24m": "number",
    "business.invoice_12m": "number", "business.revenue_12m": "number",
    "business.zero_tax_months_12m": "number", "business.zero_invoice_months_12m": "number",
    "business.tax_overdue_count": "number",
}

for _window in ("1m", "3m", "6m", "12m"):
    for _category in ("loan_approval", "credit_card_approval", "guarantor_qualification", "legal_person_credit_review"):
        MATCHING_FACT_FIELD_TYPES[f"credit.personal.query_{_window}.{_category}"] = "number"


def _unknown(reason: str = "insufficient_data", *, candidates: list[FactCandidate] | None = None) -> FactValue:
    return FactValue(status="unknown", reason=reason, candidate_values=candidates or [])


def _known(value: Any, *, unit: str | None = None, source: FactSource | None = None,
           as_of_date: str | None = None) -> FactValue:
    return FactValue(value=value, status="known", unit=unit, source=source, as_of_date=as_of_date)


def _conflict(candidates: list[FactCandidate], reason: str) -> FactValue:
    return FactValue(status="conflict", candidate_values=candidates, reason=reason)


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _source(source_type: str | None, source_id: str | None = None, source_date: str | None = None,
            evidence_ref: str | None = None) -> FactSource | None:
    if not source_type:
        return None
    return FactSource(source_type=source_type, source_id=source_id, source_date=source_date,
                      evidence_refs=[evidence_ref] if evidence_ref else [])


def _value_fact(value: Any, source: FactSource | None, *, unit: str | None = None,
                as_of_date: str | None = None, reason: str = "insufficient_data",
                allow_empty: bool = False) -> FactValue:
    missing = value is None or value == "" or (not allow_empty and value in ([], {}))
    return _unknown(reason) if missing else _known(value, unit=unit, source=source, as_of_date=as_of_date)


def _money_fact(value: Any, unit: str | None, source: FactSource | None, *, as_of_date: str | None = None) -> FactValue:
    amount = _decimal(value)
    if amount is None:
        return _unknown()
    normalized_unit = str(unit or "").strip()
    if normalized_unit not in {"元", "人民币元", "CNY"}:
        candidate = FactCandidate(value=amount, unit=normalized_unit or None, source=source)
        return _unknown("amount_unit_unverified", candidates=[candidate])
    return _known(amount, unit="CNY", source=source, as_of_date=as_of_date)


def _set_fact(tree: dict[str, Any], path: str, fact: FactValue) -> None:
    parts = path.split(".")
    current = tree
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    current[parts[-1]] = fact


def _get_node(tree: Any, path: str) -> Any:
    current = tree
    for part in path.split("."):
        if isinstance(current, BaseModel):
            current = getattr(current, part, None)
        elif isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _empty_fact_tree() -> dict[str, Any]:
    result: dict[str, Any] = {}
    for path in MATCHING_FACT_FIELD_TYPES:
        _set_fact(result, path, _unknown())
    return result


def _months_between(start: str | None, end: date) -> int | None:
    if not start:
        return None
    try:
        established = date.fromisoformat(str(start)[:10])
    except ValueError:
        return None
    if established > end:
        return None
    return (end.year - established.year) * 12 + end.month - established.month - (1 if end.day < established.day else 0)


def _query_category(row: dict[str, Any], target: str) -> Any:
    aliases = {
        "loan_approval": ("loan_approval",), "credit_card_approval": ("credit_card_approval",),
        "guarantor_qualification": ("guarantee_review", "guarantor_qualification"),
        "legal_person_credit_review": ("legal_person_review", "legal_person_credit_review"),
    }
    for key in aliases[target]:
        if key in row:
            return row.get(key)
    return None


class MatchingFactsAccessor:
    """Whitelist-only fact access; never exposes arbitrary Python attributes."""

    def __init__(self, facts: CustomerMatchingFacts | dict[str, Any]):
        self._facts = facts

    def get_fact(self, field_name: str) -> FactValue:
        if field_name not in MATCHING_FACT_FIELD_TYPES:
            raise KeyError(f"Fact field is not whitelisted: {field_name}")
        value = _get_node(self._facts, field_name)
        if isinstance(value, FactValue):
            return value
        if isinstance(value, dict):
            return FactValue.model_validate(value)
        return _unknown()


async def build_customer_matching_facts(
    customer_id: str,
    requirement_id: str,
    *,
    storage_service: Any | None = None,
    session_factory=SessionLocal,
    as_of_date: date | None = None,
) -> CustomerMatchingFacts:
    """Build matching facts from persisted structured data without writes or AI calls."""
    as_of = as_of_date or date.today()
    with session_factory() as db:
        requirement_row = db.execute(select(FinancingRequirement).where(
            FinancingRequirement.customer_id == customer_id,
            FinancingRequirement.requirement_id == requirement_id,
        )).scalar_one_or_none()
        if requirement_row is None:
            raise LookupError("融资需求不存在")
        if requirement_row.status != "confirmed":
            raise ValueError("产品匹配事实只能基于已确认的融资需求")
        requirement = requirement_to_dict(requirement_row)

    if storage_service is None:
        from backend.services import get_storage_service
        storage_service = get_storage_service()
    context = await build_comprehensive_financing_report_context(storage_service, customer_id)
    context_data = context.model_dump()
    tree = _empty_fact_tree()

    requirement_source = _source("confirmed_financing_requirement", requirement_id,
                                 requirement.get("confirmed_at"), f"financing_requirements:{requirement_id}")
    requirement_field_sources = requirement.get("field_sources") or {}
    def explicit_requirement_fact(field_name: str) -> FactValue:
        return (_value_fact(requirement.get(field_name), requirement_source, allow_empty=True)
                if requirement_field_sources.get(field_name) else _unknown("not_explicitly_confirmed"))
    requirement_values = {
        "requirement.amount": _money_fact(requirement.get("requested_amount"), requirement.get("currency"), requirement_source),
        "requirement.currency": _value_fact(requirement.get("currency"), requirement_source),
        "requirement.purpose": _value_fact(requirement.get("purpose_detail") or requirement.get("financing_purpose"), requirement_source),
        "requirement.expected_funding_date": _value_fact(requirement.get("expected_funding_date"), requirement_source),
        "requirement.accept_mortgage": _value_fact(requirement.get("accept_mortgage"), requirement_source),
        "requirement.accept_additional_guarantee": _value_fact(requirement.get("accept_additional_guarantee"), requirement_source),
        "requirement.accept_refinancing": _value_fact(requirement.get("accept_refinancing"), requirement_source),
        "requirement.registered_region": _value_fact(requirement.get("registered_region"), requirement_source),
        "requirement.operating_region": _value_fact(requirement.get("operating_region"), requirement_source),
        "requirement.preferred_banks": explicit_requirement_fact("preferred_banks"),
        "requirement.excluded_banks": explicit_requirement_fact("excluded_banks"),
    }
    term_value, term_unit = requirement.get("term_value"), requirement.get("term_unit")
    if term_value is None:
        requirement_values["requirement.term_months"] = _unknown()
    elif term_unit == "month":
        requirement_values["requirement.term_months"] = _known(int(term_value), unit="month", source=requirement_source)
    elif term_unit == "year":
        requirement_values["requirement.term_months"] = _known(int(term_value) * 12, unit="month", source=requirement_source)
    else:
        requirement_values["requirement.term_months"] = _unknown("term_unit_not_convertible",
            candidates=[FactCandidate(value=term_value, unit=term_unit, source=requirement_source)])
    for path, fact in requirement_values.items():
        _set_fact(tree, path, fact)

    subject = context_data.get("subject_profile") or {}
    subject_source = _source(subject.get("source"), None, subject.get("as_of"), "comprehensive_context.subject_profile")
    subject_mapping = {
        "customer.company_name": subject.get("enterprise_name"),
        "customer.unified_social_credit_code": subject.get("unified_social_credit_code"),
        "customer.legal_representative": subject.get("legal_representative"),
        "customer.actual_controller": subject.get("actual_controller"),
        "customer.company_established_date": subject.get("established_date"),
        "customer.registered_region": subject.get("registered_region"),
        "customer.operating_region": subject.get("operating_region"),
        "customer.enterprise_type": subject.get("enterprise_type"), "customer.industry": subject.get("industry"),
        "customer.legal_representative_age": subject.get("legal_representative_age"),
        "customer.actual_controller_age": subject.get("actual_controller_age"),
        "customer.legal_representative_shareholding": subject.get("legal_representative_shareholding"),
        "customer.actual_controller_shareholding": subject.get("actual_controller_shareholding"),
    }
    for path, value in subject_mapping.items():
        _set_fact(tree, path, _value_fact(value, subject_source, as_of_date=subject.get("as_of")))
    age_months = _months_between(subject.get("established_date"), as_of)
    _set_fact(tree, "customer.company_age_months", _value_fact(age_months, subject_source, unit="month", as_of_date=as_of.isoformat()))

    conflict_map = {"enterprise_subject": "customer.company_name", "legal_representative": "customer.legal_representative",
                    "actual_controller": "customer.actual_controller"}
    for item in context_data.get("conflicts") or []:
        path = conflict_map.get(str(item.get("type") or ""))
        if path:
            _set_fact(tree, path, _conflict([], str(item.get("message") or "multiple_sources_disagree")))

    enterprise = context_data.get("enterprise_credit") or {}
    enterprise_date = enterprise.get("as_of") or enterprise.get("source_report_date")
    enterprise_source = _source(enterprise.get("source"), None, enterprise_date, "comprehensive_context.enterprise_credit")
    _set_fact(tree, "credit.enterprise.report_date", _value_fact(enterprise_date, enterprise_source))
    current_enterprise_overdue = (enterprise.get("current_overdue_count")
                                  if "current_overdue_count" in enterprise else (enterprise.get("overdue_summary") or {}).get("current_count"))
    _set_fact(tree, "credit.enterprise.current_overdue_count", _value_fact(current_enterprise_overdue, enterprise_source, unit="count"))
    _set_fact(tree, "credit.enterprise.has_current_overdue",
              _known(bool(current_enterprise_overdue), source=enterprise_source) if current_enterprise_overdue is not None else _unknown())
    _set_fact(tree, "credit.enterprise.overdue_90d_count", _value_fact((enterprise.get("overdue_summary") or {}).get("overdue_90d_count"), enterprise_source, unit="count"))
    enterprise_money = enterprise.get("outstanding_loan_balance_money") or {}
    _set_fact(tree, "credit.enterprise.outstanding_loan_balance", _money_fact(
        enterprise_money.get("value", enterprise.get("outstanding_loan_balance")),
        enterprise_money.get("unit") or enterprise.get("unit"), enterprise_source, as_of_date=enterprise_date))
    _set_fact(tree, "credit.enterprise.outstanding_loan_count", _value_fact(enterprise.get("loan_record_count"), enterprise_source, unit="count"))
    _set_fact(tree, "credit.enterprise.loan_institution_count", _value_fact(enterprise.get("outstanding_loan_institution_count"), enterprise_source, unit="count"))
    guarantee_money = enterprise.get("guarantee_balance_money") or {}
    _set_fact(tree, "credit.enterprise.external_guarantee_balance", _money_fact(
        guarantee_money.get("value", enterprise.get("guarantee_balance")),
        guarantee_money.get("unit") or enterprise.get("unit"), enterprise_source, as_of_date=enterprise_date))
    _set_fact(tree, "credit.enterprise.abnormal_classification_count", _value_fact(
        (enterprise.get("nonperforming_summary") or {}).get("count"), enterprise_source, unit="count"))

    personal_section = context_data.get("personal_credit") or {}
    people = personal_section.get("people") or []
    person: dict[str, Any] | None = people[0] if len(people) == 1 else None
    if len(people) > 1:
        for path in ("credit.personal.report_date", "credit.personal.current_overdue_count",
                     "credit.personal.overdue_90d_count", "credit.personal.outstanding_loan_balance",
                     "credit.personal.outstanding_loan_count", "credit.personal.loan_institution_count",
                     "credit.personal.credit_card_limit", "credit.personal.credit_card_used",
                     "credit.personal.related_repayment_balance"):
            _set_fact(tree, path, _conflict([
                FactCandidate(value=row.get("loan_balance"), unit=row.get("loan_balance_unit"),
                              source=_source("personal_credit_report", None, row.get("source_report_date"), f"person:{row.get('name')}"))
                for row in people], "multiple_personal_credit_subjects"))
    if person:
        personal_date = person.get("source_report_date") or personal_section.get("as_of")
        personal_source = _source(personal_section.get("source"), None, personal_date,
                                  f"comprehensive_context.personal_credit:{person.get('name')}")
        overdue = person.get("overdue_summary") or {}
        loan_overdue, card_overdue = overdue.get("loan_overdue_account_count"), overdue.get("credit_card_overdue_account_count")
        current_overdue = int((loan_overdue or 0) + (card_overdue or 0)) if loan_overdue is not None or card_overdue is not None else None
        personal_values = {
            "credit.personal.report_date": _value_fact(personal_date, personal_source),
            "credit.personal.current_overdue_count": _value_fact(current_overdue, personal_source, unit="count"),
            "credit.personal.overdue_90d_count": _value_fact(overdue.get("overdue_90d_account_count"), personal_source, unit="count"),
            "credit.personal.outstanding_loan_count": _value_fact(person.get("loan_account_count"), personal_source, unit="count"),
            "credit.personal.loan_institution_count": _value_fact(person.get("loan_institution_count"), personal_source, unit="count"),
        }
        money_specs = {
            "credit.personal.outstanding_loan_balance": (person.get("loan_balance_money") or {}, person.get("loan_balance"), person.get("loan_balance_unit")),
            "credit.personal.credit_card_limit": (person.get("credit_card_limit_money") or {}, person.get("credit_card_limit"), None),
            "credit.personal.credit_card_used": (person.get("credit_card_used_money") or {}, person.get("credit_card_used"), None),
            "credit.personal.related_repayment_balance": (person.get("related_repayment_balance_money") or {}, person.get("related_repayment_balance"), person.get("related_repayment_balance_unit")),
        }
        for path, fact in personal_values.items():
            _set_fact(tree, path, fact)
        for path, (money, fallback, fallback_unit) in money_specs.items():
            _set_fact(tree, path, _money_fact(money.get("value", fallback), money.get("unit") or fallback_unit,
                                              personal_source, as_of_date=personal_date))
        query = person.get("query_summary") or {}
        windows = {"1m": query.get("near_1_month"), "3m": query.get("near_3_months"),
                   "6m": query.get("near_6_months"), "12m": query.get("near_1_year")}
        for window, row in windows.items():
            row = row if isinstance(row, dict) else {}
            for category in ("loan_approval", "credit_card_approval", "guarantor_qualification", "legal_person_credit_review"):
                _set_fact(tree, f"credit.personal.query_{window}.{category}",
                          _value_fact(_query_category(row, category), personal_source, unit="count"))

    financial = (context_data.get("financials") or {}).get("latest") or {}
    financial_section = context_data.get("financials") or {}
    financial_date = financial.get("period") or financial_section.get("latest_period")
    financial_source = _source(financial_section.get("source"), None, financial_date, "comprehensive_context.financials.latest")
    for path, value in {
        "financial.period_type": financial.get("period_type"), "financial.period_start": financial.get("period_start"),
        "financial.period_end": financial_date,
    }.items():
        _set_fact(tree, path, _value_fact(value, financial_source))
    for path, key in {
        "financial.total_assets": "total_assets", "financial.total_liabilities": "total_liabilities",
        "financial.net_assets": "net_assets", "financial.revenue": "revenue",
        "financial.operating_cost": "operating_cost", "financial.net_profit": "net_profit",
        "financial.accounts_receivable": "accounts_receivable", "financial.inventory": "inventory",
        "financial.short_term_borrowing": "short_term_borrowings",
        "financial.long_term_borrowing": "long_term_borrowings", "financial.operating_cash_flow": "operating_cashflow",
    }.items():
        _set_fact(tree, path, _money_fact(financial.get(key), financial.get("unit"), financial_source, as_of_date=financial_date))
    _set_fact(tree, "financial.debt_asset_ratio", _value_fact(financial.get("debt_asset_ratio"), financial_source, unit="ratio", as_of_date=financial_date))

    cashflow = context_data.get("enterprise_cashflow") or {}
    cashflow_date = cashflow.get("as_of")
    cashflow_source = _source(cashflow.get("source"), None, cashflow_date, "comprehensive_context.enterprise_cashflow")
    period = cashflow.get("statement_period") or {}
    _set_fact(tree, "cashflow.coverage_months", _value_fact(period.get("months"), cashflow_source, unit="month"))
    _set_fact(tree, "cashflow.period_start", _value_fact(period.get("start"), cashflow_source))
    _set_fact(tree, "cashflow.period_end", _value_fact(period.get("end"), cashflow_source))
    flow_unit = cashflow.get("unit") or ("CNY" if cashflow.get("source") == "enterprise_flow_aggregator" else None)
    for path, key in {
        "cashflow.total_inflow": "total_inflow", "cashflow.operating_inflow": "operating_inflow",
        "cashflow.internal_transfer_inflow": "internal_transfer_inflow",
        "cashflow.non_operating_or_unidentified_inflow": "non_operating_inflow",
        "cashflow.average_monthly_operating_inflow": "monthly_average_operating_inflow",
    }.items():
        _set_fact(tree, path, _money_fact(cashflow.get(key), flow_unit, cashflow_source, as_of_date=cashflow_date))
    _set_fact(tree, "cashflow.account_count", _value_fact(cashflow.get("account_count"), cashflow_source, unit="count"))
    classification = "preliminary" if cashflow.get("status") in {"available", "partial"} else None
    _set_fact(tree, "cashflow.classification_status", _value_fact(classification, cashflow_source))

    assets = context_data.get("assets") or {}
    asset_source = _source(assets.get("source"), None, assets.get("as_of"), "comprehensive_context.assets")
    properties, vehicles, equipment = assets.get("property") or [], assets.get("vehicle") or [], assets.get("equipment") or []
    if properties:
        _set_fact(tree, "asset.has_real_estate", _known(True, source=asset_source))
        _set_fact(tree, "asset.real_estate_count", _known(len(properties), unit="count", source=asset_source))
        _set_fact(tree, "asset.real_estate_regions", _value_fact([x.get("region") for x in properties if x.get("region")], asset_source))
        _set_fact(tree, "asset.real_estate_types", _value_fact([x.get("type") for x in properties if x.get("type")], asset_source))
    if vehicles:
        _set_fact(tree, "asset.has_vehicle", _known(True, source=asset_source))
        _set_fact(tree, "asset.vehicle_count", _known(len(vehicles), unit="count", source=asset_source))
    if equipment:
        _set_fact(tree, "asset.has_equipment", _known(True, source=asset_source))
    valued_assets = [item for group in (properties, vehicles, equipment) for item in group
                     if _decimal(item.get("market_value")) is not None and item.get("value_basis")]
    if valued_assets:
        values = [_decimal(item.get("market_value")) for item in valued_assets]
        _set_fact(tree, "asset.has_confirmed_collateral", _known(True, source=asset_source))
        _set_fact(tree, "asset.confirmed_collateral_value", _known(sum(value for value in values if value is not None), unit="CNY", source=asset_source))

    missing_domains = []
    for domain in ("asset", "business", "qualification"):
        paths = [path for path in MATCHING_FACT_FIELD_TYPES if path.startswith(domain + ".")]
        if all(MatchingFactsAccessor(tree).get_fact(path).status == "unknown" for path in paths):
            missing_domains.append(domain)
    conflicted_fields = [path for path in MATCHING_FACT_FIELD_TYPES if MatchingFactsAccessor(tree).get_fact(path).status == "conflict"]
    preliminary_fields = [path for path in (
        "cashflow.operating_inflow", "cashflow.non_operating_or_unidentified_inflow",
        "cashflow.average_monthly_operating_inflow",
    ) if MatchingFactsAccessor(tree).get_fact(path).status == "known" and classification == "preliminary"]

    return CustomerMatchingFacts(
        customer_id=customer_id, requirement_id=requirement_id, requirement_version=int(requirement["version"]),
        generated_at=datetime.now(timezone.utc).isoformat(), as_of_date=as_of.isoformat(),
        requirement=tree["requirement"], customer=tree["customer"], credit=tree["credit"],
        financial=tree["financial"], cashflow=tree["cashflow"], asset=tree["asset"],
        business=tree["business"], qualification=tree["qualification"],
        data_quality=MatchingDataQuality(missing_domains=missing_domains, conflicted_fields=conflicted_fields,
                                         preliminary_fields=preliminary_fields),
    )


__all__ = [
    "CustomerMatchingFacts", "FactCandidate", "FactSource", "FactValue", "FACTS_VERSION",
    "MATCHING_FACT_FIELD_TYPES", "MatchingFactsAccessor", "MatchingDataQuality",
    "build_customer_matching_facts",
]
