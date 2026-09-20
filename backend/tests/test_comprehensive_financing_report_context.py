"""Step 2 facts-layer tests; no OCR, LLM, report renderer, or database writes."""

from __future__ import annotations

import asyncio
import copy

import pytest

from backend.services.comprehensive_financing_report_model import ComprehensiveFinancingReportModel
from backend.services import comprehensive_financing_report_context_service as context


def extraction(kind, payload, created="2026-06-30", **extra):
    return {"extraction_type": kind, "extracted_data": payload, "created_at": created,
            "extraction_status": "success", **extra}


class ReadOnlyStorage:
    def __init__(self):
        self.customer = {"customer_id": "c1", "name": "上海测试有限公司", "customer_type": "enterprise"}
        self.extractions = []
        self.documents = []
        self.applications = []
        self.risk = None
        self.scheme = None
        self.profile = None
        self.snapshots = []
        self.flow_rules = []
        self.confirmations = []
        self.writes = 0

    async def get_customer(self, customer_id):
        return copy.deepcopy(self.customer)

    async def get_extractions_by_customer(self, customer_id):
        return copy.deepcopy(self.extractions)

    async def list_documents(self, customer_id):
        return copy.deepcopy(self.documents)

    async def list_saved_applications(self, customer_id):
        return copy.deepcopy(self.applications)

    async def get_latest_customer_risk_report(self, customer_id):
        return copy.deepcopy(self.risk)

    async def get_customer_profile(self, customer_id):
        return copy.deepcopy(self.profile)

    async def get_latest_scheme_snapshot(self, customer_id):
        return copy.deepcopy(self.scheme)

    async def list_financing_diagnostic_report_snapshots(self, customer_id):
        return copy.deepcopy(self.snapshots)

    def list_enterprise_flow_rules(self, customer_id):
        return copy.deepcopy(self.flow_rules)

    def list_income_confirmations(self, customer_id):
        return copy.deepcopy(self.confirmations)


@pytest.fixture
def storage(monkeypatch):
    store = ReadOnlyStorage()
    kyc = {"enterprise_identity": {"company_name": "上海测试有限公司", "legal_representative": "张三",
                                   "unified_social_credit_code": "91310000TEST", "registered_capital": "100万元"},
           "person_identity": {"name": "张三"}, "assets": {"properties": [], "vehicles": []}, "updated_at": ""}

    async def read_kyc(_storage, _customer_id):
        return copy.deepcopy(kyc)

    monkeypatch.setattr(context, "build_customer_kyc_profile", read_kyc)
    store.kyc = kyc
    return store


def build(storage):
    return asyncio.run(context.build_comprehensive_financing_report_context(storage, "c1"))


def add_credit(storage):
    storage.extractions += [
        extraction("enterprise_credit_report", {"basic_info": {"company_name": "上海测试有限公司", "report_date": "2026-06-30"},
                                                "credit_summary": {"active_borrowing_balance": 500},
                                                "loans": [{"institution": "银行甲", "balance": 500}],
                                                "guarantees": [{"balance": 100}], "unit": "元"}),
        extraction("personal_credit_report", {"basic_info": {"name": "张三", "report_time": "2026-06-30"},
                                              "credit_summary": {"loan_balance": 120, "credit_card_used_amount": 20, "credit_card_limit": 100},
                                              "loan_accounts": [{"balance": 120}], "credit_card_accounts": [{"limit": 100}],
                                              "related_repayment_responsibilities": [{"balance": 80}]})]


def test_enterprise_credit_structured_roles_match_person_without_kyc_role(storage):
    storage.kyc["enterprise_identity"].pop("legal_representative")
    storage.extractions = [
        extraction("enterprise_credit", {"extracted_json": {
            "report_basic": {"company_name": "上海测试有限公司", "report_date": "2026-06-01", "currency_unit": "万元"},
            "key_personnel": [{"position": "法定代表人", "name": "张三"}],
            "actual_controller": {"name": "张三"},
        }}),
        extraction("personal_credit_report", {"basic_info": {"name": "张三", "report_time": "2026-06-02"}}),
    ]
    model = build(storage)
    assert model.subject_profile["legal_representative"] == "张三"
    assert model.subject_profile["actual_controller"] == "张三"
    assert model.enterprise_credit["unit"] == "万元"
    assert len(model.personal_credit["people"]) == 1
    assert model.personal_credit["people"][0]["roles"] == ["法定代表人", "实际控制人"]


def test_saved_bank_statement_classification_is_read_only_and_conservative(storage, monkeypatch):
    monkeypatch.setattr(context, "aggregate_customer_enterprise_flows", lambda *_args, **_kw: {"source_files": []})
    storage.extractions = [extraction("bank_statement", {"extracted_json": {
        "account_name": "上海测试有限公司", "account_no": "account-1", "unit": "元",
        "period_start": "2025-04-01", "period_end": "2026-03-31",
        "transactions": [
            {"direction": "入账", "amount": 100, "category": "经营入账", "transaction_time": "2025-05-02"},
            {"direction": "入账", "amount": 70, "category": "经营入账", "is_self_transfer": True},
            {"direction": "入账", "amount": 30, "category": "往来入账"},
            {"direction": "出账", "amount": 20, "category": "经营出账"},
        ],
    }}, extraction_status="成功")]
    model = build(storage)
    flow = model.enterprise_cashflow
    assert flow["status"] == "partial"
    assert flow["account_count"] == 1
    assert flow["statement_period"]["start"] == "2025-04-01"
    assert flow["total_inflow"] == 200
    assert flow["operating_inflow"] == 100
    assert flow["internal_transfer_inflow"] == 70
    assert flow["non_operating_inflow"] == 30
    assert flow["related_party_inflow"] is None
    assert model.derived_metrics["enterprise_operating_inflow"] == 100


def test_inactive_saved_document_is_not_used(storage):
    storage.documents = [{"doc_id": "old-credit", "is_active": False}]
    storage.extractions = [extraction("enterprise_credit", {"report_basic": {"company_name": "上海测试有限公司"}}, doc_id="old-credit")]
    model = build(storage)
    assert model.enterprise_credit["status"] == "missing"


def test_structured_property_without_valuation_remains_unvalued(storage):
    storage.extractions = [extraction("property_cert", {"property_address": "测试地址"})]
    model = build(storage)
    assert model.assets["status"] == "available"
    assert model.assets["property"][0]["market_value"] is None


def add_flows(storage, monkeypatch):
    storage.extractions.append(extraction("enterprise_flow", {"summary": {"raw_total_inflow": 1500}}, "2026-12-31"))
    storage.extractions.append(extraction("personal_flow", {"summary": {"raw_total_income": 400}}, "2026-12-31"))
    monkeypatch.setattr(context, "aggregate_customer_enterprise_flows", lambda *_args, **_kw: {
        "source_files": [{}], "accounts": [{}, {}], "statement_period": {"start_date": "2026-01-01", "end_date": "2026-12-31", "months_count": 12},
        "summary": {"raw_total_inflow": 1500, "raw_total_outflow": 900, "raw_net_cashflow": 600,
                    "operating_inflow": 1000, "operating_outflow": 700, "internal_transfer_inflow": 300,
                    "related_party_inflow": 200, "excluded_inflow_total": 500},
        "monthly_summary": [], "counterparty_summary": {},
    })
    monkeypatch.setattr(context, "build_enterprise_bank_flow_diagnostic_from_aggregated", lambda *_args: {"flow_status": "normal", "quality_metrics": {}})
    monkeypatch.setattr(context, "aggregate_customer_personal_flows", lambda *_args, **_kw: {
        "source_document_count": 1,
        "customer_level_summary": {"account_count": 1, "period_start": "2026-01-01", "period_end": "2026-12-31",
                                   "confirmed_salary_income": 100, "manual_confirmed_salary_income": 30,
                                   "suspected_salary_income": 80, "loan_repayment_expense": 10},
    })


def add_financials(storage, monkeypatch, *, periods=2, revenue=1000, short=300, long=100):
    storage.extractions.append(extraction("financial_report", {"structured_json": {"balance_sheet": {}}}, "2026-12-31"))
    reports = []
    for year in range(2026 - periods + 1, 2027):
        reports.append({"company_info": {"report_period_end": f"{year}-12-31", "report_date": f"{year}-12-31", "unit": "元", "report_type": "annual"},
                        "income_statement": {"revenue": {"normalized_value": revenue if year == 2026 else 800},
                                             "net_profit": {"normalized_value": 100 if year == 2026 else 80}},
                        "balance_sheet": {"total_assets": {"normalized_value": 2000}, "total_liabilities": {"normalized_value": 800},
                                          "short_term_loans": {"normalized_value": short}, "long_term_loans": {"normalized_value": long}},
                        "cash_flow_statement": {"net_operating_cash_flow": {"normalized_value": 120}}})
    monkeypatch.setattr(context, "aggregate_customer_financial_reports", lambda *_args: {"reports": reports})
    monkeypatch.setattr(context, "build_financial_statement_diagnostic_from_report", lambda *_args: {"financial_status": "normal"})


def test_comprehensive_context_subject_profile(storage):
    report = build(storage)
    assert report.subject_profile["enterprise_name"] == "上海测试有限公司"
    assert report.subject_profile["legal_representative"] == "张三"
    assert report.subject_profile["actual_controller"] is None


def test_legal_representative_and_actual_controller_are_separate_roles(storage):
    storage.extractions.append(extraction("business_license", {"fields": {"company_name": "上海测试有限公司", "actual_controller": "李四"}}))
    add_credit(storage)
    report = build(storage)
    assert report.subject_profile["legal_representative"] == "张三"
    assert report.subject_profile["actual_controller"] == "李四"
    assert report.personal_credit["people"][0]["roles"] == ["法定代表人"]


def test_same_person_multiple_roles_are_not_duplicated(storage):
    storage.extractions.append(extraction("business_license", {"fields": {"company_name": "上海测试有限公司", "actual_controller": "张三"}}))
    add_credit(storage)
    assert build(storage).personal_credit["people"][0]["roles"] == ["法定代表人", "实际控制人"]


def test_conflicting_subject_names_mark_needs_review(storage):
    storage.extractions.append(extraction("business_license", {"fields": {"company_name": "另一家公司"}}))
    report = build(storage)
    assert report.subject_profile["status"] == "needs_review"
    assert any(item["type"] == "enterprise_subject" for item in report.conflicts)


def test_confirmed_fields_override_extracted_value(storage):
    storage.extractions.append(extraction("business_license", {"fields": {"company_name": "旧识别名称"}},
                                          confirmed_data={"confirmed_fields": {"company_name": "上海测试有限公司"}}))
    assert not any(item["type"] == "enterprise_subject" for item in build(storage).conflicts)


def test_confirmed_business_license_sets_confirmed_status(storage):
    storage.extractions.append(extraction("business_license", {"fields": {"company_name": "上海测试有限公司"}},
                                          confirmed_data={"confirmed_fields": {"company_name": "上海测试有限公司"}}))
    assert build(storage).subject_profile["status"] == "confirmed"


def test_conflicting_legal_representative_creates_review_note(storage):
    storage.extractions.append(extraction("business_license", {"fields": {"company_name": "上海测试有限公司", "legal_representative": "李四"}}))
    report = build(storage)
    assert report.subject_profile["legal_representative"] is None
    assert any(item["type"] == "legal_representative" for item in report.conflicts)


def test_conflicting_actual_controller_creates_review_note(storage):
    storage.extractions += [extraction("business_license", {"fields": {"actual_controller": "李四"}}),
                            extraction("company_articles", {"fields": {"actual_controller": "王五"}})]
    assert any(item["type"] == "actual_controller" for item in build(storage).conflicts)


def test_enterprise_credit_context_is_enterprise_only(storage):
    add_credit(storage)
    report = build(storage)
    assert report.enterprise_credit["outstanding_loan_balance"] == 500
    assert report.personal_credit["people"][0]["loan_balance"] == 120
    assert "total_group_debt" not in report.derived_metrics


def test_enterprise_guarantee_not_mixed_with_personal_liability(storage):
    add_credit(storage)
    report = build(storage)
    assert report.enterprise_credit["guarantee_balance"] == 100
    assert report.personal_credit["people"][0]["related_repayment_balance"] == 80
    assert report.derived_metrics["total_enterprise_credit_balance"] == 500


def test_enterprise_credit_keeps_source_date(storage):
    add_credit(storage)
    assert build(storage).source_dates["enterprise_credit"] == "2026-06-30"


def test_personal_credit_context_keeps_person_identity(storage):
    add_credit(storage)
    assert build(storage).personal_credit["people"][0]["name"] == "张三"


def test_unmatched_personal_credit_creates_review_note(storage):
    storage.extractions.append(extraction("personal_credit_report", {"basic_info": {"name": "陌生人"}}))
    report = build(storage)
    assert report.personal_credit["status"] == "needs_review"
    assert any(item["type"] == "personal_credit_subject" for item in report.conflicts)


def test_personal_related_repayment_not_counted_as_enterprise_debt(storage):
    add_credit(storage)
    report = build(storage)
    assert report.derived_metrics["total_related_repayment_balance"] == 80
    assert report.derived_metrics["total_enterprise_credit_balance"] == 500


@pytest.mark.parametrize("field,expected", [("total_inflow", 1500), ("operating_inflow", 1000),
                                             ("internal_transfer_inflow", 300), ("related_party_inflow", 200),
                                             ("non_operating_inflow", 500)])
def test_internal_and_related_flows_excluded(storage, monkeypatch, field, expected):
    add_flows(storage, monkeypatch)
    assert build(storage).enterprise_cashflow[field] == expected


def test_enterprise_cashflow_keeps_statement_period(storage, monkeypatch):
    add_flows(storage, monkeypatch)
    report = build(storage)
    assert report.enterprise_cashflow["statement_period"]["start"] == "2026-01-01"
    assert report.enterprise_cashflow["account_count"] == 2
    assert report.derived_metrics["monthly_average_operating_inflow"] == 83.33


def test_suspected_salary_not_counted_as_confirmed_income(storage, monkeypatch):
    add_flows(storage, monkeypatch)
    report = build(storage)
    assert report.personal_cashflow["suspected_salary_income"] == 80
    assert report.personal_cashflow["usable_salary_income"] == 130


def test_manually_confirmed_salary_is_usable_income(storage, monkeypatch):
    add_flows(storage, monkeypatch)
    assert build(storage).personal_cashflow["manually_confirmed_salary_income"] == 30


def test_missing_personal_cashflow_is_explicit(storage):
    assert build(storage).personal_cashflow["status"] == "missing"


def test_financial_context_keeps_period_and_unit(storage, monkeypatch):
    add_financials(storage, monkeypatch)
    assert build(storage).financials["latest"]["unit"] == "元"
    assert build(storage).financials["latest_period"] == "2026-12-31"


def test_financial_metrics_are_programmatic(storage, monkeypatch):
    add_financials(storage, monkeypatch)
    report = build(storage)
    assert report.derived_metrics["debt_asset_ratio"] == 0.4
    assert report.financials["trends"]["revenue_growth"] == 0.25


def test_multiple_financial_periods_are_kept(storage, monkeypatch):
    add_financials(storage, monkeypatch, periods=3)
    assert build(storage).financials["period_count"] == 3


def test_asset_without_value_does_not_get_guessed_value(storage):
    storage.kyc["assets"]["properties"].append({"property_address": "某处房产", "market_value": 1000})
    report = build(storage)
    assert report.assets["status"] == "available"
    assert report.assets["property"][0]["market_value"] is None


def test_missing_assets_are_explicit(storage):
    assert build(storage).assets["status"] == "missing"


def test_missing_financing_requirement_is_explicit(storage):
    assert build(storage).financing_requirement["status"] == "missing"


def test_existing_financing_requirement_is_read_only(storage):
    storage.applications = [{"savedAt": "2026-08-01", "applicationData": {"financing_amount": "300", "loan_purpose": "采购"}}]
    original = copy.deepcopy(storage.applications)
    report = build(storage)
    assert report.financing_requirement["amount"] == 300
    assert storage.applications == original


def test_explicit_requirement_in_old_snapshot_is_read_only(storage):
    storage.snapshots = [{"generated_at": "2026-08-01", "report_json": {"financing_requirement": {"financing_amount": 400, "loan_purpose": "设备"}}}]
    assert build(storage).financing_requirement["amount"] == 400


def test_risk_context_marks_old_result_stale(storage):
    storage.risk = {"report_id": "risk-1", "generated_at": "2025-01-01", "report_json": {"overall_assessment": {"risk_level": "low"}}}
    storage.extractions.append(extraction("business_license", {"fields": {"company_name": "上海测试有限公司"}}, "2026-06-30"))
    report = build(storage)
    assert report.risk_context["stale"] is True
    assert report.risk_context["status"] == "needs_review"


def test_saved_scheme_is_reference_only(storage):
    storage.scheme = {"snapshot_id": "scheme-1", "summary_markdown": "某产品建议", "created_at": "2026-07-01"}
    report = build(storage)
    assert report.existing_financing_plan["has_saved_result"] is True
    assert "某产品建议" not in report.model_dump_json()


def test_missing_materials_do_not_crash_context_builder(storage):
    report = build(storage)
    assert report.derived_metrics["missing_material_count"] >= 8
    assert all(item["status"] in {"missing", "available", "partial", "confirmed", "needs_review"} for item in report.data_scope["materials"])


def test_financial_vs_credit_debt_difference_creates_review_note(storage, monkeypatch):
    add_credit(storage)
    add_financials(storage, monkeypatch, short=200, long=100)
    storage.extractions[0]["extracted_data"]["basic_info"]["report_date"] = "2026-12-31"
    assert any(item["type"] == "credit_vs_financial_debt" for item in build(storage).conflicts)


def test_financial_vs_cashflow_income_difference_creates_review_note(storage, monkeypatch):
    add_flows(storage, monkeypatch)
    add_financials(storage, monkeypatch, revenue=1500)
    assert any(item["type"] == "financial_vs_cashflow_income" for item in build(storage).conflicts)


def test_source_date_gap_creates_review_note(storage, monkeypatch):
    add_credit(storage)
    add_financials(storage, monkeypatch)
    storage.extractions[0]["extracted_data"]["basic_info"]["report_date"] = "2024-01-01"
    assert any(item["type"] == "source_date_gap" for item in build(storage).conflicts)


def test_mismatched_financial_period_does_not_create_income_conflict(storage, monkeypatch):
    add_flows(storage, monkeypatch)
    add_financials(storage, monkeypatch, revenue=1500)
    # A partial-year flow is not directly comparable to a full-year statement.
    original = context.aggregate_customer_enterprise_flows
    monkeypatch.setattr(context, "aggregate_customer_enterprise_flows", lambda *args, **kwargs: {
        **original(*args, **kwargs), "statement_period": {"start_date": "2026-10-01", "end_date": "2026-12-31", "months_count": 3}})
    assert not any(item["type"] == "financial_vs_cashflow_income" for item in build(storage).conflicts)


@pytest.mark.parametrize("forbidden", ["raw_text", "ocr_text", "evidence", "markdown", "html", "base64", "prompt"])
def test_context_model_rejects_unstructured_payload(forbidden):
    with pytest.raises(ValueError):
        ComprehensiveFinancingReportModel(subject_profile={forbidden: "source content"})


def test_builder_does_not_copy_raw_source_content(storage):
    storage.extractions.append(extraction("business_license", {"fields": {"company_name": "上海测试有限公司"},
                                                             "raw_text": "RAW OCR", "evidence": "LONG EVIDENCE", "html": "<html>"}))
    serialized = build(storage).model_dump_json()
    assert all(value not in serialized for value in ("RAW OCR", "LONG EVIDENCE", "<html>"))
