from backend.services.customer_financing_diagnostic_report_service import build_customer_financing_diagnostic_report
from backend.services.enterprise_bank_flow_diagnostic_service import (
    build_enterprise_bank_flow_diagnostic_from_aggregated,
)


def _aggregated(summary=None, monthly_summary=None, accounts=None):
    return {
        "source_files": [{"document_id": "doc-flow", "file_name": "flow.xlsx"}],
        "statement_period": {"start_date": "2025-01-01", "end_date": "2025-06-30", "months_count": 6},
        "summary": summary or {},
        "monthly_summary": monthly_summary or [],
        "accounts": accounts or [{"account_name": "上海示例科技有限公司"}],
    }


def _profile(company_name="上海示例科技有限公司"):
    return {"enterprise_identity": {"company_name": company_name}}


def test_no_enterprise_bank_flow_returns_unknown():
    diagnostic = build_enterprise_bank_flow_diagnostic_from_aggregated(None, _profile())

    assert diagnostic["has_enterprise_bank_flow"] is False
    assert diagnostic["flow_status"] == "unknown"
    assert diagnostic["key_risks"]


def test_missing_enterprise_bank_flow_enters_report_required_missing():
    flow = build_enterprise_bank_flow_diagnostic_from_aggregated(None, _profile())
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "测试客户"},
        {},
        {"required_missing": []},
        {},
        {},
        {},
        flow,
    )

    assert "近6-12个月企业银行流水" in report["material_checklist"]["required_missing"]


def test_average_monthly_income_is_calculated_from_total_income_and_month_count():
    diagnostic = build_enterprise_bank_flow_diagnostic_from_aggregated(
        _aggregated(summary={"total_inflow": 600, "total_outflow": 300}),
        _profile(),
    )

    assert diagnostic["summary_metrics"]["average_monthly_income"] == 100


def test_negative_net_income_creates_risk():
    diagnostic = build_enterprise_bank_flow_diagnostic_from_aggregated(
        _aggregated(summary={"total_inflow": 100, "total_outflow": 200}),
        _profile(),
    )

    assert diagnostic["flow_status"] == "risky"
    assert "企业流水净流入为负，需关注经营现金流压力" in diagnostic["key_risks"]


def test_internal_transfer_ratio_attention_or_higher():
    diagnostic = build_enterprise_bank_flow_diagnostic_from_aggregated(
        _aggregated(summary={"total_inflow": 1000, "total_outflow": 500, "internal_transfer_total": 350}),
        _profile(),
    )

    assert diagnostic["flow_status"] in {"attention", "risky"}
    assert diagnostic["quality_metrics"]["internal_transfer_ratio"] == 0.35


def test_real_income_ratio_attention_or_higher():
    diagnostic = build_enterprise_bank_flow_diagnostic_from_aggregated(
        _aggregated(summary={"total_inflow": 1000, "total_outflow": 500, "operating_inflow": 500}),
        _profile(),
    )

    assert diagnostic["flow_status"] in {"attention", "risky"}
    assert diagnostic["quality_metrics"]["real_income_ratio"] == 0.5


def test_account_name_conflict_creates_risk():
    diagnostic = build_enterprise_bank_flow_diagnostic_from_aggregated(
        _aggregated(
            summary={"total_inflow": 1000, "total_outflow": 500},
            accounts=[{"account_name": "上海另一个公司"}],
        ),
        _profile("上海示例科技有限公司"),
    )

    assert diagnostic["account_consistency"]["is_consistent"] is False
    assert diagnostic["flow_status"] == "risky"


def test_enterprise_bank_flow_diagnostic_enters_financing_report():
    flow = build_enterprise_bank_flow_diagnostic_from_aggregated(
        _aggregated(summary={"total_inflow": 1000, "total_outflow": 500}),
        _profile(),
    )
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "测试客户"},
        {},
        {"required_missing": []},
        {},
        {},
        {},
        flow,
    )

    assert report["enterprise_bank_flow_diagnostic"] == flow


def test_report_markdown_contains_enterprise_bank_flow_section():
    flow = build_enterprise_bank_flow_diagnostic_from_aggregated(None, _profile())
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "测试客户"},
        {},
        {},
        {},
        {},
        {},
        flow,
    )

    assert "企业流水诊断" in report["report_markdown"]
