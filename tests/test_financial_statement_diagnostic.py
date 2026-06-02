from backend.services.customer_financing_diagnostic_report_service import build_customer_financing_diagnostic_report
from backend.services.financial_statement_diagnostic_service import build_financial_statement_diagnostic_from_report


def _field(value):
    return {"normalized_value": value}


def _report(
    *,
    revenue=1000,
    operating_cost=600,
    net_profit=100,
    total_assets=2000,
    total_liabilities=1000,
    current_assets=800,
    current_liabilities=400,
    operating_cash_flow_net=100,
):
    return {
        "document_type": "financial_report",
        "company_info": {
            "report_period_end": "2025-12-31",
            "report_type": "annual",
        },
        "income_statement": {
            "revenue": _field(revenue),
            "operating_cost": _field(operating_cost),
            "net_profit": _field(net_profit),
        },
        "balance_sheet": {
            "total_assets": _field(total_assets),
            "total_liabilities": _field(total_liabilities),
            "total_equity": _field(total_assets - total_liabilities),
            "current_assets_total": _field(current_assets),
            "current_liabilities_total": _field(current_liabilities),
            "cash_and_equivalents": _field(100),
            "short_term_loans": _field(50),
            "long_term_loans": _field(80),
        },
        "cash_flow_statement": {
            "net_operating_cash_flow": _field(operating_cash_flow_net),
        },
    }


def test_no_financial_statement_returns_unknown():
    diagnostic = build_financial_statement_diagnostic_from_report(None)

    assert diagnostic["has_financial_statement"] is False
    assert diagnostic["financial_status"] == "unknown"


def test_missing_financial_statement_enters_report_required_missing():
    financial = build_financial_statement_diagnostic_from_report(None)
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "测试客户"},
        {},
        {"required_missing": []},
        {},
        {},
        {},
        {},
        financial,
    )

    assert "最近一年或最近一期财务报表" in report["material_checklist"]["required_missing"]


def test_net_profit_margin_is_calculated():
    diagnostic = build_financial_statement_diagnostic_from_report(_report(revenue=1000, net_profit=100))

    assert diagnostic["profitability"]["net_profit_margin"] == 0.1


def test_asset_liability_ratio_is_calculated():
    diagnostic = build_financial_statement_diagnostic_from_report(_report(total_assets=1000, total_liabilities=500))

    assert diagnostic["debt_capacity"]["asset_liability_ratio"] == 0.5


def test_asset_liability_ratio_attention_threshold():
    diagnostic = build_financial_statement_diagnostic_from_report(_report(total_assets=1000, total_liabilities=850))

    assert diagnostic["financial_status"] in {"attention", "risky"}


def test_asset_liability_ratio_risky_threshold():
    diagnostic = build_financial_statement_diagnostic_from_report(_report(total_assets=1000, total_liabilities=1000))

    assert diagnostic["financial_status"] == "risky"


def test_current_ratio_below_one_creates_risk():
    diagnostic = build_financial_statement_diagnostic_from_report(_report(current_assets=500, current_liabilities=800))

    assert "流动比率低于1，短期偿债能力需关注" in diagnostic["key_risks"]


def test_negative_net_profit_creates_risk():
    diagnostic = build_financial_statement_diagnostic_from_report(_report(net_profit=-50))

    assert "企业净利润为负，盈利能力需关注" in diagnostic["key_risks"]


def test_negative_operating_cash_flow_creates_risk():
    diagnostic = build_financial_statement_diagnostic_from_report(_report(operating_cash_flow_net=-10))

    assert "经营活动现金流量净额为负，需关注经营现金回款能力" in diagnostic["key_risks"]


def test_financial_statement_diagnostic_enters_financing_report():
    financial = build_financial_statement_diagnostic_from_report(_report())
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "测试客户"},
        {},
        {"required_missing": []},
        {},
        {},
        {},
        {},
        financial,
    )

    assert report["financial_statement_diagnostic"] == financial


def test_report_markdown_contains_financial_statement_section():
    financial = build_financial_statement_diagnostic_from_report(None)
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "测试客户"},
        {},
        {},
        {},
        {},
        {},
        {},
        financial,
    )

    assert "财务数据诊断" in report["report_markdown"]
