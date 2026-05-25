from __future__ import annotations

import pytest

from backend.document_types import normalize_document_type_code, should_append_same_type_document
from backend.services.document_agents.orchestrator import run_document_extraction_agent
from backend.services.document_extractor_service import detect_document_type_code
from backend.services.financial_report_agent.customer_report_aggregator import aggregate_customer_financial_reports
from backend.services.financial_report_agent.orchestrator import run_financial_report_agent


def _pages(year: int, assets: float, liabilities: float, equity: float, revenue: float, profit: float, ocf: float) -> list[dict]:
    report_label = "季报" if year == 2024 else "年报"
    return [
        {
            "page": 1,
            "text": f"""财务报表报送与信息采集（企业会计准则一般企业）-{year}{report_label}
企业名称：测试制造有限公司
统一社会信用代码：91310000123456789X
报表日期：{year}-12-31
单位：元
资产负债表
货币资金 1 2,000,000.00
应收账款 2 3,000,000.00
存货 3 4,000,000.00
流动资产合计 9 9,000,000.00
资产总计 31 {assets:,.2f}
短期借款 32 8,000,000.00
流动负债合计 40 9,000,000.00
负债合计 50 {liabilities:,.2f}
所有者权益合计 60 {equity:,.2f}
负债和所有者权益总计 61 {assets:,.2f}
""",
        },
        {
            "page": 2,
            "text": f"""利润表
营业收入 1 {revenue:,.2f}
营业成本 2 80,000,000.00
营业利润 15 {profit:,.2f}
利润总额 18 {profit:,.2f}
所得税费用 19 0.00
净利润 20 {profit:,.2f}
""",
        },
        {
            "page": 3,
            "text": f"""现金流量表
销售商品、提供劳务收到的现金 1 50,000,000.00
经营活动产生的现金流量净额 10 {ocf:,.2f}
筹资活动产生的现金流量净额 20 3,000,000.00
现金及现金等价物净增加额 30 -500,000.00
期初现金及现金等价物余额 31 2,500,000.00
期末现金及现金等价物余额 32 2,000,000.00
""",
        },
    ]


CASES = [
    (2022, 84697985.94, 78474828.15, 6223157.79, 140360769.35, 429625.06, -15841870.74),
    (2023, 69320214.02, 56276448.92, 13043765.10, 100012470.73, 6690607.31, -8438844.57),
    (2024, 54688482.62, 41636748.83, 13051733.79, 60376572.48, 7968.69, -1989500.82),
]


@pytest.mark.parametrize("year,assets,liabilities,equity,revenue,profit,ocf", CASES)
def test_financial_report_core_regression(year, assets, liabilities, equity, revenue, profit, ocf) -> None:
    pages = _pages(year, assets, liabilities, equity, revenue, profit, ocf)
    result = run_financial_report_agent(
        filename=f"{year}财务报表报送与信息采集（企业会计准则一般企业）-{year}年报.pdf",
        raw_text="\n".join(item["text"] for item in pages),
        metadata={"raw_pages": pages},
    )
    data = result["structured_json"]
    assert data["balance_sheet"]["total_assets"]["normalized_value"] == assets
    assert data["balance_sheet"]["total_liabilities"]["normalized_value"] == liabilities
    assert data["balance_sheet"]["total_equity"]["normalized_value"] == equity
    assert data["income_statement"]["revenue"]["normalized_value"] == revenue
    assert data["income_statement"]["net_profit"]["normalized_value"] == profit
    assert data["cash_flow_statement"]["net_operating_cash_flow"]["normalized_value"] == ocf
    assert result["ratios_json"]["asset_liability_ratio"] is not None
    assert result["markdown_report"].startswith("# 财务报表授信分析报告")
    assert any(item["field_path"] == "balance_sheet.total_assets" for item in result["evidence_json"])


def test_financial_report_routes_and_preserves_multiple_periods() -> None:
    assert normalize_document_type_code("财务数据") == "financial_report"
    assert normalize_document_type_code("财务报表") == "financial_report"
    assert should_append_same_type_document("financial_report")
    text = "\n".join(page["text"] for page in _pages(*CASES[0]))
    assert detect_document_type_code(text) == "financial_report"
    result = run_document_extraction_agent("financial_report", text, "2022财务报表.pdf", metadata={"raw_pages": _pages(*CASES[0])})
    assert result.agent_name == "financial_report_agent"
    assert result.extracted_json["balance_sheet"]["total_assets"]["normalized_value"] == CASES[0][1]


def test_financial_report_customer_rollup_detects_continuous_negative_cashflow() -> None:
    extracted = []
    for case in CASES:
        pages = _pages(*case)
        content = run_financial_report_agent(raw_text="\n".join(item["text"] for item in pages), filename=f"{case[0]}.pdf", metadata={"raw_pages": pages})
        extracted.append({"extraction_type": "financial_report", "extracted_data": content})
    rollup = aggregate_customer_financial_reports(extracted)
    codes = {item["code"] for item in rollup["latest_credit_analysis"]["risk_findings"]}
    assert len(rollup["trend_metrics"]) == 3
    assert "continuous_negative_operating_cash_flow" in codes
    assert "declining_revenue" in codes
    assert rollup["latest_credit_analysis"]["overall_risk_level"] == "high"
