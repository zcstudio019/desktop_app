from __future__ import annotations

import pytest

from backend.document_types import normalize_document_type_code, should_append_same_type_document
from backend.services.document_agents.orchestrator import run_document_extraction_agent
from backend.services.document_extractor_service import detect_document_type_code
from backend.services.financial_report_agent.customer_report_aggregator import aggregate_customer_financial_reports
from backend.services.financial_report_agent.normalizer import normalize_amount
from backend.services.financial_report_agent.orchestrator import run_financial_report_agent
from backend.services.financial_report_agent.skills.identify_financial_report_skill import identify_financial_report


def _pages(year: int, assets: float, liabilities: float, equity: float, revenue: float, profit: float, ocf: float) -> list[dict]:
    report_label = "季报" if year == 2024 else "年报"
    return [
        {
            "page": 1,
            "text": f"""财务报表报送与信息采集（企业会计准则一般企业）-{year}{report_label}
企业名称：测试制造有限公司
统一社会信用代码：91310000123456789X
税款所属期起止：{year}-01-01至{year}-12-31
报送日期：{year + 1}-05-26
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
    assert data["company_info"]["report_type"] == ("quarterly" if year == 2024 else "annual")
    assert result["ratios_json"]["asset_liability_ratio"] is not None
    assert result["markdown_report"].startswith("## 财务报表")
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


def test_financial_report_markdown_and_display_json_are_chinese_presentations() -> None:
    pages = _pages(*CASES[0])
    result = run_financial_report_agent(
        raw_text="\n".join(item["text"] for item in pages),
        filename="2022财务报表报送与信息采集（企业会计准则一般企业）-2022年报.pdf",
        metadata={"raw_pages": pages},
    )
    markdown = result["markdown_report"]
    forbidden = [
        "document_type", "source_file", "customer_id", "company_info", "balance_sheet",
        "cash_and_equivalents", "raw_value", "normalized_value", "source_page",
        "source_text", "confidence", "financial_report", "annual", "CNY",
    ]
    required = [
        "文档类型", "来源文件", "企业信息", "资产负债表", "利润表", "现金流量表",
        "货币资金", "营业收入", "经营活动产生的现金流量净额", "置信度",
        "财务报表", "年报", "人民币",
    ]
    assert all(item not in markdown for item in forbidden)
    assert all(item in markdown for item in required)
    assert result["structured_json"]["document_type"] == "financial_report"
    assert result["display_json"]["文档类型"] == "财务报表"
    assert "company_info" not in result["display_json"]
    assert result["display_json"]["企业信息"]["报表类型"] == "年报"
    assert result["display_json"]["企业信息"]["币种"] == "人民币"


def test_financial_report_period_uses_tax_period_not_submission_date() -> None:
    pages = _pages(*CASES[0])
    result = run_financial_report_agent(
        raw_text="\n".join(item["text"] for item in pages),
        filename="2022财务报表报送与信息采集（企业会计准则一般企业）-2022年报.pdf",
        metadata={"raw_pages": pages},
    )
    info = result["structured_json"]["company_info"]
    assert info["report_period_start"] == "2022-01-01"
    assert info["report_period_end"] == "2022-12-31"
    assert info["report_date"] == "2023-05-26"


def test_identify_financial_report_supports_company_info_aliases() -> None:
    info = identify_financial_report(
        "\n".join(
            [
                "企业会计准则",
                "纳税人名称：测试科技有限公司",
                "纳税人识别号（国税）：913201055804841947",
                "税款所属期：2025-01-01至2025-12-31",
                "报表日期：2026-03-25",
            ]
        ),
        "2025财务报表.pdf",
    )
    assert info.taxpayer_id == "913201055804841947"
    assert info.accounting_standard == "business_accounting_standard"
    assert info.report_period_start == "2025-01-01"
    assert info.report_period_end == "2025-12-31"
    assert info.report_date == "2026-03-25"


def test_financial_report_derives_total_equity_when_equity_label_is_missing() -> None:
    pages = _pages(*CASES[1])
    pages[0]["text"] = pages[0]["text"].replace("所有者权益合计 60 13,043,765.10\n", "")
    result = run_financial_report_agent(
        raw_text="\n".join(item["text"] for item in pages),
        filename="2023无权益标签财务报表.pdf",
        metadata={"raw_pages": pages},
    )
    equity = result["structured_json"]["balance_sheet"]["total_equity"]
    assert equity["normalized_value"] == 13043765.10
    assert equity["confidence"] == 0.90
    assert equity["source_text"] == "由资产总计 - 负债合计计算得出"
    assert "所有者权益合计由资产总计减负债合计计算得出，原表字段未直接命中" in result["validation_warnings"]


def test_financial_report_extracts_and_renders_comparison_columns_for_2023() -> None:
    pages = [
        {
            "page": 1,
            "text": """财务报表报送与信息采集（企业会计准则一般企业）-2023年报
税款所属期起止：2023-01-01至2023-12-31
资产负债表
项目 行次 期末余额 上年年末余额
货币资金 1 1,648,909.26 3,507,503.11
资产总计 30 69,320,214.02 84,697,985.94
负债合计 53 56,276,448.92 78,474,828.15
所有者权益合计 59 13,043,765.10 6,223,157.79
负债和所有者权益总计 60 69,320,214.02 84,697,985.94
""",
        },
        {
            "page": 2,
            "text": """利润表
项目 行次 本期金额 上期金额
一、营业收入 1 100,012,470.73 140,360,769.35
减：营业成本 2 74,007,485.08 111,393,386.93
三、利润总额（亏损总额以“-”号填列） 18 6,690,607.31 330,224.42
减：所得税费用 19 0.00 -99,400.64
四、净利润（净亏损以“-”号填列） 20 6,690,607.31 429,625.06
""",
        },
        {
            "page": 3,
            "text": """现金流量表
项目 行次 本期金额 上期金额
销售商品、提供劳务收到的现金 1 94,657,666.04 156,338,816.24
收到的税费返还 2 574,202.82 0.00
收到的其他与经营活动有关的现金 3 11,395,721.33 55,966,002.98
经营活动现金流入小计 4 106,627,590.19 212,304,819.22
支付给职工以及为职工支付的现金 6 9,438,287.25 24,966,946.30
支付的各项税费 7 365,202.54 1,834,384.18
支付的其他与经营活动有关的现金 8 27,929,846.83 25,727,370.44
经营活动产生的现金流量净额 10 -8,438,844.57 -15,841,870.74
投资支付的现金 18 2,549,749.28 0.00
投资活动产生的现金流量净额 22 -2,549,749.28 -1,600,178.83
吸收投资收到的现金 23 130,000.00 1,530,000.00
取得借款收到的现金 24 9,000,000.00 33,500,000.00
筹资活动产生的现金流量净额 31 9,130,000.00 14,615,941.46
加：期初现金及现金等价物余额 34 3,507,503.11 6,333,611.22
期末现金及现金等价物余额 32 1,648,909.26 3,507,503.11
""",
        },
    ]
    result = run_financial_report_agent(
        raw_text="\n".join(item["text"] for item in pages),
        filename="2023财务报表报送与信息采集（企业会计准则一般企业）-2023年报.pdf",
        metadata={"raw_pages": pages},
    )
    data = result["structured_json"]
    cash = data["balance_sheet"]["cash_and_equivalents"]
    assets = data["balance_sheet"]["total_assets"]
    equity = data["balance_sheet"]["total_equity"]
    revenue = data["income_statement"]["revenue"]
    operating_cost = data["income_statement"]["operating_cost"]
    income_tax = data["income_statement"]["income_tax_expense"]
    net_profit = data["income_statement"]["net_profit"]
    operating_cash = data["cash_flow_statement"]["net_operating_cash_flow"]
    cashflow = data["cash_flow_statement"]
    assert cash["normalized_value"] == 1648909.26
    assert cash["previous_normalized_value"] == 3507503.11
    assert assets["normalized_value"] == 69320214.02
    assert assets["previous_normalized_value"] == 84697985.94
    assert equity["normalized_value"] == 13043765.10
    assert equity["previous_normalized_value"] == 6223157.79
    assert revenue["normalized_value"] == 100012470.73
    assert revenue["previous_normalized_value"] == 140360769.35
    assert operating_cost["normalized_value"] == 74007485.08
    assert operating_cost["previous_normalized_value"] == 111393386.93
    assert operating_cost["current_column_label"] == "本期金额"
    assert operating_cost["previous_column_label"] == "上期金额"
    assert income_tax["normalized_value"] == 0.00
    assert income_tax["previous_normalized_value"] == -99400.64
    assert income_tax["raw_value"] == "0.00"
    assert income_tax["previous_raw_value"] == "-99,400.64"
    assert net_profit["normalized_value"] == 6690607.31
    assert net_profit["previous_normalized_value"] == 429625.06
    assert operating_cash["current_column_label"] == "本期金额"
    assert operating_cash["previous_column_label"] == "上期金额"
    cashflow_expected = {
        "cash_received_from_sales": (94657666.04, 156338816.24),
        "tax_refund_received": (574202.82, 0.00),
        "other_cash_received_related_to_operating": (11395721.33, 55966002.98),
        "operating_cash_inflow_total": (106627590.19, 212304819.22),
        "cash_paid_to_employees": (9438287.25, 24966946.30),
        "taxes_paid": (365202.54, 1834384.18),
        "other_cash_paid_related_to_operating": (27929846.83, 25727370.44),
        "net_operating_cash_flow": (-8438844.57, -15841870.74),
        "cash_paid_for_investments": (2549749.28, 0.00),
        "net_investing_cash_flow": (-2549749.28, -1600178.83),
        "cash_received_from_investors": (130000.00, 1530000.00),
        "cash_received_from_borrowings": (9000000.00, 33500000.00),
        "net_financing_cash_flow": (9130000.00, 14615941.46),
        "beginning_cash_balance": (3507503.11, 6333611.22),
        "ending_cash_balance": (1648909.26, 3507503.11),
    }
    for field, (current, previous) in cashflow_expected.items():
        assert cashflow[field]["normalized_value"] == current
        assert cashflow[field]["previous_normalized_value"] == previous
    assert cash["current_value"] == cash["normalized_value"]
    assert cash["compare_value"] == cash["previous_normalized_value"]
    markdown = result["markdown_report"]
    assert "上年年末余额" in markdown
    assert "上期金额" in markdown
    assert "| 货币资金 | 1,648,909.26 | 3,507,503.11 |" in markdown
    assert "| 营业收入 | 100,012,470.73 | 140,360,769.35 |" in markdown
    assert "| 营业成本 | 74,007,485.08 | 111,393,386.93 |" in markdown
    assert "| 所得税费用 | 0.00 | -99,400.64 |" in markdown
    assert "| 收到的税费返还 | 574,202.82 | 0.00 |" in markdown
    assert "| 收到其他与经营活动有关的现金 | 11,395,721.33 | 55,966,002.98 |" in markdown
    assert "| 支付给职工以及为职工支付的现金 | 9,438,287.25 | 24,966,946.30 |" in markdown
    assert "| 支付的各项税费 | 365,202.54 | 1,834,384.18 |" in markdown
    assert "| 支付其他与经营活动有关的现金 | 27,929,846.83 | 25,727,370.44 |" in markdown
    assert "| 现金及现金等价物净增加额 |" not in markdown
    assert "| 期初现金及现金等价物余额 | 3,507,503.11 | 6,333,611.22 |" in markdown
    assert "| 营业成本 | - | -" not in markdown
    assert "| 所得税费用 | - | -" not in markdown
    assert "| 收到的税费返还 | - | -" not in markdown


def test_financial_report_amount_parser_keeps_zero_and_negative_tax_amounts() -> None:
    assert normalize_amount("0.00") == 0.00
    assert normalize_amount("-99,400.64") == -99400.64
    assert normalize_amount("－99,400.64") == -99400.64
    assert normalize_amount("（99,400.64）") == -99400.64
    assert normalize_amount("(99,400.64)") == -99400.64


def test_financial_report_extracts_extended_cash_flow_rows_and_aliases() -> None:
    pages = [{
        "page": 4,
        "text": """现金流量表
项目 行次 本期金额 上期金额
收到的其他与经营活动有关的现金 3 11.00 12.00
支付的其他与经营活动有关的现金 8 13.00 14.00
处置固定资产、无形资产和其他长期资产而收回的现金净额 13 15.00 16.00
处置子公司及其他营业单位收到的现金净额 14 17.00 18.00
收到的其他与投资活动有关的现金 15 19.00 20.00
购建固定资产、无形资产和其他长期资产所支付的现金 17 21.00 22.00
取得子公司及其他营业单位支付的现金净额 19 23.00 24.00
支付的其他与投资活动有关的现金 20 25.00 26.00
收到的其他与筹资活动有关的现金 25 27.00 28.00
分配股利、利润和偿付利息支付的现金 28 29.00 30.00
支付的其他与筹资活动有关的现金 29 31.00 32.00
汇率变动对现金及现金等价物的影响 32 -33.00 34.00
现金及现金等价物净增加额（减少以“-”号填列） 33 -35.00 0.00
加：期初现金及现金等价物余额 34 36.00 37.00
六、期末现金及现金等价物余额 35 1.00 37.00
""",
    }]
    result = run_financial_report_agent(
        raw_text=pages[0]["text"],
        filename="extended-cash-flow.pdf",
        metadata={"raw_pages": pages},
    )
    cashflow = result["structured_json"]["cash_flow_statement"]
    expected = {
        "other_cash_received_related_to_operating": (11.00, 12.00),
        "other_cash_paid_related_to_operating": (13.00, 14.00),
        "cash_received_from_disposal_assets": (15.00, 16.00),
        "cash_received_from_disposal_subsidiaries": (17.00, 18.00),
        "other_cash_received_related_to_investing": (19.00, 20.00),
        "cash_paid_for_fixed_intangible_assets": (21.00, 22.00),
        "cash_paid_for_acquisition_subsidiaries": (23.00, 24.00),
        "other_cash_paid_related_to_investing": (25.00, 26.00),
        "other_cash_received_related_to_financing": (27.00, 28.00),
        "cash_paid_for_dividends_profit_interest": (29.00, 30.00),
        "other_cash_paid_related_to_financing": (31.00, 32.00),
        "effect_of_exchange_rate_changes": (-33.00, 34.00),
        "net_cash_increase": (-35.00, 0.00),
        "beginning_cash_balance": (36.00, 37.00),
        "ending_cash_balance": (1.00, 37.00),
    }
    for field, (current, previous) in expected.items():
        assert cashflow[field]["normalized_value"] == current
        assert cashflow[field]["previous_normalized_value"] == previous
    markdown = result["markdown_report"]
    assert "| 处置子公司及其他营业单位收到的现金净额 | 17.00 | 18.00 |" in markdown
    assert "| 支付其他与筹资活动有关的现金 | 31.00 | 32.00 |" in markdown
    assert "| 汇率变动对现金及现金等价物的影响 | -33.00 | 34.00 |" in markdown
    assert "| 现金及现金等价物净增加额 | -35.00 | 0.00 |" in markdown


def test_small_business_cash_flow_extracts_cumulative_columns_and_hides_double_zero_details() -> None:
    pages = [{
        "page": 1,
        "text": """小企业会计准则 现金流量表
项目 行次 本年累计金额 上年金额
一、销售产成品、商品、提供劳务收到的现金 1 81,530,980.95 14,260,100.00
收到其他与经营活动有关的现金 2 63,196,820.37 3,069,962.48
购买原材料、商品、接受劳务支付的现金 4 79,773,041.92 3,764,197.61
支付的职工薪酬 5 3,246,766.56 600,760.98
支付的税费 6 1,436,108.53 656,739.77
支付其他与经营活动有关的现金 7 64,756,495.40 7,504,741.46
经营活动产生的现金流量净额 9 -4,484,611.09 4,803,622.66
收回短期投资、长期债券投资和长期股权投资收到的现金 10 0.00 0.00
取得投资收益收到的现金 11 0.00 0.00
处置固定资产、无形资产和其他非流动资产收回的现金净额 12 0.00 0.00
投资活动现金流入小计 13 0.00 0.00
短期投资、长期债券投资和长期股权投资支付的现金 14 200,000.00 0.00
投资活动产生的现金流量净额 16 -200,000.00 0.00
取得投资者投资收到的现金 17 0.00 0.00
取得借款收到的现金 18 0.00 0.00
偿还借款本金支付的现金 20 0.00 0.00
偿还借款利息支付的现金 21 0.00 0.00
筹资活动产生的现金流量净额 23 0.00 0.00
现金净增加额 24 -4,684,611.09 4,803,622.66
期初现金余额 25 5,000,000.00 196,377.34
期末现金余额 26 315,388.91 5,000,000.00
""",
    }]
    result = run_financial_report_agent(
        raw_text=pages[0]["text"],
        filename="小企业会计准则现金流量表.pdf",
        metadata={"raw_pages": pages},
    )
    cashflow = result["structured_json"]["cash_flow_statement"]
    expected = {
        "cash_received_from_sales": (81530980.95, 14260100.00),
        "other_cash_received_related_to_operating": (63196820.37, 3069962.48),
        "operating_cash_inflow_total": (144727801.32, 17330062.48),
        "cash_paid_for_goods_services": (79773041.92, 3764197.61),
        "cash_paid_to_employees": (3246766.56, 600760.98),
        "taxes_paid": (1436108.53, 656739.77),
        "other_cash_paid_related_to_operating": (64756495.40, 7504741.46),
        "operating_cash_outflow_total": (149212412.41, 12526439.82),
        "net_operating_cash_flow": (-4484611.09, 4803622.66),
        "cash_paid_for_investments": (200000.00, 0.00),
        "investing_cash_outflow_total": (200000.00, 0.00),
        "net_investing_cash_flow": (-200000.00, 0.00),
    }
    for field, (current, previous) in expected.items():
        assert cashflow[field]["normalized_value"] == current
        assert cashflow[field]["previous_normalized_value"] == previous
    assert cashflow["cash_received_from_investment_recovery"]["normalized_value"] == 0.00
    assert cashflow["cash_received_from_investment_recovery"]["previous_normalized_value"] == 0.00
    assert cashflow["operating_cash_inflow_total"]["source_text"] == "由现金流量明细项计算得出"
    assert cashflow["operating_cash_inflow_total"]["confidence"] == 0.90
    markdown = result["markdown_report"]
    for row in [
        "| 销售商品、提供劳务收到的现金 | 81,530,980.95 | 14,260,100.00 |",
        "| 收到其他与经营活动有关的现金 | 63,196,820.37 | 3,069,962.48 |",
        "| 经营活动现金流入小计 | 144,727,801.32 | 17,330,062.48 |",
        "| 购买商品、接受劳务支付的现金 | 79,773,041.92 | 3,764,197.61 |",
        "| 支付给职工以及为职工支付的现金 | 3,246,766.56 | 600,760.98 |",
        "| 支付的各项税费 | 1,436,108.53 | 656,739.77 |",
        "| 经营活动现金流出小计 | 149,212,412.41 | 12,526,439.82 |",
        "| 投资支付的现金 | 200,000.00 | 0.00 |",
        "| 投资活动现金流出小计 | 200,000.00 | 0.00 |",
        "| 投资活动产生的现金流量净额 | -200,000.00 | 0.00 |",
    ]:
        assert row in markdown
    for hidden_row in [
        "收回投资收到的现金",
        "取得投资收益收到的现金",
        "处置固定资产、无形资产和其他长期资产收回的现金净额",
        "吸收投资收到的现金",
        "取得借款收到的现金",
        "偿还债务支付的现金",
        "分配股利、利润或偿付利息支付的现金",
        "收到的税费返还",
        "处置子公司及其他营业单位收到的现金净额",
        "收到其他与投资活动有关的现金",
        "取得子公司及其他营业单位支付的现金净额",
        "支付其他与投资活动有关的现金",
        "收到其他与筹资活动有关的现金",
        "支付其他与筹资活动有关的现金",
        "汇率变动对现金及现金等价物的影响",
    ]:
        assert f"| {hidden_row} |" not in markdown
    assert "| 投资活动现金流入小计 | 0.00 | 0.00 |" in markdown
    assert "| 筹资活动产生的现金流量净额 | 0.00 | 0.00 |" in markdown
    assert "| 筹资活动现金流入小计 |" not in markdown
    assert "| 筹资活动现金流出小计 |" not in markdown
