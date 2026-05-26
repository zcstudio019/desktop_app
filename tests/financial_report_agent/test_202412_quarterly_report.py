from __future__ import annotations

from backend.services.financial_report_agent.markdown_renderer import render_financial_report_markdown
from backend.services.financial_report_agent.orchestrator import run_financial_report_agent


PAGES = [
    {
        "page": 1,
        "text": """202412财务报表报送与信息采集（企业会计准则一般企业）（季报）304371
资 产 负 债 表
税款所属时间：2024-10-01至2024-12-31
纳税人名称：上海乐芙兰电子商务有限公司
纳税人识别号：913201055804841947
资产负债表日：2024-12-31
金额单位：元至角分
资 产 行次 期末余额 上年年末余额 负债和所有者权益 行次 期末余额 上年年末余额
货 币 资 金 1 150,161.66 3,507,503.11 短 期 借 款 31 25,020,000.00 26,500,000.00
应 收 账 款 4 6,855,741.28 23,553,234.10 应 付 账 款 35 4,555,321.71 13,680,915.69
预 付 款 项 5 21,265,501.03 38,836,913.00 预 收 款 项 36 108,822.15 0.00
其 他 应 收 款 6 21,841,388.72 16,694,523.62 应 付 职 工 薪 酬 39 343,166.95 0.00
存 货 7 285,068.68 285,068.68 应 交 税 费 40 888,198.67 0.00
流 动 资 产 合 计 12 50,397,861.37 82,877,242.51 其 他 应 付 款 41 2,721,239.35 0.00
长 期 股 权 投 资 18 3,946,396.93 0.00 流 动 负 债 合 计 46 33,636,748.83 78,474,828.15
固 定 资 产 20 67,954.74 0.00 长 期 借 款 48 8,000,000.00 0.00
无 形 资 产 22 275,959.79 0.00 非 流 动 负 债 合 计 52 8,000,000.00 0.00
非 流 动 资 产 合 计 29 4,290,621.25 1,820,743.43 负 债 合 计 53 41,636,748.83 78,474,828.15
实 收 资 本 54 2,660,000.00 2,660,000.00
未 分 配 利 润 58 10,391,733.79 3,563,157.79
所 有 者 权 益（或 股 东 权
益） 合 计 59 13,051,733.79 13,043,765.10
资 产 总 计 30 54,688,482.62 84,697,985.94 负 债 和 所 有 者 权 益 总 计 60 54,688,482.62 84,697,985.94
""",
    },
    {
        "page": 2,
        "text": """利 润 表
税款所属时间：2024-10-01至2024-12-31
项 目 行次 本期金额 上期金额
营 业 收 入 1 60,376,572.48 100,012,470.73
营 业 成 本 2 43,031,536.70 80,000,000.00
税 金 及 附 加 3 74,553.13 0.00
销 售 费 用 4 3,917,732.10 0.00
管 理 费 用 5 1,385,764.40 0.00
研 发 费 用 6 10,953,282.20 0.00
财 务 费 用 7 1,073,691.68 0.00
投 资 收 益 10 33,020.17 0.00
营 业 利 润 15 -26,967.56 0.00
营 业 外 收 入 16 34,941.40 0.00
营 业 外 支 出 17 5.15 0.00
利 润 总 额 18 7,968.69 0.00
所 得 税 费 用 19 0.00 0.00
净 利 润 20 7,968.69 0.00
综 合 收 益 总 额 21 7,968.69 0.00
""",
    },
    {
        "page": 3,
        "text": """现金流量表
项目 行次 本期金额 上期金额
收到的税费返还 2 298.26 35,239.66
收到其他与经营活动有关的现金 3 449,101.77 15,642,383.10
支付给职工以及为职工支付的现金 6 406,894.31 5,473,139.34
支付的各项税费 7 0.00 784,496.16
支付其他与经营活动有关的现金 8 1,215,354.87 16,188,635.86
现金及现金等价物净增加额（减少以“-”号填列） 33 -102,166.23 -1,498,747.60
加：期初现金及现金等价物余额 34 252,327.89 1,648,909.26
六、期末现金及现金等价物余额 35 150,161.66 150,161.66
""",
    },
]


def _result() -> dict:
    return run_financial_report_agent(
        raw_text="\n".join(page["text"] for page in PAGES),
        filename="202412财务报表报送与信息采集（企业会计准则一般企业）（季报）304371.pdf",
        metadata={"raw_pages": PAGES},
    )


def test_202412_quarterly_core_fields_and_company_info() -> None:
    data = _result()["structured_json"]
    info = data["company_info"]
    assert data["document_type"] == "financial_report"
    assert info["report_type"] == "quarterly"
    assert info["company_name"] == "上海乐芙兰电子商务有限公司"
    assert info["taxpayer_id"] == "913201055804841947"
    assert info["report_period_start"] == "2024-10-01"
    assert info["report_period_end"] == "2024-12-31"
    assert info["report_date"] == "2024-12-31"
    assert info["currency"] == "CNY"
    assert info["unit"] == "元"

    balance_expected = {
        "cash_and_equivalents": 150161.66,
        "accounts_receivable": 6855741.28,
        "prepayments": 21265501.03,
        "other_receivables": 21841388.72,
        "inventory": 285068.68,
        "current_assets_total": 50397861.37,
        "long_term_equity_investment": 3946396.93,
        "fixed_assets": 67954.74,
        "intangible_assets": 275959.79,
        "non_current_assets_total": 4290621.25,
        "total_assets": 54688482.62,
        "short_term_loans": 25020000.00,
        "accounts_payable": 4555321.71,
        "advance_receipts": 108822.15,
        "employee_benefits_payable": 343166.95,
        "taxes_payable": 888198.67,
        "other_payables": 2721239.35,
        "current_liabilities_total": 33636748.83,
        "long_term_loans": 8000000.00,
        "non_current_liabilities_total": 8000000.00,
        "total_liabilities": 41636748.83,
        "paid_in_capital": 2660000.00,
        "undistributed_profit": 10391733.79,
        "total_equity": 13051733.79,
        "total_liabilities_and_equity": 54688482.62,
    }
    for field, expected in balance_expected.items():
        assert data["balance_sheet"][field]["normalized_value"] == expected
    equity = data["balance_sheet"]["total_equity"]
    assert equity["confidence"] == 0.96
    assert equity["previous_normalized_value"] == 13043765.10
    assert equity["current_column_label"] == "期末余额"
    assert equity["previous_column_label"] == "上年年末余额"
    assert "所 有 者 权 益" in equity["source_text"]
    assert equity["source_text"] != "由资产总计 - 负债合计计算得出"

    income_expected = {
        "revenue": 60376572.48,
        "operating_cost": 43031536.70,
        "taxes_and_surcharges": 74553.13,
        "selling_expenses": 3917732.10,
        "admin_expenses": 1385764.40,
        "rd_expenses": 10953282.20,
        "finance_expenses": 1073691.68,
        "investment_income": 33020.17,
        "operating_profit": -26967.56,
        "non_operating_income": 34941.40,
        "non_operating_expense": 5.15,
        "total_profit": 7968.69,
        "income_tax_expense": 0.00,
        "net_profit": 7968.69,
        "comprehensive_income_total": 7968.69,
    }
    for field, expected in income_expected.items():
        assert data["income_statement"][field]["normalized_value"] == expected

    cashflow_expected = {
        "tax_refund_received": (298.26, 35239.66),
        "other_cash_received_related_to_operating": (449101.77, 15642383.10),
        "cash_paid_to_employees": (406894.31, 5473139.34),
        "taxes_paid": (0.00, 784496.16),
        "other_cash_paid_related_to_operating": (1215354.87, 16188635.86),
        "net_cash_increase": (-102166.23, -1498747.60),
        "beginning_cash_balance": (252327.89, 1648909.26),
        "ending_cash_balance": (150161.66, 150161.66),
    }
    for field, (current, previous) in cashflow_expected.items():
        assert data["cash_flow_statement"][field]["normalized_value"] == current
        assert data["cash_flow_statement"][field]["previous_normalized_value"] == previous


def test_202412_quarterly_markdown_displays_extracted_rows() -> None:
    result = _result()
    markdown = result["markdown_report"]
    for text in [
        "资产负债表摘要", "利润表摘要", "货币资金", "150,161.66", "短期借款",
        "25,020,000.00", "资产总计", "54,688,482.62", "营业收入",
        "60,376,572.48", "净利润", "7,968.69",
        "上年年末余额", "上期金额", "13,043,765.10", "100,012,470.73",
        "收到的税费返还", "298.26", "35,239.66", "支付的各项税费", "0.00", "784,496.16",
        "现金及现金等价物净增加额", "-102,166.23", "期初现金及现金等价物余额", "252,327.89",
    ]:
        assert text in markdown
    assert "| 货币资金 | -" not in markdown
    assert "| 营业收入 | -" not in markdown
    assert "| 收到的税费返还 | - | -" not in markdown
    assert "| 支付的各项税费 | 0.00 | 784,496.16 |" in markdown


def test_renderer_accepts_structured_json_directly() -> None:
    structured = _result()["structured_json"]
    markdown = render_financial_report_markdown(structured)
    assert "54,688,482.62" in markdown
    assert "60,376,572.48" in markdown
