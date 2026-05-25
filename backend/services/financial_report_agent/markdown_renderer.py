from __future__ import annotations

from typing import Any

from .normalizer import value_of


def _amount(section: dict[str, Any], key: str) -> str:
    value = value_of(section.get(key) or {})
    return "-" if value is None else f"{value:,.2f}"


def _ratio(value: Any) -> str:
    return "-" if value is None else f"{float(value):.2%}"


def render_financial_report_markdown(data: dict[str, Any]) -> str:
    info = data.get("company_info") or {}
    balance = data.get("balance_sheet") or {}
    income = data.get("income_statement") or {}
    cashflow = data.get("cash_flow_statement") or {}
    ratios = data.get("financial_ratios") or {}
    risk = data.get("bank_credit_analysis") or {}
    trends = data.get("trend_metrics") or []
    lines = [
        "# 财务报表授信分析报告",
        "",
        "## 1. 企业基本信息",
        f"- 企业名称：{info.get('company_name') or '-'}",
        f"- 报告期间：{info.get('report_period_start') or '-'} 至 {info.get('report_period_end') or '-'}",
        f"- 会计准则：{info.get('accounting_standard') or '-'}；报表类型：{info.get('report_type') or '-'}；单位：元",
        "",
        "## 2. 三张表摘要",
        "| 项目 | 金额（元） |",
        "| --- | ---: |",
        f"| 资产总计 | {_amount(balance, 'total_assets')} |",
        f"| 负债合计 | {_amount(balance, 'total_liabilities')} |",
        f"| 所有者权益合计 | {_amount(balance, 'total_equity')} |",
        f"| 营业收入 | {_amount(income, 'revenue')} |",
        f"| 净利润 | {_amount(income, 'net_profit')} |",
        f"| 经营活动现金流量净额 | {_amount(cashflow, 'net_operating_cash_flow')} |",
        "",
        "## 3. 银行授信核心指标表",
        "| 指标 | 数值 |",
        "| --- | ---: |",
        f"| 资产负债率 | {_ratio(ratios.get('asset_liability_ratio'))} |",
        f"| 流动比率 | {ratios.get('current_ratio') if ratios.get('current_ratio') is not None else '-'} |",
        f"| 速动比率 | {ratios.get('quick_ratio') if ratios.get('quick_ratio') is not None else '-'} |",
        f"| 毛利率 | {_ratio(ratios.get('gross_margin'))} |",
        f"| 净利率 | {_ratio(ratios.get('net_margin'))} |",
        f"| 经营现金流/收入 | {_ratio(ratios.get('operating_cash_flow_to_revenue'))} |",
        "",
        "## 4. 经营趋势分析",
    ]
    if trends:
        lines.extend(["| 期间 | 营业收入（元） | 净利润（元） | 经营现金流（元） |", "| --- | ---: | ---: | ---: |"])
        for item in trends:
            lines.append(f"| {item.get('period') or '-'} | {item.get('revenue', 0):,.2f} | {item.get('net_profit', 0):,.2f} | {item.get('net_operating_cash_flow', 0):,.2f} |")
    else:
        lines.append("- 当前仅解析本期报表，待补充历史期间后生成趋势比较。")
    lines.extend([
        "",
        "## 5. 偿债能力分析",
        f"- 资产负债率：{_ratio(ratios.get('asset_liability_ratio'))}；现金比率：{_ratio(ratios.get('cash_ratio'))}。",
        "",
        "## 6. 盈利能力分析",
        f"- 毛利率：{_ratio(ratios.get('gross_margin'))}；净利率：{_ratio(ratios.get('net_margin'))}。",
        "",
        "## 7. 现金流质量分析",
        f"- 经营现金流/收入：{_ratio(ratios.get('operating_cash_flow_to_revenue'))}；筹资依赖度：{_ratio(ratios.get('financing_dependence'))}。",
        "",
        "## 8. 异常科目分析",
    ])
    findings = risk.get("risk_findings") or []
    lines.extend([f"- [{item.get('risk_level')}] {item.get('title')}：{item.get('description')}" for item in findings] or ["- 未识别到需单列提示的异常财务项目。"])
    lines.extend(["", "## 9. 银行贷款审核关注点"])
    lines.extend([f"- {item}" for item in risk.get("key_bank_questions") or []] or ["- 按常规贷前调查核实收入、负债与现金流真实性。"])
    lines.extend(["", "## 10. 缺失材料清单"])
    lines.extend([f"- {item.get('material')}：{item.get('reason')}" for item in risk.get("missing_materials") or []])
    lines.extend([
        "",
        "## 11. 综合授信建议",
        f"- 风险等级：{risk.get('overall_risk_level') or 'unknown'}",
        f"- 授信观点：{risk.get('credit_view') or '-'}",
        f"- 建议策略：{risk.get('suggested_credit_strategy') or '-'}",
    ])
    warnings = data.get("validation_warnings") or []
    if warnings:
        lines.extend(["", "## 数据校验提示"] + [f"- {warning}" for warning in warnings])
    return "\n".join(lines)
