from __future__ import annotations

from typing import Any

from ..normalizer import value_of
from ..schema import RiskFinding


def _v(section: dict[str, Any], field: str) -> float | None:
    return value_of(section.get(field) or {})


def detect_abnormal_items(
    current: dict[str, Any], history: list[dict[str, Any]] | None = None
) -> list[RiskFinding]:
    history = history or []
    balance = current.get("balance_sheet") or {}
    income = current.get("income_statement") or {}
    cashflow = current.get("cash_flow_statement") or {}
    ratios = current.get("financial_ratios") or {}
    findings: list[RiskFinding] = []

    def add(code: str, level: str, title: str, description: str, evidence: list[str], suggestion: str) -> None:
        findings.append(RiskFinding(code=code, risk_level=level, title=title, description=description, evidence=evidence, suggestion=suggestion))

    assets = _v(balance, "total_assets") or 0
    current_liabilities = _v(balance, "current_liabilities_total") or 0
    cash = _v(balance, "cash_and_equivalents") or 0
    short_loans = _v(balance, "short_term_loans") or 0
    ocf = _v(cashflow, "net_operating_cash_flow")
    net_profit = _v(income, "net_profit")
    financing_cf = _v(cashflow, "net_financing_cash_flow")
    if ocf is not None and ocf < 0:
        prior_negative = sum(1 for item in history if (_v(item.get("cash_flow_statement") or {}, "net_operating_cash_flow") or 0) < 0)
        if prior_negative >= 1:
            add("continuous_negative_operating_cash_flow", "high", "经营现金流连续为负", "多个报告期主营经营未能形成净现金流入。", [f"本期经营现金流净额 {ocf:,.2f} 元", f"历史负值期数 {prior_negative}"], "核验回款周期、订单真实性并审慎控制新增授信额度。")
    if net_profit is not None and net_profit > 0 and ocf is not None and ocf < 0:
        add("profit_positive_cashflow_negative", "medium_high", "盈利与现金流背离", "净利润为正但经营现金流为负。", [f"净利润 {net_profit:,.2f} 元", f"经营现金流 {ocf:,.2f} 元"], "补充应收回款明细及经营现金流解释。")
    if current_liabilities and cash / current_liabilities < 0.1:
        add("low_cash", "medium_high", "货币资金覆盖偏低", "可立即动用现金对流动负债覆盖不足。", [f"现金/流动负债 {cash/current_liabilities:.2%}"], "核查银行存款流水及备用流动性安排。")
    if assets and short_loans / assets >= 0.3:
        add("high_short_term_loans", "medium_high", "短期借款占比较高", "短债对资产规模占用偏高。", [f"短期借款/资产 {short_loans/assets:.2%}"], "提供到期还款与续贷排期。")
    for field, code, title in (
        ("other_receivables", "high_other_receivables", "其他应收款占比过高"),
        ("prepayments", "high_prepayments", "预付款项占比过高"),
    ):
        amount = _v(balance, field) or 0
        if assets and amount / assets >= 0.1:
            add(code, "medium", title, "该科目对资产占比偏高，可能影响资产质量。", [f"占总资产 {amount/assets:.2%}"], "提供明细、账龄及关联方说明。")
    leverage = ratios.get("asset_liability_ratio")
    if leverage is not None and leverage >= 0.7:
        add("high_asset_liability_ratio", "high", "资产负债率过高", "杠杆水平超过授信审查常见关注线。", [f"资产负债率 {leverage:.2%}"], "结合抵质押和现金流压降授信风险敞口。")
    equity = _v(balance, "total_equity") or 0
    if assets and equity / assets < 0.15:
        add("weak_equity", "medium_high", "净资产基础较弱", "所有者权益占资产比重偏低。", [f"权益/资产 {equity/assets:.2%}"], "关注股东增资或风险缓释安排。")
    if ocf is not None and ocf < 0 and financing_cf is not None and financing_cf > 0:
        add("financing_covers_operating_gap", "medium_high", "筹资资金补经营缺口", "经营现金流为负同时筹资现金净流入。", [f"经营现金流 {ocf:,.2f} 元", f"筹资现金流 {financing_cf:,.2f} 元"], "核实融资依赖和还本付息来源。")
    if (_v(income, "interest_expense") or 0) == 0 and short_loans + (_v(balance, "long_term_loans") or 0) > 0:
        add("loan_with_zero_interest_expense", "medium", "存在借款但利息费用为零", "可能存在科目填报或期间口径异常。", ["借款余额大于 0，利息费用为 0"], "取得利息支出明细和借款合同复核。")

    ordered = sorted(history + [current], key=lambda item: str((item.get("company_info") or {}).get("report_period_end") or ""))
    if len(ordered) >= 2:
        previous = ordered[-2]
        prior_income, prior_balance = previous.get("income_statement") or {}, previous.get("balance_sheet") or {}
        revenue = _v(income, "revenue")
        old_revenue = _v(prior_income, "revenue")
        if revenue is not None and old_revenue and revenue < old_revenue:
            recent_revenues = [_v(item.get("income_statement") or {}, "revenue") for item in ordered[-3:]]
            continuous = len(recent_revenues) == 3 and all(
                recent_revenues[index] is not None
                and recent_revenues[index + 1] is not None
                and recent_revenues[index] > recent_revenues[index + 1]
                for index in range(2)
            )
            title = "收入连续下滑" if continuous else "收入下滑"
            description = "营业收入已连续多个报告期下降。" if continuous else "营业收入较上一期下降。"
            add("declining_revenue", "medium_high" if continuous else "medium", title, description, [f"本期 {revenue:,.2f} 元，上期 {old_revenue:,.2f} 元"], "核实订单、客户集中度与销售回款。")
        old_assets = _v(prior_balance, "total_assets")
        if assets and old_assets and assets < old_assets:
            add("declining_assets", "medium", "资产总额下降", "资产规模较上一期收缩。", [f"本期 {assets:,.2f} 元，上期 {old_assets:,.2f} 元"], "核实资产处置和经营收缩原因。")
        for field, code, title in (
            ("accounts_receivable", "abnormal_accounts_receivable_growth", "应收账款异常增长"),
            ("inventory", "abnormal_inventory_growth", "存货异常增长"),
        ):
            current_value = _v(balance, field)
            old_value = _v(prior_balance, field)
            if current_value is not None and old_value and current_value / old_value >= 1.3:
                add(code, "medium_high", title, "较上一期增长超过 30%。", [f"增长 {(current_value/old_value-1):.2%}"], "补充明细、账龄/库龄及减值政策。")
        old_margin = (previous.get("financial_ratios") or {}).get("gross_margin")
        margin = ratios.get("gross_margin")
        if margin is not None and old_margin is not None and abs(margin - old_margin) >= 0.1:
            add("abnormal_gross_margin_change", "medium", "毛利率异常波动", "毛利率较上一期波动超过 10 个百分点。", [f"本期 {margin:.2%}，上期 {old_margin:.2%}"], "核查收入成本结转与产品结构变化。")
    return findings
