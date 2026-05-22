from __future__ import annotations

from typing import Any


def detect_risk_signals(summary: dict[str, Any], transactions: list[dict[str, Any]], income_analysis: dict[str, Any], expense_analysis: dict[str, Any], months: int) -> list[dict[str, str]]:
    signals: list[dict[str, str]] = []

    def add(code: str, level: str, message: str, evidence: str) -> None:
        signals.append({"code": code, "level": level, "message": message, "evidence": evidence})

    stable = float(summary.get("stable_income") or 0)
    raw_income = float(summary.get("raw_total_income") or 0)
    if months < 3:
        add("insufficient_statement_period", "medium", "流水周期不足", f"识别周期约 {months} 个月")
    if float(summary.get("salary_income") or 0) <= 0:
        add("salary_missing", "medium", "未识别到工资收入", "工资/薪资/代发类关键词收入为 0")
    if float(summary.get("operating_income") or 0) < raw_income * 0.2:
        add("operating_income_weak", "low", "经营收入占比较低", f"经营收入 {summary.get('operating_income') or 0}")
    if raw_income and float(summary.get("internal_transfer_income") or 0) / raw_income > 0.3:
        add("high_internal_transfer_ratio", "high", "内部转账占比较高", f"内部转账收入 {summary.get('internal_transfer_income') or 0}")
    if float(summary.get("loan_inflow") or 0) > 0:
        add("loan_inflow_as_income", "high", "存在贷款流入疑似充当收入", f"贷款流入 {summary.get('loan_inflow') or 0}")
    if expense_analysis.get("has_frequent_loan_or_credit_card_repayment"):
        add("frequent_loan_repayment", "medium", "频繁贷款/信用卡还款", "识别到多笔贷款或信用卡还款支出")
    if float(summary.get("credit_card_repayment_expense") or 0) > max(stable * 0.4, 30000):
        add("high_credit_card_repayment", "medium", "信用卡还款压力较大", f"信用卡还款 {summary.get('credit_card_repayment_expense') or 0}")
    if income_analysis.get("income_volatility", 0) > 0.8:
        add("income_unstable", "medium", "收入不稳定", f"稳定收入波动率 {income_analysis.get('income_volatility')}")
    if any("fast_in_fast_out" in (tx.get("risk_tags") or []) for tx in transactions):
        add("fast_in_fast_out", "high", "存在快进快出", "识别到短时间内大额进出")
    if expense_analysis.get("has_abnormal_large_expense"):
        add("abnormal_large_transaction", "medium", "存在异常大额交易", "存在单笔大额支出")
    return signals
