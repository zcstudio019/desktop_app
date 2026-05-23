from __future__ import annotations

from typing import Any


def detect_risk_signals(
    summary: dict[str, Any],
    transactions: list[dict[str, Any]],
    income_analysis: dict[str, Any],
    expense_analysis: dict[str, Any],
    months: int,
    *,
    cash_retention_analysis: dict[str, Any] | None = None,
    repayment_analysis: dict[str, Any] | None = None,
    fast_in_fast_out_analysis: dict[str, Any] | None = None,
    flow_nature: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    cash_retention_analysis = cash_retention_analysis or {}
    repayment_analysis = repayment_analysis or {}
    fast_in_fast_out_analysis = fast_in_fast_out_analysis or {}
    flow_nature = flow_nature or {}
    signals: list[dict[str, str]] = []

    def add(code: str, level: str, message: str, evidence: str) -> None:
        if code not in {item["code"] for item in signals}:
            signals.append({"code": code, "level": level, "message": message, "evidence": evidence})

    raw_income = float(summary.get("raw_total_income") or income_analysis.get("raw_total_income") or 0)
    raw_expense = float(summary.get("raw_total_expense") or expense_analysis.get("raw_total_expense") or 0)
    verified_income = float(summary.get("verified_income") or income_analysis.get("verified_income") or 0)
    unknown_inflow = float(summary.get("unknown_inflow") or income_analysis.get("unknown_inflow") or 0)
    stable = float(summary.get("stable_income") or income_analysis.get("stable_income") or 0)
    confirmed_salary = float(income_analysis.get("confirmed_salary_income") or income_analysis.get("verified_salary_income") or summary.get("salary_income") or 0)
    suspected_salary = float(income_analysis.get("suspected_salary_income") or summary.get("suspected_salary_income") or 0)
    loan_repayment = float(summary.get("loan_repayment_expense") or expense_analysis.get("loan_repayment_expense") or 0)
    loan_ratio = float(expense_analysis.get("loan_repayment_ratio") or (loan_repayment / raw_expense if raw_expense else 0))
    retention_ratio = float(cash_retention_analysis.get("retention_ratio") or summary.get("retention_ratio") or 0)

    if months < 3:
        add("insufficient_statement_period", "medium", "流水周期不足", f"识别周期约 {months} 个月")
    if raw_income > 0 and unknown_inflow / raw_income >= 0.5:
        add("income_source_unclear", "high", "收入主要为来源不明汇入", f"来源不明汇入 {unknown_inflow:.2f}，占原始收入 {unknown_inflow / raw_income:.1%}")
    if verified_income <= 0:
        add("weak_verified_income", "high", "可采信工资/经营收入很低或为 0", "未识别到明确工资、薪资、货款、服务费、销售款等收入")
    elif verified_income < raw_income * 0.3:
        add("weak_verified_income", "medium", "可采信收入占比较低", f"可采信收入 {verified_income:.2f}，原始收入 {raw_income:.2f}")
    if repayment_analysis.get("is_repayment_account_flow") or flow_nature.get("primary_type") == "repayment_account_flow":
        add("repayment_account_flow", "high", "该流水主要体现还款账户特征", "; ".join(repayment_analysis.get("evidence") or flow_nature.get("reasons") or []))
    if loan_ratio >= 0.6:
        add("high_loan_repayment_ratio", "high", "贷款相关支出占总支出比例较高", f"贷款还款支出 {loan_repayment:.2f}，占总支出 {loan_ratio:.1%}")
    elif loan_ratio >= 0.35:
        add("high_loan_repayment_ratio", "medium", "贷款相关支出占比较高", f"贷款还款支出占比 {loan_ratio:.1%}")
    if fast_in_fast_out_analysis.get("has_fast_in_fast_out"):
        add("fast_in_fast_out", "high", "存在汇入后快速转出或还贷", f"匹配 {fast_in_fast_out_analysis.get('matched_count') or 0} 组，金额 {fast_in_fast_out_analysis.get('matched_amount') or 0}")
    if raw_income > 0 and retention_ratio <= 0.05:
        add("weak_cash_retention", "high", "账户沉淀弱，净流入占收入比例很低", f"沉淀率 {retention_ratio:.2%}")
    if raw_income > 0 and raw_expense > 0:
        matched = min(raw_income, raw_expense) / max(raw_income, raw_expense)
        if matched >= 0.95:
            add("income_expense_highly_matched", "high", "收入与支出金额高度接近", f"收入支出匹配度 {matched:.1%}")
    if stable <= 0 and (unknown_inflow > 0 or loan_ratio >= 0.6):
        add("cannot_use_as_primary_income_proof", "high", "不建议作为主收入证明", "缺少明确工资/经营收入证据，且存在还款账户或来源不明汇入特征")
    if confirmed_salary <= 0 and suspected_salary > 0:
        source_names = []
        for tx in transactions:
            if (tx.get("salary_detection") or {}).get("salary_type") == "suspected_salary" and tx.get("counterparty_name"):
                source_names.append(str(tx.get("counterparty_name")))
        source_text = "、".join(list(dict.fromkeys(source_names))[:3]) or "疑似单位付款方"
        add("salary_suspected_only", "medium", "存在疑似工资收入，但未识别到明确工资摘要", f"摘要为代发类款项，付款方为{source_text}，连续多月出现")
    elif confirmed_salary <= 0:
        add("salary_missing", "medium", "未识别到明确工资收入", "confirmed_salary_income 为 0")
    if confirmed_salary > 0 and income_analysis.get("salary_continuity_level") in {"none", "weak"}:
        add("salary_unstable", "medium", "工资金额或发放周期稳定性不足", f"工资连续性：{income_analysis.get('salary_continuity_level') or 'unknown'}")
    if any(
        (tx.get("salary_detection") or {}).get("salary_type") in {"suspected_salary", "unknown"}
        and (tx.get("salary_detection") or {}).get("matched_keywords")
        and not str(tx.get("counterparty_name") or "").strip()
        for tx in transactions
    ):
        add("salary_counterparty_missing", "medium", "疑似工资交易缺少付款方信息", "摘要存在代发/转账/汇款特征，但付款方为空，无法确认单位发薪")
    if float(summary.get("operating_income") or income_analysis.get("verified_operating_income") or 0) < raw_income * 0.2:
        add("operating_income_weak", "low", "经营收入弱", f"经营收入 {summary.get('operating_income') or income_analysis.get('verified_operating_income') or 0}")
    if float(summary.get("loan_inflow") or income_analysis.get("loan_inflow") or 0) > 0:
        add("loan_inflow_as_income", "high", "存在贷款流入疑似充当收入", f"贷款流入 {summary.get('loan_inflow') or income_analysis.get('loan_inflow') or 0}")
    if expense_analysis.get("has_frequent_loan_or_credit_card_repayment"):
        add("frequent_loan_repayment", "medium", "频繁贷款/信用卡还款", "识别到多笔贷款或信用卡还款支出")
    if float(expense_analysis.get("credit_card_repayment_expense") or 0) > max(stable * 0.4, 30000):
        add("high_credit_card_repayment", "medium", "信用卡还款压力较大", f"信用卡还款 {expense_analysis.get('credit_card_repayment_expense') or 0}")
    return signals
