from __future__ import annotations

from typing import Any


def build_financing_summary(
    statement_summary: dict[str, Any],
    monthly_trends: list[dict[str, Any]],
    counterparties: list[dict[str, Any]],
    risk_signals: list[dict[str, Any]],
    loan_related_transactions: list[dict[str, Any]],
) -> dict[str, str]:
    total_credit = float(statement_summary.get("total_credit_amount") or 0)
    total_debit = float(statement_summary.get("total_debit_amount") or 0)
    active_months = len(monthly_trends)
    concentration = max((float(item.get("concentration") or 0) for item in counterparties), default=0)
    cash_flow_stability = "收入较稳定" if active_months >= 3 and not any(item.get("is_abnormal_volatility") for item in monthly_trends) else "收入稳定性需结合更多月份核验"
    business_reality = "存在多笔经营往来，可作为经营真实性参考" if len(counterparties) >= 2 and concentration < 0.8 else "对手方较集中，需核验真实交易背景"
    repayment_capacity = "现金流覆盖支出情况较好" if total_credit >= total_debit else "支出高于收入，还款能力需谨慎评估"
    abnormal_flow_risk = "未发现突出异常信号" if not risk_signals else "存在异常流水信号，建议逐项核验"
    suggested_limit = ""
    if total_credit:
        suggested_limit = f"可参考月均收入的1-3倍测算，当前月均收入约 {statement_summary.get('monthly_average_credit') or 0} 元"
    return {
        "cash_flow_stability": cash_flow_stability,
        "business_reality": business_reality,
        "repayment_capacity": repayment_capacity,
        "abnormal_flow_risk": abnormal_flow_risk,
        "suggested_credit_limit_reference": suggested_limit,
        "summary": f"账户共识别收入 {round(total_credit, 2)} 元、支出 {round(total_debit, 2)} 元，融资相关交易 {len(loan_related_transactions)} 笔。",
    }
