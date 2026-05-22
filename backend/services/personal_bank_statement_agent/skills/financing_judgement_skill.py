from __future__ import annotations

from typing import Any


def build_financing_judgement(summary: dict[str, Any], risk_signals: list[dict[str, Any]], months: int) -> dict[str, Any]:
    risk_codes = {item.get("code") for item in risk_signals}
    stable = float(summary.get("stable_income") or 0)
    debt_pay = float(summary.get("loan_repayment_expense") or 0) + float(summary.get("credit_card_repayment_expense") or 0)
    high_risk = {"loan_inflow_as_income", "high_internal_transfer_ratio", "fast_in_fast_out"} & risk_codes
    missing: list[str] = []
    if months < 3:
        missing.append("补充近 6-12 个月完整个人流水")
    if "frequent_loan_repayment" in risk_codes or "high_credit_card_repayment" in risk_codes:
        missing.append("补充负债明细、信用卡账单或贷款还款计划")
    if stable >= 30000 and not high_risk:
        income_quality = "强"
    elif stable > 0 and len(high_risk) <= 1:
        income_quality = "中"
    elif stable > 0:
        income_quality = "弱"
    else:
        income_quality = "无法判断"
    if stable > debt_pay * 2 and stable > 0:
        repayment_capacity = "强"
    elif stable > debt_pay and stable > 0:
        repayment_capacity = "中"
    elif stable > 0:
        repayment_capacity = "弱"
    else:
        repayment_capacity = "无法判断"
    suspicious_flow_risk = "高" if high_risk else ("中" if risk_signals else "低")
    if income_quality in {"强", "中"} and suspicious_flow_risk == "低":
        usage = "可作为主收入证明"
    elif income_quality in {"强", "中"}:
        usage = "可作为辅助材料"
    elif income_quality == "弱":
        usage = "仅供参考"
    else:
        usage = "暂不建议采信"
    return {
        "income_quality": income_quality,
        "repayment_capacity": repayment_capacity,
        "suspicious_flow_risk": suspicious_flow_risk,
        "recommended_usage": usage,
        "missing_materials": missing,
    }
