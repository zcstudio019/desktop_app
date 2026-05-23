from __future__ import annotations

from typing import Any


def build_financing_judgement(
    summary: dict[str, Any],
    risk_signals: list[dict[str, Any]],
    months: int,
    *,
    income_verification: dict[str, Any] | None = None,
    expense_analysis: dict[str, Any] | None = None,
    repayment_analysis: dict[str, Any] | None = None,
    fast_in_fast_out_analysis: dict[str, Any] | None = None,
    cash_retention_analysis: dict[str, Any] | None = None,
    flow_nature: dict[str, Any] | None = None,
) -> dict[str, Any]:
    income_verification = income_verification or {}
    expense_analysis = expense_analysis or {}
    repayment_analysis = repayment_analysis or {}
    fast_in_fast_out_analysis = fast_in_fast_out_analysis or {}
    cash_retention_analysis = cash_retention_analysis or {}
    flow_nature = flow_nature or {}
    risk_codes = {item.get("code") for item in risk_signals}
    raw_income = float(income_verification.get("raw_total_income") or summary.get("raw_total_income") or 0)
    verified = float(income_verification.get("verified_income") or summary.get("verified_income") or 0)
    confirmed_salary = float(income_verification.get("confirmed_salary_income") or income_verification.get("verified_salary_income") or summary.get("salary_income") or 0)
    suspected_salary = float(income_verification.get("suspected_salary_income") or summary.get("suspected_salary_income") or 0)
    salary_months = int(income_verification.get("salary_months") or summary.get("salary_months") or 0)
    salary_continuity = str(income_verification.get("salary_continuity_level") or "none")
    salary_confidence = float(income_verification.get("salary_confidence") or summary.get("salary_confidence") or 0)
    unknown = float(income_verification.get("unknown_inflow") or summary.get("unknown_inflow") or 0)
    unknown_ratio = unknown / raw_income if raw_income else 0.0
    loan_ratio = float(expense_analysis.get("loan_repayment_ratio") or 0)
    is_repayment_flow = bool(repayment_analysis.get("is_repayment_account_flow") or flow_nature.get("primary_type") == "repayment_account_flow")

    missing = []
    if months < 3:
        missing.append("补充近 6-12 个月完整个人流水")
    if verified <= 0 or unknown_ratio >= 0.5:
        missing.extend(["补充工资卡流水", "补充经营收款流水", "补充带完整对方户名和用途的流水"])
    if suspected_salary > 0 and confirmed_salary <= 0:
        missing.append("核实疑似工资付款方是否为真实任职单位")
    if loan_ratio >= 0.35 or is_repayment_flow:
        missing.extend(["结合个人征信核对贷款余额、月供和还款记录", "说明每月汇款汇入的真实资金来源"])
    missing = list(dict.fromkeys(missing))

    if confirmed_salary > 0 and salary_months >= 6 and salary_continuity == "strong" and salary_confidence >= 0.75 and not is_repayment_flow:
        income_quality = "强"
    elif suspected_salary > 0 and confirmed_salary <= 0:
        income_quality = "中" if unknown_ratio < 0.5 else "无法完全确认"
    elif verified <= 0 and raw_income > 0 and unknown_ratio >= 0.5:
        income_quality = "弱"
    elif verified >= 30000 and not is_repayment_flow:
        income_quality = "强"
    elif verified > 0 and not is_repayment_flow:
        income_quality = "中"
    elif verified > 0:
        income_quality = "弱"
    else:
        income_quality = "无法判断"

    if is_repayment_flow or verified <= 0:
        repayment_capacity = "无法单独判断"
    elif verified > float(expense_analysis.get("loan_repayment_expense") or 0) * 2:
        repayment_capacity = "强"
    elif verified > 0:
        repayment_capacity = "中"
    else:
        repayment_capacity = "无法判断"

    if is_repayment_flow or "fast_in_fast_out" in risk_codes or "weak_cash_retention" in risk_codes:
        suspicious_flow_risk = "中高"
    elif risk_signals:
        suspicious_flow_risk = "中"
    else:
        suspicious_flow_risk = "低"

    if is_repayment_flow:
        recommended_usage = "可作为还款账户流水"
    elif income_quality in {"强", "中"} and suspicious_flow_risk == "低":
        recommended_usage = "可作为主收入证明"
    elif income_quality in {"强", "中"}:
        recommended_usage = "可作为辅助材料"
    elif raw_income > 0:
        recommended_usage = "仅供参考"
    else:
        recommended_usage = "暂不建议采信"

    if is_repayment_flow:
        final_summary = (
            "该流水进账与支出高度接近，收入主要为来源不明汇入或非明确工资/经营收入，"
            "支出中贷款还款/贷款回收占比较高，账户沉淀弱。该流水可证明客户存在持续还款行为，"
            "但不能直接证明其稳定收入来源，不建议单独作为主收入证明。"
        )
    elif verified > 0:
        final_summary = "该流水识别到明确工资或经营收入，可作为收入证明材料之一，仍建议结合征信、纳税、经营收款账户等资料交叉验证。"
    elif suspected_salary > 0:
        final_summary = "该流水仅识别到疑似工资收入，尚缺少明确工资摘要或完整付款方证据，需人工核实任职单位、发薪用途和连续性后再判断收入质量。"
    else:
        final_summary = "该流水未识别到明确可采信收入来源，暂不建议单独用于收入或还款能力判断。"

    return {
        "income_quality": income_quality,
        "repayment_capacity": repayment_capacity,
        "suspicious_flow_risk": suspicious_flow_risk,
        "recommended_usage": recommended_usage,
        "final_summary": final_summary,
        "missing_materials": missing,
    }
