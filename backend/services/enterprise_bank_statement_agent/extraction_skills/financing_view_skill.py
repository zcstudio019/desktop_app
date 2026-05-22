from __future__ import annotations

from typing import Any


def build_financing_view(summary: dict[str, Any], risk_analysis: dict[str, Any]) -> dict[str, Any]:
    bank_recognizable = summary.get("operating_inflow")
    if bank_recognizable is None:
        bank_recognizable = summary.get("estimated_operating_inflow")
    checklist = ["近6-12个月完整银行流水", "主要客户合同/订单", "主要交易发票", "纳税申报或完税证明"]
    if summary.get("excluded_related_party_inflow"):
        checklist.append("关联方交易背景说明")
    if summary.get("excluded_personal_inflow"):
        checklist.append("个人往来用途说明")
    products = ["经营贷", "流动资金贷款"]
    if float(bank_recognizable or 0) <= 0:
        products = ["资料补充后再评估"]
    explanation = [
        f"原始进账 {summary.get('total_inflow', 0)} 元不等同于销售收入。",
        f"剔除内部转账、关联方和个人往来后，初步估算银行可能认可经营性进账 {bank_recognizable or 0} 元。",
    ]
    conclusion = "流水具备初步审查价值，但需结合风险信号和补充材料审慎判断。"
    if risk_analysis.get("overall_level") == "high":
        conclusion = "流水存在较高审查风险，建议先补充交易背景、合同发票和更长期流水。"
    return {
        "bank_recognizable_inflow": bank_recognizable,
        "adjusted_operating_inflow": bank_recognizable,
        "excluded_internal_transfer_amount": summary.get("excluded_internal_transfer_amount"),
        "excluded_related_party_inflow": summary.get("excluded_related_party_inflow"),
        "excluded_personal_inflow": summary.get("excluded_personal_inflow"),
        "suggested_credit_products": products,
        "material_checklist": checklist,
        "bank_explanation": explanation,
        "conclusion": conclusion,
    }
