from __future__ import annotations

from typing import Any

from ..schema import BankCreditAnalysis
from .detect_abnormal_items_skill import detect_abnormal_items
from .generate_missing_materials_skill import generate_missing_materials


def analyze_bank_credit_risk(current: dict[str, Any], history: list[dict[str, Any]] | None = None) -> BankCreditAnalysis:
    findings = detect_abnormal_items(current, history)
    order = {"low": 0, "medium": 1, "medium_high": 2, "high": 3}
    level = max((item.risk_level for item in findings), key=lambda item: order[item], default="low")
    ratios = current.get("financial_ratios") or {}
    positives: list[str] = []
    if (ratios.get("net_margin") or 0) > 0:
        positives.append("本期保持盈利。")
    if (ratios.get("asset_liability_ratio") or 1) < 0.6:
        positives.append("资产负债率处于相对可控区间。")
    negatives = [item.title for item in findings]
    questions = [item.suggestion for item in findings[:6]]
    if level in {"high", "medium_high"}:
        view = "财务表现存在影响第一还款来源或杠杆承受能力的事项，授信应审慎。"
        strategy = "以真实回款和风险缓释为前提，控制额度与期限，必要时追加担保或抵质押。"
    elif level == "medium":
        view = "可继续授信审查，但需补齐财务明细并核实异常项目。"
        strategy = "结合流水和纳税资料核实后设置匹配经营周期的授信方案。"
    else:
        view = "当前结构化财报未显示明显授信财务风险，仍需完成常规交叉验证。"
        strategy = "按常规准入和现金流核验流程推进。"
    return BankCreditAnalysis(
        overall_risk_level=level,
        credit_view=view,
        positive_factors=positives,
        negative_factors=negatives,
        key_bank_questions=questions,
        missing_materials=generate_missing_materials(findings),
        suggested_credit_strategy=strategy,
        risk_findings=findings,
    )
