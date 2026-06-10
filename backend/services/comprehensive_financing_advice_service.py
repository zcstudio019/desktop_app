from __future__ import annotations

from typing import Any


STATUS_LABELS = {
    "not_ready": "暂不建议进件",
    "cautious": "谨慎推进",
    "recommendable": "可推进",
    "high_quality": "优质客户",
}

PRODUCT_NAMES = {
    "mortgage_loan": "抵押类贷款",
    "credit_business_loan": "信用类经营贷",
    "tax_invoice_loan": "税票贷/发票贷",
    "bank_flow_loan": "流水贷",
    "renewal_or_refinance": "续贷/置换",
    "short_term_turnover": "短期周转",
    "defer_application": "暂缓申请",
}


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _clean_list(items: list[Any]) -> list[str]:
    cleaned: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace(",", "").replace("，", "").strip()
    for unit in ["元", "万元", "万", "%"]:
        text = text.replace(unit, "")
    try:
        return float(text)
    except ValueError:
        return None


def _has_property_asset(kyc_profile: dict[str, Any] | None) -> bool:
    profile = _as_dict(kyc_profile)
    assets = _as_dict(profile.get("assets"))
    return bool(_as_list(assets.get("properties")))


def _has_any_asset(kyc_profile: dict[str, Any] | None) -> bool:
    profile = _as_dict(kyc_profile)
    assets = _as_dict(profile.get("assets"))
    return bool(_as_list(assets.get("properties")) or _as_list(assets.get("vehicles")))


def _has_vehicle_asset(kyc_profile: dict[str, Any] | None) -> bool:
    profile = _as_dict(kyc_profile)
    assets = _as_dict(profile.get("assets"))
    return bool(_as_list(assets.get("vehicles")))


def _status(section: dict[str, Any], key: str, default: str = "unknown") -> str:
    return str(section.get(key) or default)


def _flow_has_serious_risk(flow: dict[str, Any]) -> bool:
    risks = "；".join(_clean_list(_as_list(flow.get("key_risks"))))
    return any(keyword in risks for keyword in ["账户不一致", "大量内部转账", "真实经营收入占比偏低", "净流入为负"])


def _financial_has_serious_risk(financial: dict[str, Any]) -> bool:
    risks = "；".join(_clean_list(_as_list(financial.get("key_risks"))))
    return any(keyword in risks for keyword in ["资不抵债", "持续亏损", "偿债压力", "现金流为负"])


def _has_positive_flow_or_financial_signal(flow: dict[str, Any], financial: dict[str, Any]) -> bool:
    if _clean_list(_as_list(flow.get("positive_signals"))) or _clean_list(_as_list(financial.get("positive_signals"))):
        return True
    flow_metrics = _as_dict(flow.get("summary_metrics"))
    profitability = _as_dict(financial.get("profitability"))
    return bool((_num(flow_metrics.get("average_monthly_net_income")) or 0) > 0 or (_num(profitability.get("net_profit")) or 0) > 0)


def _score(report: dict[str, Any], kyc_profile: dict[str, Any] | None) -> int:
    readiness = _as_dict(report.get("financing_readiness"))
    enterprise_credit = _as_dict(report.get("enterprise_credit_diagnostic"))
    personal_credit = _as_dict(report.get("personal_credit_diagnostic"))
    enterprise_flow = _as_dict(report.get("enterprise_bank_flow_diagnostic"))
    financial = _as_dict(report.get("financial_statement_diagnostic"))

    score = 100
    readiness_level = _status(readiness, "readiness_level", "not_ready")
    if readiness_level == "not_ready":
        score -= 25
    elif readiness_level == "basic_ready":
        score -= 10

    score -= {"unknown": 15, "attention": 15, "risky": 35}.get(_status(enterprise_credit, "credit_status"), 0)
    score -= {"unknown": 15, "attention": 15, "risky": 35}.get(_status(personal_credit, "credit_status"), 0)
    score -= {"unknown": 20, "attention": 15, "risky": 35}.get(_status(enterprise_flow, "flow_status"), 0)
    score -= {"unknown": 10, "attention": 10, "risky": 25}.get(_status(financial, "financial_status"), 0)
    score -= min(len(_clean_list(_as_list(report.get("risk_highlights")))) * 3, 15)

    if _has_any_asset(kyc_profile):
        score += 5
    if _status(enterprise_flow, "flow_status") == "normal":
        score += 5
    if _status(financial, "financial_status") == "normal":
        score += 5
    if _status(enterprise_credit, "credit_status") == "normal":
        score += 5
    if _status(personal_credit, "credit_status") == "normal":
        score += 5

    return max(0, min(100, int(score)))


def _overall_status(report: dict[str, Any], score: int, flow_serious: bool, financial_serious: bool) -> str:
    readiness = _as_dict(report.get("financing_readiness"))
    enterprise_credit = _as_dict(report.get("enterprise_credit_diagnostic"))
    personal_credit = _as_dict(report.get("personal_credit_diagnostic"))
    enterprise_flow = _as_dict(report.get("enterprise_bank_flow_diagnostic"))
    financial = _as_dict(report.get("financial_statement_diagnostic"))
    risk_highlights = _clean_list(_as_list(report.get("risk_highlights")))
    material_score = int(readiness.get("score") or 0)

    if (
        readiness.get("usable_for_financing") is False
        or _status(readiness, "readiness_level", "not_ready") == "not_ready"
        or _status(enterprise_credit, "credit_status") == "risky"
        or _status(personal_credit, "credit_status") == "risky"
        or _status(enterprise_flow, "flow_status") == "risky"
        or _status(financial, "financial_status") == "risky"
        or enterprise_credit.get("has_enterprise_credit_report") is False
        or personal_credit.get("has_personal_credit_report") is False
        or enterprise_flow.get("has_enterprise_bank_flow") is False
    ):
        return "not_ready"

    has_attention = (
        _status(enterprise_credit, "credit_status") == "attention"
        or _status(personal_credit, "credit_status") == "attention"
        or _status(enterprise_flow, "flow_status") == "attention"
        or _status(financial, "financial_status") == "attention"
        or _status(readiness, "readiness_level", "not_ready") == "basic_ready"
        or material_score < 80
        or len(risk_highlights) >= 3
    )
    if has_attention:
        return "cautious"

    if (
        _status(readiness, "readiness_level") == "ready"
        and _status(enterprise_credit, "credit_status") == "normal"
        and _status(personal_credit, "credit_status") == "normal"
        and _status(enterprise_flow, "flow_status") == "normal"
        and _status(financial, "financial_status") == "normal"
        and not flow_serious
        and not financial_serious
        and len(risk_highlights) <= 1
    ):
        return "high_quality" if score >= 90 else "recommendable"

    return "recommendable"


def _product(product_type: str, fit_level: str, reason: str) -> dict[str, str]:
    return {
        "product_type": product_type,
        "product_name": PRODUCT_NAMES[product_type],
        "fit_level": fit_level,
        "reason": reason,
    }


def _product_directions(report: dict[str, Any], kyc_profile: dict[str, Any] | None, overall_status: str) -> list[dict[str, str]]:
    enterprise_credit = _as_dict(report.get("enterprise_credit_diagnostic"))
    personal_credit = _as_dict(report.get("personal_credit_diagnostic"))
    enterprise_flow = _as_dict(report.get("enterprise_bank_flow_diagnostic"))
    financial = _as_dict(report.get("financial_statement_diagnostic"))
    credit_status = _status(enterprise_credit, "credit_status")
    personal_status = _status(personal_credit, "credit_status")
    flow_status = _status(enterprise_flow, "flow_status")
    financial_status = _status(financial, "financial_status")
    products: list[dict[str, str]] = []

    if overall_status == "not_ready":
        products.append(_product("defer_application", "high", "当前存在关键资料缺口或高风险项，建议先暂缓正式进件。"))

    has_property = _has_property_asset(kyc_profile)
    products.append(
        _product(
            "mortgage_loan",
            "high" if has_property and credit_status != "risky" and personal_status != "risky" else "low",
            "已识别房产资产，可优先评估抵押类产品。" if has_property else "暂未识别可用于抵押的房产资料。",
        )
    )

    if credit_status == "risky" or personal_status == "risky" or flow_status == "risky":
        credit_fit = "not_suitable"
        credit_reason = "征信或流水存在高风险项，暂不适合信用类经营贷。"
    elif credit_status == "normal" and personal_status == "normal" and flow_status == "normal":
        credit_fit = "high"
        credit_reason = "企业征信、个人征信和经营流水整体正常，可尝试信用类经营贷。"
    elif "attention" in {credit_status, personal_status, flow_status}:
        credit_fit = "medium"
        credit_reason = "存在需关注事项，信用类经营贷可谨慎推进并补充解释材料。"
    else:
        credit_fit = "low"
        credit_reason = "核心信用或流水资料尚不充分。"
    products.append(_product("credit_business_loan", credit_fit, credit_reason))

    profitability = _as_dict(financial.get("profitability"))
    if financial.get("has_financial_statement") is False:
        tax_fit = "low"
        tax_reason = "暂未上传财务数据，税票类额度判断依据不足。"
    elif financial_status == "normal" or (_num(profitability.get("revenue")) or 0) > 0 or (_num(profitability.get("net_profit")) or 0) > 0:
        tax_fit = "medium"
        tax_reason = "财务收入或盈利信号可作为税票贷/发票贷的辅助评估依据。"
    else:
        tax_fit = "low"
        tax_reason = "财务收入和盈利信号不足，税票类产品匹配度较低。"
    products.append(_product("tax_invoice_loan", tax_fit, tax_reason))

    flow_summary = _as_dict(enterprise_flow.get("summary_metrics"))
    flow_quality = _as_dict(enterprise_flow.get("quality_metrics"))
    real_income_ratio = _num(flow_quality.get("real_income_ratio"))
    monthly_income = _num(flow_summary.get("average_monthly_income")) or 0
    if enterprise_flow.get("has_enterprise_bank_flow") is False:
        flow_fit = "low"
        flow_reason = "暂未上传企业流水，无法评估流水贷。"
    elif flow_status == "normal" and monthly_income > 0 and (real_income_ratio is None or real_income_ratio >= 0.6):
        flow_fit = "high"
        flow_reason = "企业流水状态正常，月均收入和真实经营收入占比较适合流水贷。"
    elif flow_status == "attention":
        flow_fit = "medium"
        flow_reason = "流水存在需关注事项，可补充交易解释后谨慎评估。"
    else:
        flow_fit = "low"
        flow_reason = "流水质量或收入稳定性不足。"
    products.append(_product("bank_flow_loan", flow_fit, flow_reason))

    debt_summary = _as_dict(enterprise_credit.get("debt_summary"))
    loan_summary = _as_dict(enterprise_credit.get("loan_summary"))
    usage_rate = _num(debt_summary.get("credit_usage_rate"))
    upcoming_due_loans = _as_list(loan_summary.get("upcoming_due_loans"))
    if upcoming_due_loans or (usage_rate is not None and usage_rate >= 0.8):
        renew_fit = "high" if upcoming_due_loans and usage_rate is not None and usage_rate >= 0.8 else "medium"
        renew_reason = "存在即将到期贷款或授信使用率偏高，可评估续贷/置换方案。"
    else:
        renew_fit = "low"
        renew_reason = "暂未识别明显续贷或置换触发因素。"
    products.append(_product("renewal_or_refinance", renew_fit, renew_reason))

    net_income = _num(flow_summary.get("net_income"))
    avg_net = _num(flow_summary.get("average_monthly_net_income"))
    if monthly_income > 0 and ((net_income is not None and net_income <= 0) or (avg_net is not None and avg_net <= 0)):
        turnover_fit = "medium"
        turnover_reason = "企业有收入但净流入偏弱，可考虑短期周转并同步改善现金流。"
    else:
        turnover_fit = "low"
        turnover_reason = "暂未识别明确短期周转资金缺口。"
    products.append(_product("short_term_turnover", turnover_fit, turnover_reason))

    return products


def build_comprehensive_financing_advice(
    report: dict[str, Any] | None,
    kyc_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    report = _as_dict(report)
    enterprise_credit = _as_dict(report.get("enterprise_credit_diagnostic"))
    personal_credit = _as_dict(report.get("personal_credit_diagnostic"))
    enterprise_flow = _as_dict(report.get("enterprise_bank_flow_diagnostic"))
    financial = _as_dict(report.get("financial_statement_diagnostic"))
    readiness = _as_dict(report.get("financing_readiness"))
    risk_highlights = _clean_list(_as_list(report.get("risk_highlights")))

    score = _score(report, kyc_profile)
    flow_serious = _flow_has_serious_risk(enterprise_flow)
    financial_serious = _financial_has_serious_risk(financial)
    overall_status = _overall_status(report, score, flow_serious, financial_serious)

    main_shortcomings = _clean_list(
        _as_list(_as_dict(report.get("material_checklist")).get("required_missing"))
        + risk_highlights[:5]
    )
    key_strengths = _clean_list(
        _as_list(enterprise_credit.get("positive_signals"))
        + _as_list(personal_credit.get("positive_signals"))
        + _as_list(enterprise_flow.get("positive_signals"))
        + _as_list(financial.get("positive_signals"))
    )
    if _has_any_asset(kyc_profile):
        key_strengths = _clean_list(key_strengths + ["已识别可用于融资评估的资产资料"])
    if _has_vehicle_asset(kyc_profile):
        key_strengths = _clean_list(key_strengths + ["客户已提供车辆资产资料，可作为辅助增信材料"])
    if not key_strengths and overall_status in {"recommendable", "high_quality"}:
        key_strengths = ["核心资料和主要风控模块整体可支持进一步融资评估"]

    priority_actions = _clean_list(
        main_shortcomings[:3]
        + _as_list(report.get("next_actions"))
        + _as_list(_as_dict(report.get("material_checklist")).get("recommended_supplements"))
    )[:8]
    if not priority_actions:
        priority_actions = ["进入产品匹配和额度测算前，建议客户经理复核关键字段与资料原件一致性"]

    risk_summary = risk_highlights[:8]
    if not risk_summary:
        risk_summary = ["暂未识别明确高风险事项，仍需结合征信原件、流水明细和银行准入规则复核"]

    products = _product_directions(report, kyc_profile, overall_status)
    status_text = STATUS_LABELS[overall_status]
    if overall_status == "not_ready":
        summary = "当前客户暂不建议直接进件，应优先补齐关键资料或处理高风险事项。"
        script = "建议先和客户确认缺失资料与主要风险项，优先补齐征信、流水和主体资料，待资料完整后再进入产品匹配。"
    elif overall_status == "cautious":
        summary = "当前客户可谨慎推进，需先解释关注项并补强关键佐证材料。"
        script = "可以先按意向产品收集补充材料，同时把征信、流水或财务关注点提前整理成银行可接受的说明。"
    elif overall_status == "high_quality":
        summary = "当前客户资料质量较好，可优先推进融资方案匹配和额度测算。"
        script = "客户资料基础较完整，可直接沟通融资用途、期限、额度和可接受成本，并同步准备银行进件清单。"
    else:
        summary = "当前客户具备进一步融资评估基础，可推进产品方向筛选。"
        script = "建议围绕客户资金用途和可提供增信资料，优先筛选匹配度较高的产品方向并开展额度预估。"

    return {
        "overall_status": overall_status,
        "financing_readiness_score": score,
        "recommended_product_directions": products,
        "main_shortcomings": main_shortcomings,
        "key_strengths": key_strengths,
        "priority_actions": priority_actions,
        "risk_summary": risk_summary,
        "sales_follow_up_script": script,
        "summary": f"{status_text}：{summary}",
    }
