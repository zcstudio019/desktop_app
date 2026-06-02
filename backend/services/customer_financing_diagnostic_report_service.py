from __future__ import annotations

from typing import Any

from backend.services.comprehensive_financing_advice_service import build_comprehensive_financing_advice


READINESS_LABELS = {
    "not_ready": "未就绪",
    "basic_ready": "基本就绪",
    "ready": "已就绪",
}

REPORT_STATUS_LABELS = {
    "draft": "草稿报告",
}

CREDIT_STATUS_LABELS = {
    "unknown": "未知",
    "normal": "正常",
    "attention": "需关注",
    "risky": "风险较高",
}

QUERY_RISK_LABELS = {
    "unknown": "未知",
    "low": "低",
    "medium": "中",
    "high": "高",
}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _clean_list(items: list[Any]) -> list[str]:
    cleaned: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned


def _first_text(data: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = data.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _customer_summary(customer: dict[str, Any] | None, customer_id: str) -> dict[str, str]:
    customer = customer or {}
    return {
        "customer_name": _first_text(customer, ["customer_name", "name", "企业名称", "客户名称"]) or customer_id,
        "customer_type": _first_text(customer, ["customer_type", "type", "客户类型"]),
        "phone": _first_text(customer, ["phone", "mobile", "contact_phone", "联系电话", "手机号"]),
        "intent_level": _first_text(customer, ["intent_level", "intention_level", "意向等级"]),
        "status": _first_text(customer, ["status", "客户状态", "state"]),
    }


def _readiness_label(value: str) -> str:
    return READINESS_LABELS.get(value, value or "未就绪")


def _yes_no(value: bool) -> str:
    return "是，已具备初步融资评估条件" if value else "否，请先补充关键资料或处理字段冲突"


def _format_number(value: Any) -> str:
    if value is None or value == "":
        return "未识别"
    return str(value)


def _format_rate(value: Any) -> str:
    if value is None or value == "":
        return "未识别"
    try:
        return f"{float(value) * 100:.2f}%"
    except (TypeError, ValueError):
        return str(value)


def _yes_no_plain(value: Any) -> str:
    if value is None:
        return "未识别"
    return "是" if bool(value) else "否"


def _markdown_list(items: list[str], empty_text: str) -> str:
    if not items:
        return f"- {empty_text}"
    return "\n".join(f"- {item}" for item in items)


def _build_report_markdown(report: dict[str, Any]) -> str:
    summary = report.get("customer_summary") if isinstance(report.get("customer_summary"), dict) else {}
    readiness = report.get("financing_readiness") if isinstance(report.get("financing_readiness"), dict) else {}
    checklist = report.get("material_checklist") if isinstance(report.get("material_checklist"), dict) else {}
    enterprise_credit = report.get("enterprise_credit_diagnostic") if isinstance(report.get("enterprise_credit_diagnostic"), dict) else {}
    debt_summary = enterprise_credit.get("debt_summary") if isinstance(enterprise_credit.get("debt_summary"), dict) else {}
    loan_summary = enterprise_credit.get("loan_summary") if isinstance(enterprise_credit.get("loan_summary"), dict) else {}
    personal_credit = report.get("personal_credit_diagnostic") if isinstance(report.get("personal_credit_diagnostic"), dict) else {}
    personal_debt = personal_credit.get("debt_summary") if isinstance(personal_credit.get("debt_summary"), dict) else {}
    overdue_summary = personal_credit.get("overdue_summary") if isinstance(personal_credit.get("overdue_summary"), dict) else {}
    query_summary = personal_credit.get("query_summary") if isinstance(personal_credit.get("query_summary"), dict) else {}
    serious_summary = personal_credit.get("serious_negative_summary") if isinstance(personal_credit.get("serious_negative_summary"), dict) else {}
    enterprise_flow = report.get("enterprise_bank_flow_diagnostic") if isinstance(report.get("enterprise_bank_flow_diagnostic"), dict) else {}
    flow_summary = enterprise_flow.get("summary_metrics") if isinstance(enterprise_flow.get("summary_metrics"), dict) else {}
    flow_quality = enterprise_flow.get("quality_metrics") if isinstance(enterprise_flow.get("quality_metrics"), dict) else {}
    flow_consistency = enterprise_flow.get("account_consistency") if isinstance(enterprise_flow.get("account_consistency"), dict) else {}
    financial_statement = report.get("financial_statement_diagnostic") if isinstance(report.get("financial_statement_diagnostic"), dict) else {}
    financial_period = financial_statement.get("period") if isinstance(financial_statement.get("period"), dict) else {}
    profitability = financial_statement.get("profitability") if isinstance(financial_statement.get("profitability"), dict) else {}
    debt_capacity = financial_statement.get("debt_capacity") if isinstance(financial_statement.get("debt_capacity"), dict) else {}
    liquidity = financial_statement.get("liquidity") if isinstance(financial_statement.get("liquidity"), dict) else {}
    cash_flow = financial_statement.get("cash_flow") if isinstance(financial_statement.get("cash_flow"), dict) else {}
    required_missing = _clean_list(_as_list(checklist.get("required_missing")))
    optional_missing = _clean_list(_as_list(checklist.get("optional_missing")))
    risk_highlights = _clean_list(_as_list(report.get("risk_highlights")))
    next_actions = _clean_list(_as_list(report.get("next_actions")))
    enterprise_credit_risks = _clean_list(_as_list(enterprise_credit.get("key_risks")))
    enterprise_credit_actions = _clean_list(_as_list(enterprise_credit.get("recommended_actions")))
    personal_credit_risks = _clean_list(_as_list(personal_credit.get("key_risks")))
    personal_credit_actions = _clean_list(_as_list(personal_credit.get("recommended_actions")))
    enterprise_flow_risks = _clean_list(_as_list(enterprise_flow.get("key_risks")))
    enterprise_flow_actions = _clean_list(_as_list(enterprise_flow.get("recommended_actions")))
    financial_risks = _clean_list(_as_list(financial_statement.get("key_risks")))
    financial_actions = _clean_list(_as_list(financial_statement.get("recommended_actions")))
    comprehensive_advice = report.get("comprehensive_financing_advice") if isinstance(report.get("comprehensive_financing_advice"), dict) else {}
    product_directions = _as_list(comprehensive_advice.get("recommended_product_directions"))
    strengths = _clean_list(_as_list(comprehensive_advice.get("key_strengths")))
    shortcomings = _clean_list(_as_list(comprehensive_advice.get("main_shortcomings")))
    priority_actions = _clean_list(_as_list(comprehensive_advice.get("priority_actions")))
    advice_risks = _clean_list(_as_list(comprehensive_advice.get("risk_summary")))
    advice_status_labels = {
        "not_ready": "暂不建议进件",
        "cautious": "谨慎推进",
        "recommendable": "可推进",
        "high_quality": "优质客户",
    }
    advice_fit_labels = {
        "high": "高",
        "medium": "中",
        "low": "低",
        "not_suitable": "不适合",
    }

    def _product_direction_lines(items: list[Any]) -> list[str]:
        lines: list[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = item.get("product_name") or item.get("product_type") or "未识别产品方向"
            fit = advice_fit_labels.get(str(item.get("fit_level") or ""), str(item.get("fit_level") or "未识别"))
            reason = item.get("reason") or "暂无匹配原因"
            lines.append(f"{name}（匹配度：{fit}）：{reason}")
        return lines

    return "\n".join(
        [
            "# 客户融资诊断报告",
            "",
            "## 一、客户基础信息",
            f"- 客户名称：{summary.get('customer_name') or '未记录'}",
            f"- 客户类型：{summary.get('customer_type') or '未记录'}",
            f"- 联系电话：{summary.get('phone') or '未记录'}",
            f"- 客户状态：{summary.get('status') or '未记录'}",
            f"- 意向等级：{summary.get('intent_level') or '未记录'}",
            "",
            "## 二、融资资料诊断",
            f"- 是否具备初步融资评估条件：{_yes_no(bool(readiness.get('usable_for_financing')))}",
            f"- 资料完整度：{readiness.get('score') or 0}",
            f"- 准备状态：{_readiness_label(str(readiness.get('readiness_level') or 'not_ready'))}",
            f"- 诊断摘要：{readiness.get('summary') or '暂无足够资料，当前为初步报告'}",
            "",
            "## 三、企业征信诊断",
            f"- 是否已上传企业征信：{'是' if enterprise_credit.get('has_enterprise_credit_report') else '否，尚未上传企业征信报告'}",
            f"- 企业征信状态：{CREDIT_STATUS_LABELS.get(str(enterprise_credit.get('credit_status') or 'unknown'), str(enterprise_credit.get('credit_status') or '未知'))}",
            f"- 当前未结清借贷余额：{_format_number(debt_summary.get('total_unsettled_balance'))}",
            f"- 授信使用率：{_format_rate(debt_summary.get('credit_usage_rate'))}",
            f"- 未结清贷款笔数：{loan_summary.get('active_loan_count') or 0}",
            "- 主要风险：",
            _markdown_list(enterprise_credit_risks, "暂无明确企业征信风险" if enterprise_credit.get("has_enterprise_credit_report") else "尚未上传企业征信报告"),
            "- 建议动作：",
            _markdown_list(enterprise_credit_actions, "暂无明确企业征信建议"),
            "",
            "## 四、个人征信诊断",
            f"- 是否已上传个人征信：{'是' if personal_credit.get('has_personal_credit_report') else '否，尚未上传个人征信报告'}",
            f"- 个人征信状态：{CREDIT_STATUS_LABELS.get(str(personal_credit.get('credit_status') or 'unknown'), str(personal_credit.get('credit_status') or '未知'))}",
            f"- 当前贷款余额：{_format_number(personal_debt.get('loan_balance'))}",
            f"- 信用卡已用额度：{_format_number(personal_debt.get('credit_card_used_amount'))}",
            f"- 最近3个月查询次数：{_format_number(query_summary.get('last_3_months_query_count'))}",
            f"- 最近6个月查询次数：{_format_number(query_summary.get('last_6_months_query_count'))}",
            f"- 查询风险等级：{QUERY_RISK_LABELS.get(str(query_summary.get('query_risk_level') or 'unknown'), str(query_summary.get('query_risk_level') or '未知'))}",
            f"- 是否存在逾期：{'是' if overdue_summary.get('has_loan_overdue') or overdue_summary.get('has_credit_card_overdue') else '否'}",
            f"- 是否存在严重负面：{'是' if serious_summary.get('has_serious_negative') else '否'}",
            "- 主要风险：",
            _markdown_list(personal_credit_risks, "暂无明确个人征信风险" if personal_credit.get("has_personal_credit_report") else "尚未上传个人征信报告"),
            "- 建议动作：",
            _markdown_list(personal_credit_actions, "暂无明确个人征信建议"),
            "",
            "## 五、企业流水诊断",
            f"- 是否已上传企业流水：{'是' if enterprise_flow.get('has_enterprise_bank_flow') else '否，尚未上传企业流水'}",
            f"- 流水诊断状态：{CREDIT_STATUS_LABELS.get(str(enterprise_flow.get('flow_status') or 'unknown'), str(enterprise_flow.get('flow_status') or '未知'))}",
            f"- 流水期间：{flow_summary.get('period_start') or '未识别'} 至 {flow_summary.get('period_end') or '未识别'}",
            f"- 总收入：{_format_number(flow_summary.get('total_income'))}",
            f"- 总支出：{_format_number(flow_summary.get('total_expense'))}",
            f"- 净流入：{_format_number(flow_summary.get('net_income'))}",
            f"- 月均收入：{_format_number(flow_summary.get('average_monthly_income'))}",
            f"- 月均净流入：{_format_number(flow_summary.get('average_monthly_net_income'))}",
            f"- 可采信经营收入：{_format_number(flow_quality.get('real_income_amount'))}",
            f"- 内部转账占比：{_format_rate(flow_quality.get('internal_transfer_ratio'))}",
            f"- 户名一致性：{_yes_no_plain(flow_consistency.get('is_consistent'))}",
            "- 主要风险：",
            _markdown_list(enterprise_flow_risks, "暂无明确企业流水风险" if enterprise_flow.get("has_enterprise_bank_flow") else "尚未上传企业流水"),
            "- 建议动作：",
            _markdown_list(enterprise_flow_actions, "暂无明确企业流水建议"),
            "",
            "## 六、财务数据诊断",
            f"- 是否已上传财务数据：{'是' if financial_statement.get('has_financial_statement') else '否，尚未上传财务报表'}",
            f"- 财务诊断状态：{CREDIT_STATUS_LABELS.get(str(financial_statement.get('financial_status') or 'unknown'), str(financial_statement.get('financial_status') or '未知'))}",
            f"- 最近期间：{financial_period.get('latest_period') or '未识别'}",
            f"- 报表类型：{financial_period.get('statement_type') or '未识别'}",
            f"- 营业收入：{_format_number(profitability.get('revenue'))}",
            f"- 净利润：{_format_number(profitability.get('net_profit'))}",
            f"- 资产总额：{_format_number(debt_capacity.get('total_assets'))}",
            f"- 负债总额：{_format_number(debt_capacity.get('total_liabilities'))}",
            f"- 资产负债率：{_format_rate(debt_capacity.get('asset_liability_ratio'))}",
            f"- 流动比率：{_format_number(liquidity.get('current_ratio'))}",
            f"- 经营活动现金流净额：{_format_number(cash_flow.get('operating_cash_flow_net'))}",
            "- 主要风险：",
            _markdown_list(financial_risks, "暂无明确财务数据风险" if financial_statement.get("has_financial_statement") else "尚未上传财务报表"),
            "- 建议动作：",
            _markdown_list(financial_actions, "暂无明确财务数据建议"),
            "",
            "## 七、综合融资建议",
            f"- 综合状态：{advice_status_labels.get(str(comprehensive_advice.get('overall_status') or 'not_ready'), str(comprehensive_advice.get('overall_status') or 'not_ready'))}",
            f"- 融资准备度分数：{comprehensive_advice.get('financing_readiness_score') or 0}",
            "- 推荐产品方向：",
            _markdown_list(_product_direction_lines(product_directions), "暂未形成明确产品方向"),
            "- 主要优势：",
            _markdown_list(strengths, "暂无明确优势信号"),
            "- 主要短板：",
            _markdown_list(shortcomings, "暂无明确短板"),
            "- 风险摘要：",
            _markdown_list(advice_risks, "暂无明确风险摘要"),
            "- 优先行动建议：",
            _markdown_list(priority_actions, "暂无明确优先行动建议"),
            f"- 客户经理跟进话术：{comprehensive_advice.get('sales_follow_up_script') or '暂不建议进件，请先补齐资料'}",
            "",
            "## 七、主要风险提醒",
            _markdown_list(risk_highlights, "暂无明确风险提醒"),
            "",
            "## 八、缺失资料清单",
            "### 必缺资料",
            _markdown_list(required_missing, "暂无必缺资料"),
            "",
            "### 可选补充资料",
            _markdown_list(optional_missing, "暂无可选缺失资料"),
            "",
            "## 九、建议下一步",
            _markdown_list(next_actions, "暂无明确下一步建议"),
            "",
            "## 十、说明",
            "当前报告基于 KYC 资料、企业征信结构化结果、个人征信结构化结果、企业流水结构化结果、财务数据结构化结果、人工确认字段和资料完整性规则自动生成，尚未纳入个人流水综合判断。",
        ]
    )


def build_customer_financing_diagnostic_report(
    customer_id: str,
    customer: dict[str, Any] | None,
    kyc_profile: dict[str, Any] | None,
    kyc_completeness: dict[str, Any] | None,
    kyc_diagnostic: dict[str, Any] | None,
    enterprise_credit_diagnostic: dict[str, Any] | None = None,
    personal_credit_diagnostic: dict[str, Any] | None = None,
    enterprise_bank_flow_diagnostic: dict[str, Any] | None = None,
    financial_statement_diagnostic: dict[str, Any] | None = None,
) -> dict[str, Any]:
    completeness = kyc_completeness or {}
    diagnostic = kyc_diagnostic or {}
    enterprise_credit = enterprise_credit_diagnostic or {}
    personal_credit = personal_credit_diagnostic or {}
    enterprise_flow = enterprise_bank_flow_diagnostic or {}
    financial_statement = financial_statement_diagnostic or {}
    required_missing = _clean_list(_as_list(completeness.get("required_missing")))
    if enterprise_credit and not enterprise_credit.get("has_enterprise_credit_report"):
        required_missing = _clean_list(required_missing + ["企业征信报告"])
    if personal_credit and not personal_credit.get("has_personal_credit_report"):
        required_missing = _clean_list(required_missing + ["法人/实际控制人个人征信报告"])
    if enterprise_flow and not enterprise_flow.get("has_enterprise_bank_flow"):
        required_missing = _clean_list(required_missing + ["近6-12个月企业银行流水"])
    if financial_statement and not financial_statement.get("has_financial_statement"):
        required_missing = _clean_list(required_missing + ["最近一年或最近一期财务报表"])
    optional_missing = _clean_list(_as_list(completeness.get("optional_missing")))
    recommended_supplements = _clean_list(
        _as_list(completeness.get("suggestions"))
        + _as_list(diagnostic.get("recommended_actions"))
        + _as_list(enterprise_credit.get("recommended_actions"))
        + _as_list(personal_credit.get("recommended_actions"))
        + _as_list(enterprise_flow.get("recommended_actions"))
        + _as_list(financial_statement.get("recommended_actions"))
    )
    risk_highlights = _clean_list(
        _as_list(diagnostic.get("key_risks"))
        + _as_list(completeness.get("conflicts"))
        + _as_list(completeness.get("warnings"))
        + _as_list(enterprise_credit.get("key_risks"))
        + _as_list(personal_credit.get("key_risks"))
        + _as_list(enterprise_flow.get("key_risks"))
        + _as_list(financial_statement.get("key_risks"))
    )
    next_actions = _clean_list(
        _as_list(diagnostic.get("recommended_actions"))
        + _as_list(enterprise_credit.get("recommended_actions"))
        + _as_list(personal_credit.get("recommended_actions"))
        + _as_list(enterprise_flow.get("recommended_actions"))
        + _as_list(financial_statement.get("recommended_actions"))
    )
    score = int(diagnostic.get("material_completeness_score") or completeness.get("completeness_score") or 0)
    readiness_level = str(diagnostic.get("readiness_level") or "not_ready")
    usable_for_financing = bool(diagnostic.get("usable_for_financing"))
    summary = str(diagnostic.get("summary") or "暂无足够资料，当前为初步报告")

    report = {
        "customer_id": customer_id,
        "report_type": "customer_financing_diagnostic",
        "report_status": "draft",
        "customer_summary": _customer_summary(customer, customer_id),
        "kyc_diagnostic": diagnostic,
        "enterprise_credit_diagnostic": enterprise_credit,
        "personal_credit_diagnostic": personal_credit,
        "enterprise_bank_flow_diagnostic": enterprise_flow,
        "financial_statement_diagnostic": financial_statement,
        "material_checklist": {
            "required_missing": required_missing,
            "optional_missing": optional_missing,
            "recommended_supplements": recommended_supplements,
        },
        "risk_highlights": risk_highlights,
        "financing_readiness": {
            "usable_for_financing": usable_for_financing,
            "readiness_level": readiness_level,
            "score": score,
            "summary": summary,
        },
        "next_actions": next_actions,
        "report_markdown": "",
    }
    report["comprehensive_financing_advice"] = build_comprehensive_financing_advice(report, kyc_profile)
    report["report_markdown"] = _build_report_markdown(report)
    return report
