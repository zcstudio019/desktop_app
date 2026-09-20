"""Deterministic Markdown renderer for the comprehensive financing report."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from backend.services.comprehensive_financing_analysis_service import (
    ComprehensiveFinancingAnalysisResult,
)
from backend.services.comprehensive_financing_report_model import (
    ComprehensiveFinancingReportModel,
)


STATUS_LABELS = {
    "confirmed": "已确认",
    "available": "已获取",
    "partial": "部分资料",
    "missing": "资料不足",
    "needs_review": "待核验",
}

PATH_STATUS_LABELS = {
    "potential": "可进一步评估",
    "conditional": "有条件",
    "insufficient_data": "资料不足",
}

SOURCE_LABELS = {
    "subject_profile": "主体资料",
    "financing_requirement": "融资需求",
    "enterprise_credit": "企业征信",
    "personal_credit": "个人征信",
    "enterprise_cashflow": "企业流水",
    "personal_cashflow": "个人流水",
    "financials": "财务报表",
    "assets": "资产资料",
    "risk_context": "风险评估",
    "existing_financing_plan": "已有融资方案",
    "derived_metrics": "程序计算指标",
    "conflicts": "资料冲突核验",
    "data_scope": "数据范围",
    "data_quality": "资料完整度",
    "source_dates": "资料日期",
}

MATERIAL_LABELS = {
    "enterprise_kyc": "KYC主体资料",
    "enterprise_credit": "企业征信",
    "personal_credit": "个人征信",
    "enterprise_cashflow": "企业流水",
    "personal_cashflow": "个人流水",
    "financial_statements": "财务报表",
    "assets": "资产资料",
    "financing_requirement": "融资需求",
    "risk_assessment": "风险评估",
    "financing_plan": "已有融资方案",
    "financial_cashflow_period_mismatch": "财务与流水期间",
    "enterprise_cashflow_classification": "企业流水分类",
    "source_date_comparability": "资料时点可比性",
}

ASSET_LABELS = {
    "property": "房产",
    "vehicle": "车辆",
    "equipment": "设备",
    "intellectual_property": "知识产权",
    "equity": "股权",
    "deposit": "存单",
    "other_collateral": "其他抵质押物",
}


def _clean(value: Any) -> str:
    if value is None or value == "" or value == [] or value == {}:
        return "资料不足"
    text = str(value).strip().replace("|", "\\|")
    return "；".join(part.strip() for part in text.splitlines() if part.strip()) or "资料不足"


def _join(values: Iterable[Any], empty: str = "资料不足") -> str:
    items = [_clean(value) for value in values if value not in (None, "", [], {})]
    return "、".join(items) if items else empty


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def format_money(value: Any, unit: Any) -> str:
    if isinstance(value, dict):
        unit = value.get("unit") or unit
        value = value.get("value")
    number = _number(value)
    if number is None:
        return "资料不足"
    normalized_unit = str(unit or "").strip()
    if not normalized_unit:
        return f"{number:,.2f}（单位待核验）"
    if normalized_unit == "元":
        amount = f"{number:,.2f}"
    else:
        amount = f"{number:,.2f}".rstrip("0").rstrip(".")
    return f"{amount}{_clean(normalized_unit)}"


def format_ratio(value: Any) -> str:
    number = _number(value)
    return "资料不足" if number is None else f"{number * 100:,.2f}%"


def render_markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    rendered_rows = [[_clean(cell) for cell in row] for row in rows]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rendered_rows)
    return "\n".join(lines)


def _material_period(material_type: str, material: dict[str, Any], model: ComprehensiveFinancingReportModel) -> str:
    if material_type == "enterprise_cashflow":
        period = model.enterprise_cashflow.get("statement_period") or {}
        start, end = period.get("start"), period.get("end")
        if start or end:
            return f"{_clean(start)} 至 {_clean(end)}"
    if material_type == "personal_cashflow":
        period = model.personal_cashflow.get("statement_period") or {}
        start, end = period.get("start"), period.get("end")
        if start or end:
            return f"{_clean(start)} 至 {_clean(end)}"
    if material_type == "financial_statements" and model.financials.get("latest_period"):
        return f"最新 {_clean(model.financials.get('latest_period'))}"
    return _clean(material.get("latest_date"))


def _source_text(source_sections: list[str]) -> str:
    return _join((SOURCE_LABELS.get(source, "资料来源待核验") for source in source_sections))


def _bullets(items: list[Any], empty: str = "资料不足") -> str:
    if not items:
        return f"- {empty}"
    return "\n".join(f"- {_clean(item)}" for item in items)


def _analysis_text(summary: str, sources: list[str]) -> str:
    return f"{_clean(summary)}\n\n数据来源：{_source_text(sources)}"


def _render_data_scope(model: ComprehensiveFinancingReportModel) -> str:
    rows = []
    for material in model.data_scope.get("materials", []):
        material_type = str(material.get("type") or "")
        rows.append([
            MATERIAL_LABELS.get(material_type, "其他资料"),
            STATUS_LABELS.get(material.get("status"), "待核验"),
            _material_period(material_type, material, model),
        ])
    return render_markdown_table(["资料类型", "状态", "数据期间/日期"], rows)


def _render_subject(model: ComprehensiveFinancingReportModel) -> str:
    subject = model.subject_profile
    shareholders = [row.get("name") for row in subject.get("shareholders", []) if isinstance(row, dict)]
    rows = [
        ["企业名称", subject.get("enterprise_name")],
        ["统一社会信用代码", subject.get("unified_social_credit_code")],
        ["法定代表人", subject.get("legal_representative")],
        ["实际控制人", subject.get("actual_controller")],
        ["股东", _join(shareholders)],
        ["注册资本", subject.get("registered_capital")],
        ["成立时间", subject.get("established_date")],
        ["企业类型", subject.get("enterprise_type")],
        ["注册地址", subject.get("registered_address")],
        ["经营地址", subject.get("operating_address")],
        ["主营/经营范围", subject.get("business_scope")],
    ]
    return render_markdown_table(["项目", "内容"], rows)


def _render_requirement(model: ComprehensiveFinancingReportModel) -> str:
    requirement = model.financing_requirement
    if requirement.get("status") == "missing":
        return "> 当前尚未确认明确融资金额、用途、期限及担保偏好，本报告暂不对具体融资金额、期限或产品作确定性建议。"
    amount = format_money(requirement.get("amount"), requirement.get("amount_unit"))
    rows = [
        ["融资主体", requirement.get("financing_subject")], ["融资金额", amount],
        ["币种", requirement.get("currency")], ["融资用途", requirement.get("purpose")],
        ["融资期限", requirement.get("term")], ["预计用款时间", requirement.get("expected_use_date")],
        ["还款偏好", requirement.get("repayment_preference")], ["担保偏好", requirement.get("guarantee_preference")],
        ["抵押物", requirement.get("collateral")], ["现有银行", _join(requirement.get("existing_banks") or [])],
        ["偏好银行", _join(requirement.get("preferred_banks") or [])], ["排除银行", _join(requirement.get("excluded_banks") or [])],
    ]
    return render_markdown_table(["项目", "当前情况"], rows)


def _render_core_metrics(model: ComprehensiveFinancingReportModel) -> str:
    derived = model.derived_metrics
    cashflow = model.enterprise_cashflow
    financial = model.financials.get("latest") or {}
    enterprise_credit = model.enterprise_credit
    cashflow_unit = cashflow.get("unit")
    financial_unit = financial.get("unit")
    rows = [
        ["企业征信融资余额", format_money(derived.get("total_enterprise_credit_balance"), enterprise_credit.get("unit"))],
        ["个人征信贷款余额", format_money(derived.get("total_personal_credit_balance_money") or derived.get("total_personal_credit_balance"), model.personal_credit.get("unit"))],
        ["法人相关还款责任余额", format_money(derived.get("total_related_repayment_balance_money") or derived.get("total_related_repayment_balance"), model.personal_credit.get("unit"))],
        ["企业流水总流入", format_money(derived.get("enterprise_total_inflow"), cashflow_unit)],
        ["初步经营入账", format_money(derived.get("enterprise_operating_inflow"), cashflow_unit)],
        ["月均经营入账", format_money(derived.get("monthly_average_operating_inflow"), cashflow_unit)],
        ["最新总资产", format_money(financial.get("total_assets"), financial_unit)],
        ["最新总负债", format_money(financial.get("total_liabilities"), financial_unit)],
        ["最新净资产", format_money(financial.get("net_assets"), financial_unit)],
        ["资产负债率", format_ratio(derived.get("debt_asset_ratio"))],
        ["最新净利润", format_money(financial.get("net_profit"), financial_unit)],
        ["最新经营现金流", format_money(financial.get("operating_cashflow"), financial_unit)],
    ]
    return render_markdown_table(["指标", "当前情况"], rows)


def _render_cashflow(model: ComprehensiveFinancingReportModel) -> str:
    flow = model.enterprise_cashflow
    unit = flow.get("unit")
    period = flow.get("statement_period") or {}
    rows = [
        ["流水覆盖期间", f"{_clean(period.get('start'))} 至 {_clean(period.get('end'))}"],
        ["账户数", flow.get("account_count")],
        ["总流入", format_money(flow.get("total_inflow"), unit)],
        ["初步经营入账", format_money(flow.get("operating_inflow"), unit)],
        ["内部互转流入", format_money(flow.get("internal_transfer_inflow"), unit)],
        ["关联方流入", format_money(flow.get("related_party_inflow"), unit)],
        ["其他非经营/未识别流入", format_money(flow.get("non_operating_inflow"), unit)],
        ["月均经营入账", format_money(flow.get("monthly_average_operating_inflow"), unit)],
    ]
    note = ""
    if flow.get("status") == "partial":
        note = "\n\n> 当前企业流水可用于初步分析，但关联方分类等口径仍需进一步核验，因此“经营入账”属于当前已保存分类下的初步统计，不代表最终核定真实经营收入。"
    return render_markdown_table(["指标", "当前情况"], rows) + note


def _financial_rows(period: dict[str, Any]) -> list[list[Any]]:
    unit = period.get("unit")
    return [
        ["财务期间", period.get("period")], ["报表口径", "月度" if period.get("period_type") == "monthly" else period.get("period_type")],
        ["营业收入", format_money(period.get("revenue"), unit)], ["营业成本", format_money(period.get("operating_cost"), unit)],
        ["净利润", format_money(period.get("net_profit"), unit)], ["总资产", format_money(period.get("total_assets"), unit)],
        ["总负债", format_money(period.get("total_liabilities"), unit)], ["净资产", format_money(period.get("net_assets"), unit)],
        ["应收账款", format_money(period.get("accounts_receivable"), unit)], ["存货", format_money(period.get("inventory"), unit)],
        ["短期借款", format_money(period.get("short_term_borrowings"), unit)], ["长期借款", format_money(period.get("long_term_borrowings"), unit)],
        ["经营现金流", format_money(period.get("operating_cashflow"), unit)], ["资产负债率", format_ratio(period.get("debt_asset_ratio"))],
    ]


def _render_financial_trend(model: ComprehensiveFinancingReportModel) -> str:
    periods = model.financials.get("periods") or []
    if not periods:
        return "资料不足"
    headers = ["指标"] + [_clean(period.get("period")) for period in periods]
    metric_rows = []
    for label, key in (("营业收入", "revenue"), ("净利润", "net_profit"), ("总资产", "total_assets"),
                       ("总负债", "total_liabilities"), ("净资产", "net_assets")):
        metric_rows.append([label] + [format_money(period.get(key), period.get("unit")) for period in periods])
    note = "\n\n> 各期数据按原报表口径列示；月度与年度期间不直接作同比判断。"
    return render_markdown_table(headers, metric_rows) + note


def _render_enterprise_credit(model: ComprehensiveFinancingReportModel) -> str:
    credit = model.enterprise_credit
    rows = [
        ["未结清融资余额", format_money(credit.get("outstanding_loan_balance"), credit.get("unit"))],
        ["融资机构数", credit.get("outstanding_loan_institution_count")],
        ["逾期记录数", (credit.get("overdue_summary") or {}).get("count")],
        ["不良/异常分类记录数", (credit.get("nonperforming_summary") or {}).get("count")],
        ["企业对外担保余额", format_money(credit.get("guarantee_balance"), credit.get("unit"))],
        ["到期/待核验记录数", len(credit.get("upcoming_or_past_due_records") or [])],
        ["征信查询次数", (credit.get("query_summary") or {}).get("count")],
        ["征信报告日期", credit.get("source_report_date")],
    ]
    return render_markdown_table(["项目", "当前情况"], rows)


def _render_personal_credit(model: ComprehensiveFinancingReportModel) -> str:
    rows = []
    unit = model.personal_credit.get("unit")
    for person in model.personal_credit.get("people") or []:
        overdue = person.get("overdue_summary") or {}
        overdue_text = (f"贷款 {_clean(overdue.get('loan_overdue_account_count'))}；"
                        f"信用卡 {_clean(overdue.get('credit_card_overdue_account_count'))}；"
                        f"90天以上 {_clean(overdue.get('overdue_90d_account_count'))}")
        card = (f"额度 {format_money(person.get('credit_card_limit_money') or person.get('credit_card_limit'), unit)}；"
                f"已用 {format_money(person.get('credit_card_used_money') or person.get('credit_card_used'), unit)}")
        rows.append([
            person.get("name"), _join(person.get("roles") or []), format_money(person.get("loan_balance_money") or person.get("loan_balance"), unit),
            card, overdue_text,
            format_money(person.get("related_repayment_balance_money") or person.get("related_repayment_balance"), unit), person.get("source_report_date"),
        ])
    if not rows:
        rows.append(["资料不足"] * 7)
    summary = render_markdown_table(
        ["姓名", "角色", "贷款余额", "信用卡", "逾期账户", "相关还款责任", "报告日期"], rows
    )
    query_rows = []
    for person in model.personal_credit.get("people") or []:
        for window in (person.get("query_summary") or {}).get("windows") or []:
            query_rows.append([
                person.get("name"), window.get("window"), window.get("loan_approval"),
                window.get("credit_card_approval"), window.get("guarantee_review"), window.get("legal_person_review"),
            ])
    if not query_rows:
        return summary + "\n\n**征信查询窗口：** 资料不足"
    return summary + "\n\n**征信查询窗口：**\n\n" + render_markdown_table(
        ["姓名", "查询窗口", "贷款审批", "信用卡审批", "担保资格审查", "法人资信审查"], query_rows
    )


def _render_assets(model: ComprehensiveFinancingReportModel) -> str:
    if model.assets.get("status") == "missing":
        return "> 当前资料中未找到可用于本次分析的稳定结构化资产资料，因此暂无法判断房产、车辆、设备或其他资产对融资增信的支持程度。"
    rows = []
    for kind, label in ASSET_LABELS.items():
        for asset in model.assets.get(kind) or []:
            rows.append([label, asset.get("name"), format_money(asset.get("market_value"), asset.get("unit")), asset.get("value_basis")])
    return render_markdown_table(["资产类型", "资产名称", "参考价值", "价值依据"], rows or [["资料不足"] * 4])


def _render_strengths(analysis: ComprehensiveFinancingAnalysisResult) -> str:
    if not analysis.financing_strengths:
        return "当前结构化分析未形成可展示的融资优势。"
    sections = []
    for item in analysis.financing_strengths:
        sections.append(
            f"### {item.title}\n\n- 事实依据：{_clean(item.fact)}\n- 对融资评估的意义：{_clean(item.impact)}\n- 数据来源：{_source_text(item.source_sections)}"
        )
    return "\n\n".join(sections)


def _render_constraints(analysis: ComprehensiveFinancingAnalysisResult) -> str:
    if not analysis.financing_constraints:
        return "当前结构化分析未形成可展示的融资障碍。"
    sections = []
    for item in analysis.financing_constraints:
        sections.append(
            f"### {item.title}\n\n- 当前事实：{_clean(item.fact)}\n- 对融资的影响：{_clean(item.impact)}\n- 建议动作：{_clean(item.required_action)}\n- 数据来源：{_source_text(item.source_sections)}"
        )
    return "\n\n".join(sections)


def _render_issues(analysis: ComprehensiveFinancingAnalysisResult) -> str:
    if not analysis.core_issues:
        return "当前结构化分析未形成可展示的核心问题。"
    sections = []
    for index, item in enumerate(analysis.core_issues[:5], 1):
        sections.append(
            f"### {index}. {item.issue}\n\n**事实：**\n\n{_bullets(item.facts)}\n\n"
            f"**融资影响：**\n\n{_clean(item.financing_impact)}\n\n"
            f"**下一步：**\n\n{_clean(item.next_action)}\n\n数据来源：{_source_text(item.source_sections)}"
        )
    return "\n\n".join(sections)


def _render_paths(analysis: ComprehensiveFinancingAnalysisResult) -> str:
    if not analysis.financing_paths:
        return "本次分析未生成可展示的融资路径，建议先补齐关键资料后再评估。"
    rows = [[item.path, PATH_STATUS_LABELS.get(item.status, "待核验"), _join(item.basis),
             _join(item.missing_conditions), _source_text(item.source_sections)] for item in analysis.financing_paths]
    return render_markdown_table(["融资路径", "当前状态", "依据", "当前缺口", "数据来源"], rows)


def _render_actions(analysis: ComprehensiveFinancingAnalysisResult) -> str:
    groups = (
        ("### 🔴 立即处理", analysis.action_plan.immediate),
        ("### 🟡 短期处理", analysis.action_plan.short_term),
        ("### 🟢 中期优化", analysis.action_plan.medium_term),
    )
    blocks = []
    for title, actions in groups:
        if not actions:
            continue
        lines = [f"{index}. {_clean(item.action)}（依据：{_clean(item.basis)}；数据来源：{_source_text(item.source_sections)}）"
                 for index, item in enumerate(actions, 1)]
        blocks.append(title + "\n\n" + "\n".join(lines))
    return "\n\n".join(blocks) if blocks else "当前结构化分析未生成行动事项。"


def _render_limitations(analysis: ComprehensiveFinancingAnalysisResult) -> str:
    rows = [[MATERIAL_LABELS.get(item.material_type, "其他资料"), item.limitation, item.impact, item.required_data]
            for item in analysis.data_limitations]
    return render_markdown_table(["资料类型", "当前限制", "分析影响", "需要补充/核验"], rows) if rows else "本次结构化分析未列出额外资料限制。"


def render_comprehensive_financing_report(
    report_model: ComprehensiveFinancingReportModel,
    analysis_result: ComprehensiveFinancingAnalysisResult,
    generated_at: str | datetime,
) -> str:
    """Render one complete report without calculation, inference, storage, or LLM calls."""
    generated_text = generated_at.isoformat(sep=" ", timespec="seconds") if isinstance(generated_at, datetime) else _clean(generated_at)
    customer_name = report_model.subject_profile.get("enterprise_name") or report_model.customer.get("name")
    latest = report_model.financials.get("latest") or {}
    fallback_notice = ""
    if analysis_result.validation_fallback_used:
        fallback_notice = "\n\n> 本次部分综合判断采用保守模式生成，建议结合补充资料进一步核验。"
    summary = analysis_result.executive_summary
    readiness_labels = {
        "ready_for_further_evaluation": "可进入下一步评估",
        "conditionally_ready": "具备一定基础但存在前置条件",
        "needs_data_completion": "需补充资料",
        "needs_issue_resolution": "需先解决关键问题",
    }
    return f"""# 客户综合融资分析报告

企业客户：{_clean(customer_name)}  
报告生成时间：{generated_text}
{fallback_notice}

## 数据范围

{_render_data_scope(report_model)}

## 综合摘要

**综合观察：** {_clean(summary.overall_observation)}

**当前融资准备状态：** {readiness_labels.get(summary.current_financing_readiness, '待核验')}

**主要优势：**

{_bullets(summary.main_strengths)}

**主要限制：**

{_bullets(summary.main_constraints)}

**关键缺失资料：**

{_bullets(summary.key_missing_information)}

## 一、客户融资画像

### 1. 主体基本信息

{_render_subject(report_model)}

### 2. 当前融资需求

{_render_requirement(report_model)}

### 3. 核心融资指标

{_render_core_metrics(report_model)}

## 二、企业经营与流水分析

### 1. 企业流水概览

{_render_cashflow(report_model)}

### 2. 经营与流水分析

**经营分析：**

{_analysis_text(analysis_result.business_analysis.summary, analysis_result.business_analysis.source_sections)}

**流水分析：**

{_analysis_text(analysis_result.cashflow_analysis.summary, analysis_result.cashflow_analysis.source_sections)}

## 三、财务分析

### 1. 核心财务指标

{render_markdown_table(['指标', '当前值'], _financial_rows(latest))}

### 2. 财务趋势

{_render_financial_trend(report_model)}

### 3. 财务分析结论

{_analysis_text(analysis_result.financial_analysis.summary, analysis_result.financial_analysis.source_sections)}

## 四、征信与负债分析

### 1. 企业征信摘要

{_render_enterprise_credit(report_model)}

### 2. 法人 / 实际控制人征信摘要

{_render_personal_credit(report_model)}

### 3. 企业与个人负债联动

{_analysis_text(analysis_result.enterprise_person_linkage.summary, analysis_result.enterprise_person_linkage.source_sections)}

**征信综合分析：**

{_analysis_text(analysis_result.credit_analysis.summary, analysis_result.credit_analysis.source_sections)}

## 五、资产与增信条件

{_render_assets(report_model)}

**分析结论：**

{_analysis_text(analysis_result.asset_and_enhancement_analysis.summary, analysis_result.asset_and_enhancement_analysis.source_sections)}

## 六、融资优势

{_render_strengths(analysis_result)}

## 七、融资障碍

{_render_constraints(analysis_result)}

## 八、当前核心问题

{_render_issues(analysis_result)}

## 九、融资路径方向

{_render_paths(analysis_result)}

## 十、行动计划

{_render_actions(analysis_result)}

## 十一、资料缺口与分析限制

{_render_limitations(analysis_result)}

## 十二、综合结论

### 综合判断

{_clean(analysis_result.conclusion.overall)}

### 当前融资方向

{_clean(analysis_result.conclusion.financing_direction)}

### 前置条件

{_bullets(analysis_result.conclusion.prerequisites)}

### 一句话结论

> {_clean(analysis_result.conclusion.one_sentence)}
""".strip()
