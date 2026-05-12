from __future__ import annotations

from typing import Any


def _value(value: Any) -> str:
    if value in (None, ""):
        return "未识别"
    return str(value)


def _count(value: Any) -> str:
    return "未识别" if value is None or value == "" else str(value)


def _rate(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "未识别"
    return f"{value * 100:.2f}%"


def _yes_no(value: Any) -> str:
    return "是" if bool(value) else "否"


def _record_line(record: dict[str, Any], fields: tuple[tuple[str, str], ...]) -> str:
    parts = [f"{label}: {_value(record.get(key))}" for key, label in fields]
    return "- " + "；".join(parts)


def render_personal_credit_markdown(report: dict[str, Any]) -> str:
    basic = report.get("basic_info") or {}
    summary = report.get("credit_summary") or {}
    loans = report.get("loan_accounts") or []
    cards = report.get("credit_card_accounts") or []
    guarantees = report.get("guarantees") or []
    overdue = report.get("overdue_records") or []
    public_records = report.get("public_records") or []
    queries = report.get("query_records") or []
    indicators = report.get("personal_credit_indicators") or {}
    risk_flags = report.get("risk_flags") or []
    warnings = report.get("warnings") or []
    missing = report.get("missing_fields") or []

    lines: list[str] = [
        "## 个人征信摘要",
        "",
        "### 报告基础信息",
        f"- 姓名: {_value(basic.get('name'))}",
        f"- 证件类型: {_value(basic.get('id_type'))}",
        f"- 证件号码: {_value(basic.get('id_number'))}",
        f"- 报告编号: {_value(basic.get('report_number'))}",
        f"- 报告时间: {_value(basic.get('report_time'))}",
        f"- 婚姻状况: {_value(basic.get('marital_status'))}",
        "",
        "### 信贷记录概要",
        f"- 信用卡账户数: {_count(summary.get('credit_card_account_count'))}",
        f"- 信用卡当前有效账户数: {_count(summary.get('credit_card_active_count'))}",
        f"- 购房贷款账户数: {_count(summary.get('housing_loan_account_count'))}",
        f"- 其他贷款账户数: {_count(summary.get('other_loan_account_count'))}",
        f"- 担保笔数: {_count(summary.get('guarantee_count'))}",
        "",
        "### 贷款账户明细",
    ]
    if loans:
        for item in loans:
            lines.append(_record_line(item, (("institution", "机构"), ("business_type", "业务类型"), ("amount", "发放金额"), ("balance", "余额"), ("account_status", "状态"), ("five_category", "五级分类"))))
    else:
        lines.append("- 暂未识别贷款账户明细")

    lines.extend(["", "### 信用卡账户明细"])
    if cards:
        for item in cards:
            lines.append(_record_line(item, (("institution", "发卡机构"), ("card_type", "卡类型"), ("credit_limit", "授信额度"), ("used_limit", "已用额度"), ("account_status", "状态"))))
    else:
        lines.append("- 暂未识别信用卡账户明细")

    lines.extend(["", "### 担保信息"])
    if guarantees:
        for item in guarantees:
            lines.append(_record_line(item, (("guarantee_for", "被担保人"), ("guarantee_amount", "担保金额"), ("guarantee_balance", "担保余额"), ("guarantee_status", "状态"))))
    else:
        lines.append("- 暂未识别担保信息")

    lines.extend(["", "### 逾期/异常记录"])
    if overdue:
        for item in overdue:
            lines.append(_record_line(item, (("record_type", "类型"), ("institution", "机构"), ("amount", "金额"), ("months", "月数"), ("status", "状态"))))
    else:
        lines.append("- 暂未识别逾期或异常记录")

    lines.extend(["", "### 公共记录"])
    if public_records:
        for item in public_records:
            lines.append(_record_line(item, (("record_type", "类型"), ("record_date", "日期"), ("authority", "机构"), ("amount", "金额"))))
    else:
        lines.append("- 暂未识别公共记录")

    lines.extend(["", "### 查询记录"])
    if queries:
        for item in queries:
            lines.append(_record_line(item, (("query_date", "日期"), ("query_institution", "查询机构"), ("query_reason", "原因"), ("query_type", "类型"))))
    else:
        lines.append("- 暂未识别查询记录")

    lines.extend([
        "",
        "### 风险提示",
        f"- 当前逾期: {_yes_no(indicators.get('has_current_overdue'))}",
        f"- 90天以上逾期: {_yes_no(indicators.get('has_90d_overdue'))}",
        f"- 呆账/代偿/核销/强制执行: {_yes_no(indicators.get('has_bad_debt_or_compensation'))}",
        f"- 近1个月贷款审批查询次数: {_count(indicators.get('loan_approval_queries_1m'))}",
        f"- 近3个月贷款审批查询次数: {_count(indicators.get('loan_approval_queries_3m'))}",
        f"- 近6个月贷款审批查询次数: {_count(indicators.get('loan_approval_queries_6m'))}",
        f"- 信用卡使用率: {_rate(indicators.get('credit_card_usage_rate'))}",
        f"- 综合风险等级: {_value(indicators.get('risk_level'))}",
    ])
    risk_items = [*risk_flags, *warnings, *(indicators.get("risk_reasons") or [])]
    if missing:
        risk_items.append("缺失字段: " + ", ".join(missing))
    if risk_items:
        lines.extend(f"- {item}" for item in dict.fromkeys(str(item) for item in risk_items if item))
    else:
        lines.append("- 暂未发现明确风险提示")
    return "\n".join(lines).strip()
