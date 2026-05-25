from __future__ import annotations

from typing import Any


def _money(value: Any) -> str:
    try:
        return f"{float(value or 0):,.2f}"
    except Exception:
        return str(value or "-")


def _pct(value: Any) -> str:
    try:
        return f"{float(value or 0) * 100:.2f}%"
    except Exception:
        return "-"


def render_personal_bank_statement_markdown(data: dict[str, Any]) -> str:
    summary = data.get("deterministic_summary") or data.get("raw_summary") or {}
    income = data.get("income_verification") or {}
    expense = data.get("expense_analysis") or {}
    retention = data.get("cash_retention_analysis") or {}
    repayment = data.get("repayment_analysis") or {}
    fast = data.get("fast_in_fast_out_analysis") or {}
    nature = data.get("flow_nature") or {}
    judgement = data.get("financing_judgement") or {}
    period = data.get("statement_period") or {}
    lines = [
        "# 个人流水摘要",
        "",
        "## 基础信息",
        f"- 银行：{data.get('bank_name') or '-'}",
        f"- 户名：{data.get('account_name') or (data.get('owner') or {}).get('name') or '-'}",
        f"- 账号：{data.get('account_no') or '-'}",
        f"- 账户类型：{data.get('account_type') or '-'}",
        f"- 流水期间：{period.get('start_date') or '-'} 至 {period.get('end_date') or '-'}",
        f"- 币种：{data.get('currency') or '人民币'}",
        "",
        "## 总体流水",
        f"- 总收入：{_money(summary.get('total_income'))}",
        f"- 总支出：{_money(summary.get('total_expense'))}",
        f"- 净流入：{_money(summary.get('net_cash_flow'))}",
        f"- 收入笔数：{summary.get('income_count') or 0}",
        f"- 支出笔数：{summary.get('expense_count') or 0}",
        f"- 账户沉淀率：{_pct(retention.get('retention_ratio'))}",
        "",
        "## 收入采信分析",
        f"- 原始收入：{_money(income.get('raw_total_income'))}",
        f"- 可采信工资收入：{_money(income.get('verified_salary_income'))}",
        f"- 可采信经营收入：{_money(income.get('verified_operating_income'))}",
        f"- 来源不明汇入：{_money(income.get('unknown_inflow'))}",
        f"- 利息收入：{_money(income.get('interest_income'))}",
        f"- 可采信稳定收入：{_money(income.get('stable_income'))}",
        f"- 月均可采信收入：{_money(income.get('avg_monthly_verified_income'))}",
        "",
        "## 工资收入识别",
        f"- 明确工资收入：{_money(income.get('confirmed_salary_income') or income.get('verified_salary_income'))}",
        f"- 疑似工资收入：{_money(income.get('suspected_salary_income'))}",
        f"- 已人工确认工资收入：{_money(income.get('manual_confirmed_salary_income'))}",
        f"- 人工驳回工资收入：{_money(income.get('manual_rejected_salary_income'))}",
        f"- 可采信工资收入：{_money(income.get('verified_salary_income'))}",
        f"- 低置信疑似工资收入：{_money(income.get('low_confidence_suspected_salary_income') or income.get('suspected_salary_income_low_confidence'))}",
        f"- 工资收入笔数：{income.get('salary_income_count') or 0}",
        f"- 疑似工资笔数：{income.get('suspected_salary_count') or 0}",
        f"- 低置信疑似工资笔数：{income.get('suspected_salary_count_low_confidence') or 0}",
        f"- 工资覆盖月份：{income.get('salary_months') or 0}",
        f"- 月均明确工资：{_money(income.get('salary_avg_monthly_amount'))}",
        f"- 工资发放稳定性：{income.get('salary_continuity_level') or 'none'}",
        f"- 工资识别置信度：{_pct(income.get('salary_confidence'))}",
        "- 主要发薪单位：",
    ]
    salary_sources = income.get("salary_sources") or []
    if salary_sources:
        for source in salary_sources[:5]:
            manual_status = str(source.get("manual_status") or "")
            manual_label = {"confirmed": "，人工已确认", "rejected": "，人工已驳回", "pending": "，待人工复核"}.get(manual_status, "")
            lines.append(
                f"  - {source.get('counterparty_name') or '未知付款方'}："
                f"{_money(source.get('amount'))}，{source.get('count') or 0} 笔，"
                f"{source.get('salary_type') or '-'}{manual_label}"
            )
    else:
        lines.append("  - 暂无")
    notes = income.get("salary_detection_notes") or []
    lines += [
        "- 疑似工资待核实说明：",
    ]
    if notes:
        for note in notes:
            lines.append(f"  - {note}")
    else:
        lines.append("  - 暂无")
    confirmed_sources = [
        source for source in salary_sources
        if source.get("manual_status") in {"confirmed", "rejected"}
    ]
    if confirmed_sources:
        lines.append("- 人工确认记录：")
        for source in confirmed_sources[:5]:
            status = "确认采信为工资" if source.get("manual_status") == "confirmed" else "驳回为非工资"
            lines.append(
                f"  - {source.get('counterparty_name') or '未知付款方'}：{status}；"
                f"说明：{source.get('confirm_reason') or '-'}；"
                f"确认人：{source.get('confirmed_by') or '-'}；"
                f"确认时间：{source.get('confirmed_at') or '-'}"
            )
    lines += [
        "",
        "## 支出与还款分析",
        f"- 总支出：{_money(expense.get('raw_total_expense'))}",
        f"- 贷款还款支出：{_money(expense.get('loan_repayment_expense'))}",
        f"- 信用卡还款支出：{_money(expense.get('credit_card_repayment_expense'))}",
        f"- 线上贷款/小贷还款：{_money(expense.get('online_loan_repayment_expense'))}",
        f"- 快捷支付/消费支出：{_money(expense.get('quick_payment_expense'))}",
        f"- 投资证券转账支出：{_money(expense.get('investment_expense'))}",
        f"- 本人账户转出：{_money(expense.get('internal_transfer_expense'))}",
        f"- 个人往来转出：{_money(expense.get('related_party_transfer_expense'))}",
        f"- 现金取款：{_money(expense.get('cash_withdrawal'))}",
        f"- 月均贷款还款：{_money(expense.get('avg_monthly_loan_repayment'))}",
        f"- 贷款还款支出占比：{_pct(expense.get('loan_repayment_ratio'))}",
        "",
        "## 流水性质判断",
        f"- 主要类型：{nature.get('primary_type') or 'unknown'}",
        f"- 置信度：{_pct(nature.get('confidence'))}",
        f"- 是否工资流水：{'是' if nature.get('primary_type') == 'salary_flow' else '否'}",
        f"- 是否经营流水：{'是' if nature.get('primary_type') == 'operating_flow' else '否'}",
        f"- 是否还款账户流水：{'是' if nature.get('primary_type') == 'repayment_account_flow' or repayment.get('is_repayment_account_flow') else '否'}",
        "- 判断依据：",
    ]
    for reason in nature.get("reasons") or []:
        lines.append(f"  - {reason}")
    lines += [
        "",
        "## 快进快出分析",
        f"- 是否存在快进快出：{'是' if fast.get('has_fast_in_fast_out') else '否'}",
        f"- 匹配组数：{fast.get('matched_count') or 0}",
        f"- 匹配金额：{_money(fast.get('matched_amount'))}",
    ]
    for match in (fast.get("matches") or [])[:10]:
        lines.append(
            f"- {match.get('income_date') or '-'} 汇入 {_money(match.get('income_amount'))}，"
            f"{match.get('expense_date') or '-'} 支出/还贷 {_money(match.get('expense_amount'))}，"
            f"间隔 {match.get('days_between') or 0} 天，匹配度 {_pct(match.get('match_ratio'))}"
        )
    lines += [
        "",
        "## 风险提示",
    ]
    signals = data.get("risk_signals") or []
    if signals:
        for signal in signals:
            lines.append(f"- [{signal.get('level')}] {signal.get('code')}：{signal.get('message')}；依据：{signal.get('evidence') or '-'}")
    else:
        lines.append("- 暂未识别到明确风险信号")
    lines += [
        "",
        "## 融资判断",
        f"- 收入质量：{judgement.get('income_quality') or '无法判断'}",
        f"- 还款能力：{judgement.get('repayment_capacity') or '无法判断'}",
        f"- 疑似刷流水风险：{judgement.get('suspicious_flow_risk') or '无法判断'}",
        f"- 建议用途：{judgement.get('recommended_usage') or '仅供参考'}",
        f"- 综合结论：{judgement.get('final_summary') or '-'}",
        "- 建议补充材料：",
    ]
    missing = judgement.get("missing_materials") or []
    if missing:
        for item in missing:
            lines.append(f"  - {item}")
    else:
        lines.append("  - -")
    return "\n".join(lines)
