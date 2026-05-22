from __future__ import annotations

from typing import Any


def _money(value: Any) -> str:
    try:
        return f"{float(value or 0):,.2f}"
    except Exception:
        return str(value or "-")


def render_personal_bank_statement_markdown(data: dict[str, Any]) -> str:
    owner = data.get("owner") or {}
    summary = data.get("customer_level_summary") or {}
    accounts = data.get("accounts") or []
    first_account = accounts[0] if accounts else {}
    judgement = data.get("financing_judgement") or {}
    lines = [
        "# 个人流水摘要",
        "",
        "## 基础信息",
        f"- 户名：{owner.get('name') or first_account.get('account_name') or '-'}",
        f"- 银行：{first_account.get('bank_name') or '-'}",
        f"- 账号：{first_account.get('account_no') or '-'}",
        f"- 流水期间：{summary.get('period_start') or '-'} 至 {summary.get('period_end') or '-'}",
        "",
        "## 总体流水",
        f"- 原始收入：{_money(summary.get('raw_total_income'))}",
        f"- 原始支出：{_money(summary.get('raw_total_expense'))}",
        f"- 稳定收入：{_money(summary.get('stable_income'))}",
        f"- 月均稳定收入：{_money(summary.get('avg_monthly_stable_income'))}",
        f"- 净经营现金流：{_money(summary.get('net_operating_cash_flow'))}",
        "",
        "## 收入分析",
        f"- 工资收入：{_money(summary.get('salary_income'))}",
        f"- 经营收入：{_money(summary.get('operating_income'))}",
        f"- 其他稳定收入：{_money(sum(float((a.get('clean_summary') or {}).get('other_stable_income') or 0) for a in accounts if isinstance(a, dict)))}",
        "- 主要收入来源：",
    ]
    for item in first_account.get("top_income_counterparties") or []:
        lines.append(f"  - {item.get('name') or '-'}：{_money(item.get('amount'))}（{item.get('count') or 0}笔）")
    income_analysis = data.get("income_analysis") or {}
    lines += [
        f"- 收入连续性：连续 {income_analysis.get('income_continuous_months') or 0} 个月识别到稳定收入",
        "",
        "## 支出分析",
    ]
    clean = first_account.get("clean_summary") or {}
    lines += [
        f"- 生活支出：{_money(clean.get('living_expense'))}",
        f"- 经营支出：{_money(clean.get('operating_expense'))}",
        f"- 贷款还款：{_money(clean.get('loan_repayment_expense'))}",
        f"- 信用卡还款：{_money(clean.get('credit_card_repayment_expense'))}",
        "- 主要支出去向：",
    ]
    for item in first_account.get("top_expense_counterparties") or []:
        lines.append(f"  - {item.get('name') or '-'}：{_money(item.get('amount'))}（{item.get('count') or 0}笔）")
    lines += [
        "",
        "## 净化说明",
        f"- 本人账户互转收入剔除：{_money(summary.get('internal_transfer_income'))}",
        f"- 贷款流入剔除：{_money(summary.get('loan_inflow'))}",
        f"- 理财/退款/非收入项按交易分类剔除或单列：{_money(sum(float((a.get('clean_summary') or {}).get('investment_transfer_income') or 0) + float((a.get('clean_summary') or {}).get('refund_income') or 0) + float((a.get('clean_summary') or {}).get('non_operating_income') or 0) for a in accounts if isinstance(a, dict)))}",
        f"- 剔除后可采信收入：{_money(summary.get('stable_income'))}",
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
        f"- 建议补充材料：{', '.join(judgement.get('missing_materials') or []) or '-'}",
    ]
    return "\n".join(lines)
