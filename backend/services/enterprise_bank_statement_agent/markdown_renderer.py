from __future__ import annotations

from typing import Any


def _money(value: Any) -> str:
    if value is None or value == "":
        return "-"
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return str(value)


def render_enterprise_bank_statement_markdown(result: dict[str, Any]) -> str:
    basic = result.get("account_basic_info") or {}
    summary = result.get("statement_summary") or {}
    lines = ["# 企业银行流水解析结果", ""]
    lines += [
        "## 一、账户基础信息",
        f"- 客户名称：{basic.get('company_name') or '-'}",
        f"- 银行名称：{basic.get('bank_name') or '-'}",
        f"- 开户行：{basic.get('branch_name') or '-'}",
        f"- 账号：{basic.get('account_number') or '-'}",
        f"- 币种：{basic.get('currency') or '-'}",
        f"- 流水期间：{basic.get('statement_period_start') or '-'} 至 {basic.get('statement_period_end') or '-'}",
        f"- 期初/期末余额：{_money(basic.get('opening_balance'))} / {_money(basic.get('closing_balance'))}",
        "",
        "## 二、流水汇总",
        f"- 借方总金额：{_money(summary.get('total_debit_amount'))}，笔数：{summary.get('total_debit_count') if summary.get('total_debit_count') is not None else '-'}",
        f"- 贷方总金额：{_money(summary.get('total_credit_amount'))}，笔数：{summary.get('total_credit_count') if summary.get('total_credit_count') is not None else '-'}",
        f"- 总交易笔数：{summary.get('total_transaction_count') if summary.get('total_transaction_count') is not None else '-'}",
        f"- 日均余额：{_money(summary.get('average_daily_balance'))}",
        f"- 月均收入/支出：{_money(summary.get('monthly_average_credit'))} / {_money(summary.get('monthly_average_debit'))}",
        "",
        "## 三、月度趋势",
        "| 月份 | 收入 | 支出 | 净流入 | 笔数 | 月末余额 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in (result.get("monthly_trends") or [])[:12]:
        lines.append(
            f"| {item.get('month') or '-'} | {_money(item.get('credit_amount'))} | {_money(item.get('debit_amount'))} | {_money(item.get('net_inflow'))} | {item.get('transaction_count') or 0} | {_money(item.get('month_end_balance'))} |"
        )
    lines += ["", "## 四、主要对手方分析", "| 对手方 | 角色 | 收入 | 支出 | 笔数 | 集中度 |", "| --- | --- | ---: | ---: | ---: | ---: |"]
    for item in (result.get("counterparty_analysis") or [])[:10]:
        lines.append(f"| {item.get('counterparty_name') or '-'} | {item.get('role') or '-'} | {_money(item.get('credit_amount'))} | {_money(item.get('debit_amount'))} | {item.get('transaction_count') or 0} | {float(item.get('concentration') or 0):.2%} |")
    lines += ["", "## 五、大额交易"]
    for item in (result.get("large_transactions") or [])[:10]:
        tx = item.get("transaction") if isinstance(item.get("transaction"), dict) else {}
        lines.append(f"- {item.get('reason') or '-'}：{tx.get('transaction_date') or '-'} {tx.get('counterparty_name') or item.get('counterparty_name') or '-'} {_money(item.get('amount'))}")
    if not (result.get("large_transactions") or []):
        lines.append("- 暂未识别到大额交易")
    lines += ["", "## 六、融资相关交易"]
    for item in (result.get("loan_related_transactions") or [])[:10]:
        tx = item.get("transaction") or {}
        lines.append(f"- {tx.get('transaction_date') or '-'} {tx.get('summary') or '-'}，关键词：{', '.join(item.get('matched_keywords') or [])}")
    if not (result.get("loan_related_transactions") or []):
        lines.append("- 暂未识别到融资相关交易")
    lines += ["", "## 七、风险信号"]
    for item in result.get("risk_signals") or []:
        lines.append(f"- [{item.get('level') or 'info'}] {item.get('type') or '-'}：{item.get('detail') or '-'}")
    if not (result.get("risk_signals") or []):
        lines.append("- 暂未识别到明确风险信号")
    financing = result.get("financing_analysis") or {}
    lines += [
        "",
        "## 八、银行融资视角分析",
        f"- 现金流稳定性：{financing.get('cash_flow_stability') or '-'}",
        f"- 经营真实性：{financing.get('business_reality') or '-'}",
        f"- 还款能力：{financing.get('repayment_capacity') or '-'}",
        f"- 异常流水风险：{financing.get('abnormal_flow_risk') or '-'}",
        f"- 授信额度参考：{financing.get('suggested_credit_limit_reference') or '-'}",
        f"- 综合摘要：{financing.get('summary') or '-'}",
        "",
        "## 九、数据质量与提醒",
    ]
    for warning in result.get("warnings") or []:
        lines.append(f"- {warning}")
    if not (result.get("warnings") or []):
        lines.append("- 未发现明显数据质量提醒")
    lines += ["", "### 交易明细预览（前20条）", "| 日期 | 摘要 | 对手方 | 借方 | 贷方 | 余额 |", "| --- | --- | --- | ---: | ---: | ---: |"]
    for tx in (result.get("transactions") or [])[:20]:
        lines.append(f"| {tx.get('transaction_date') or '-'} | {tx.get('summary') or '-'} | {tx.get('counterparty_name') or '-'} | {_money(tx.get('debit_amount'))} | {_money(tx.get('credit_amount'))} | {_money(tx.get('balance'))} |")
    return "\n".join(lines).strip()
