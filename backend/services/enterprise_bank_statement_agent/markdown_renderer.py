from __future__ import annotations

from typing import Any


def _money(value: Any) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return str(value)


def render_enterprise_bank_statement_markdown(data: dict[str, Any]) -> str:
    period = data.get("statement_period") or {}
    summary = data.get("summary") or {}
    counterparty = data.get("counterparty_summary") or {}
    risk = data.get("risk_analysis") or {}
    financing = data.get("financing_view") or {}
    lines = [
        "# 企业流水分析报告",
        "",
        "## 一、基础信息",
        f"- 客户名称：{data.get('company_name') or '-'}",
        f"- 流水期间：{period.get('start_date') or '-'} 至 {period.get('end_date') or '-'}",
        f"- 银行账户数量：{summary.get('account_count') or 0}",
        f"- 交易笔数：{summary.get('transaction_count') or 0}",
        f"- 资料来源文件：{data.get('source_file') or '-'}",
        "",
        "## 二、总体流水汇总",
        "| 指标 | 金额/数值 |",
        "| --- | ---: |",
        f"| 总收入 | {_money(summary.get('total_inflow'))} |",
        f"| 总支出 | {_money(summary.get('total_outflow'))} |",
        f"| 净流入 | {_money(summary.get('net_cashflow'))} |",
        f"| 月均收入 | {_money(summary.get('average_monthly_inflow'))} |",
        f"| 月均支出 | {_money(summary.get('average_monthly_outflow'))} |",
        f"| 月均净流入 | {_money(summary.get('average_monthly_net_cashflow'))} |",
        f"| 银行可能认可经营性回款估算 | {_money(financing.get('bank_recognizable_inflow'))} |",
        "",
        "## 二点五、经营性流水净化",
        f"- 原始收入：{_money(summary.get('raw_total_inflow') if summary.get('raw_total_inflow') is not None else summary.get('total_inflow'))}",
        f"- 原始支出：{_money(summary.get('raw_total_outflow') if summary.get('raw_total_outflow') is not None else summary.get('total_outflow'))}",
        f"- 内部转账收入剔除：{_money(summary.get('internal_transfer_inflow'))}",
        f"- 内部转账支出剔除：{_money(summary.get('internal_transfer_outflow'))}",
        f"- 关联方收入剔除/列示：{_money(summary.get('related_party_inflow') if summary.get('related_party_inflow') is not None else summary.get('excluded_related_party_inflow'))}",
        f"- 个人往来收入剔除/列示：{_money(summary.get('personal_transfer_inflow') if summary.get('personal_transfer_inflow') is not None else summary.get('excluded_personal_inflow'))}",
        f"- 银行可能认可经营性回款：{_money(summary.get('operating_inflow') if summary.get('operating_inflow') is not None else financing.get('bank_recognizable_inflow'))}",
        f"- 经营性净流入：{_money(summary.get('operating_net_cashflow') if summary.get('operating_net_cashflow') is not None else summary.get('estimated_operating_net_cashflow'))}",
        "",
        "## 三、各银行账户汇总",
        "| 银行 | 户名 | 账号 | 收入 | 支出 | 净流入 | 笔数 | 期末余额 |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in data.get("accounts") or []:
        lines.append(f"| {item.get('bank_name') or '-'} | {item.get('account_name') or '-'} | {item.get('account_number') or '-'} | {_money(item.get('total_inflow'))} | {_money(item.get('total_outflow'))} | {_money(item.get('net_cashflow'))} | {item.get('transaction_count') or 0} | {_money(item.get('ending_balance'))} |")
    lines += ["", "## 四、月度趋势分析", "| 月份 | 收入 | 支出 | 净流入 | 收入笔数 | 支出笔数 | 月末余额 |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for item in data.get("monthly_summary") or []:
        lines.append(f"| {item.get('month')} | {_money(item.get('inflow'))} | {_money(item.get('outflow'))} | {_money(item.get('net_cashflow'))} | {item.get('inflow_count') or 0} | {item.get('outflow_count') or 0} | {_money(item.get('ending_balance'))} |")
    lines += ["", "## 五、主要收入来源", "| 对手方 | 收入 | 笔数 | 分类 | 风险提示 |", "| --- | ---: | ---: | --- | --- |"]
    for item in counterparty.get("top_inflow_counterparties") or []:
        lines.append(f"| {item.get('name')} | {_money(item.get('inflow'))} | {item.get('transaction_count') or 0} | {item.get('category_guess') or '-'} | {item.get('risk_note') or '-'} |")
    lines += ["", "## 六、主要支出对象", "| 对手方 | 支出 | 笔数 | 分类 | 风险提示 |", "| --- | ---: | ---: | --- | --- |"]
    for item in counterparty.get("top_outflow_counterparties") or []:
        lines.append(f"| {item.get('name')} | {_money(item.get('outflow'))} | {item.get('transaction_count') or 0} | {item.get('category_guess') or '-'} | {item.get('risk_note') or '-'} |")
    lines += [
        "",
        "## 六点五、内部往来/左手倒右手明细",
        "| 对手方 | 账号 | 银行 | 收入 | 支出 | 笔数 | 剔除原因 |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    internal_items = counterparty.get("internal_transfer_counterparties") or []
    if internal_items:
        for item in internal_items[:20]:
            lines.append(
                f"| {item.get('name') or '-'} | {item.get('account') or item.get('counterparty_account') or '-'} | {item.get('bank') or '-'} | {_money(item.get('inflow'))} | {_money(item.get('outflow'))} | {item.get('transaction_count') or item.get('count') or 0} | {item.get('risk_note') or '本方/关联主体互转，经营流水口径剔除'} |"
            )
    else:
        lines.append("| 未识别到内部转账规则命中 | - | - | - | - | - | 请检查关联公司名单是否完整 |")
    lines += [
        "",
        "## 六点六、主要真实经营对手方",
        "| 对手方 | 类型 | 收入 | 支出 | 笔数 | 是否计入经营流水 |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for item in (counterparty.get("top_inflow_counterparties") or [])[:10]:
        lines.append(f"| {item.get('name') or '-'} | {item.get('category_guess') or item.get('nature') or '-'} | {_money(item.get('inflow'))} | {_money(item.get('outflow'))} | {item.get('transaction_count') or item.get('count') or 0} | 是 |")
    related_amount = sum(float(item.get("inflow") or 0) + float(item.get("outflow") or 0) for item in counterparty.get("related_party_counterparties") or [])
    personal_amount = sum(float(item.get("inflow") or 0) + float(item.get("outflow") or 0) for item in counterparty.get("personal_counterparties") or [])
    lines += [
        "",
        "## 七、关联方与个人往来分析",
        f"- 关联方往来金额：{_money(related_amount)}",
        f"- 个人往来金额：{_money(personal_amount)}",
        "- 对银行授信的影响：关联方和个人往来通常需要补充业务背景，银行可能不按经营性收入全额认定。",
        "",
        "## 八、资金沉淀与余额分析",
        f"- 低余额次数：{summary.get('low_balance_transaction_count') or 0}",
        f"- 余额低位阈值：{_money(summary.get('low_balance_threshold'))}",
        "- 大额进账后快速转出情况见风险信号。",
        "",
        "## 九、银行贷款审查风险点",
    ]
    for signal in risk.get("signals") or []:
        refs = ", ".join(str(item) for item in signal.get("evidence_refs") or [])
        lines.append(f"- [{signal.get('level')}] {signal.get('title')}：{signal.get('description')} 证据：{refs or '-'} 建议：{signal.get('suggestion') or '-'}")
    if not (risk.get("signals") or []):
        lines.append("- 暂未识别到明确风险信号")
    lines += [
        "",
        "## 十、融资建议",
        f"- 适合申请的产品类型：{', '.join(financing.get('suggested_credit_products') or []) or '-'}",
        f"- 银行认可流水口径：{_money(financing.get('bank_recognizable_inflow'))}",
        f"- 建议补充材料：{', '.join(financing.get('material_checklist') or []) or '-'}",
        "- 对客户经理/融资顾问的话术：",
    ]
    for item in financing.get("bank_explanation") or []:
        lines.append(f"  - {item}")
    lines += [
        "",
        "## 十一、综合结论",
        f"- 流水规模评价：总进账 {_money(summary.get('total_inflow'))}，月均进账 {_money(summary.get('average_monthly_inflow'))}。",
        f"- 经营真实性评价：银行认可经营性回款估算 {_money(summary.get('estimated_operating_inflow'))}，不把总进账直接等同销售收入。",
        f"- 资金沉淀评价：净流入 {_money(summary.get('net_cashflow'))}，低余额次数 {summary.get('low_balance_transaction_count') or 0}。",
        f"- 关联方风险评价：剔除关联方收入 {_money(summary.get('excluded_related_party_inflow'))}。",
        f"- 个人往来风险评价：剔除个人往来收入 {_money(summary.get('excluded_personal_inflow'))}。",
        f"- 综合融资可行性：{financing.get('conclusion') or '-'}",
        "",
        "### 交易明细预览（前20条）",
        "| 日期 | 摘要 | 对手方 | 收入 | 支出 | 余额 | 分类 |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for tx in (data.get("transactions") or [])[:20]:
        lines.append(f"| {tx.get('transaction_date') or '-'} | {tx.get('summary') or tx.get('purpose') or '-'} | {tx.get('counterparty_name') or '-'} | {_money(tx.get('credit_amount'))} | {_money(tx.get('debit_amount'))} | {_money(tx.get('balance'))} | {tx.get('category') or '-'} |")
    return "\n".join(lines)
