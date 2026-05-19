from __future__ import annotations

from datetime import datetime
from typing import Any


def _signal(code: str, level: str, title: str, description: str, amount: float | None = None, ratio: float | None = None, evidence_refs: list[str] | None = None, suggestion: str | None = None) -> dict[str, Any]:
    return {"code": code, "level": level, "title": title, "description": description, "amount": amount, "ratio": ratio, "evidence_refs": evidence_refs or [], "suggestion": suggestion}


def detect_risk_signals(
    transactions: list[dict[str, Any]],
    summary: dict[str, Any],
    monthly_summary: list[dict[str, Any]],
    counterparty_summary: dict[str, Any],
    months_count: int | None,
) -> dict[str, Any]:
    signals: list[dict[str, Any]] = []
    total_inflow = float(summary.get("total_inflow") or 0)
    total_outflow = float(summary.get("total_outflow") or 0)
    net = float(summary.get("net_cashflow") or 0)
    if total_inflow > 0 and abs(net) / total_inflow < 0.03:
        signals.append(_signal("NET_CASHFLOW_TOO_LOW", "medium", "净流入偏低", "收入和支出高度接近，资金沉淀弱。", amount=net, ratio=round(abs(net) / total_inflow, 4), suggestion="补充余额沉淀、应收账款和订单材料。"))
    low_balance_count = int(summary.get("low_balance_transaction_count") or 0)
    if transactions and (low_balance_count >= 5 or low_balance_count / len(transactions) > 0.2):
        signals.append(_signal("LOW_BALANCE_FREQUENT", "medium", "低余额交易较多", "账户多次低于 5000 元，偿债缓冲偏弱。", amount=low_balance_count, ratio=round(low_balance_count / len(transactions), 4), suggestion="说明资金调拨安排，并补充其他账户流水。"))
    related_amount = sum(float(tx.get("credit_amount") or tx.get("debit_amount") or 0) for tx in transactions if tx.get("is_related_party"))
    if total_inflow + total_outflow > 0 and related_amount / (total_inflow + total_outflow) > 0.2:
        signals.append(_signal("RELATED_PARTY_HIGH", "medium", "关联方往来占比较高", "关联方流水银行可能打折认定。", amount=round(related_amount, 2), ratio=round(related_amount / (total_inflow + total_outflow), 4), suggestion="补充关联交易背景、合同和发票。"))
    personal_amount = sum(float(tx.get("credit_amount") or tx.get("debit_amount") or 0) for tx in transactions if tx.get("is_personal_counterparty"))
    if total_inflow + total_outflow > 0 and personal_amount / (total_inflow + total_outflow) > 0.1:
        signals.append(_signal("PERSONAL_COUNTERPARTY_HIGH", "medium", "个人往来金额较高", "个人账户往来需要说明用途。", amount=round(personal_amount, 2), ratio=round(personal_amount / (total_inflow + total_outflow), 4), suggestion="补充个人往来明细说明和业务凭证。"))
    concentration = counterparty_summary.get("customer_concentration_top5_ratio")
    if concentration and concentration > 0.7:
        signals.append(_signal("CUSTOMER_CONCENTRATION_HIGH", "medium", "客户集中度高", "Top5 客户收入占比较高。", ratio=concentration, suggestion="补充核心客户合作稳定性材料。"))
    large_inflows = [tx for tx in transactions if tx.get("direction") == "inflow" and float(tx.get("normalized_amount") or 0) >= 100000 and tx.get("transaction_date")]
    large_outflows = [tx for tx in transactions if tx.get("direction") == "outflow" and float(tx.get("normalized_amount") or 0) >= 100000 and tx.get("transaction_date")]
    for inflow in large_inflows:
        try:
            in_date = datetime.strptime(inflow["transaction_date"], "%Y-%m-%d")
        except ValueError:
            continue
        for outflow in large_outflows:
            try:
                out_date = datetime.strptime(outflow["transaction_date"], "%Y-%m-%d")
            except ValueError:
                continue
            if 0 <= (out_date - in_date).days <= 3 and abs(float(inflow.get("normalized_amount") or 0) - float(outflow.get("normalized_amount") or 0)) <= float(inflow.get("normalized_amount") or 0) * 0.2:
                signals.append(_signal("QUICK_OUTFLOW_AFTER_INFLOW", "high", "大额进账后快速转出", "大额收入后 1-3 日内出现相近金额大额支出，可能被认为是过账流水。", amount=float(inflow.get("normalized_amount") or 0), evidence_refs=[inflow.get("transaction_id"), outflow.get("transaction_id")], suggestion="补充交易合同、发票和付款背景。"))
                break
        if any(item["code"] == "QUICK_OUTFLOW_AFTER_INFLOW" for item in signals):
            break
    internal_amount = float(summary.get("excluded_internal_transfer_amount") or 0)
    if total_inflow + total_outflow > 0 and internal_amount / (total_inflow + total_outflow) > 0.2:
        signals.append(_signal("INTERNAL_TRANSFER_HIGH", "medium", "内部转账占比较高", "需剔除内部转账后看真实经营流水。", amount=internal_amount, ratio=round(internal_amount / (total_inflow + total_outflow), 4), suggestion="提供各账户用途和内部调拨说明。"))
    if months_count is not None and months_count < 6:
        signals.append(_signal("SHORT_STATEMENT_PERIOD", "low", "流水周期不足", "银行通常更关注 6-12 个月连续流水。", ratio=months_count / 6, suggestion="补充更长周期流水。"))
    if float(summary.get("estimated_operating_inflow") or 0) <= 0 and total_inflow > 0:
        signals.append(_signal("VALID_OPERATING_INFLOW_LOW", "high", "有效经营收入不足", "剔除内部、关联方和个人往来后，银行可认可经营性流水不足。", suggestion="补充合同、发票、纳税和其他账户流水。"))
    score = min(100, len([s for s in signals if s["level"] == "high"]) * 30 + len([s for s in signals if s["level"] == "medium"]) * 15 + len([s for s in signals if s["level"] == "low"]) * 5)
    level = "high" if score >= 60 else "medium" if score >= 25 else "low"
    strengths = []
    weaknesses = [item["title"] for item in signals[:5]]
    if total_inflow > 0:
        strengths.append("已识别到企业账户进账流水")
    if summary.get("account_count", 0) > 1:
        strengths.append("覆盖多个银行账户，可交叉核验经营活动")
    return {"overall_level": level, "overall_score": score, "signals": signals, "strengths": strengths, "weaknesses": weaknesses}
