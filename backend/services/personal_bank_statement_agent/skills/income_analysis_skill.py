from __future__ import annotations

from collections import defaultdict
from statistics import mean, pstdev
from typing import Any

from ..normalizer import round2


def _month(date: Any) -> str:
    text = str(date or "")
    return text[:7] if len(text) >= 7 else "unknown"


def analyze_income(transactions: list[dict[str, Any]], month_count: int | None = None) -> dict[str, Any]:
    monthly: dict[str, dict[str, float]] = defaultdict(lambda: {
        "income": 0.0,
        "verified_salary_income": 0.0,
        "verified_operating_income": 0.0,
        "verified_other_stable_income": 0.0,
        "unknown_inflow": 0.0,
        "interest_income": 0.0,
        "verified_income": 0.0,
        "stable_income": 0.0,
        "confirmed_salary_income": 0.0,
        "suspected_salary_income": 0.0,
    })
    totals = defaultdict(float)
    notes: list[str] = []
    for tx in transactions:
        credit = float(tx.get("credit_amount") or 0)
        if tx.get("direction") != "income" or credit <= 0:
            continue
        category = str(tx.get("category") or "")
        month = _month(tx.get("transaction_date"))
        monthly[month]["income"] += credit
        totals["raw_total_income"] += credit
        if category == "salary_income":
            totals["confirmed_salary_income"] += credit
            totals["verified_salary_income"] += credit
            monthly[month]["verified_salary_income"] += credit
            monthly[month]["confirmed_salary_income"] += credit
            totals["salary_income_count"] += 1
        elif category == "suspected_salary_income":
            totals["suspected_salary_income"] += credit
            totals["suspected_salary_count"] += 1
            monthly[month]["suspected_salary_income"] += credit
        elif category == "operating_income":
            totals["verified_operating_income"] += credit
            monthly[month]["verified_operating_income"] += credit
        elif category == "other_stable_income":
            totals["verified_other_stable_income"] += credit
            monthly[month]["verified_other_stable_income"] += credit
        elif category == "unknown_inflow":
            totals["unknown_inflow"] += credit
            monthly[month]["unknown_inflow"] += credit
        elif category == "interest_income":
            totals["interest_income"] += credit
            monthly[month]["interest_income"] += credit
        elif category == "loan_inflow":
            totals["loan_inflow"] += credit
        elif category == "internal_transfer":
            totals["internal_transfer_income"] += credit
        elif category == "related_party_transfer":
            totals["related_party_income"] += credit
        elif category == "investment_redeem_income":
            totals["investment_redeem_income"] += credit
        elif category == "refund":
            totals["refund_income"] += credit
        else:
            totals["non_verified_income"] += credit

    totals["verified_salary_income"] = totals["confirmed_salary_income"]
    verified_income = totals["verified_salary_income"] + totals["verified_operating_income"] + totals["verified_other_stable_income"]
    stable_income = verified_income
    non_verified_income = (
        totals["raw_total_income"]
        - verified_income
    )
    totals["verified_income"] = verified_income
    totals["stable_income"] = stable_income
    totals["non_verified_income"] = max(0.0, non_verified_income)
    divisor = max(1, int(month_count or len([m for m in monthly if m != "unknown"]) or 1))
    totals["avg_monthly_verified_income"] = verified_income / divisor
    totals["avg_monthly_stable_income"] = stable_income / divisor
    if totals["unknown_inflow"] > 0:
        notes.append("汇款汇入/转账收入未命中工资或经营用途，未计入稳定收入")
    if totals["interest_income"] > 0:
        notes.append("利息收入单列统计，不作为主要收入采信")
    if totals["loan_inflow"] > 0:
        notes.append("贷款/借款流入已剔除稳定收入")
    if totals["suspected_salary_income"] > 0:
        notes.append("疑似工资收入单列展示，默认不并入可采信工资收入")
    values = [item["stable_income"] for key, item in sorted(monthly.items()) if key != "unknown"]
    for item in monthly.values():
        item["verified_income"] = item["verified_salary_income"] + item["verified_operating_income"] + item["verified_other_stable_income"]
        item["stable_income"] = item["verified_income"]
    salary_months = len({
        str(tx.get("transaction_date") or "")[:7]
        for tx in transactions
        if tx.get("category") == "salary_income" and str(tx.get("transaction_date") or "")[:7]
    })
    salary_sources: dict[str, dict[str, Any]] = defaultdict(lambda: {"counterparty_name": "未知付款方", "amount": 0.0, "count": 0, "months": set(), "salary_type": ""})
    salary_confidences: list[float] = []
    continuous_months = 0
    continuity_level = "none"
    salary_detection_notes: list[str] = []
    for tx in transactions:
        detection = tx.get("salary_detection") or {}
        salary_type = detection.get("salary_type")
        if salary_type not in {"confirmed_salary", "suspected_salary"}:
            continue
        name = str(tx.get("counterparty_name") or "未知付款方")
        source = salary_sources[name]
        source["counterparty_name"] = name
        source["amount"] += float(tx.get("credit_amount") or 0)
        source["count"] += 1
        source["months"].add(str(tx.get("transaction_date") or "")[:7])
        if salary_type == "confirmed_salary":
            source["salary_type"] = "confirmed_salary"
            salary_confidences.append(float(detection.get("confidence") or 0))
            continuous_months = max(continuous_months, int(detection.get("continuous_months") or 0))
        elif not source["salary_type"]:
            source["salary_type"] = "suspected_salary"
    if continuous_months >= 6:
        continuity_level = "strong"
    elif continuous_months >= 3:
        continuity_level = "medium"
    elif continuous_months > 0:
        continuity_level = "weak"
    if totals["confirmed_salary_income"] <= 0:
        salary_detection_notes.append("未识别到明确工资收入")
    if totals["suspected_salary_income"] > 0:
        salary_detection_notes.append("存在疑似工资收入，需人工核实付款方、用途和发放规律")
    source_rows = []
    for item in salary_sources.values():
        source_rows.append({
            "counterparty_name": item["counterparty_name"],
            "amount": round2(item["amount"]),
            "count": item["count"],
            "months": sorted(month for month in item["months"] if month),
            "salary_type": item["salary_type"] or "unknown",
        })
    source_rows.sort(key=lambda item: float(item.get("amount") or 0), reverse=True)
    return {
        **{key: round2(value) for key, value in totals.items()},
        "confirmed_salary_income": round2(totals["confirmed_salary_income"]),
        "suspected_salary_income": round2(totals["suspected_salary_income"]),
        "salary_income_count": int(totals["salary_income_count"]),
        "suspected_salary_count": int(totals["suspected_salary_count"]),
        "salary_months": salary_months,
        "salary_avg_monthly_amount": round2(totals["confirmed_salary_income"] / salary_months) if salary_months else 0.0,
        "salary_continuity_level": continuity_level,
        "salary_confidence": round2(sum(salary_confidences) / len(salary_confidences)) if salary_confidences else 0.0,
        "salary_sources": source_rows[:10],
        "salary_detection_notes": salary_detection_notes,
        "conservative_verified_income": round2(verified_income),
        "aggressive_estimated_income": round2(verified_income + totals["suspected_salary_income"]),
        "verification_notes": notes,
        "monthly_income": [{"month": month, **{k: round2(v) for k, v in item.items()}} for month, item in sorted(monthly.items())],
        "income_continuous_months": sum(1 for value in values if value > 0),
        "max_monthly_income": round2(max(values, default=0)),
        "min_monthly_income": round2(min(values, default=0)),
        "income_volatility": round((pstdev(values) / mean(values)), 4) if len(values) > 1 and mean(values) else 0.0,
        "has_large_one_off_income": bool(values and max(values) > max(mean(values) * 2.5, 50000)),
        "has_loan_inflow_as_income": totals["loan_inflow"] > 0,
    }
