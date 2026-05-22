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
            totals["verified_salary_income"] += credit
            monthly[month]["verified_salary_income"] += credit
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
    values = [item["stable_income"] for key, item in sorted(monthly.items()) if key != "unknown"]
    for item in monthly.values():
        item["verified_income"] = item["verified_salary_income"] + item["verified_operating_income"] + item["verified_other_stable_income"]
        item["stable_income"] = item["verified_income"]
    return {
        **{key: round2(value) for key, value in totals.items()},
        "verification_notes": notes,
        "monthly_income": [{"month": month, **{k: round2(v) for k, v in item.items()}} for month, item in sorted(monthly.items())],
        "income_continuous_months": sum(1 for value in values if value > 0),
        "max_monthly_income": round2(max(values, default=0)),
        "min_monthly_income": round2(min(values, default=0)),
        "income_volatility": round((pstdev(values) / mean(values)), 4) if len(values) > 1 and mean(values) else 0.0,
        "has_large_one_off_income": bool(values and max(values) > max(mean(values) * 2.5, 50000)),
        "has_loan_inflow_as_income": totals["loan_inflow"] > 0,
    }
