from __future__ import annotations

from collections import defaultdict
from statistics import mean, pstdev
from typing import Any


def _month(date: Any) -> str:
    text = str(date or "")
    return text[:7] if len(text) >= 7 else "unknown"


def analyze_income(transactions: list[dict[str, Any]]) -> dict[str, Any]:
    monthly: dict[str, dict[str, float]] = defaultdict(lambda: {"income": 0.0, "salary_income": 0.0, "operating_income": 0.0, "stable_income": 0.0})
    loan_as_income = False
    for tx in transactions:
        credit = float(tx.get("credit_amount") or 0)
        if credit <= 0:
            continue
        month = _month(tx.get("transaction_date"))
        category = tx.get("category")
        monthly[month]["income"] += credit
        if category == "salary_income":
            monthly[month]["salary_income"] += credit
            monthly[month]["stable_income"] += credit
        elif category == "operating_income":
            monthly[month]["operating_income"] += credit
            monthly[month]["stable_income"] += credit
        elif category == "other_stable_income":
            monthly[month]["stable_income"] += credit
        elif category == "loan_inflow":
            loan_as_income = True
    values = [item["stable_income"] for _, item in sorted(monthly.items()) if _ != "unknown"]
    return {
        "monthly_income": [{"month": month, **{k: round(v, 2) for k, v in item.items()}} for month, item in sorted(monthly.items())],
        "avg_monthly_stable_income": round(mean(values), 2) if values else 0.0,
        "income_continuous_months": sum(1 for value in values if value > 0),
        "max_monthly_income": round(max(values), 2) if values else 0.0,
        "min_monthly_income": round(min(values), 2) if values else 0.0,
        "income_volatility": round((pstdev(values) / mean(values)), 4) if len(values) > 1 and mean(values) else 0.0,
        "has_large_one_off_income": bool(values and max(values) > max(mean(values) * 2.5, 50000)),
        "has_loan_inflow_as_income": loan_as_income,
    }
