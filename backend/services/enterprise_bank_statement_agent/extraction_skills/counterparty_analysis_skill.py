from __future__ import annotations

from collections import defaultdict
from typing import Any


def analyze_counterparties(transactions: list[dict[str, Any]], total_inflow: float = 0.0, total_outflow: float = 0.0) -> dict[str, Any]:
    stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"name": "", "inflow": 0.0, "outflow": 0.0, "net": 0.0, "transaction_count": 0, "category_guess": None, "is_related_party": False, "is_personal_counterparty": False, "risk_note": None})
    for tx in transactions:
        name = str(tx.get("counterparty_name") or "未知对手方").strip() or "未知对手方"
        item = stats[name]
        item["name"] = name
        item["inflow"] += float(tx.get("credit_amount") or 0)
        item["outflow"] += float(tx.get("debit_amount") or 0)
        item["transaction_count"] += 1
        item["is_related_party"] = item["is_related_party"] or bool(tx.get("is_related_party"))
        item["is_personal_counterparty"] = item["is_personal_counterparty"] or bool(tx.get("is_personal_counterparty"))
        item["category_guess"] = item["category_guess"] or tx.get("category")
    items = []
    for item in stats.values():
        item["inflow"] = round(item["inflow"], 2)
        item["outflow"] = round(item["outflow"], 2)
        item["net"] = round(item["inflow"] - item["outflow"], 2)
        if item["is_related_party"]:
            item["risk_note"] = "疑似关联方，银行可能打折认定"
        elif item["is_personal_counterparty"]:
            item["risk_note"] = "疑似个人账户往来，需补充用途说明"
        items.append(item)
    top_inflow = sorted(items, key=lambda x: x["inflow"], reverse=True)[:10]
    top_outflow = sorted(items, key=lambda x: x["outflow"], reverse=True)[:10]
    top5_inflow = sum(item["inflow"] for item in top_inflow[:5])
    top5_outflow = sum(item["outflow"] for item in top_outflow[:5])
    return {
        "top_inflow_counterparties": top_inflow,
        "top_outflow_counterparties": top_outflow,
        "related_party_counterparties": [item for item in items if item["is_related_party"]],
        "personal_counterparties": [item for item in items if item["is_personal_counterparty"]],
        "customer_concentration_top5_ratio": round(top5_inflow / total_inflow, 4) if total_inflow else None,
        "supplier_concentration_top5_ratio": round(top5_outflow / total_outflow, 4) if total_outflow else None,
    }
