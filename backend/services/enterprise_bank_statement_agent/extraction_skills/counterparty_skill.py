from __future__ import annotations

from collections import defaultdict
from typing import Any


def analyze_counterparties(transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "counterparty_name": "",
            "transaction_count": 0,
            "credit_amount": 0.0,
            "debit_amount": 0.0,
            "concentration": 0.0,
            "is_possible_related_party": False,
            "is_personal_account": False,
        }
    )
    total_flow = sum(float(tx.get("credit_amount") or 0) + float(tx.get("debit_amount") or 0) for tx in transactions)
    for tx in transactions:
        name = str(tx.get("counterparty_name") or "未知对手方").strip() or "未知对手方"
        item = stats[name]
        item["counterparty_name"] = name
        item["transaction_count"] += 1
        item["credit_amount"] += float(tx.get("credit_amount") or 0)
        item["debit_amount"] += float(tx.get("debit_amount") or 0)
        if 2 <= len(name) <= 4 and not any(word in name for word in ("公司", "银行", "集团", "有限", "商行")):
            item["is_personal_account"] = True
        if any(word in name for word in ("关联", "股东", "法人", "实际控制", "集团")):
            item["is_possible_related_party"] = True
    result = []
    for item in stats.values():
        flow = item["credit_amount"] + item["debit_amount"]
        item["credit_amount"] = round(item["credit_amount"], 2)
        item["debit_amount"] = round(item["debit_amount"], 2)
        item["concentration"] = round(flow / total_flow, 4) if total_flow else 0.0
        item["role"] = "主要收入来源客户" if item["credit_amount"] >= item["debit_amount"] else "主要支出对象"
        result.append(item)
    return sorted(result, key=lambda x: x["credit_amount"] + x["debit_amount"], reverse=True)
