from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from ..normalizer import normalize_text

logger = logging.getLogger(__name__)


def _amount(tx: dict[str, Any]) -> float:
    return float(tx.get("credit_amount") or tx.get("debit_amount") or 0)


def _counterparty_name(tx: dict[str, Any]) -> str:
    name = normalize_text(tx.get("counterparty_name")) or normalize_text(tx.get("payee_name"))
    return name or "未知对手方"


def _counterparty_account(tx: dict[str, Any]) -> str:
    return str(tx.get("counterparty_account") or tx.get("payee_account") or "")


def _make_stat() -> dict[str, Any]:
    return {
        "name": "",
        "account": "",
        "bank": "",
        "inflow": 0.0,
        "outflow": 0.0,
        "net": 0.0,
        "amount": 0.0,
        "count": 0,
        "transaction_count": 0,
        "first_date": None,
        "last_date": None,
        "nature": None,
        "exclude_from_operating": False,
        "category_guess": None,
        "is_related_party": False,
        "is_personal_counterparty": False,
        "is_internal_transfer": False,
        "risk_note": None,
    }


def _update_date_range(item: dict[str, Any], date_value: Any) -> None:
    date_text = str(date_value or "")[:10]
    if not date_text:
        return
    if not item.get("first_date") or date_text < item["first_date"]:
        item["first_date"] = date_text
    if not item.get("last_date") or date_text > item["last_date"]:
        item["last_date"] = date_text


def analyze_counterparties(transactions: list[dict[str, Any]], total_inflow: float = 0.0, total_outflow: float = 0.0) -> dict[str, Any]:
    stats: dict[str, dict[str, Any]] = defaultdict(_make_stat)
    for tx in transactions:
        name = _counterparty_name(tx)
        account = _counterparty_account(tx)
        bank = normalize_text(tx.get("counterparty_bank"))
        nature = tx.get("nature") or ("internal_transfer" if tx.get("is_internal_transfer") else "operating")
        key = f"{name}|{account}|{bank}|{nature}"
        item = stats[key]
        item["name"] = name
        item["account"] = item["account"] or account
        item["bank"] = item["bank"] or bank
        item["inflow"] += float(tx.get("credit_amount") or 0)
        item["outflow"] += float(tx.get("debit_amount") or 0)
        item["amount"] += _amount(tx)
        item["count"] += 1
        item["transaction_count"] += 1
        item["nature"] = nature
        item["exclude_from_operating"] = item["exclude_from_operating"] or bool(tx.get("exclude_from_operating"))
        item["is_internal_transfer"] = item["is_internal_transfer"] or bool(tx.get("is_internal_transfer"))
        item["is_related_party"] = item["is_related_party"] or bool(tx.get("is_related_party"))
        item["is_personal_counterparty"] = item["is_personal_counterparty"] or bool(tx.get("is_personal_counterparty"))
        item["category_guess"] = item["category_guess"] or tx.get("category")
        _update_date_range(item, tx.get("transaction_date") or tx.get("post_date"))

    items = []
    for item in stats.values():
        item["inflow"] = round(item["inflow"], 2)
        item["outflow"] = round(item["outflow"], 2)
        item["net"] = round(item["inflow"] - item["outflow"], 2)
        item["amount"] = round(item["amount"], 2)
        if item["is_internal_transfer"]:
            item["risk_note"] = "本方或同字号关联主体内部往来，银行经营流水口径应剔除"
        elif item["is_related_party"]:
            item["risk_note"] = "疑似关联方，银行可能打折认定"
        elif item["is_personal_counterparty"]:
            item["risk_note"] = "疑似个人账户往来，需补充用途说明"
        items.append(item)

    operating_items = [item for item in items if not item.get("exclude_from_operating")]
    top_inflow = sorted([item for item in operating_items if item.get("inflow", 0) > 0], key=lambda x: x["inflow"], reverse=True)[:10]
    top_outflow = sorted([item for item in operating_items if item.get("outflow", 0) > 0], key=lambda x: x["outflow"], reverse=True)[:10]
    internal_items = sorted([item for item in items if item.get("is_internal_transfer")], key=lambda x: x["amount"], reverse=True)
    related_items = sorted([item for item in items if item.get("is_related_party")], key=lambda x: x["amount"], reverse=True)
    personal_items = sorted([item for item in items if item.get("is_personal_counterparty")], key=lambda x: x["amount"], reverse=True)
    top5_inflow = sum(item["inflow"] for item in top_inflow[:5])
    top5_outflow = sum(item["outflow"] for item in top_outflow[:5])
    logger.info(
        "[EnterpriseFlow][CounterpartySummary] top_inflow=%s top_outflow=%s",
        [(item["name"], item["inflow"]) for item in top_inflow[:5]],
        [(item["name"], item["outflow"]) for item in top_outflow[:5]],
    )
    return {
        "top_inflow_counterparties": top_inflow,
        "top_outflow_counterparties": top_outflow,
        "internal_transfer_counterparties": internal_items[:20],
        "related_party_counterparties": related_items[:20],
        "personal_counterparties": personal_items[:20],
        "customer_concentration_top5_ratio": round(top5_inflow / total_inflow, 4) if total_inflow else None,
        "supplier_concentration_top5_ratio": round(top5_outflow / total_outflow, 4) if total_outflow else None,
    }
