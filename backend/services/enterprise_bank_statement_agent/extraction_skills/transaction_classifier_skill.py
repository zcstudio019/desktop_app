from __future__ import annotations

from statistics import quantiles
from typing import Any

from ..normalizer import guess_company_core_name, is_probable_person_name, normalize_text

ORG_WORDS = ("公司", "有限公司", "银行", "合作社", "中心", "集团", "工程", "建材", "供应链", "材料", "矿业")


def _blob(tx: dict[str, Any]) -> str:
    return " ".join(normalize_text(tx.get(field)) for field in ("summary", "purpose", "counterparty_name"))


def classify_transactions(transactions: list[dict[str, Any]], company_name: str | None = None, metadata: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    metadata = metadata or {}
    company_core = guess_company_core_name(company_name)
    related_keywords = set(metadata.get("related_party_keywords") or [])
    amounts = sorted(float(tx.get("normalized_amount") or 0) for tx in transactions if float(tx.get("normalized_amount") or 0) > 0)
    p90 = quantiles(amounts, n=10)[-1] if len(amounts) >= 10 else 100000.0
    large_threshold = max(100000.0, p90)

    for tx in transactions:
        text = _blob(tx)
        counterparty = normalize_text(tx.get("counterparty_name"))
        tags = list(tx.get("tags") or [])
        if counterparty and (counterparty == normalize_text(company_name) or (company_core and company_core in counterparty and company_core in normalize_text(company_name))):
            tx["is_internal_transfer"] = True
            tags.append("internal_transfer")
        if any(word and word in counterparty for word in related_keywords) or (company_core and company_core in counterparty and not tx["is_internal_transfer"]):
            tx["is_related_party"] = True
            tags.append("related_party")
        if is_probable_person_name(counterparty):
            tx["is_personal_counterparty"] = True
            tags.append("personal_counterparty")
        if any(word in text for word in ("内部转账", "户间转账", "调拨")):
            tx["is_internal_transfer"] = True
            tags.append("internal_transfer_keyword")
        if any(word in text for word in ("工资", "薪资", "奖金", "社保", "公积金")):
            category = "工资薪酬"
        elif any(word in text for word in ("税务", "税款", "缴税", "国库")):
            category = "税费"
        elif any(word in text for word in ("贷款", "还款", "利息", "贴现", "承兑", "保证金", "担保费", "扣息", "手续费")):
            category = "贷款还款" if "还款" in text or "贷款" in text else "利息"
        elif tx["is_internal_transfer"]:
            category = "内部户间转账"
        elif tx["is_related_party"]:
            category = "关联方往来"
        elif tx["is_personal_counterparty"]:
            category = "个人往来"
        elif tx.get("direction") == "inflow" and any(word in text for word in ("货款", "材料款", "工程款", "销售款", "结算款", "服务费", "回款")):
            category = "经营收入"
        elif tx.get("direction") == "outflow" and any(word in text for word in ("货款", "材料款", "采购款", "运费", "装卸费", "水泥", "砂石", "沥青", "矿业", "工程款")):
            category = "经营支出"
        elif tx.get("direction") == "inflow":
            category = "客户回款" if any(word in counterparty for word in ORG_WORDS) else "其他"
        elif tx.get("direction") == "outflow":
            category = "供应商付款" if any(word in counterparty for word in ORG_WORDS) else "其他"
        else:
            category = "其他"
        tx["category"] = category
        tx["is_large_amount"] = float(tx.get("normalized_amount") or 0) >= large_threshold
        if tx["is_large_amount"]:
            tags.append("large_amount")
        tx["tags"] = sorted(set(tags))
    return transactions
