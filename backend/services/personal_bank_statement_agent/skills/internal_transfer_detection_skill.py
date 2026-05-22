from __future__ import annotations

from difflib import SequenceMatcher
from typing import Any

from ..normalizer import normalize_account_number, normalize_text


TRANSFER_HINTS = ("本人转账", "账户互转", "同名", "转存", "本行转账", "跨行转账")


def _similar(a: str, b: str) -> bool:
    if not a or not b:
        return False
    return a == b or SequenceMatcher(None, a, b).ratio() >= 0.88


def detect_internal_transfers(transactions: list[dict[str, Any]], accounts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    owner_names = {normalize_text(item.get("account_name")) for item in accounts if normalize_text(item.get("account_name"))}
    own_accounts = {normalize_account_number(item.get("account_no")) for item in accounts if normalize_account_number(item.get("account_no"))}
    details: list[dict[str, Any]] = []
    for tx in transactions:
        cp_name = normalize_text(tx.get("counterparty_name"))
        cp_account = normalize_account_number(tx.get("counterparty_account"))
        summary = normalize_text(tx.get("summary"))
        name_match = any(_similar(cp_name, owner_name) for owner_name in owner_names)
        account_match = bool(cp_account and cp_account in own_accounts)
        has_hint = any(word in summary for word in TRANSFER_HINTS)
        if name_match or account_match:
            tx["is_internal_transfer"] = True
            tx["category"] = "internal_transfer"
            tx["evidence"] = "对手方户名或账号与本人账户匹配，判定为本人账户互转"
        elif has_hint and not cp_name and not cp_account:
            tx["is_possible_internal_transfer"] = True
            tx.setdefault("risk_tags", []).append("possible_internal_transfer")
            tx["evidence"] = "摘要存在转账类描述但缺少对手方，未直接剔除"
        if tx.get("is_internal_transfer") or tx.get("is_possible_internal_transfer"):
            details.append(tx)
    summary = {
        "internal_transfer_income": round(sum(float(tx.get("credit_amount") or 0) for tx in transactions if tx.get("is_internal_transfer")), 2),
        "internal_transfer_expense": round(sum(float(tx.get("debit_amount") or 0) for tx in transactions if tx.get("is_internal_transfer")), 2),
        "internal_transfer_transactions": details,
    }
    return transactions, summary
