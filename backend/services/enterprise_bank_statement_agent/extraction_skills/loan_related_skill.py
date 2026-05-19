from __future__ import annotations

from typing import Any

LOAN_KEYWORDS = ("贷款", "还款", "扣息", "利息", "本息", "贴现", "承兑", "银承", "保理", "保证金", "担保费", "融资租赁", "小额贷款", "手续费")


def detect_loan_related_transactions(transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for tx in transactions:
        blob = " ".join(str(tx.get(field) or "") for field in ("summary", "purpose", "counterparty_name", "category"))
        matched = [word for word in LOAN_KEYWORDS if word in blob]
        if matched:
            result.append({"matched_keywords": matched, "transaction_id": tx.get("transaction_id"), "transaction": tx})
    return result
