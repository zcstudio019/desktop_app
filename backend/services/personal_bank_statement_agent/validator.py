from __future__ import annotations

from typing import Any


def validate_personal_bank_statement_result(data: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    if data.get("doc_type") != "personal_flow":
        warnings.append("个人流水输出 doc_type 已归一为 personal_flow")
    accounts = data.get("accounts") if isinstance(data.get("accounts"), list) else []
    if not accounts:
        warnings.append("未识别到有效个人流水账户")
    tx_count = sum(len(account.get("transactions") or []) for account in accounts if isinstance(account, dict))
    if tx_count == 0:
        warnings.append("未识别到有效个人流水交易明细")
    for account in accounts:
        if not isinstance(account, dict):
            continue
        for tx in account.get("transactions") or []:
            if not isinstance(tx, dict):
                continue
            debit = float(tx.get("debit_amount") or 0)
            credit = float(tx.get("credit_amount") or 0)
            if tx.get("direction") == "income" and credit <= 0:
                warnings.append("存在收入方向但 credit_amount 非正数的交易，已保留待人工核验")
            if tx.get("direction") == "expense" and debit <= 0:
                warnings.append("存在支出方向但 debit_amount 非正数的交易，已保留待人工核验")
    return list(dict.fromkeys(warnings))
