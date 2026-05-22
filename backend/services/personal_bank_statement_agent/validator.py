from __future__ import annotations

from typing import Any


def validate_personal_bank_statement_result(data: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    if data.get("doc_type") != "personal_flow":
        warnings.append("个人流水输出 doc_type 已归一为 personal_flow")
    transactions = data.get("transactions") if isinstance(data.get("transactions"), list) else []
    accounts = data.get("accounts") if isinstance(data.get("accounts"), list) else []
    if not accounts and not transactions:
        warnings.append("未识别到有效个人流水账户或交易明细")
    if not transactions:
        for account in accounts:
            if isinstance(account, dict):
                transactions.extend(account.get("transactions") or [])
    if not transactions:
        warnings.append("未识别到有效个人流水交易明细")
    for tx in transactions:
        if not isinstance(tx, dict):
            continue
        debit = float(tx.get("debit_amount") or 0)
        credit = float(tx.get("credit_amount") or 0)
        if tx.get("direction") == "income" and credit <= 0:
            warnings.append("存在收入方向但 credit_amount 非正数的交易，已保留待人工核验")
        if tx.get("direction") == "expense" and debit <= 0:
            warnings.append("存在支出方向但 debit_amount 非正数的交易，已保留待人工核验")
        if tx.get("category") == "unknown_inflow" and tx.get("is_stable_income"):
            warnings.append("unknown_inflow 不应计入 stable_income，已按未采信收入处理")
        if tx.get("category") == "loan_inflow" and tx.get("is_stable_income"):
            warnings.append("loan_inflow 不应计入 stable_income，已按未采信收入处理")
    income = data.get("income_verification") if isinstance(data.get("income_verification"), dict) else {}
    raw_income = float(income.get("raw_total_income") or 0)
    verified_income = float(income.get("verified_income") or 0)
    stable_income = float(income.get("stable_income") or 0)
    expected_verified = (
        float(income.get("verified_salary_income") or 0)
        + float(income.get("verified_operating_income") or 0)
        + float(income.get("verified_other_stable_income") or 0)
    )
    if abs(verified_income - expected_verified) > 0.01:
        warnings.append("verified_income 应等于可采信工资、经营和其他稳定收入合计")
    if abs(stable_income - verified_income) > 0.01:
        warnings.append("stable_income 应等于 verified_income，来源不明汇入、贷款流入、内部互转不计入稳定收入")
    if verified_income > raw_income + 0.01:
        warnings.append("可采信收入不能大于原始收入")
    return list(dict.fromkeys(warnings))
