from __future__ import annotations

from typing import Any

from ..normalizer import (
    direction_from_amounts,
    normalize_account_number,
    normalize_amount,
    normalize_date,
    normalize_text,
    round2,
)


def extract_transactions(workbook: dict[str, Any], accounts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    by_sheet = {item.get("sheet_name"): item for item in accounts}
    transactions: list[dict[str, Any]] = []
    source_file = str(workbook.get("source_file") or "")
    for sheet in workbook.get("sheets") or []:
        sheet_name = normalize_text(sheet.get("sheet_name"))
        account = by_sheet.get(sheet_name) or {}
        for row in sheet.get("rows") or []:
            debit = round2(normalize_amount(row.get("debit_amount")))
            credit = round2(normalize_amount(row.get("credit_amount")))
            signed_amount = normalize_amount(
                row.get("transaction_amount")
                or row.get("amount")
                or (row.get("raw") or {}).get("交易金额")
                or (row.get("raw") or {}).get("金额")
                or (row.get("raw") or {}).get("发生额")
            )
            if debit <= 0 and credit <= 0 and signed_amount is not None:
                if signed_amount > 0:
                    credit = round2(signed_amount)
                elif signed_amount < 0:
                    debit = round2(abs(signed_amount))
            if debit <= 0 and credit <= 0:
                continue
            direction = direction_from_amounts(debit, credit)
            if direction == "unknown":
                warnings.append(f"{sheet_name} 第{row.get('row_number')}行借贷方向无法唯一判断")
            raw = row.get("raw") or {}
            summary = normalize_text(
                row.get("summary")
                or row.get("purpose")
                or row.get("remark")
                or raw.get("交易摘要")
                or raw.get("摘要")
                or raw.get("交易名称")
            )
            tx = {
                "transaction_id": f"{sheet_name or 'sheet'}:{row.get('row_number') or len(transactions) + 1}",
                "source_file": source_file,
                "sheet_name": sheet_name,
                "row_number": row.get("row_number"),
                "account_no": account.get("account_no") or normalize_account_number(row.get("account_number")) or "",
                "transaction_date": normalize_date(row.get("transaction_date") or row.get("post_date")) or "",
                "summary": summary,
                "counterparty_name": normalize_text(
                    row.get("counterparty_name")
                    or row.get("payee_name")
                    or raw.get("对手信息")
                    or raw.get("Counter Party")
                    or raw.get("counterparty")
                    or raw.get("对方户名")
                    or raw.get("对方名称")
                    or raw.get("交易对手")
                    or raw.get("对手方")
                ) or "",
                "counterparty_account": normalize_account_number(row.get("counterparty_account") or row.get("payee_account")) or "",
                "debit_amount": debit if debit > 0 else 0.0,
                "credit_amount": credit if credit > 0 else 0.0,
                "balance": round2(normalize_amount(row.get("balance"))),
                "direction": direction,
                "category": "other",
                "is_internal_transfer": False,
                "is_possible_internal_transfer": False,
                "is_related_party": False,
                "is_loan_inflow": False,
                "is_salary": False,
                "is_operating_income": False,
                "is_credit_card_repayment": False,
                "risk_tags": [],
                "evidence": "",
            }
            transactions.append(tx)
    if not transactions:
        warnings.append("未识别到有效个人流水交易明细")
    return transactions, warnings
