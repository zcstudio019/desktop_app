from __future__ import annotations

from typing import Any

from ..normalizer import normalize_account_number, normalize_amount, normalize_currency, normalize_date, normalize_text, normalize_transaction_direction


def _looks_like_placeholder_21(value: Any, row: dict[str, Any], column_values: list[Any]) -> bool:
    # Some exported statements fill blank amount cells with "21". Only null it
    # when this amount column has many 21 values and the paired debit/credit
    # side or balance does not support treating it as a real transaction.
    if normalize_text(value) != "21":
        return False
    count_21 = sum(1 for item in column_values if normalize_text(item) == "21")
    if count_21 < 3:
        return False
    balance = normalize_amount(row.get("balance"))
    other_amount = normalize_amount(row.get("credit_amount") if value == row.get("debit_amount") else row.get("debit_amount"))
    return bool(balance is not None or other_amount not in (None, 0))


def extract_transactions(workbook: dict[str, Any], accounts: list[dict[str, Any]], metadata: dict[str, Any] | None = None) -> tuple[list[dict[str, Any]], list[str]]:
    metadata = metadata or {}
    warnings: list[str] = []
    account_by_sheet = {item.get("sheet_name"): item for item in accounts}
    transactions: list[dict[str, Any]] = []
    source_file = workbook.get("source_file") or metadata.get("filename")

    for sheet in workbook.get("sheets") or []:
        sheet_name = sheet.get("sheet_name")
        rows = sheet.get("rows") or []
        debit_values = [row.get("debit_amount") for row in rows]
        credit_values = [row.get("credit_amount") for row in rows]
        account = account_by_sheet.get(sheet_name) or {}
        for row in rows:
            debit = None if _looks_like_placeholder_21(row.get("debit_amount"), row, debit_values) else normalize_amount(row.get("debit_amount"))
            credit = None if _looks_like_placeholder_21(row.get("credit_amount"), row, credit_values) else normalize_amount(row.get("credit_amount"))
            if debit in (None, 0) and credit in (None, 0):
                continue
            direction = normalize_transaction_direction(debit, credit)
            tags: list[str] = []
            if debit not in (None, 0) and credit not in (None, 0):
                tags.append("both_debit_credit_present")
                warnings.append(f"{sheet_name} 第{row.get('row_number')}行同时存在借方和贷方金额，direction 标记为 unknown")
            tx = {
                "transaction_id": f"{sheet_name or 'sheet'}:{row.get('row_number') or len(transactions) + 1}",
                "source_file": source_file,
                "sheet_name": sheet_name,
                "row_number": row.get("row_number"),
                "account_id": account.get("account_id") or (sheet.get("meta") or {}).get("account_id") or f"{(sheet.get('meta') or {}).get('bank_name') or sheet_name}:{(sheet.get('meta') or {}).get('account_number') or sheet_name}",
                "bank_name": account.get("bank_name") or (sheet.get("meta") or {}).get("bank_name") or sheet_name,
                "account_name": account.get("account_name") or (sheet.get("meta") or {}).get("account_name"),
                "account_number": normalize_account_number(account.get("account_number") or (sheet.get("meta") or {}).get("account_number")),
                "transaction_date": normalize_date(row.get("transaction_date") or row.get("post_date")),
                "post_date": normalize_date(row.get("post_date")) or normalize_date(row.get("transaction_date")),
                "summary": normalize_text(row.get("summary")),
                "purpose": normalize_text(row.get("purpose")),
                "counterparty_name": normalize_text(row.get("counterparty_name")) or None,
                "counterparty_account": normalize_account_number(row.get("counterparty_account")),
                "counterparty_bank": normalize_text(row.get("counterparty_bank")) or None,
                "debit_amount": debit,
                "credit_amount": credit,
                "balance": normalize_amount(row.get("balance")),
                "currency": normalize_currency(row.get("currency") or account.get("currency")),
                "direction": direction,
                "normalized_amount": float(credit or debit or 0),
                "category": None,
                "sub_category": None,
                "is_internal_transfer": False,
                "is_related_party": False,
                "is_personal_counterparty": False,
                "is_large_amount": False,
                "is_suspicious": False,
                "tags": tags,
                "raw": row.get("raw") if isinstance(row.get("raw"), dict) else dict(row),
            }
            transactions.append(tx)
    if not transactions:
        warnings.append("未识别到有效交易明细")
    return transactions, warnings
