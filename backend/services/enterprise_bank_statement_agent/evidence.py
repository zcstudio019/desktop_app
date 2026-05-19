from __future__ import annotations

from typing import Any


def build_transaction_evidence(transactions: list[dict[str, Any]], limit: int = 50) -> list[dict[str, Any]]:
    evidence = []
    for tx in transactions[:limit]:
        evidence.append(
            {
                "evidence_id": f"tx:{tx.get('transaction_id')}",
                "source_file": tx.get("source_file"),
                "sheet_name": tx.get("sheet_name"),
                "row_number": tx.get("row_number"),
                "field": "transaction",
                "value": tx.get("normalized_amount"),
                "note": f"{tx.get('transaction_date') or ''} {tx.get('summary') or ''} {tx.get('counterparty_name') or ''}".strip(),
            }
        )
    return evidence
