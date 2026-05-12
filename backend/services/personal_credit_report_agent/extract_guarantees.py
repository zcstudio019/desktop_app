from __future__ import annotations

from typing import Any

from .evidence import clean_amount, clean_value, split_numbered_blocks, value_after_label
from .schema import GUARANTEE_FIELDS, ensure_record_fields


def extract_guarantees(sections: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        text = str(sections.get("guarantees") or "")
        records: list[dict[str, Any]] = []
        for block in split_numbered_blocks(text):
            if "担保" not in block:
                continue
            record = {
                "guarantee_for": value_after_label(block, ("被担保人", "担保对象", "为")),
                "guarantee_amount": clean_amount(value_after_label(block, ("担保金额", "担保本金"))),
                "guarantee_balance": clean_amount(value_after_label(block, ("担保余额", "余额"))),
                "guarantee_status": value_after_label(block, ("状态", "担保状态")),
                "evidence_text": clean_value(block[:800]),
            }
            records.append(ensure_record_fields(record, GUARANTEE_FIELDS))
        return records
    except Exception:
        return []
