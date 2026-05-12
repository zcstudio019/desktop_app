from __future__ import annotations

from typing import Any

from .evidence import clean_amount, clean_value, split_numbered_blocks, value_after_label
from .schema import OVERDUE_RECORD_FIELDS, ensure_record_fields


def extract_overdue_records(sections: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        text = "\n".join(str(sections.get(key) or "") for key in ("credit_summary", "credit_transaction_details"))
        records: list[dict[str, Any]] = []
        for block in split_numbered_blocks(text):
            if not any(keyword in block for keyword in ("逾期", "呆账", "止付", "冻结", "异常")):
                continue
            record = {
                "record_type": "逾期" if "逾期" in block else "异常",
                "institution": value_after_label(block, ("机构", "发卡机构", "贷款机构")),
                "amount": clean_amount(value_after_label(block, ("逾期金额", "金额", "余额"))),
                "months": value_after_label(block, ("逾期月份", "逾期月数", "月数")),
                "status": value_after_label(block, ("状态", "账户状态")),
                "evidence_text": clean_value(block[:800]),
            }
            records.append(ensure_record_fields(record, OVERDUE_RECORD_FIELDS))
        return records
    except Exception:
        return []
