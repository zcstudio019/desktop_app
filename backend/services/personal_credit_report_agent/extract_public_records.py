from __future__ import annotations

from typing import Any

from .evidence import clean_amount, clean_value, split_numbered_blocks, value_after_label
from .schema import PUBLIC_RECORD_FIELDS, ensure_record_fields


def extract_public_records(sections: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        text = str(sections.get("public_records") or "")
        records: list[dict[str, Any]] = []
        for block in split_numbered_blocks(text):
            if not any(keyword in block for keyword in ("欠税", "民事", "强制执行", "行政处罚", "低保", "公积金", "公共")):
                continue
            record = {
                "record_type": value_after_label(block, ("记录类型", "类型")) or clean_value(block.splitlines()[0] if block.splitlines() else ""),
                "record_date": value_after_label(block, ("日期", "发生日期")),
                "content": value_after_label(block, ("内容", "说明")),
                "amount": clean_amount(value_after_label(block, ("金额", "标的"))),
                "authority": value_after_label(block, ("机关", "法院", "机构")),
                "evidence_text": clean_value(block[:800]),
            }
            records.append(ensure_record_fields(record, PUBLIC_RECORD_FIELDS))
        return records
    except Exception:
        return []
