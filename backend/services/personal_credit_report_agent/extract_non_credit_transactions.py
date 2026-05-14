from __future__ import annotations

import re
from typing import Any

from .evidence import clean_amount, clean_value
from .schema import NON_CREDIT_TRANSACTION_FIELDS, ensure_record_fields


STOP_TITLES = (
    "公共记录",
    "公共信息",
    "查询记录",
    "查询记录明细",
    "机构查询记录",
    "本人查询记录",
    "说明",
    "本人声明",
    "异议标注",
)


def _section_text(sections: dict[str, Any], text: str) -> str:
    section = str(sections.get("non_credit_transactions") or "")
    if section:
        return section
    source = str(text or sections.get("full_text") or "")
    start = source.find("非信贷交易记录")
    if start < 0:
        return ""
    tail = source[start:]
    stop_positions = [tail.find(title) for title in STOP_TITLES if tail.find(title) > 0]
    end = min(stop_positions) if stop_positions else len(tail)
    return tail[:end]


def _compact(text: str) -> str:
    source = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    source = re.sub(r"[ \t\u3000]+", " ", source)
    source = re.sub(r"\n+", " ", source)
    return clean_value(source)


def extract_non_credit_transactions(sections: dict[str, Any], text: str) -> list[dict[str, Any]]:
    try:
        section = _section_text(sections, text)
        if not section:
            return []
        compact = _compact(section)
        compact = re.sub(r"^非信贷交易记录\s*", "", compact)
        records: list[dict[str, Any]] = []
        no_record_match = re.search(r"系统中没有您最近\s*5\s*年内的非信贷交易记录", compact)
        if no_record_match:
            evidence = "系统中没有您最近5年内的非信贷交易记录。"
            return [
                ensure_record_fields(
                    {
                        "record_type": "系统中没有您最近5年内的非信贷交易记录",
                        "date": "",
                        "institution": "",
                        "amount": "",
                        "content": "",
                        "evidence": evidence,
                    },
                    NON_CREDIT_TRANSACTION_FIELDS,
                )
            ]
        for block in re.split(r"\n\s*\n+|(?=\d+[\.、])", section):
            block = _compact(block)
            if not block or "非信贷" not in block:
                continue
            record = {
                "record_type": clean_value(block.split("，", 1)[0]),
                "date": "",
                "institution": "",
                "amount": clean_amount(""),
                "content": clean_value(block),
                "evidence": clean_value(block[:800]),
            }
            records.append(ensure_record_fields(record, NON_CREDIT_TRANSACTION_FIELDS))
        return records
    except Exception:
        return []
