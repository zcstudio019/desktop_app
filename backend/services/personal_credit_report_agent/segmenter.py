from __future__ import annotations

import re
import unicodedata
from typing import Any


SECTION_TITLES: dict[str, tuple[str, ...]] = {
    "report_basic_info": ("报告基础信息",),
    "personal_basic_info": ("个人基本信息",),
    "credit_summary": ("信贷记录概要", "信息概要", "信贷概要"),
    "credit_transaction_details": ("信贷交易信息明细",),
    "loan_accounts": ("贷款账户明细",),
    "credit_card_accounts": ("贷记卡账户明细", "准贷记卡账户明细"),
    "guarantees": ("担保信息",),
    "public_records": ("公共信息",),
    "query_records": ("查询记录", "机构查询记录明细", "本人查询记录明细"),
}

TITLE_TO_KEY = {
    title: key
    for key, titles in SECTION_TITLES.items()
    for title in titles
}


def normalize_text(text: str) -> str:
    source = unicodedata.normalize("NFKC", str(text or ""))
    source = source.replace("\r\n", "\n").replace("\r", "\n")
    source = re.sub(r"[ \t\u3000]+", " ", source)
    source = re.sub(r"\n{3,}", "\n\n", source)
    return source.strip()


def compact_title(text: str) -> str:
    return re.sub(r"[\s:：、.．\-—_]+", "", unicodedata.normalize("NFKC", text or ""))


def _find_title_positions(text: str) -> list[tuple[int, str, str]]:
    compact_targets = {compact_title(title): (key, title) for title, key in TITLE_TO_KEY.items()}
    positions: list[tuple[int, str, str]] = []
    offset = 0
    for line in text.splitlines(keepends=True):
        if not line.strip():
            offset += len(line)
            continue
        compact_line = compact_title(line)
        for compact, (key, title) in compact_targets.items():
            if compact and compact in compact_line:
                positions.append((offset, key, title))
                break
        offset += len(line)
    unique: list[tuple[int, str, str]] = []
    seen_positions: set[int] = set()
    for item in sorted(positions, key=lambda x: x[0]):
        if item[0] in seen_positions:
            continue
        seen_positions.add(item[0])
        unique.append(item)
    return unique


def segment_report(text: str) -> dict[str, Any]:
    normalized = normalize_text(text)
    sections: dict[str, Any] = {"full_text": normalized}
    for key in SECTION_TITLES:
        sections[key] = ""

    positions = _find_title_positions(normalized)
    if not positions:
        sections["report_basic_info"] = normalized[:4000]
        sections["personal_basic_info"] = normalized[:4000]
        sections["credit_summary"] = normalized
        sections["credit_transaction_details"] = normalized
        sections["query_records"] = normalized
        return sections

    for index, (start, key, _title) in enumerate(positions):
        end = positions[index + 1][0] if index + 1 < len(positions) else len(normalized)
        block = normalized[start:end].strip()
        if sections.get(key):
            sections[key] = f"{sections[key]}\n\n{block}".strip()
        else:
            sections[key] = block

    if not sections["report_basic_info"]:
        sections["report_basic_info"] = normalized[:4000]
    if not sections["personal_basic_info"]:
        sections["personal_basic_info"] = sections["report_basic_info"]
    if not sections["credit_transaction_details"]:
        sections["credit_transaction_details"] = normalized
    if not sections["query_records"]:
        sections["query_records"] = normalized
    return sections
