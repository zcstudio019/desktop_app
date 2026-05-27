from __future__ import annotations

from typing import Any
import re


TABLE_MARKERS = {
    "balance_sheet": ("资产负债表", "资产总计", "负债和所有者权益"),
    "income_statement": ("利润表", "营业收入", "净利润"),
    "cash_flow_statement": ("现金流量表", "经营活动产生的现金流量", "现金及现金等价物"),
    "equity_change_statement": ("所有者权益变动表", "股东权益变动表"),
}


def build_pages(
    raw_text: str,
    raw_pages: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    pages: list[dict[str, Any]] = []
    for index, page in enumerate(raw_pages or [], start=1):
        if isinstance(page, dict) and str(page.get("text") or "").strip():
            copied = dict(page)
            copied["page"] = int(page.get("page") or index)
            copied["text"] = str(page.get("text") or "")
            pages.append(copied)
    if not pages:
        pages = [{"page": 1, "text": str(raw_text or "")}]
    extra = metadata or {}
    for key in ("tables", "table_rows", "rows"):
        if key in extra and key not in pages[0]:
            pages[0][key] = extra[key]
    return pages


def segment_financial_report(
    raw_text: str,
    raw_pages: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pages = build_pages(raw_text, raw_pages, metadata)
    sections: dict[str, list[dict[str, Any]]] = {key: [] for key in TABLE_MARKERS}
    for page in pages:
        text = page["text"]
        compact_text = re.sub(r"\s+", "", text)
        for key, markers in TABLE_MARKERS.items():
            if any(marker in compact_text for marker in markers):
                sections[key].append(page)
    full_text = "\n".join(page["text"] for page in pages)
    for key in sections:
        if not sections[key]:
            sections[key] = pages
    return {"pages": pages, "full_text": full_text, "sections": sections}
