from __future__ import annotations

import json
import re
from typing import Any


DOC_TYPE = "bank_reconciliation_detail"
DOC_TYPE_NAME = "银行对账明细"

_DOC_TYPE_KEYS = (
    "doc_type",
    "type",
    "document_type",
    "document_type_code",
)
_MARKDOWN_KEYS = (
    "display_markdown",
    "markdown",
    "report_markdown",
)
_DIRTY_MARKERS = (
    "data：",
    "data:",
    "markdown：",
    "markdown:",
    "display markdown：",
    "display markdown:",
    "display_markdown",
    "report markdown：",
    "report markdown:",
    "report_markdown",
    "structured data：",
    "structured data:",
    "structured_data",
    "transactions：",
    "transactions:",
    "raw result：",
    "raw result:",
    "raw_result",
    "fields：",
    "fields:",
    "normalized data：",
    "normalized data:",
    "normalized_data",
)


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _records(value: Any) -> list[dict[str, Any]]:
    root = _as_dict(value)
    if not root:
        return []
    items = [
        root,
        _as_dict(root.get("data")),
        _as_dict(root.get("structured_data")),
        _as_dict(root.get("structuredData")),
        _as_dict(root.get("extracted_json")),
        _as_dict(root.get("extractedJson")),
        _as_dict(root.get("extracted_data")),
        _as_dict(root.get("extractedData")),
        _as_dict(root.get("result")),
        _as_dict(root.get("content")),
    ]
    return [item for item in items if item]


def _is_bank_reconciliation_detail(value: Any) -> bool:
    if isinstance(value, str):
        return "## 银行对账明细" in value
    for record in _records(value):
        for key in _DOC_TYPE_KEYS:
            if str(record.get(key) or "").strip() == DOC_TYPE:
                return True
    return False


def _monthly_counts(records: list[dict[str, Any]]) -> dict[str, str]:
    counts: dict[str, str] = {}
    for record in records:
        monthly = _as_dict(record.get("monthly"))
        for month, item in monthly.items():
            row = _as_dict(item)
            count = row.get("count") or row.get("transaction_count") or row.get("transactionCount")
            if re.fullmatch(r"\d{4}-\d{2}", str(month)) and count not in (None, ""):
                counts[str(month)] = str(count)
    return counts


def _is_dirty_markdown(markdown: str) -> bool:
    lowered = markdown.lower()
    return sum(1 for marker in _DIRTY_MARKERS if marker.lower() in lowered) >= 1 or markdown.count("## 银行对账明细") > 1


def _candidate_markdowns(result: Any) -> list[str]:
    records = _records(result)
    candidates: list[str] = []

    def add(value: Any) -> None:
        if isinstance(value, str) and value.strip():
            candidates.append(value.strip())

    root = records[0] if records else {}
    data = _as_dict(root.get("data"))
    structured_data = _as_dict(root.get("structured_data") or root.get("structuredData"))
    add(root.get("display_markdown"))
    add(data.get("display_markdown"))
    add(root.get("markdown"))
    add(root.get("report_markdown"))
    add(data.get("markdown"))
    add(data.get("report_markdown"))
    add(structured_data.get("display_markdown"))
    add(structured_data.get("markdown"))
    add(structured_data.get("report_markdown"))
    if isinstance(result, str):
        add(result)
    for record in records:
        for key in _MARKDOWN_KEYS:
            add(record.get(key))
    return list(dict.fromkeys(candidates))


def _extract_first_report(markdown: str) -> str:
    text = markdown.strip()
    start = text.find("## 银行对账明细")
    if start >= 0:
        text = text[start:]
    second = text.find("## 银行对账明细", len("## 银行对账明细"))
    if second >= 0:
        text = text[:second]
    marker_positions = [
        pos
        for marker in _DIRTY_MARKERS
        for pos in [text.lower().find(marker.lower())]
        if pos > 0
    ]
    if marker_positions:
        text = text[: min(marker_positions)]
    return text.strip()


def _cleanup_markdown(markdown: str, records: list[dict[str, Any]]) -> str:
    text = _extract_first_report(markdown)
    month_counts = _monthly_counts(records)
    forbidden_line = re.compile(
        r"^\s*[-*]?\s*(title|type|document\s*type|doc\s*type|doc\s*type\s*name|data|markdown|display\s*markdown|report\s*markdown|structured\s*data|structured_data|display_markdown|report_markdown|transactions)\s*[:：]",
        re.IGNORECASE,
    )
    cleaned_lines: list[str] = []
    for raw_line in text.splitlines():
        if forbidden_line.search(raw_line):
            continue
        line = raw_line.replace("{", "").replace("}", "").replace("[", "").replace("]", "")
        if re.match(r"^\|\s*\d{4}-\d{2}\s*\|", line):
            month_match = re.match(r"^\|\s*(\d{4}-\d{2})\s*\|", line)
            month = month_match.group(1) if month_match else ""
            closed = line.strip() if line.strip().endswith("|") else f"{line.strip()} |"
            cells = [cell.strip() for cell in closed.split("|")[1:-1]]
            if len(cells) == 4:
                line = f"{closed} {month_counts.get(month) or '0'} |"
            else:
                line = closed
        cleaned_lines.append(line)
    text = "\n".join(cleaned_lines)
    text = re.sub(r"\b(null|None|undefined|true|false)\b", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if not text:
        text = "## 银行对账明细\n\n- 提取状态：成功\n- 展示结果：暂无可展示内容"
    return text


def sanitize_bank_reconciliation_detail_display(result: Any) -> Any:
    """Return a display-only payload for bank_reconciliation_detail results."""
    if not _is_bank_reconciliation_detail(result):
        return result
    records = _records(result)
    candidates = _candidate_markdowns(result)
    markdown = next((item for item in candidates if "## 银行对账明细" in item and not _is_dirty_markdown(item)), "")
    if not markdown:
        markdown = next((item for item in candidates if "## 银行对账明细" in item), "")
    if not markdown:
        markdown = "## 银行对账明细\n\n- 提取状态：成功\n- 展示结果：暂无可展示内容"
    return {
        "doc_type": DOC_TYPE,
        "doc_type_name": DOC_TYPE_NAME,
        "display_markdown": _cleanup_markdown(markdown, records),
    }
