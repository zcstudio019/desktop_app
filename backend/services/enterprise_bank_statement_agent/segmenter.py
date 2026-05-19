from __future__ import annotations

import re
from typing import Any


def segment_bank_statement_text(text: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    metadata = metadata or {}
    raw_pages = metadata.get("raw_pages") if isinstance(metadata.get("raw_pages"), list) else []
    source = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    if raw_pages:
        pages = [
            {"page": item.get("page") or index + 1, "text": str(item.get("text") or "")}
            for index, item in enumerate(raw_pages)
            if isinstance(item, dict)
        ]
    else:
        pages = [{"page": 1, "text": source}]
    lines = []
    for page in pages:
        for raw_line in str(page.get("text") or "").split("\n"):
            line = re.sub(r"[ \t\u3000]+", " ", raw_line).strip()
            if line:
                lines.append({"page": page["page"], "text": line})
    return {"text": source, "pages": pages, "lines": lines, "metadata": metadata}
