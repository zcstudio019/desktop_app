from __future__ import annotations

from typing import Any


def make_evidence(field: str, value: Any, source_text: str, source_page: int | None = None) -> dict[str, Any]:
    return {
        "field": field,
        "value": value,
        "source_page": source_page,
        "source_text": str(source_text or "")[:500],
    }
