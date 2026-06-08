from __future__ import annotations

from typing import Any


def build_evidence(fields: dict[str, Any], text: str, *, page_role: str = "") -> dict[str, Any]:
    preview = str(text or "")[:500]
    return {
        key: {
            "evidence_text": str(value),
            "page_role": page_role,
            "raw_text_preview": preview,
        }
        for key, value in (fields or {}).items()
        if value
    }
