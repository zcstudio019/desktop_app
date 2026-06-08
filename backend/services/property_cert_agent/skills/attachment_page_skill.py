from __future__ import annotations

from typing import Any


def extract(payload: dict[str, Any]) -> dict[str, Any]:
    text = str(payload.get("text") or "").strip()
    fields = {"附记": text[:1000]} if text else {}
    return {"fields": fields, "warnings": [], "page_role": "attachment_page", "supplemental": True}
