from __future__ import annotations

from typing import Any


def segment_pages(text: str, raw_pages: list[dict[str, Any]] | None = None, filename: str = "") -> list[dict[str, Any]]:
    pages = [page for page in (raw_pages or []) if isinstance(page, dict) and str(page.get("text") or "").strip()]
    if pages:
        return [
            {
                "page_index": int(page.get("page") or page.get("page_index") or index + 1),
                "filename": str(page.get("filename") or filename),
                "text": str(page.get("text") or ""),
                "metadata": page,
            }
            for index, page in enumerate(pages)
        ]
    return [{"page_index": 1, "filename": filename, "text": str(text or ""), "metadata": {}}]
