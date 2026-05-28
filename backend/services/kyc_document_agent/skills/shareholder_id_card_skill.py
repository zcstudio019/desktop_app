from __future__ import annotations

from typing import Any

from .id_card_skill import extract as _extract_id_card


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    result = _extract_id_card(payload)
    result["doc_type"] = "shareholder_id_card"
    result["doc_type_name"] = "股东身份证"
    return result
