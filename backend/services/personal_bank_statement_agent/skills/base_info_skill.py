from __future__ import annotations

from typing import Any

from ..normalizer import normalize_text


def extract_owner_info(workbook: dict[str, Any], metadata: dict[str, Any] | None = None) -> dict[str, str]:
    metadata = metadata or {}
    name = normalize_text(metadata.get("customer_name") or metadata.get("customerName") or "")
    mobile = normalize_text(metadata.get("mobile") or metadata.get("phone") or "")
    id_no = normalize_text(metadata.get("id_no_masked") or metadata.get("id_number_masked") or "")
    if not name:
        for sheet in workbook.get("sheets") or []:
            meta = sheet.get("meta") or {}
            candidate = normalize_text(meta.get("account_name"))
            if candidate and "银行" not in candidate:
                name = candidate
                break
    return {"name": name, "id_no_masked": id_no, "mobile": mobile}
