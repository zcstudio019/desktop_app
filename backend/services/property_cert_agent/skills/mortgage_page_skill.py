from __future__ import annotations

from typing import Any

from .common import label_value


def extract(payload: dict[str, Any]) -> dict[str, Any]:
    text = str(payload.get("text") or "")
    fields: dict[str, Any] = {}
    for key in ("抵押权人", "被担保债权数额", "债务履行期限", "抵押登记"):
        value = label_value(text, (key,))
        if value:
            fields[key] = value
    if not fields and text.strip():
        fields["抵押信息"] = text.strip()[:1000]
    return {"fields": fields, "warnings": [], "page_role": "mortgage_page", "supplemental": True}
