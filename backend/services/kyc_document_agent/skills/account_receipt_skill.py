from __future__ import annotations

from typing import Any

from .account_permit_skill import _extract_account


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    return _extract_account(payload, "account_receipt")
