from __future__ import annotations

from typing import Any

from . import empty_extract


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    return empty_extract(payload, "special_business_license")
