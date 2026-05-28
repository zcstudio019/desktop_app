from __future__ import annotations

from typing import Any

from .property_cert_skill import _extract_property


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    return _extract_property(payload, "real_estate_cert")
