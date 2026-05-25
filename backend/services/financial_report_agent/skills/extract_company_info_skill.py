from __future__ import annotations

from typing import Any

from ..schema import CompanyInfo
from .identify_financial_report_skill import identify_financial_report


def extract_company_info(text: str, filename: str = "", metadata: dict[str, Any] | None = None) -> CompanyInfo:
    info = identify_financial_report(text, filename, metadata)
    metadata = metadata or {}
    if not info.company_name:
        info.company_name = str(metadata.get("company_name") or metadata.get("customer_name") or "")
    return info
