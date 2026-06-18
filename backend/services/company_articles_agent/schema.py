from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any


DOC_TYPE = "company_articles"
DOC_TYPE_NAME = "公司章程"
SCHEMA_VERSION = "company_articles_v10_multi_contribution_months"


STRUCTURED_DATA_KEYS = (
    "title",
    "company_name",
    "company_address",
    "business_scope",
    "registered_capital",
    "registered_capital_amount",
    "currency",
    "shareholders",
    "capital_check",
    "governance",
    "major_resolution_rules",
    "equity_transfer_summary",
    "finance_and_profit_summary",
    "dissolution_and_liquidation_summary",
    "senior_management_obligations_summary",
    "articles_effective_rule",
    "signature_info",
    "page_count",
    "warnings",
)


@dataclass(slots=True)
class Shareholder:
    name: str = ""
    subscribed_amount: str = ""
    subscribed_amount_number: float | None = None
    contribution_method: str = ""
    contribution_deadline: str = ""
    contribution_ratio: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CompanyArticlesResult:
    doc_type: str = DOC_TYPE
    doc_type_name: str = DOC_TYPE_NAME
    agent_type: str = "company_articles_agent"
    schema_version: str = SCHEMA_VERSION
    extraction_status: str = "partial"
    title: str = ""
    company_name: str = ""
    company_address: str = ""
    business_scope: str = ""
    registered_capital: str = ""
    registered_capital_amount: float | None = None
    currency: str = "人民币"
    shareholders: list[Shareholder] = field(default_factory=list)
    capital_check: dict[str, Any] = field(default_factory=dict)
    governance: dict[str, str] = field(default_factory=dict)
    major_resolution_rules: dict[str, str] = field(default_factory=dict)
    equity_transfer_summary: str = ""
    finance_and_profit_summary: str = ""
    dissolution_and_liquidation_summary: str = ""
    senior_management_obligations_summary: str = ""
    articles_effective_rule: str = ""
    signature_info: dict[str, Any] = field(default_factory=dict)
    page_count: int = 0
    warnings: list[str] = field(default_factory=list)
    markdown: str = ""
    display_markdown: str = ""
    raw_text_preview: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def structured_data_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["shareholders"] = [item.to_dict() if isinstance(item, Shareholder) else item for item in self.shareholders]
        return {key: data.get(key) for key in STRUCTURED_DATA_KEYS}

    def to_dict(self) -> dict[str, Any]:
        markdown = self.display_markdown or self.markdown
        structured_data = self.structured_data_dict()
        data = {
            "doc_type": self.doc_type,
            "doc_type_name": self.doc_type_name,
            "document_type_code": self.doc_type,
            "document_type_name": self.doc_type_name,
            "storage_label": self.doc_type_name,
            "schema_version": self.schema_version,
            "extraction_version": SCHEMA_VERSION,
            "extraction_status": self.extraction_status,
            "structured_data": structured_data,
            "display_markdown": markdown,
            "markdown": markdown,
            "report_markdown": markdown,
            "markdown_summary": markdown,
        }
        return data
