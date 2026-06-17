from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any


DOC_TYPE = "company_articles"
DOC_TYPE_NAME = "公司章程"
SCHEMA_VERSION = "company_articles.agent.v1"


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
    raw_text_preview: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["shareholders"] = [item.to_dict() if isinstance(item, Shareholder) else item for item in self.shareholders]
        data["document_type_code"] = self.doc_type
        data["document_type_name"] = self.doc_type_name
        data["storage_label"] = self.doc_type_name
        data["report_markdown"] = self.markdown
        data["markdown_summary"] = self.markdown
        return data
