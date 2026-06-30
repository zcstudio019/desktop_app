from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


DOC_TYPE = "contract"
DOC_TYPE_NAME = "合同"
SCHEMA_VERSION = "contract_agent.v1"

CONTRACT_CATEGORY_NAMES = {
    "construction_subcontract": "建设工程专业分包合同",
    "material_purchase": "物资采购合同",
    "consulting_service": "咨询服务合同",
    "unknown_contract": "其他合同",
}


@dataclass(slots=True)
class ContractParty:
    role: str = ""
    name: str = ""
    unified_social_credit_code: str = ""
    legal_representative: str = ""
    contact: str = ""
    phone: str = ""
    address: str = ""
    bank_name: str = ""
    bank_account: str = ""
    taxpayer_id: str = ""
    stamp_status: str = ""


@dataclass(slots=True)
class ContractResult:
    doc_type: str = DOC_TYPE
    doc_type_name: str = DOC_TYPE_NAME
    agent_type: str = "contract_agent"
    owner_type: str = "company"
    schema_version: str = SCHEMA_VERSION
    extraction_status: str = "partial"
    contract_category: str = "unknown_contract"
    contract_category_name: str = "其他合同"
    title: str = ""
    project_name: str = ""
    contract_no: str = ""
    source_file: str = ""
    original_status: str = "可查看"
    page_count: int = 0
    signing_date: str = ""
    signing_place: str = ""
    effective_condition: str = ""
    copies: str = ""
    parties: list[ContractParty] = field(default_factory=list)
    project: dict[str, Any] = field(default_factory=dict)
    amount: dict[str, Any] = field(default_factory=dict)
    duration: dict[str, Any] = field(default_factory=dict)
    payment_nodes: list[dict[str, Any]] = field(default_factory=list)
    settlement: dict[str, Any] = field(default_factory=dict)
    line_items: list[dict[str, Any]] = field(default_factory=list)
    line_item_summary: dict[str, Any] = field(default_factory=dict)
    clauses: dict[str, Any] = field(default_factory=dict)
    signature: dict[str, Any] = field(default_factory=dict)
    quality: dict[str, Any] = field(default_factory=dict)
    validation: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    markdown: str = ""
    display_markdown: str = ""

    def structured_data_dict(self) -> dict[str, Any]:
        data = asdict(self)
        for key in ("markdown", "display_markdown", "evidence"):
            data.pop(key, None)
        return data

    def to_dict(self) -> dict[str, Any]:
        markdown = self.display_markdown or self.markdown
        return {
            "doc_type": self.doc_type,
            "doc_type_name": self.doc_type_name,
            "document_type_code": self.doc_type,
            "document_type_name": self.doc_type_name,
            "storage_label": self.doc_type_name,
            "agent_type": self.agent_type,
            "owner_type": self.owner_type,
            "schema_version": self.schema_version,
            "extraction_status": self.extraction_status,
            "contract_category": self.contract_category,
            "contract_category_name": self.contract_category_name,
            "structured_data": self.structured_data_dict(),
            "markdown_result": markdown,
            "display_markdown": markdown,
            "markdown": markdown,
            "report_markdown": markdown,
            "markdown_summary": markdown,
            "evidence": self.evidence,
            "warnings": self.warnings,
        }
