from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


def to_plain_dict(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    if hasattr(model, "dict"):
        return model.dict()
    return dict(model or {})


class StatementPeriod(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    months_count: Optional[int] = None


class BankAccountStatement(BaseModel):
    account_id: str
    bank_name: Optional[str] = None
    account_name: Optional[str] = None
    account_number: Optional[str] = None
    branch_name: Optional[str] = None
    currency: Optional[str] = None
    sheet_name: Optional[str] = None
    opening_balance: Optional[float] = None
    ending_balance: Optional[float] = None
    total_inflow: float = 0.0
    total_outflow: float = 0.0
    net_cashflow: float = 0.0
    transaction_count: int = 0
    inflow_count: int = 0
    outflow_count: int = 0


class BankTransaction(BaseModel):
    transaction_id: str
    source_file: Optional[str] = None
    sheet_name: Optional[str] = None
    row_number: Optional[int] = None
    account_id: Optional[str] = None
    bank_name: Optional[str] = None
    account_name: Optional[str] = None
    account_number: Optional[str] = None
    transaction_date: Optional[str] = None
    post_date: Optional[str] = None
    summary: Optional[str] = None
    purpose: Optional[str] = None
    counterparty_name: Optional[str] = None
    counterparty_account: Optional[str] = None
    counterparty_bank: Optional[str] = None
    payee_name: Optional[str] = None
    payee_account: Optional[str] = None
    debit_amount: Optional[float] = None
    credit_amount: Optional[float] = None
    balance: Optional[float] = None
    currency: Optional[str] = None
    direction: Literal["inflow", "outflow", "unknown"] = "unknown"
    normalized_amount: float = 0.0
    category: Optional[str] = None
    sub_category: Optional[str] = None
    is_internal_transfer: bool = False
    is_related_party: bool = False
    is_personal_counterparty: bool = False
    is_large_amount: bool = False
    is_suspicious: bool = False
    nature: Optional[str] = None
    exclude_from_operating: bool = False
    nature_reason: Optional[str] = None
    nature_confidence: Optional[float] = None
    tags: List[str] = Field(default_factory=list)
    raw: Dict[str, Any] = Field(default_factory=dict)


class BankStatementSummary(BaseModel):
    raw_total_inflow: float = 0.0
    raw_total_outflow: float = 0.0
    raw_net_cashflow: float = 0.0
    total_inflow: float = 0.0
    total_outflow: float = 0.0
    net_cashflow: float = 0.0
    transaction_count: int = 0
    inflow_count: int = 0
    outflow_count: int = 0
    account_count: int = 0
    bank_count: int = 0
    average_monthly_inflow: Optional[float] = None
    average_monthly_outflow: Optional[float] = None
    average_monthly_net_cashflow: Optional[float] = None
    max_single_inflow: Optional[float] = None
    max_single_outflow: Optional[float] = None
    low_balance_transaction_count: Optional[int] = None
    low_balance_threshold: float = 5000
    estimated_operating_inflow: Optional[float] = None
    estimated_operating_outflow: Optional[float] = None
    estimated_operating_net_cashflow: Optional[float] = None
    excluded_internal_transfer_amount: Optional[float] = None
    excluded_related_party_inflow: Optional[float] = None
    excluded_personal_inflow: Optional[float] = None
    internal_transfer_inflow: float = 0.0
    internal_transfer_outflow: float = 0.0
    related_party_inflow: float = 0.0
    related_party_outflow: float = 0.0
    personal_transfer_inflow: float = 0.0
    personal_transfer_outflow: float = 0.0
    operating_inflow: float = 0.0
    operating_outflow: float = 0.0
    operating_net_cashflow: float = 0.0


class MonthlyCashflowSummary(BaseModel):
    month: str
    inflow: float = 0.0
    outflow: float = 0.0
    net_cashflow: float = 0.0
    inflow_count: int = 0
    outflow_count: int = 0
    ending_balance: Optional[float] = None


class CounterpartyStat(BaseModel):
    name: str
    account: Optional[str] = None
    bank: Optional[str] = None
    inflow: float = 0.0
    outflow: float = 0.0
    net: float = 0.0
    amount: float = 0.0
    count: int = 0
    transaction_count: int = 0
    first_date: Optional[str] = None
    last_date: Optional[str] = None
    nature: Optional[str] = None
    exclude_from_operating: bool = False
    category_guess: Optional[str] = None
    is_related_party: bool = False
    is_personal_counterparty: bool = False
    is_internal_transfer: bool = False
    risk_note: Optional[str] = None


class CounterpartySummary(BaseModel):
    top_inflow_counterparties: List[CounterpartyStat] = Field(default_factory=list)
    top_outflow_counterparties: List[CounterpartyStat] = Field(default_factory=list)
    internal_transfer_counterparties: List[CounterpartyStat] = Field(default_factory=list)
    related_party_counterparties: List[CounterpartyStat] = Field(default_factory=list)
    personal_counterparties: List[CounterpartyStat] = Field(default_factory=list)
    customer_concentration_top5_ratio: Optional[float] = None
    supplier_concentration_top5_ratio: Optional[float] = None


class RiskSignal(BaseModel):
    code: str
    level: Literal["low", "medium", "high"]
    title: str
    description: str
    amount: Optional[float] = None
    ratio: Optional[float] = None
    evidence_refs: List[str] = Field(default_factory=list)
    suggestion: Optional[str] = None


class BankStatementRiskAnalysis(BaseModel):
    overall_level: Literal["low", "medium", "high"] = "low"
    overall_score: int = 0
    signals: List[RiskSignal] = Field(default_factory=list)
    strengths: List[str] = Field(default_factory=list)
    weaknesses: List[str] = Field(default_factory=list)


class FinancingView(BaseModel):
    bank_recognizable_inflow: Optional[float] = None
    adjusted_operating_inflow: Optional[float] = None
    excluded_internal_transfer_amount: Optional[float] = None
    excluded_related_party_inflow: Optional[float] = None
    excluded_personal_inflow: Optional[float] = None
    suggested_credit_products: List[str] = Field(default_factory=list)
    material_checklist: List[str] = Field(default_factory=list)
    bank_explanation: List[str] = Field(default_factory=list)
    conclusion: str = ""


class EvidenceItem(BaseModel):
    evidence_id: str
    source_file: Optional[str] = None
    sheet_name: Optional[str] = None
    row_number: Optional[int] = None
    field: Optional[str] = None
    value: Optional[Any] = None
    note: Optional[str] = None


class EnterpriseBankStatementExtraction(BaseModel):
    document_type: str = "enterprise_flow"
    normalized_document_type: str = "enterprise_bank_statement"
    company_name: Optional[str] = None
    source_file: Optional[str] = None
    statement_period: StatementPeriod = Field(default_factory=StatementPeriod)
    accounts: List[BankAccountStatement] = Field(default_factory=list)
    transactions: List[BankTransaction] = Field(default_factory=list)
    summary: BankStatementSummary = Field(default_factory=BankStatementSummary)
    monthly_summary: List[MonthlyCashflowSummary] = Field(default_factory=list)
    counterparty_summary: CounterpartySummary = Field(default_factory=CounterpartySummary)
    risk_analysis: BankStatementRiskAnalysis = Field(default_factory=BankStatementRiskAnalysis)
    financing_view: FinancingView = Field(default_factory=FinancingView)
    evidence: List[EvidenceItem] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
