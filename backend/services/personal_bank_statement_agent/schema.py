from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


def to_plain_dict(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    if hasattr(model, "dict"):
        return model.dict()
    return dict(model or {})


class StatementPeriod(BaseModel):
    start_date: str = ""
    end_date: str = ""


class Owner(BaseModel):
    name: str = ""
    id_no_masked: str = ""
    mobile: str = ""


class RawSummary(BaseModel):
    total_income: float = 0.0
    total_expense: float = 0.0
    income_count: int = 0
    expense_count: int = 0
    net_cash_flow: float = 0.0


class CleanSummary(BaseModel):
    salary_income: float = 0.0
    operating_income: float = 0.0
    other_stable_income: float = 0.0
    non_operating_income: float = 0.0
    internal_transfer_income: float = 0.0
    related_party_income: float = 0.0
    loan_inflow: float = 0.0
    refund_income: float = 0.0
    investment_transfer_income: float = 0.0
    living_expense: float = 0.0
    operating_expense: float = 0.0
    loan_repayment_expense: float = 0.0
    credit_card_repayment_expense: float = 0.0
    internal_transfer_expense: float = 0.0
    investment_expense: float = 0.0
    net_operating_cash_flow: float = 0.0


class Transaction(BaseModel):
    transaction_id: str = ""
    source_file: str = ""
    sheet_name: str = ""
    row_number: int | None = None
    account_no: str = ""
    transaction_date: str = ""
    summary: str = ""
    counterparty_name: str = ""
    counterparty_account: str = ""
    debit_amount: float = 0.0
    credit_amount: float = 0.0
    balance: float = 0.0
    direction: Literal["income", "expense", "unknown"] = "unknown"
    category: str = "other"
    is_internal_transfer: bool = False
    is_possible_internal_transfer: bool = False
    is_related_party: bool = False
    is_loan_inflow: bool = False
    is_salary: bool = False
    is_operating_income: bool = False
    is_credit_card_repayment: bool = False
    risk_tags: list[str] = Field(default_factory=list)
    evidence: str = ""


class Account(BaseModel):
    bank_name: str = ""
    account_name: str = ""
    account_no: str = ""
    currency: str = "人民币"
    statement_period: StatementPeriod = Field(default_factory=StatementPeriod)
    raw_summary: RawSummary = Field(default_factory=RawSummary)
    clean_summary: CleanSummary = Field(default_factory=CleanSummary)
    monthly_trend: list[dict[str, Any]] = Field(default_factory=list)
    top_income_counterparties: list[dict[str, Any]] = Field(default_factory=list)
    top_expense_counterparties: list[dict[str, Any]] = Field(default_factory=list)
    transactions: list[Transaction] = Field(default_factory=list)


class CustomerLevelSummary(BaseModel):
    account_count: int = 0
    period_start: str = ""
    period_end: str = ""
    raw_total_income: float = 0.0
    raw_total_expense: float = 0.0
    salary_income: float = 0.0
    operating_income: float = 0.0
    stable_income: float = 0.0
    internal_transfer_income: float = 0.0
    loan_inflow: float = 0.0
    net_operating_cash_flow: float = 0.0
    avg_monthly_income: float = 0.0
    avg_monthly_stable_income: float = 0.0
    income_stability_score: float = 0.0
    repayment_capacity_score: float = 0.0


class RiskSignal(BaseModel):
    code: str
    level: Literal["low", "medium", "high"] = "low"
    message: str = ""
    evidence: str = ""


class FinancingJudgement(BaseModel):
    income_quality: str = "无法判断"
    repayment_capacity: str = "无法判断"
    suspicious_flow_risk: str = "无法判断"
    recommended_usage: str = "仅供参考"
    missing_materials: list[str] = Field(default_factory=list)


class PersonalBankStatementExtraction(BaseModel):
    doc_type: str = "personal_flow"
    document_type: str = "personal_flow"
    normalized_document_type: str = "personal_bank_statement"
    source_file: str = ""
    owner: Owner = Field(default_factory=Owner)
    accounts: list[Account] = Field(default_factory=list)
    customer_level_summary: CustomerLevelSummary = Field(default_factory=CustomerLevelSummary)
    risk_signals: list[RiskSignal] = Field(default_factory=list)
    financing_judgement: FinancingJudgement = Field(default_factory=FinancingJudgement)
    extraction_status: str = "success"
    warnings: list[str] = Field(default_factory=list)
