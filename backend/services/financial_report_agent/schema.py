from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


def to_plain_dict(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    if hasattr(model, "dict"):
        return model.dict()
    return dict(model or {})


class EvidenceItem(BaseModel):
    field_path: str = ""
    source_file: str = ""
    source_page: int | None = None
    table_name: str = ""
    row_label: str = ""
    column_label: str = ""
    raw_text: str = ""
    confidence: float = 0.0


class AmountField(BaseModel):
    raw_value: str = ""
    normalized_value: float | None = None
    previous_raw_value: str = ""
    previous_normalized_value: float | None = None
    current_value: float | None = None
    compare_value: float | None = None
    current_column_label: str = ""
    previous_column_label: str = ""
    source_page: int | None = None
    source_text: str = ""
    confidence: float = 0.0


class CompanyInfo(BaseModel):
    document_type: str = "financial_report"
    accounting_standard: str = "unknown"
    report_type: Literal["annual", "quarterly", "monthly", "unknown"] = "unknown"
    report_period_start: str = ""
    report_period_end: str = ""
    company_name: str = ""
    taxpayer_id: str = ""
    report_date: str = ""
    currency: str = "CNY"
    unit: str = "元"


class BalanceSheet(BaseModel):
    cash_and_equivalents: AmountField = Field(default_factory=AmountField)
    trading_financial_assets: AmountField = Field(default_factory=AmountField)
    notes_receivable: AmountField = Field(default_factory=AmountField)
    accounts_receivable: AmountField = Field(default_factory=AmountField)
    receivables_financing: AmountField = Field(default_factory=AmountField)
    prepayments: AmountField = Field(default_factory=AmountField)
    other_receivables: AmountField = Field(default_factory=AmountField)
    inventory: AmountField = Field(default_factory=AmountField)
    current_assets_total: AmountField = Field(default_factory=AmountField)
    long_term_equity_investment: AmountField = Field(default_factory=AmountField)
    fixed_assets: AmountField = Field(default_factory=AmountField)
    construction_in_progress: AmountField = Field(default_factory=AmountField)
    intangible_assets: AmountField = Field(default_factory=AmountField)
    long_term_prepaid_expenses: AmountField = Field(default_factory=AmountField)
    non_current_assets_total: AmountField = Field(default_factory=AmountField)
    total_assets: AmountField = Field(default_factory=AmountField)
    short_term_loans: AmountField = Field(default_factory=AmountField)
    notes_payable: AmountField = Field(default_factory=AmountField)
    accounts_payable: AmountField = Field(default_factory=AmountField)
    advance_receipts: AmountField = Field(default_factory=AmountField)
    contract_liabilities: AmountField = Field(default_factory=AmountField)
    employee_benefits_payable: AmountField = Field(default_factory=AmountField)
    taxes_payable: AmountField = Field(default_factory=AmountField)
    other_payables: AmountField = Field(default_factory=AmountField)
    current_liabilities_total: AmountField = Field(default_factory=AmountField)
    non_current_liabilities_due_within_one_year: AmountField = Field(default_factory=AmountField)
    long_term_loans: AmountField = Field(default_factory=AmountField)
    non_current_liabilities_total: AmountField = Field(default_factory=AmountField)
    total_liabilities: AmountField = Field(default_factory=AmountField)
    paid_in_capital: AmountField = Field(default_factory=AmountField)
    capital_reserve: AmountField = Field(default_factory=AmountField)
    surplus_reserve: AmountField = Field(default_factory=AmountField)
    undistributed_profit: AmountField = Field(default_factory=AmountField)
    total_equity: AmountField = Field(default_factory=AmountField)
    total_liabilities_and_equity: AmountField = Field(default_factory=AmountField)


class IncomeStatement(BaseModel):
    revenue: AmountField = Field(default_factory=AmountField)
    operating_cost: AmountField = Field(default_factory=AmountField)
    taxes_and_surcharges: AmountField = Field(default_factory=AmountField)
    selling_expenses: AmountField = Field(default_factory=AmountField)
    admin_expenses: AmountField = Field(default_factory=AmountField)
    rd_expenses: AmountField = Field(default_factory=AmountField)
    finance_expenses: AmountField = Field(default_factory=AmountField)
    interest_expense: AmountField = Field(default_factory=AmountField)
    interest_income: AmountField = Field(default_factory=AmountField)
    other_income: AmountField = Field(default_factory=AmountField)
    investment_income: AmountField = Field(default_factory=AmountField)
    operating_profit: AmountField = Field(default_factory=AmountField)
    non_operating_income: AmountField = Field(default_factory=AmountField)
    non_operating_expense: AmountField = Field(default_factory=AmountField)
    total_profit: AmountField = Field(default_factory=AmountField)
    income_tax_expense: AmountField = Field(default_factory=AmountField)
    net_profit: AmountField = Field(default_factory=AmountField)
    comprehensive_income_total: AmountField = Field(default_factory=AmountField)


class CashFlowStatement(BaseModel):
    cash_received_from_sales: AmountField = Field(default_factory=AmountField)
    tax_refund_received: AmountField = Field(default_factory=AmountField)
    other_cash_received_related_to_operating: AmountField = Field(default_factory=AmountField)
    operating_cash_inflow_total: AmountField = Field(default_factory=AmountField)
    cash_paid_for_goods_services: AmountField = Field(default_factory=AmountField)
    cash_paid_to_employees: AmountField = Field(default_factory=AmountField)
    taxes_paid: AmountField = Field(default_factory=AmountField)
    other_cash_paid_related_to_operating: AmountField = Field(default_factory=AmountField)
    operating_cash_outflow_total: AmountField = Field(default_factory=AmountField)
    net_operating_cash_flow: AmountField = Field(default_factory=AmountField)
    cash_received_from_investment_recovery: AmountField = Field(default_factory=AmountField)
    investment_income_cash_received: AmountField = Field(default_factory=AmountField)
    cash_received_from_disposal_assets: AmountField = Field(default_factory=AmountField)
    investing_cash_inflow_total: AmountField = Field(default_factory=AmountField)
    cash_paid_for_fixed_intangible_assets: AmountField = Field(default_factory=AmountField)
    cash_paid_for_investments: AmountField = Field(default_factory=AmountField)
    investing_cash_outflow_total: AmountField = Field(default_factory=AmountField)
    net_investing_cash_flow: AmountField = Field(default_factory=AmountField)
    cash_received_from_investors: AmountField = Field(default_factory=AmountField)
    cash_received_from_borrowings: AmountField = Field(default_factory=AmountField)
    financing_cash_inflow_total: AmountField = Field(default_factory=AmountField)
    cash_paid_for_debt_repayment: AmountField = Field(default_factory=AmountField)
    cash_paid_for_dividends_profit_interest: AmountField = Field(default_factory=AmountField)
    financing_cash_outflow_total: AmountField = Field(default_factory=AmountField)
    net_financing_cash_flow: AmountField = Field(default_factory=AmountField)
    net_cash_increase: AmountField = Field(default_factory=AmountField)
    beginning_cash_balance: AmountField = Field(default_factory=AmountField)
    ending_cash_balance: AmountField = Field(default_factory=AmountField)


class EquityChangeStatement(BaseModel):
    beginning_equity: AmountField = Field(default_factory=AmountField)
    owner_contributions: AmountField = Field(default_factory=AmountField)
    profit_distribution: AmountField = Field(default_factory=AmountField)
    comprehensive_income: AmountField = Field(default_factory=AmountField)
    ending_equity: AmountField = Field(default_factory=AmountField)


class FinancialRatios(BaseModel):
    asset_liability_ratio: float | None = None
    debt_to_equity_ratio: float | None = None
    current_ratio: float | None = None
    quick_ratio: float | None = None
    cash_ratio: float | None = None
    interest_bearing_debt: float | None = None
    short_debt_cash_coverage: float | None = None
    gross_profit: float | None = None
    gross_margin: float | None = None
    operating_margin: float | None = None
    net_margin: float | None = None
    expense_ratio: float | None = None
    operating_cash_flow_to_revenue: float | None = None
    sales_cash_collection_ratio: float | None = None
    operating_cash_flow_to_short_term_debt: float | None = None
    financing_dependence: float | None = None
    ar_turnover: float | None = None
    inventory_turnover: float | None = None
    total_asset_turnover: float | None = None


class RiskFinding(BaseModel):
    code: str
    risk_level: Literal["low", "medium", "medium_high", "high"] = "medium"
    title: str
    description: str = ""
    evidence: list[str] = Field(default_factory=list)
    suggestion: str = ""


class MissingMaterialSuggestion(BaseModel):
    material: str
    reason: str = ""
    priority: Literal["low", "medium", "high"] = "medium"


class BankCreditAnalysis(BaseModel):
    overall_risk_level: Literal["low", "medium", "medium_high", "high"] = "low"
    credit_view: str = ""
    positive_factors: list[str] = Field(default_factory=list)
    negative_factors: list[str] = Field(default_factory=list)
    key_bank_questions: list[str] = Field(default_factory=list)
    missing_materials: list[MissingMaterialSuggestion] = Field(default_factory=list)
    suggested_credit_strategy: str = ""
    risk_findings: list[RiskFinding] = Field(default_factory=list)


class FinancialReportExtractionResult(BaseModel):
    document_type: str = "financial_report"
    source_file: str = ""
    customer_id: str = ""
    company_info: CompanyInfo = Field(default_factory=CompanyInfo)
    balance_sheet: BalanceSheet = Field(default_factory=BalanceSheet)
    income_statement: IncomeStatement = Field(default_factory=IncomeStatement)
    cash_flow_statement: CashFlowStatement = Field(default_factory=CashFlowStatement)
    equity_change_statement: EquityChangeStatement | None = None
    financial_ratios: FinancialRatios = Field(default_factory=FinancialRatios)
    bank_credit_analysis: BankCreditAnalysis = Field(default_factory=BankCreditAnalysis)
    evidence: list[EvidenceItem] = Field(default_factory=list)
    validation_warnings: list[str] = Field(default_factory=list)
    trend_metrics: list[dict[str, Any]] = Field(default_factory=list)
    report_markdown: str = ""
