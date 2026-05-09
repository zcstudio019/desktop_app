from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class ReportMeta:
    query_org: str = ""
    report_time: str = ""
    customer_name: str = ""
    unified_social_credit_code: str = ""


@dataclass(slots=True)
class CreditSummary:
    unsettled_credit_balance: float | None = None
    unsettled_credit_institution_count: int | None = None
    short_term_loan_balance: float | None = None
    medium_long_term_loan_balance: float | None = None
    credit_line_count: int | None = None
    external_guarantee_balance: float | None = None


@dataclass(slots=True)
class LoanRecord:
    institution_name: str = ""
    business_type: str = ""
    guarantee_type: str = ""
    loan_amount: float | None = None
    balance: float | None = None
    start_date: str = ""
    end_date: str = ""
    five_category: str = ""
    overdue_months: int = 0
    status: str = ""
    evidence_text: str = ""
    source_section: str = ""
    page_no: int | None = None
    confidence: float = 0.0


@dataclass(slots=True)
class CreditLineRecord:
    institution_name: str = ""
    credit_type: str = ""
    credit_revolving: bool | None = None
    credit_amount: float | None = None
    used_amount: float | None = None
    available_amount: float | None = None
    effective_date: str = ""
    expiry_date: str = ""
    status: str = ""
    evidence_text: str = ""
    source_section: str = "credit_lines"
    confidence: float = 0.0


@dataclass(slots=True)
class BusinessRecord:
    institution_name: str = ""
    business_type: str = ""
    five_category: str = ""
    account_count: int | None = None
    balance: float | None = None
    evidence_text: str = ""
    source_section: str = ""
    confidence: float = 0.0


@dataclass(slots=True)
class ValidationResult:
    is_valid: bool = False
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    reconciliation: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Confidence:
    overall: float = 0.0
    by_section: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class AgentResult:
    report_meta: ReportMeta = field(default_factory=ReportMeta)
    credit_summary: CreditSummary = field(default_factory=CreditSummary)
    short_term_loans: list[LoanRecord] = field(default_factory=list)
    medium_long_term_loans: list[LoanRecord] = field(default_factory=list)
    credit_lines: list[CreditLineRecord] = field(default_factory=list)
    bills: list[BusinessRecord] = field(default_factory=list)
    letters_of_credit: list[BusinessRecord] = field(default_factory=list)
    guarantees: list[BusinessRecord] = field(default_factory=list)
    external_guarantees: list[BusinessRecord] = field(default_factory=list)
    abnormal_records: list[dict[str, Any]] = field(default_factory=list)
    validation: ValidationResult = field(default_factory=ValidationResult)
    confidence: Confidence = field(default_factory=Confidence)
    raw_evidence_map: dict[str, str] = field(default_factory=dict)
    debug: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
