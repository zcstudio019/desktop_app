from __future__ import annotations

from typing import Any

from .schemas import AgentResult, ValidationResult


INVALID_INSTITUTION_NAMES = {"", "公司", "有限", "有限公司", "股份有限公司", "银行", "分行", "支行"}
FORBIDDEN_SHORT_KEYWORDS = ["融资型租赁", "融资租赁", "售后回租", "固定资产贷款", "项目贷款", "中长期"]


def _amount_sum(items: list[Any], attr: str) -> float:
    total = 0.0
    for item in items or []:
        value = getattr(item, attr, None)
        if isinstance(value, (int, float)):
            total += float(value)
    return round(total, 2)


def _loan_key(loan: Any) -> tuple[Any, ...]:
    return (
        getattr(loan, "institution_name", ""),
        getattr(loan, "business_type", ""),
        getattr(loan, "start_date", ""),
        getattr(loan, "end_date", ""),
        getattr(loan, "loan_amount", None),
        getattr(loan, "balance", None),
        getattr(loan, "guarantee_type", ""),
    )


def validate_agent_result(result: AgentResult, expected_counts: dict[str, int] | None = None) -> ValidationResult:
    expected_counts = expected_counts or {}
    errors: list[str] = []
    warnings: list[str] = []
    reconciliation: dict[str, Any] = {}

    short_count = len(result.short_term_loans)
    medium_count = len(result.medium_long_term_loans)
    credit_line_count = len(result.credit_lines)
    expected_short = expected_counts.get("short_term_loans") or 0
    expected_medium = expected_counts.get("medium_long_term_loans") or 0
    expected_credit_lines = expected_counts.get("credit_lines") or 0

    if expected_short and short_count != expected_short:
        warnings.append(f"short_term_count_mismatch: expected={expected_short}, actual={short_count}")
    if expected_medium and medium_count != expected_medium:
        warnings.append(f"medium_long_count_mismatch: expected={expected_medium}, actual={medium_count}")
    if expected_credit_lines and credit_line_count != expected_credit_lines:
        warnings.append(f"credit_line_count_mismatch: expected={expected_credit_lines}, actual={credit_line_count}")

    seen: set[tuple[Any, ...]] = set()
    duplicates = 0
    for loan in [*result.short_term_loans, *result.medium_long_term_loans]:
        key = _loan_key(loan)
        if key in seen:
            duplicates += 1
        seen.add(key)
        if (loan.institution_name or "").strip() in INVALID_INSTITUTION_NAMES:
            errors.append(f"invalid_institution_name: {loan.source_section} {loan.institution_name!r}")
        if any(keyword in loan.evidence_text for keyword in ["银行承兑汇票", "商业承兑汇票", "信用证", "保函"]):
            errors.append(f"non_loan_record_mixed_into_loan: {loan.source_section}")

    for loan in result.short_term_loans:
        if any(keyword in (loan.business_type or "") for keyword in FORBIDDEN_SHORT_KEYWORDS):
            errors.append(f"forbidden_business_in_short_term_loans: {loan.business_type}")

    if duplicates:
        warnings.append(f"duplicate_loan_records_detected: {duplicates}")

    short_sum = _amount_sum(result.short_term_loans, "balance")
    medium_sum = _amount_sum(result.medium_long_term_loans, "balance")
    reconciliation["short_term_detail_balance_sum"] = short_sum
    reconciliation["medium_long_detail_balance_sum"] = medium_sum
    if result.credit_summary.short_term_loan_balance is not None and abs(short_sum - result.credit_summary.short_term_loan_balance) > 1:
        warnings.append("short_term_balance_reconciliation_mismatch")
    if result.credit_summary.medium_long_term_loan_balance is not None and abs(medium_sum - result.credit_summary.medium_long_term_loan_balance) > 1:
        warnings.append("medium_long_balance_reconciliation_mismatch")
    if result.credit_summary.medium_long_term_loan_balance and not result.medium_long_term_loans:
        warnings.append("medium_long_term_balance_without_details")
    revolving_sum = _amount_sum(result.revolving_overdrafts, "balance")
    low_confidence_revolving = any(
        getattr(item, "warning", "") == "summary_balance_fallback_detail"
        or float(getattr(item, "confidence", 0) or 0) < 0.5
        for item in result.revolving_overdrafts
    )
    if result.credit_summary.revolving_overdraft_balance and not result.revolving_overdrafts:
        warnings.append("revolving_balance_without_details")
    elif result.credit_summary.revolving_overdraft_balance and low_confidence_revolving:
        warnings.append("revolving_detail_low_confidence")
    reconciliation["revolving_overdraft_detail_balance_sum"] = revolving_sum
    if result.credit_summary.revolving_overdraft_balance is not None and result.revolving_overdrafts:
        revolving_balance_match = abs(revolving_sum - float(result.credit_summary.revolving_overdraft_balance or 0)) <= 0.01
        reconciliation["revolving_balance_match"] = revolving_balance_match
        if not revolving_balance_match:
            warnings.append("revolving_balance_mismatch")

    all_evidence = "\n".join(
        [loan.evidence_text for loan in [*result.short_term_loans, *result.medium_long_term_loans]]
        + list((result.raw_evidence_map or {}).values())
    )
    has_webank_evidence = all(keyword in all_evidence for keyword in ["浙江网商银行股份有限公司", "流动资金贷款", "30", "保证", "5"])
    has_webank_short = any(
        "浙江网商银行股份有限公司" in (loan.institution_name or "")
        and loan.business_type == "流动资金贷款"
        and loan.loan_amount == 30
        and loan.balance == 5
        for loan in result.short_term_loans
    )
    if has_webank_evidence and not has_webank_short:
        warnings.append("missing_possible_short_term_loan: 浙江网商银行股份有限公司")

    return ValidationResult(
        is_valid=not errors,
        errors=errors,
        warnings=warnings,
        reconciliation=reconciliation,
    )
