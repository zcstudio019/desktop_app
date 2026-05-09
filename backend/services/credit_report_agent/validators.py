from __future__ import annotations

from typing import Any

from .schemas import AgentResult, ValidationResult


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
        warnings.append(f"短期借款笔数不一致：标题 {expected_short} 笔，明细识别 {short_count} 笔")
    if expected_medium and medium_count != expected_medium:
        warnings.append(f"中长期借款笔数不一致：标题 {expected_medium} 笔，明细识别 {medium_count} 笔")
    if expected_credit_lines and credit_line_count != expected_credit_lines:
        warnings.append(f"授信信息笔数不一致：标题 {expected_credit_lines} 笔，明细识别 {credit_line_count} 笔")

    seen: set[tuple[Any, ...]] = set()
    duplicates = 0
    for loan in [*result.short_term_loans, *result.medium_long_term_loans]:
        key = _loan_key(loan)
        if key in seen:
            duplicates += 1
        seen.add(key)
        if not loan.institution_name:
            warnings.append(f"{loan.source_section} 存在机构名称为空的贷款记录")
        if any(keyword in loan.evidence_text for keyword in ["银行承兑汇票", "信用证", "保函"]):
            errors.append(f"{loan.source_section} 疑似混入票据/信用证/保函记录")

    if duplicates:
        warnings.append(f"检测到 {duplicates} 条完全重复贷款业务")

    short_sum = _amount_sum(result.short_term_loans, "balance")
    medium_sum = _amount_sum(result.medium_long_term_loans, "balance")
    reconciliation["short_term_detail_balance_sum"] = short_sum
    reconciliation["medium_long_detail_balance_sum"] = medium_sum
    if result.credit_summary.short_term_loan_balance is not None and abs(short_sum - result.credit_summary.short_term_loan_balance) > 1:
        warnings.append("短期借款明细余额合计与概要余额不一致")
    if result.credit_summary.medium_long_term_loan_balance is not None and abs(medium_sum - result.credit_summary.medium_long_term_loan_balance) > 1:
        warnings.append("中长期借款明细余额合计与概要余额不一致")

    return ValidationResult(
        is_valid=not errors,
        errors=errors,
        warnings=warnings,
        reconciliation=reconciliation,
    )
