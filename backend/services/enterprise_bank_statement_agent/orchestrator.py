from __future__ import annotations

import re
from typing import Any

from .extraction_skills import (
    analyze_counterparties,
    analyze_monthly_trends,
    build_financing_summary,
    detect_large_transactions,
    detect_loan_related_transactions,
    detect_risk_signals,
    extract_account_basic_info,
    extract_or_derive_account_summary,
    extract_transactions,
)
from .markdown_renderer import render_enterprise_bank_statement_markdown
from .normalizer import normalize_transactions
from .schema import empty_enterprise_bank_statement_result
from .segmenter import segment_bank_statement_text
from .validator import validate_enterprise_bank_statement_result


def _confidence(result: dict[str, Any]) -> float:
    score = 0.35
    basic = result.get("account_basic_info") or {}
    if basic.get("company_name"):
        score += 0.1
    if basic.get("account_number"):
        score += 0.1
    if result.get("transactions"):
        score += 0.25
    if result.get("statement_summary", {}).get("total_transaction_count"):
        score += 0.1
    if result.get("warnings"):
        score -= min(0.2, len(result["warnings"]) * 0.03)
    return round(max(0.0, min(score, 0.95)), 2)


def _default_year(text: str) -> str | None:
    match = re.search(r"(20\d{2})", str(text or ""))
    return match.group(1) if match else None


def run_enterprise_bank_statement_agent(
    text: str,
    document_type: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = metadata or {}
    segments = segment_bank_statement_text(text, metadata)
    result = empty_enterprise_bank_statement_result()
    warnings: list[str] = []

    account_basic_info, basic_warnings, evidence = extract_account_basic_info(segments)
    warnings.extend(basic_warnings)

    raw_transactions, tx_warnings = extract_transactions(segments, account_basic_info)
    warnings.extend(tx_warnings)
    transactions = normalize_transactions(raw_transactions, default_year=_default_year(text))

    statement_summary, summary_warnings = extract_or_derive_account_summary(segments, transactions, account_basic_info)
    warnings.extend(summary_warnings)

    monthly_trends = analyze_monthly_trends(transactions)
    counterparties = analyze_counterparties(transactions)
    large_transactions = detect_large_transactions(transactions, statement_summary)
    loan_related_transactions = detect_loan_related_transactions(transactions)
    risk_signals = detect_risk_signals(transactions, monthly_trends, counterparties, loan_related_transactions)
    financing_analysis = build_financing_summary(
        statement_summary,
        monthly_trends,
        counterparties,
        risk_signals,
        loan_related_transactions,
    )

    result.update(
        {
            "document_type": "enterprise_bank_statement",
            "account_basic_info": account_basic_info,
            "statement_summary": statement_summary,
            "monthly_trends": monthly_trends,
            "transactions": transactions,
            "counterparty_analysis": counterparties,
            "large_transactions": large_transactions,
            "loan_related_transactions": loan_related_transactions,
            "risk_signals": risk_signals,
            "financing_analysis": financing_analysis,
            "evidence": evidence,
        }
    )
    validation_warnings = validate_enterprise_bank_statement_result(result)
    warnings.extend(validation_warnings)
    result["warnings"] = list(dict.fromkeys(warnings))
    markdown = render_enterprise_bank_statement_markdown(result)
    confidence = _confidence(result)
    return {
        "title": "企业银行流水解析结果",
        "type": "enterprise_bank_statement",
        "document_type_code": "enterprise_bank_statement",
        "schema_version": "enterprise_bank_statement.agent.v1",
        "skill_name": "enterprise_bank_statement_agent",
        "extracted_json": result,
        "markdown_summary": markdown,
        "markdown": markdown,
        "summary": markdown,
        "data": result,
        "confidence": confidence,
        "warnings": result["warnings"],
        "evidence": evidence,
        "raw_text_preview": str(text or "")[:3000],
    }
