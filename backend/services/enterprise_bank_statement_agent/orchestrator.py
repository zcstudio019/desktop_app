from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .evidence import build_transaction_evidence
from .excel_reader import read_excel_workbook
from .extraction_skills import (
    analyze_counterparties,
    analyze_monthly_trends,
    build_account_summary,
    build_financing_view,
    classify_transactions,
    detect_large_transactions,
    detect_loan_related_transactions,
    detect_risk_signals,
    extract_account_basic_info,
    extract_transactions,
)
from .markdown_renderer import render_enterprise_bank_statement_markdown
from .schema import (
    BankAccountStatement,
    BankStatementRiskAnalysis,
    BankStatementSummary,
    BankTransaction,
    CounterpartySummary,
    EnterpriseBankStatementExtraction,
    EvidenceItem,
    FinancingView,
    MonthlyCashflowSummary,
    StatementPeriod,
    to_plain_dict,
)
from .validator import validate_enterprise_bank_statement_result


logger = logging.getLogger(__name__)

SUPPORTED_TYPES = {"enterprise_flow", "enterprise_bank_statement", "bank_statement_enterprise", "company_bank_statement", "企业流水", "银行流水"}


def _confidence(data: dict[str, Any]) -> float:
    score = 0.45
    if data.get("accounts"):
        score += 0.15
    if data.get("transactions"):
        score += 0.25
    if (data.get("summary") or {}).get("total_inflow", 0) > 0:
        score += 0.1
    if data.get("warnings"):
        score -= min(0.2, len(data["warnings"]) * 0.03)
    return round(max(0.0, min(score, 0.95)), 2)


def _stable_extraction(
    *,
    document_type: str,
    filename: str | None,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    model = EnterpriseBankStatementExtraction(
        document_type=document_type or "enterprise_flow",
        source_file=filename,
        warnings=warnings or [],
    )
    return to_plain_dict(model)


def _log_tailong_final_account(
    workbook: dict[str, Any],
    accounts: list[dict[str, Any]],
    transactions: list[dict[str, Any]],
    warnings: list[str],
) -> None:
    tailong_sheets = [
        sheet for sheet in workbook.get("sheets") or []
        if "泰隆" in str(sheet.get("sheet_name") or sheet.get("meta", {}).get("bank_name") or "")
    ]
    tailong_account = next(
        (
            account for account in accounts
            if "泰隆" in str(account.get("bank_name") or account.get("sheet_name") or "")
        ),
        None,
    )
    if not tailong_sheets and not tailong_account:
        return

    sheet = tailong_sheets[0] if tailong_sheets else {}
    meta = sheet.get("meta") or {}
    debug = sheet.get("debug") or {}
    tailong_transactions = [
        tx for tx in transactions
        if "泰隆" in str(tx.get("bank_name") or tx.get("sheet_name") or "")
    ]
    inflow_sum = round(sum(float(tx.get("credit_amount") or 0) for tx in tailong_transactions), 2)
    outflow_sum = round(sum(float(tx.get("debit_amount") or 0) for tx in tailong_transactions), 2)
    payload = {
        "bank_name": (tailong_account or meta).get("bank_name"),
        "sheet_name": (tailong_account or meta).get("sheet_name") or sheet.get("sheet_name"),
        "account_name": (tailong_account or meta).get("account_name"),
        "account_number": (tailong_account or meta).get("account_number"),
        "total_inflow": (tailong_account or {}).get("total_inflow"),
        "total_outflow": (tailong_account or {}).get("total_outflow"),
        "inflow_count": (tailong_account or {}).get("inflow_count"),
        "outflow_count": (tailong_account or {}).get("outflow_count"),
        "transaction_count": (tailong_account or {}).get("transaction_count") or len(tailong_transactions),
        "header_summary": debug.get("header_summary") or {
            "total_inflow": meta.get("summary_inflow"),
            "total_outflow": meta.get("summary_outflow"),
            "inflow_count": meta.get("summary_inflow_count"),
            "outflow_count": meta.get("summary_outflow_count"),
        },
        "detected_columns": debug.get("detected_columns"),
        "column_mapping": debug.get("column_mapping"),
        "inflow_sum_from_transactions": inflow_sum,
        "outflow_sum_from_transactions": outflow_sum,
        "warnings": [item for item in warnings if "泰隆" in str(item)],
    }
    logger.info("[EnterpriseFlow][Tailong][FINAL_ACCOUNT] %s", payload)


def run_enterprise_bank_statement_agent(
    file_path: str | None = None,
    filename: str | None = None,
    text: str | None = None,
    raw_text: str | None = None,
    document_type: str = "enterprise_flow",
    customer_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = metadata or {}
    rows = metadata.get("rows") if isinstance(metadata.get("rows"), list) else None
    source_text = text if text is not None else raw_text
    source_file = filename or (Path(file_path).name if file_path else None)
    warnings: list[str] = []

    if file_path or rows:
        workbook = read_excel_workbook(file_path=file_path, rows=rows, filename=source_file)
        warnings.extend(workbook.get("warnings") or [])
    else:
        workbook = {"source_file": source_file, "sheets": [], "warnings": ["未提供 Excel 文件路径，文本流水 fallback 暂返回稳定空结构"]}
        warnings.extend(workbook["warnings"])

    basic, accounts, basic_warnings = extract_account_basic_info(workbook, {**metadata, "customer_name": metadata.get("customer_name")})
    warnings.extend(basic_warnings)
    transactions, tx_warnings = extract_transactions(workbook, accounts, {**metadata, "filename": source_file})
    warnings.extend(tx_warnings)
    transactions = classify_transactions(transactions, basic.get("company_name"), metadata)
    months_count = (basic.get("statement_period") or {}).get("months_count")
    summary, accounts, summary_warnings = build_account_summary(transactions, accounts, months_count)
    warnings.extend(summary_warnings)
    _log_tailong_final_account(workbook, accounts, transactions, warnings)
    monthly_summary = analyze_monthly_trends(transactions)
    counterparty_summary = analyze_counterparties(transactions, summary.get("total_inflow", 0), summary.get("total_outflow", 0))
    large_transactions = detect_large_transactions(transactions, summary)
    loan_related_transactions = detect_loan_related_transactions(transactions)
    risk_analysis = detect_risk_signals(transactions, summary, monthly_summary, counterparty_summary, months_count)
    financing_view = build_financing_view(summary, risk_analysis)
    evidence = build_transaction_evidence(transactions)

    data = EnterpriseBankStatementExtraction(
        document_type=document_type or "enterprise_flow",
        normalized_document_type="enterprise_bank_statement",
        company_name=basic.get("company_name"),
        source_file=source_file,
        statement_period=StatementPeriod(**(basic.get("statement_period") or {})),
        accounts=[BankAccountStatement(**item) for item in accounts],
        transactions=[BankTransaction(**item) for item in transactions],
        summary=BankStatementSummary(**summary),
        monthly_summary=[MonthlyCashflowSummary(**item) for item in monthly_summary],
        counterparty_summary=CounterpartySummary(**counterparty_summary),
        risk_analysis=BankStatementRiskAnalysis(**risk_analysis),
        financing_view=FinancingView(**financing_view),
        evidence=[EvidenceItem(**item) for item in evidence],
        warnings=[],
    )
    extracted_json = to_plain_dict(data)
    validation_warnings = validate_enterprise_bank_statement_result(extracted_json)
    warnings.extend(validation_warnings)
    extracted_json["warnings"] = list(dict.fromkeys(warnings))
    markdown = render_enterprise_bank_statement_markdown(extracted_json)
    confidence = _confidence(extracted_json)
    return {
        "title": "企业流水分析报告",
        "type": document_type or "enterprise_flow",
        "document_type": document_type or "enterprise_flow",
        "document_type_code": document_type or "enterprise_flow",
        "normalized_document_type": "enterprise_bank_statement",
        "schema_version": "enterprise_bank_statement.agent.v2",
        "skill_name": "enterprise_bank_statement_agent",
        "extracted_json": extracted_json,
        "markdown_summary": markdown,
        "markdown": markdown,
        "summary": markdown,
        "data": extracted_json,
        "confidence": confidence,
        "warnings": extracted_json["warnings"],
        "evidence": extracted_json.get("evidence") or [],
        "large_transactions": large_transactions,
        "loan_related_transactions": loan_related_transactions,
        "raw_text_preview": str(source_text or "")[:3000],
    }
