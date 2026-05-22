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


def _infer_customer_name(customer_id: str | None, metadata: dict[str, Any]) -> str | None:
    for key in ("customer_name", "customerName", "company_name", "companyName"):
        value = metadata.get(key)
        if value:
            return str(value)
    if customer_id and customer_id.startswith("enterprise_"):
        return customer_id[len("enterprise_") :]
    return customer_id


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


def _build_internal_transfer_details(transactions: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    internal = [tx for tx in transactions if tx.get("is_internal_transfer")]
    details: list[dict[str, Any]] = []
    counterparties: dict[str, dict[str, Any]] = {}
    for tx in internal:
        amount = float(tx.get("credit_amount") or tx.get("debit_amount") or 0)
        direction = "inflow" if float(tx.get("credit_amount") or 0) > 0 else "outflow"
        name = str(tx.get("counterparty_name") or tx.get("payee_name") or "未知对手方")
        account = str(tx.get("counterparty_account") or tx.get("payee_account") or "")
        key = f"{name}|{account}"
        item = counterparties.setdefault(key, {"name": name, "account": account, "amount": 0.0, "count": 0})
        item["amount"] = round(float(item["amount"]) + amount, 2)
        item["count"] = int(item["count"]) + 1
        details.append(
            {
                "date": tx.get("transaction_date") or tx.get("post_date"),
                "direction": direction,
                "amount": round(amount, 2),
                "counterparty_name": tx.get("counterparty_name"),
                "counterparty_account": tx.get("counterparty_account"),
                "counterparty_bank": tx.get("counterparty_bank"),
                "payee_name": tx.get("payee_name"),
                "payee_account": tx.get("payee_account"),
                "purpose": tx.get("purpose"),
                "summary": tx.get("summary"),
                "reason": tx.get("nature_reason"),
                "confidence": tx.get("nature_confidence"),
            }
        )
    details.sort(key=lambda item: float(item.get("amount") or 0), reverse=True)
    summary = {
        "inflow_amount": round(sum(float(tx.get("credit_amount") or 0) for tx in internal), 2),
        "outflow_amount": round(sum(float(tx.get("debit_amount") or 0) for tx in internal), 2),
        "total_amount": round(sum(float(tx.get("credit_amount") or tx.get("debit_amount") or 0) for tx in internal), 2),
        "count": len(internal),
        "top_counterparties": sorted(counterparties.values(), key=lambda item: float(item.get("amount") or 0), reverse=True)[:10],
    }
    return summary, details[:200]


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
    customer_name = _infer_customer_name(customer_id, metadata)
    if customer_name and not metadata.get("customer_name"):
        metadata = {**metadata, "customer_name": customer_name}
    rows = metadata.get("rows") if isinstance(metadata.get("rows"), list) else None
    source_text = text if text is not None else raw_text
    source_file = filename or (Path(file_path).name if file_path else None)
    warnings: list[str] = []

    if file_path or rows:
        workbook = read_excel_workbook(file_path=file_path, rows=rows, filename=source_file)
        warnings.extend(workbook.get("warnings") or [])
        logger.info(
            "[EnterpriseFlow][Workbook] file=%s ext=%s sheet_count=%s",
            source_file or file_path or "",
            Path(file_path).suffix.lower() if file_path else "",
            len(workbook.get("sheets") or []),
        )
        for sheet in workbook.get("sheets") or []:
            meta = sheet.get("meta") or {}
            logger.info(
                "[EnterpriseFlow][Sheet] sheet=%s detected_bank=%s account_number=%s rows=%s",
                sheet.get("sheet_name") or meta.get("sheet_name") or "",
                meta.get("bank_name") or "",
                meta.get("account_number") or "",
                len(sheet.get("rows") or []),
            )
    else:
        workbook = {"source_file": source_file, "sheets": [], "warnings": ["未提供 Excel 文件路径，文本流水 fallback 暂返回稳定空结构"]}
        warnings.extend(workbook["warnings"])

    basic, accounts, basic_warnings = extract_account_basic_info(workbook, {**metadata, "customer_name": customer_name})
    warnings.extend(basic_warnings)
    transactions, tx_warnings = extract_transactions(workbook, accounts, {**metadata, "filename": source_file})
    warnings.extend(tx_warnings)
    transactions = classify_transactions(transactions, basic.get("company_name") or customer_name, metadata)
    months_count = (basic.get("statement_period") or {}).get("months_count")
    summary, accounts, summary_warnings = build_account_summary(transactions, accounts, months_count)
    warnings.extend(summary_warnings)
    for account in accounts:
        logger.info(
            "[EnterpriseFlow][SheetSummary] sheet=%s bank=%s inflow=%s outflow=%s tx_count=%s",
            account.get("sheet_name") or "",
            account.get("bank_name") or "",
            account.get("total_inflow") or 0,
            account.get("total_outflow") or 0,
            account.get("transaction_count") or 0,
        )
    _log_tailong_final_account(workbook, accounts, transactions, warnings)
    monthly_summary = analyze_monthly_trends(transactions)
    counterparty_summary = analyze_counterparties(transactions, summary.get("total_inflow", 0), summary.get("total_outflow", 0))
    large_transactions = detect_large_transactions(transactions, summary)
    loan_related_transactions = detect_loan_related_transactions(transactions)
    risk_analysis = detect_risk_signals(transactions, summary, monthly_summary, counterparty_summary, months_count)
    financing_view = build_financing_view(summary, risk_analysis)
    evidence = build_transaction_evidence(transactions)
    internal_transfer_summary, internal_transfer_transactions = _build_internal_transfer_details(transactions)

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
        internal_transfer_summary=internal_transfer_summary,
        internal_transfer_transactions=internal_transfer_transactions,
        evidence=[EvidenceItem(**item) for item in evidence],
        warnings=[],
    )
    extracted_json = to_plain_dict(data)
    validation_warnings = validate_enterprise_bank_statement_result(extracted_json)
    warnings.extend(validation_warnings)
    empty_result = not extracted_json.get("accounts") or all(
        float(account.get("total_inflow") or 0) == 0
        and float(account.get("total_outflow") or 0) == 0
        and int(account.get("transaction_count") or 0) == 0
        for account in extracted_json.get("accounts") or []
    )
    if empty_result:
        reason = "未识别到有效企业流水交易，请检查银行模板适配"
        warnings.append(reason)
        extracted_json["extraction_status"] = "failed"
        logger.warning("[EnterpriseFlow][ExtractionEmpty] file=%s reason=%s", source_file or file_path or "", reason)
    else:
        extracted_json["extraction_status"] = "success"
    extracted_json["warnings"] = list(dict.fromkeys(warnings))
    markdown = render_enterprise_bank_statement_markdown(extracted_json)
    if empty_result:
        markdown = "## 企业流水解析提示\n\n本文件未识别到有效交易明细，请检查银行模板适配或重新上传原始 Excel。\n\n" + markdown
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
