from __future__ import annotations

import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

from .markdown_renderer import render_personal_bank_statement_markdown
from .normalizer import months_count, round2
from .schema import (
    Account,
    CleanSummary,
    CustomerLevelSummary,
    PersonalBankStatementExtraction,
    RawSummary,
    StatementPeriod,
    Transaction,
    to_plain_dict,
)
from .segmenter import read_personal_bank_statement_workbook
from .skills import (
    analyze_counterparties,
    analyze_expenses,
    analyze_income,
    build_financing_judgement,
    classify_transactions,
    detect_internal_transfers,
    detect_risk_signals,
    extract_account_info,
    extract_owner_info,
    extract_transactions,
)
from .validator import validate_personal_bank_statement_result

logger = logging.getLogger(__name__)

SUPPORTED_TYPES = {
    "personal_flow",
    "personal_bank_statement",
    "bank_statement_personal",
    "individual_bank_statement",
    "个人流水",
    "个人银行流水",
}


def _sum(transactions: list[dict[str, Any]], field: str, category: str | None = None) -> float:
    return round2(sum(float(tx.get(field) or 0) for tx in transactions if category is None or tx.get("category") == category))


def _build_monthly_trend(transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_month: dict[str, dict[str, float]] = defaultdict(lambda: {
        "raw_income": 0.0,
        "raw_expense": 0.0,
        "salary_income": 0.0,
        "operating_income": 0.0,
        "stable_income": 0.0,
        "loan_repayment": 0.0,
        "credit_card_repayment": 0.0,
    })
    for tx in transactions:
        date = str(tx.get("transaction_date") or "")
        month = date[:7] if len(date) >= 7 else "unknown"
        item = by_month[month]
        credit = float(tx.get("credit_amount") or 0)
        debit = float(tx.get("debit_amount") or 0)
        category = tx.get("category")
        item["raw_income"] += credit
        item["raw_expense"] += debit
        if category == "salary_income":
            item["salary_income"] += credit
            item["stable_income"] += credit
        elif category == "operating_income":
            item["operating_income"] += credit
            item["stable_income"] += credit
        elif category == "other_stable_income":
            item["stable_income"] += credit
        elif category == "loan_repayment":
            item["loan_repayment"] += debit
        elif category == "credit_card_repayment":
            item["credit_card_repayment"] += debit
    return [{"month": month, **{k: round2(v) for k, v in values.items()}} for month, values in sorted(by_month.items())]


def _build_clean_summary(transactions: list[dict[str, Any]]) -> dict[str, Any]:
    salary = _sum(transactions, "credit_amount", "salary_income")
    operating_income = _sum(transactions, "credit_amount", "operating_income")
    other_stable = _sum(transactions, "credit_amount", "other_stable_income")
    internal_income = _sum(transactions, "credit_amount", "internal_transfer")
    related_income = _sum(transactions, "credit_amount", "related_party_transfer")
    loan_inflow = _sum(transactions, "credit_amount", "loan_inflow")
    refund_income = _sum(transactions, "credit_amount", "refund")
    investment_income = _sum(transactions, "credit_amount", "investment_transfer")
    living = _sum(transactions, "debit_amount", "living_expense")
    operating_expense = _sum(transactions, "debit_amount", "operating_expense")
    loan_repayment = _sum(transactions, "debit_amount", "loan_repayment")
    credit_card = _sum(transactions, "debit_amount", "credit_card_repayment")
    internal_expense = _sum(transactions, "debit_amount", "internal_transfer")
    investment_expense = _sum(transactions, "debit_amount", "investment_transfer")
    stable = salary + operating_income + other_stable
    non_operating = round2(sum(float(tx.get("credit_amount") or 0) for tx in transactions if tx.get("direction") == "income" and tx.get("category") in {"other", "investment_transfer", "refund", "loan_inflow", "related_party_transfer"}))
    return {
        "salary_income": salary,
        "operating_income": operating_income,
        "other_stable_income": other_stable,
        "non_operating_income": non_operating,
        "internal_transfer_income": internal_income,
        "related_party_income": related_income,
        "loan_inflow": loan_inflow,
        "refund_income": refund_income,
        "investment_transfer_income": investment_income,
        "living_expense": living,
        "operating_expense": operating_expense,
        "loan_repayment_expense": loan_repayment,
        "credit_card_repayment_expense": credit_card,
        "internal_transfer_expense": internal_expense,
        "investment_expense": investment_expense,
        "net_operating_cash_flow": round2(stable - operating_expense - loan_repayment - credit_card),
    }


def _confidence(data: dict[str, Any]) -> float:
    score = 0.45
    if data.get("accounts"):
        score += 0.2
    if sum(len(item.get("transactions") or []) for item in data.get("accounts") or []) > 0:
        score += 0.25
    if data.get("risk_signals"):
        score += 0.02
    if data.get("warnings"):
        score -= min(0.2, len(data.get("warnings") or []) * 0.03)
    return round(max(0.0, min(score, 0.95)), 2)


def run_personal_bank_statement_agent(
    file_path: str | None = None,
    filename: str | None = None,
    text: str | None = None,
    raw_text: str | None = None,
    document_type: str = "personal_flow",
    customer_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = metadata or {}
    source_file = filename or (Path(file_path).name if file_path else "")
    warnings: list[str] = []
    try:
        workbook = read_personal_bank_statement_workbook(
            file_path=file_path,
            rows=metadata.get("rows") if isinstance(metadata.get("rows"), list) else None,
            filename=source_file,
        )
        warnings.extend(workbook.get("warnings") or [])
        owner = extract_owner_info(workbook, metadata)
        accounts_meta, period, account_warnings = extract_account_info(workbook, owner)
        warnings.extend(account_warnings)
        transactions, tx_warnings = extract_transactions(workbook, accounts_meta)
        warnings.extend(tx_warnings)
        transactions, internal_summary = detect_internal_transfers(transactions, accounts_meta)
        transactions = classify_transactions(transactions)
        income_analysis = analyze_income(transactions)
        expense_analysis = analyze_expenses(transactions)
        counterparty = analyze_counterparties(transactions)
        raw_income = _sum(transactions, "credit_amount")
        raw_expense = _sum(transactions, "debit_amount")
        clean = _build_clean_summary(transactions)
        monthly = _build_monthly_trend(transactions)
        observed_months = len([item for item in monthly if item.get("month") != "unknown"])
        month_count = months_count(period.get("start_date"), period.get("end_date"), observed_months)
        stable_income = round2(clean["salary_income"] + clean["operating_income"] + clean["other_stable_income"])
        customer_summary = {
            "account_count": len(accounts_meta),
            "period_start": period.get("start_date") or "",
            "period_end": period.get("end_date") or "",
            "raw_total_income": raw_income,
            "raw_total_expense": raw_expense,
            "salary_income": clean["salary_income"],
            "operating_income": clean["operating_income"],
            "stable_income": stable_income,
            "internal_transfer_income": clean["internal_transfer_income"],
            "loan_inflow": clean["loan_inflow"],
            "net_operating_cash_flow": clean["net_operating_cash_flow"],
            "avg_monthly_income": round2(raw_income / month_count),
            "avg_monthly_stable_income": round2(stable_income / month_count),
            "income_stability_score": round2(max(0, 100 - float(income_analysis.get("income_volatility") or 0) * 100)),
            "repayment_capacity_score": round2(min(100, (stable_income / max(1.0, clean["loan_repayment_expense"] + clean["credit_card_repayment_expense"])) * 50)) if stable_income else 0.0,
        }
        risk_signals = detect_risk_signals(customer_summary | clean, transactions, income_analysis, expense_analysis, month_count)
        judgement = build_financing_judgement(customer_summary | clean, risk_signals, month_count)
        by_account_no: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for tx in transactions:
            by_account_no[str(tx.get("account_no") or "")].append(tx)
        accounts = []
        for account in accounts_meta or [{"account_no": "", "bank_name": "", "account_name": owner.get("name") or "", "currency": "人民币", "statement_period": period}]:
            account_txs = by_account_no.get(str(account.get("account_no") or ""), transactions if len(accounts_meta) <= 1 else [])
            account_clean = _build_clean_summary(account_txs)
            account_raw_income = _sum(account_txs, "credit_amount")
            account_raw_expense = _sum(account_txs, "debit_amount")
            accounts.append(
                Account(
                    bank_name=account.get("bank_name") or "",
                    account_name=account.get("account_name") or owner.get("name") or "",
                    account_no=account.get("account_no") or "",
                    currency=account.get("currency") or "人民币",
                    statement_period=StatementPeriod(**(account.get("statement_period") or period)),
                    raw_summary=RawSummary(
                        total_income=account_raw_income,
                        total_expense=account_raw_expense,
                        income_count=sum(1 for tx in account_txs if float(tx.get("credit_amount") or 0) > 0),
                        expense_count=sum(1 for tx in account_txs if float(tx.get("debit_amount") or 0) > 0),
                        net_cash_flow=round2(account_raw_income - account_raw_expense),
                    ),
                    clean_summary=CleanSummary(**account_clean),
                    monthly_trend=_build_monthly_trend(account_txs),
                    top_income_counterparties=counterparty.get("top_income_counterparties") or [],
                    top_expense_counterparties=counterparty.get("top_expense_counterparties") or [],
                    transactions=[Transaction(**tx) for tx in account_txs],
                )
            )
        data = PersonalBankStatementExtraction(
            source_file=source_file,
            owner=owner,
            accounts=accounts,
            customer_level_summary=CustomerLevelSummary(**customer_summary),
            risk_signals=risk_signals,
            financing_judgement=judgement,
            warnings=[],
        )
        extracted_json = to_plain_dict(data)
        extracted_json["internal_transfer_summary"] = internal_summary
        extracted_json["income_analysis"] = income_analysis
        extracted_json["expense_analysis"] = expense_analysis
        validation_warnings = validate_personal_bank_statement_result(extracted_json)
        warnings.extend(validation_warnings)
        if validation_warnings and not transactions:
            extracted_json["extraction_status"] = "failed"
        extracted_json["warnings"] = list(dict.fromkeys(str(item) for item in warnings if item))
        markdown = render_personal_bank_statement_markdown(extracted_json)
    except Exception as exc:
        logger.exception("[PersonalFlow] extraction failed file=%s", source_file)
        extracted_json = to_plain_dict(
            PersonalBankStatementExtraction(
                source_file=source_file,
                extraction_status="failed",
                warnings=[f"个人流水解析失败：{exc}"],
            )
        )
        markdown = render_personal_bank_statement_markdown(extracted_json)
    return {
        "title": "个人流水分析报告",
        "type": "personal_flow",
        "document_type": "personal_flow",
        "document_type_code": "personal_flow",
        "normalized_document_type": "personal_bank_statement",
        "schema_version": "personal_bank_statement.agent.v1",
        "skill_name": "personal_bank_statement_agent",
        "extracted_json": extracted_json,
        "markdown_summary": markdown,
        "markdown": markdown,
        "summary": markdown,
        "data": extracted_json,
        "confidence": _confidence(extracted_json),
        "warnings": extracted_json.get("warnings") or [],
        "raw_text_preview": str(text if text is not None else raw_text or "")[:3000],
    }
