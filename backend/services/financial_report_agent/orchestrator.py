from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

from .markdown_renderer import render_financial_report_markdown
from .normalizer import detect_unit, value_of
from .schema import FinancialReportExtractionResult, to_plain_dict
from .segmenter import segment_financial_report
from .skills import (
    analyze_bank_credit_risk,
    calculate_financial_ratios,
    extract_balance_sheet,
    extract_cash_flow_statement,
    extract_company_info,
    extract_equity_change_statement,
    extract_income_statement,
)
from .validator import validate_financial_report_result


def _period(item: dict[str, Any]) -> str:
    return str((item.get("company_info") or {}).get("report_period_end") or "")


def _metric(item: dict[str, Any], section: str, field: str) -> float:
    return value_of((item.get(section) or {}).get(field) or {}) or 0.0


def aggregate_financial_report_periods(reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "period": _period(item),
            "revenue": _metric(item, "income_statement", "revenue"),
            "net_profit": _metric(item, "income_statement", "net_profit"),
            "net_operating_cash_flow": _metric(item, "cash_flow_statement", "net_operating_cash_flow"),
            "total_assets": _metric(item, "balance_sheet", "total_assets"),
            "asset_liability_ratio": (item.get("financial_ratios") or {}).get("asset_liability_ratio"),
            "gross_margin": (item.get("financial_ratios") or {}).get("gross_margin"),
        }
        for item in sorted(reports, key=_period)
    ]


def run_financial_report_agent(
    *,
    raw_text: str = "",
    text: str | None = None,
    file_path: str | None = None,
    filename: str | None = None,
    customer_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = metadata or {}
    source_file = filename or (Path(file_path).name if file_path else "")
    source_text = str(text if text is not None else raw_text or "")
    raw_pages = metadata.get("raw_pages") if isinstance(metadata.get("raw_pages"), list) else None
    segmented = segment_financial_report(source_text, raw_pages)
    info = extract_company_info(segmented["full_text"], source_file, metadata)
    _, multiplier = detect_unit(segmented["full_text"])
    balance, balance_evidence = extract_balance_sheet(segmented["sections"]["balance_sheet"], source_file, multiplier)
    income, income_evidence = extract_income_statement(segmented["sections"]["income_statement"], source_file, multiplier)
    cashflow, cashflow_evidence = extract_cash_flow_statement(segmented["sections"]["cash_flow_statement"], source_file, multiplier)
    equity = None
    equity_evidence = []
    if any("所有者权益变动表" in page["text"] or "股东权益变动表" in page["text"] for page in segmented["pages"]):
        equity, equity_evidence = extract_equity_change_statement(segmented["sections"]["equity_change_statement"], source_file, multiplier)
    history = [
        item for item in (metadata.get("historical_financial_reports") or [])
        if isinstance(item, dict)
    ]
    prior = sorted(history, key=_period)[-1] if history else None
    provisional = FinancialReportExtractionResult(
        source_file=source_file,
        customer_id=customer_id or "",
        company_info=info,
        balance_sheet=balance,
        income_statement=income,
        cash_flow_statement=cashflow,
        equity_change_statement=equity,
        evidence=balance_evidence + income_evidence + cashflow_evidence + equity_evidence,
    )
    data = to_plain_dict(provisional)
    ratios = calculate_financial_ratios(data["balance_sheet"], data["income_statement"], data["cash_flow_statement"], prior)
    data["financial_ratios"] = to_plain_dict(ratios)
    risk = analyze_bank_credit_risk(data, history)
    data["bank_credit_analysis"] = to_plain_dict(risk)
    all_reports = history + [data]
    data["trend_metrics"] = aggregate_financial_report_periods(all_reports)
    data["validation_warnings"] = validate_financial_report_result(data)
    data["report_markdown"] = render_financial_report_markdown(data)
    core_values = [
        _metric(data, "balance_sheet", "total_assets"),
        _metric(data, "income_statement", "revenue"),
        _metric(data, "cash_flow_statement", "net_operating_cash_flow"),
    ]
    confidence = round(sum(1 for value in core_values if value != 0) / 3 * 0.9, 2)
    return {
        "title": "财务报表授信分析报告",
        "type": "financial_report",
        "document_type": "financial_report",
        "document_type_code": "financial_report",
        "schema_version": "financial_report.agent.v1",
        "skill_name": "financial_report_agent",
        "structured_json": data,
        "ratios_json": data["financial_ratios"],
        "risk_findings_json": data["bank_credit_analysis"]["risk_findings"],
        "markdown_report": data["report_markdown"],
        "evidence_json": data["evidence"],
        "validation_warnings": data["validation_warnings"],
        "extracted_json": data,
        "data": data,
        "markdown_summary": data["report_markdown"],
        "markdown": data["report_markdown"],
        "confidence": confidence,
        "warnings": data["validation_warnings"],
        "evidence": {"items": data["evidence"]},
    }
