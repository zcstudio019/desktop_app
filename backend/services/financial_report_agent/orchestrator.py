from __future__ import annotations

import logging
from decimal import Decimal
from pathlib import Path
from typing import Any

from .display_mapper import to_display_json
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

logger = logging.getLogger(__name__)


def _period(item: dict[str, Any]) -> str:
    return str((item.get("company_info") or {}).get("report_period_end") or "")


def _metric(item: dict[str, Any], section: str, field: str) -> float:
    return value_of((item.get(section) or {}).get(field) or {}) or 0.0


def _source_text_with_fallback(raw_text: str, text: str | None, metadata: dict[str, Any]) -> str:
    candidates: list[str] = [str(text if text is not None else raw_text or "")]
    for key in ("layout_text", "table_text", "ocr_text", "ocr_text_fallback", "page_text"):
        value = metadata.get(key)
        if isinstance(value, str):
            candidates.append(value)
    for item in metadata.get("raw_pages") or []:
        if isinstance(item, dict):
            candidates.append(str(item.get("text") or ""))
    collected: list[str] = []
    for candidate in candidates:
        candidate = candidate.strip()
        if candidate and candidate not in collected:
            collected.append(candidate)
    return "\n\n".join(collected)


def _non_empty_amount_count(section: Any) -> tuple[int, int]:
    payload = to_plain_dict(section)
    amount_fields = [item for item in payload.values() if isinstance(item, dict) and "normalized_value" in item]
    return len(amount_fields), sum(1 for item in amount_fields if item.get("normalized_value") is not None)


def _backfill_first_row_comparisons_from_history(
    balance: Any,
    cashflow: Any,
    history: list[dict[str, Any]],
) -> None:
    """Backfill missing first-row comparisons from the prior stored report."""
    if not history:
        return
    prior = sorted(history, key=_period)[-1]
    targets = (
        (balance, "balance_sheet", "cash_and_equivalents", "货币资金", "期末余额"),
        (cashflow, "cash_flow_statement", "cash_received_from_sales", "销售商品、提供劳务收到的现金", "本期金额"),
    )
    for section_model, prior_section, field_name, label, prior_column_label in targets:
        field = getattr(section_model, field_name, None)
        if field is None or field.normalized_value is None or field.previous_normalized_value is not None:
            continue
        prior_field = ((prior.get(prior_section) or {}).get(field_name) or {})
        prior_value = value_of(prior_field)
        if prior_value is None:
            continue
        prior_file = str(prior.get("source_file") or "上一期财务报表")
        field.previous_raw_value = f"{prior_value:,.2f}"
        field.previous_normalized_value = prior_value
        field.compare_value = prior_value
        field.previous_source = "fallback_from_previous_report"
        field.previous_source_text = f"由上一期财务报表{label}{prior_column_label}回填（{prior_file}）"
        if field.source_text:
            field.source_text = f"{field.source_text}；{field.previous_source_text}"
        else:
            field.source_text = field.previous_source_text


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
    source_text = _source_text_with_fallback(raw_text, text, metadata)
    raw_pages = metadata.get("raw_pages") if isinstance(metadata.get("raw_pages"), list) else None
    segmented = segment_financial_report(source_text, raw_pages, metadata)
    info = extract_company_info(segmented["full_text"], source_file, metadata)
    logger.info(
        "[FinancialReportAgent][DEBUG] company_info=%s",
        {
            "company_name": bool(info.company_name),
            "taxpayer_id": bool(info.taxpayer_id),
            "report_period_start": info.report_period_start,
            "report_period_end": info.report_period_end,
            "report_date": info.report_date,
            "report_type": info.report_type,
        },
    )
    _, multiplier = detect_unit(segmented["full_text"])
    balance, balance_evidence = extract_balance_sheet(segmented["sections"]["balance_sheet"], source_file, multiplier)
    history = [
        item for item in (metadata.get("historical_financial_reports") or [])
        if isinstance(item, dict)
    ]
    cashflow, cashflow_evidence = extract_cash_flow_statement(segmented["sections"]["cash_flow_statement"], source_file, multiplier)
    _backfill_first_row_comparisons_from_history(balance, cashflow, history)
    logger.info(
        "[DEBUG][cash_and_equivalents] balance_sheet.cash_and_equivalents=%s",
        to_plain_dict(balance.cash_and_equivalents),
    )
    total_fields, non_empty = _non_empty_amount_count(balance)
    logger.info("[FinancialReportAgent][DEBUG] balance_sheet_fields=%s non_empty=%s", total_fields, non_empty)
    income, income_evidence = extract_income_statement(segmented["sections"]["income_statement"], source_file, multiplier)
    total_fields, non_empty = _non_empty_amount_count(income)
    logger.info("[FinancialReportAgent][DEBUG] income_statement_fields=%s non_empty=%s", total_fields, non_empty)
    logger.info(
        "[DEBUG][cash_received_from_sales] cash_flow_statement.cash_received_from_sales=%s",
        to_plain_dict(cashflow.cash_received_from_sales),
    )
    total_fields, non_empty = _non_empty_amount_count(cashflow)
    logger.info("[FinancialReportAgent][DEBUG] cash_flow_fields=%s non_empty=%s", total_fields, non_empty)
    equity = None
    equity_evidence = []
    if any("所有者权益变动表" in page["text"] or "股东权益变动表" in page["text"] for page in segmented["pages"]):
        equity, equity_evidence = extract_equity_change_statement(segmented["sections"]["equity_change_statement"], source_file, multiplier)
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
    logger.info(
        "[FinancialReportAgent][DEBUG] financial_ratios_fields=%s non_empty=%s",
        len(data["financial_ratios"]),
        sum(1 for value in data["financial_ratios"].values() if value is not None),
    )
    risk = analyze_bank_credit_risk(data, history)
    data["bank_credit_analysis"] = to_plain_dict(risk)
    all_reports = history + [data]
    data["trend_metrics"] = aggregate_financial_report_periods(all_reports)
    data["validation_warnings"] = validate_financial_report_result(data)
    logger.info("[FinancialReportAgent][DEBUG] validation_warnings=%s", data["validation_warnings"])
    display_json = to_display_json(data)
    logger.info("[FinancialReportAgent][DEBUG] markdown_input_keys=%s", list(display_json.keys()))
    data["report_markdown"] = render_financial_report_markdown(display_json)
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
        "display_json": display_json,
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
