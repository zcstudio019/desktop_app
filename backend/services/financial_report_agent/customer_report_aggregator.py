from __future__ import annotations

from typing import Any

from .orchestrator import aggregate_financial_report_periods
from .risk_analyzer import analyze_financial_credit_risk


FINANCIAL_REPORT_TYPES = {"financial_report", "financial_data", "财务报表", "财务数据"}


def collect_financial_reports(extractions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for extraction in extractions or []:
        if str(extraction.get("extraction_type") or extraction.get("document_type") or "") not in FINANCIAL_REPORT_TYPES:
            continue
        payload = extraction.get("extracted_data") or extraction
        report = payload.get("structured_json") or payload.get("extracted_json") or payload.get("data") or payload
        if isinstance(report, dict) and report.get("balance_sheet"):
            reports.append(report)
    return sorted(reports, key=lambda item: str((item.get("company_info") or {}).get("report_period_end") or ""))


def aggregate_customer_financial_reports(extractions: list[dict[str, Any]]) -> dict[str, Any]:
    reports = collect_financial_reports(extractions)
    if not reports:
        return {"reports": [], "trend_metrics": [], "latest_credit_analysis": {}}
    latest = reports[-1]
    prior = reports[:-1]
    return {
        "reports": reports,
        "trend_metrics": aggregate_financial_report_periods(reports),
        "latest_credit_analysis": analyze_financial_credit_risk(latest, prior),
    }
