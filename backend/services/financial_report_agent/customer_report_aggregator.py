from __future__ import annotations

import logging
import re
from typing import Any

from .display_mapper import to_display_json
from .markdown_renderer import render_financial_report_markdown
from .orchestrator import aggregate_financial_report_periods
from .risk_analyzer import analyze_financial_credit_risk


logger = logging.getLogger(__name__)
FINANCIAL_REPORT_TYPES = {"financial_report", "financial_data", "财务报表", "财务数据"}
EMPTY_VALUES = {"", "unknown", "未知", "-"}
STANDARD_LABELS = {
    "enterprise_accounting_standard": "企业会计准则一般企业",
    "small_enterprise_accounting_standard": "小企业会计准则",
    "small_business_accounting_standard": "小企业会计准则",
    "business_accounting_standard": "企业会计准则",
    "unknown": "-",
}


def _report_from_extraction(extraction: dict[str, Any]) -> dict[str, Any] | None:
    if str(extraction.get("extraction_type") or extraction.get("document_type") or "") not in FINANCIAL_REPORT_TYPES:
        return None
    payload = extraction.get("extracted_data") or extraction
    report = payload.get("structured_json") or payload.get("extracted_json") or payload.get("data") or payload
    return report if isinstance(report, dict) and report.get("balance_sheet") else None


def _period(report: dict[str, Any]) -> str:
    info = report.get("company_info") or {}
    return str(info.get("report_period_end") or info.get("report_date") or "")


def _non_empty(value: Any) -> bool:
    return str(value if value is not None else "").strip() not in EMPTY_VALUES


def pick_first_non_empty(reports: list[dict[str, Any]], paths: list[tuple[str, ...]]) -> tuple[Any, str]:
    """Pick the first non-empty value from newest to oldest reports."""
    for report in reversed(reports):
        file_name = str(report.get("source_file") or "财务报表")
        for path in paths:
            value: Any = report
            for part in path:
                value = value.get(part) if isinstance(value, dict) else None
            if _non_empty(value):
                return value, f"{file_name}:{'.'.join(path)}"
    return "", ""


def _fallback_text(report: dict[str, Any], extraction: dict[str, Any]) -> str:
    payload = extraction.get("extracted_data") or {}
    values = [
        report.get("report_markdown"),
        report.get("source_text"),
        report.get("extracted_text"),
        payload.get("report_markdown") if isinstance(payload, dict) else "",
        payload.get("markdown_report") if isinstance(payload, dict) else "",
        payload.get("markdown_summary") if isinstance(payload, dict) else "",
        payload.get("markdown") if isinstance(payload, dict) else "",
        payload.get("extracted_text") if isinstance(payload, dict) else "",
    ]
    return "\n".join(str(item) for item in values if item)


def _normalize_standard(value: Any) -> str:
    text = str(value or "").strip()
    if text in STANDARD_LABELS:
        return text
    if "小企业会计准则" in text:
        return "small_enterprise_accounting_standard"
    if "企业会计准则一般企业" in text:
        return "enterprise_accounting_standard"
    if "企业会计准则" in text:
        return "business_accounting_standard"
    return "unknown"


def _resolve_company_info(entries: list[tuple[dict[str, Any], dict[str, Any]]]) -> tuple[dict[str, Any], dict[str, str]]:
    reports = [report for report, _extraction in entries]
    latest = reports[-1] if reports else {}
    latest_info = dict(latest.get("company_info") or {})
    sources: dict[str, str] = {}

    for field in ("company_name", "currency", "unit", "report_period_start", "report_period_end", "report_type"):
        value, source = pick_first_non_empty(reports, [("company_info", field), (field,)])
        if _non_empty(value):
            latest_info[field] = value
            sources[field] = source

    taxpayer_id, source = pick_first_non_empty(reports, [("company_info", "taxpayer_id"), ("taxpayer_id",)])
    if not _non_empty(taxpayer_id):
        for report, extraction in reversed(entries):
            match = re.search(
                r"(?:纳税人识别号(?:[（(](?:国税|地税)[）)])?|统一社会信用代码)\s*[:：]\s*([A-Z0-9]{15,20})",
                _fallback_text(report, extraction),
            )
            if match:
                taxpayer_id = match.group(1)
                source = f"{report.get('source_file') or '财务报表'}:report_markdown"
                break
    latest_info["taxpayer_id"] = str(taxpayer_id).strip() if _non_empty(taxpayer_id) else ""
    sources["taxpayer_id"] = source or "missing"

    standard, source = pick_first_non_empty(reports, [("company_info", "accounting_standard"), ("accounting_standard",)])
    standard_code = _normalize_standard(standard)
    if standard_code == "unknown":
        for report, extraction in reversed(entries):
            file_text = str(report.get("source_file") or extraction.get("file_name") or "")
            body = f"{file_text}\n{_fallback_text(report, extraction)}"
            standard_code = _normalize_standard(body)
            if standard_code != "unknown":
                source = f"{report.get('source_file') or file_text or '财务报表'}:source_file/report_markdown"
                break
    latest_info["accounting_standard"] = standard_code
    sources["accounting_standard"] = source or "missing"

    report_date, source = pick_first_non_empty(
        reports,
        [
            ("company_info", "report_date"),
            ("report_date",),
            ("company_info", "balance_sheet_date"),
            ("balance_sheet_date",),
            ("company_info", "statement_date"),
            ("statement_date",),
        ],
    )
    if not _non_empty(report_date):
        for report, extraction in reversed(entries):
            match = re.search(r"(?:报送日期|资产负债表日|报表日期)\s*[:：]\s*(20\d{2}-\d{2}-\d{2})", _fallback_text(report, extraction))
            if match:
                report_date = match.group(1)
                source = f"{report.get('source_file') or '财务报表'}:report_markdown"
                break
    latest_info["report_date"] = str(report_date).strip() if _non_empty(report_date) else ""
    sources["report_date"] = source or "missing"
    return latest_info, sources


def collect_financial_reports(extractions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    reports = [report for extraction in extractions or [] if (report := _report_from_extraction(extraction))]
    return sorted(reports, key=_period)


def _current_amount(report: dict[str, Any], section: str, field: str) -> float | None:
    item = ((report.get(section) or {}).get(field) or {})
    value = item.get("normalized_value") if isinstance(item, dict) else None
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _backfill_report_field(
    report: dict[str, Any],
    prior: dict[str, Any],
    section: str,
    field: str,
    label: str,
    prior_column_label: str,
) -> bool:
    item = ((report.get(section) or {}).get(field) or {})
    if not isinstance(item, dict) or item.get("normalized_value") is None:
        return False
    if item.get("previous_normalized_value") is not None:
        return False
    previous = _current_amount(prior, section, field)
    if previous is None:
        return False
    prior_file = str(prior.get("source_file") or "上一期财务报表")
    source_note = f"由上一期财务报表{label}{prior_column_label}回填（{prior_file}）"
    item["previous_raw_value"] = f"{previous:,.2f}"
    item["previous_normalized_value"] = previous
    item["compare_value"] = previous
    item["previous_source"] = "fallback_from_previous_report"
    item["previous_source_text"] = source_note
    source_text = str(item.get("source_text") or "")
    item["source_text"] = f"{source_text}；{source_note}" if source_text else source_note
    return True


def backfill_financial_report_comparison_values(extractions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Repair legacy stored rows whose first comparison value was omitted."""
    entries = [
        (report, extraction)
        for extraction in extractions or []
        if (report := _report_from_extraction(extraction))
    ]
    entries.sort(key=lambda item: _period(item[0]))
    changed: list[dict[str, Any]] = []
    for index in range(1, len(entries)):
        report, extraction = entries[index]
        prior = entries[index - 1][0]
        updated = _backfill_report_field(report, prior, "balance_sheet", "cash_and_equivalents", "货币资金", "期末余额")
        updated = _backfill_report_field(
            report, prior, "cash_flow_statement", "cash_received_from_sales", "销售商品、提供劳务收到的现金", "本期金额"
        ) or updated
        if not updated:
            continue
        payload = extraction.get("extracted_data") or {}
        if isinstance(payload, dict):
            markdown = render_financial_report_markdown(to_display_json(report))
            report["report_markdown"] = markdown
            for key in ("structured_json", "extracted_json", "data"):
                candidate = payload.get(key)
                if isinstance(candidate, dict) and candidate.get("document_type") == "financial_report":
                    candidate.update(report)
            for key in ("report_markdown", "markdown_report", "markdown_summary", "markdown"):
                payload[key] = markdown
        changed.append(extraction)
        logger.info(
            "[FinancialReportSummary][comparison_backfill] source_file=%s previous_source=fallback_from_previous_report",
            report.get("source_file") or "",
        )
    return changed


def aggregate_customer_financial_reports(extractions: list[dict[str, Any]]) -> dict[str, Any]:
    backfill_financial_report_comparison_values(extractions)
    entries = [
        (report, extraction)
        for extraction in extractions or []
        if (report := _report_from_extraction(extraction))
    ]
    entries.sort(key=lambda item: _period(item[0]))
    reports = [report for report, _extraction in entries]
    if not reports:
        return {"reports": [], "trend_metrics": [], "latest_credit_analysis": {}, "company_info": {}}
    company_info, company_info_sources = _resolve_company_info(entries)
    latest = reports[-1]
    prior = reports[:-1]
    logger.info(
        "[FinancialReportSummary][company_info] %s",
        {
            "latest_report_file": latest.get("source_file") or "",
            "taxpayer_id_source": company_info_sources.get("taxpayer_id"),
            "taxpayer_id": company_info.get("taxpayer_id") or "-",
            "accounting_standard_source": company_info_sources.get("accounting_standard"),
            "accounting_standard": STANDARD_LABELS.get(str(company_info.get("accounting_standard")), "-"),
            "report_date_source": company_info_sources.get("report_date"),
            "report_date": company_info.get("report_date") or "-",
        },
    )
    return {
        "reports": reports,
        "trend_metrics": aggregate_financial_report_periods(reports),
        "latest_credit_analysis": analyze_financial_credit_risk(latest, prior),
        "company_info": company_info,
        "company_info_sources": company_info_sources,
    }
