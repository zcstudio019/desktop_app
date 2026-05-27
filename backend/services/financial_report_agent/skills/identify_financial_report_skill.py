from __future__ import annotations

from datetime import date
import logging
import re
from typing import Any

from ..normalizer import detect_unit
from ..schema import CompanyInfo

logger = logging.getLogger(__name__)


def _first(patterns: tuple[str, ...], text: str) -> str:
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip(" ：:，,")
    return ""


def _first_with_source(patterns: tuple[tuple[str, str], ...], text: str) -> tuple[str, str]:
    for source_name, pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip(" ：:，,"), source_name
    return "", "missing"


def _normalize_date(value: str) -> str:
    return str(value or "").replace("年", "-").replace("月", "-").replace("日", "").replace("/", "-").replace(".", "-")


def _period_report_type(start: str, end: str) -> tuple[str, str]:
    if not start or not end:
        return "unknown", ""
    try:
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end)
    except ValueError:
        return "unknown", ""
    if start_date.year == end_date.year and (start_date.month, start_date.day, end_date.month, end_date.day) == (1, 1, 12, 31):
        return "annual", "report_period_full_year"
    if start_date.day == 1 and (end_date.year - start_date.year) * 12 + end_date.month - start_date.month == 2:
        return "quarterly", "report_period_three_months"
    if start_date.day == 1 and start_date.year == end_date.year and start_date.month == end_date.month:
        return "monthly", "report_period_one_month"
    return "unknown", ""


def _accounting_standard(compact_source: str) -> tuple[str, str]:
    if "小企业会计准则" in compact_source or "适用执行小企业会计准则的企业" in compact_source:
        return "small_business_accounting_standard", "document_text_or_filename"
    if "企业会计准则一般企业" in compact_source:
        return "enterprise_accounting_standard", "document_text_or_filename"
    if "企业会计准则" in compact_source:
        return "business_accounting_standard", "document_text_or_filename"
    style_markers = (
        "销售产成品、商品、提供劳务收到的现金",
        "购买原材料、商品、接受劳务支付的现金",
        "短期投资",
        "固定资产原价",
        "固定资产账面价值",
    )
    style_count = sum(1 for marker in style_markers if marker in compact_source)
    uses_small_business_columns = "期末金额" in compact_source and "年初余额" in compact_source
    if style_count >= 1 or uses_small_business_columns:
        return "small_business_accounting_standard", "small_business_table_style"
    return "unknown", "unidentified"


def _history_company_value(
    metadata: dict[str, Any], field: str, company_name: str, *, allowed_values: set[str] | None = None
) -> tuple[str, str]:
    for report in reversed(metadata.get("historical_financial_reports") or []):
        if not isinstance(report, dict):
            continue
        info = report.get("company_info") or {}
        if not isinstance(info, dict):
            continue
        historical_name = str(info.get("company_name") or "").strip()
        if company_name and historical_name and historical_name != company_name:
            continue
        value = str(info.get(field) or "").strip()
        if value in {"", "-", "unknown"}:
            continue
        if allowed_values is not None and value not in allowed_values:
            continue
        return value, "fallback_from_same_customer_financial_reports"
    return "", "missing"


def identify_financial_report(text: str, filename: str = "", metadata: dict[str, Any] | None = None) -> CompanyInfo:
    metadata = metadata or {}
    source = f"{filename}\n{text}"
    compact_source = re.sub(r"\s+", "", source)
    unit, _ = detect_unit(source)
    standard, accounting_standard_source = _accounting_standard(compact_source)
    report_date_raw, report_date_source = _first_with_source(
        (
            ("document_submission_date", r"(?:报送日期|报告日期|报表日期|申报日期|填报日期|编制日期)\s*[:：]?\s*((?:20\d{2})[-年/.]\d{1,2}[-月/.]\d{1,2}日?)"),
            ("statement_date", r"(?:资产负债表日)\s*[:：]?\s*((?:20\d{2})[-年/.]\d{1,2}[-月/.]\d{1,2}日?)"),
        ),
        compact_source,
    )
    report_date = _normalize_date(report_date_raw)
    period_match = re.search(
        r"(?:税款所属期起止|税款所属期|税款所属时间)\s*[:：]?\s*"
        r"((?:20\d{2})[-年/.]\d{1,2}[-月/.]\d{1,2}日?)\s*(?:至|到|[-~—－])\s*"
        r"((?:20\d{2})[-年/.]\d{1,2}[-月/.]\d{1,2}日?)",
        compact_source,
    )
    if period_match:
        start = _normalize_date(period_match.group(1))
        end = _normalize_date(period_match.group(2))
    else:
        start = ""
        end = ""
    report_type, report_type_source = _period_report_type(start, end)
    if report_type == "unknown":
        if "季报" in compact_source or "季度" in compact_source:
            report_type, report_type_source = "quarterly", "document_text_or_filename"
        elif "月报" in compact_source or re.search(r"20\d{2}(?:0[1-9]|1[0-2])财务报表", compact_source):
            report_type, report_type_source = "monthly", "document_text_or_filename"
        elif "年报" in compact_source or "年度" in compact_source:
            report_type, report_type_source = "annual", "document_text_or_filename"
        else:
            report_type_source = "unidentified"
    year_match = re.search(r"(20\d{2})", filename or compact_source)
    year = year_match.group(1) if year_match else (report_date[:4] if report_date else "")
    if report_type == "annual" and year and not start:
        start, end = f"{year}-01-01", f"{year}-12-31"
    company_name = _first((r"(?:企业名称|公司名称|纳税人名称|编制单位)\s*[:：]\s*([^:：\n\r]+?有限公司|[^:：\n\r]+?公司)",), compact_source)
    if not company_name:
        company_name, _ = _history_company_value(metadata, "company_name", "")
    taxpayer_id, taxpayer_id_source = _first_with_source(
        (
            (
                "document_text",
                r"(?:纳税人识别号(?:[（(](?:国税|地税)[）)])?(?:/统一社会信用代码)?|统一社会信用代码)\s*[:：]?\s*([0-9A-Z]{12,30})",
            ),
        ),
        compact_source,
    )
    if not taxpayer_id:
        taxpayer_id, taxpayer_id_source = _history_company_value(metadata, "taxpayer_id", company_name)
    if standard == "unknown":
        standard, fallback_source = _history_company_value(
            metadata,
            "accounting_standard",
            company_name,
            allowed_values={
                "small_business_accounting_standard",
                "small_enterprise_accounting_standard",
                "enterprise_accounting_standard",
                "business_accounting_standard",
            },
        )
        if standard:
            if standard == "small_enterprise_accounting_standard":
                standard = "small_business_accounting_standard"
            accounting_standard_source = fallback_source
        else:
            standard = "unknown"
    info = CompanyInfo(
        accounting_standard=standard,
        report_type=report_type,
        report_period_start=start,
        report_period_end=end,
        company_name=company_name,
        taxpayer_id=taxpayer_id,
        report_date=report_date,
        currency="CNY",
        unit=unit,
    )
    logger.info(
        "[FinancialReportAgent][company_info_normalized] %s",
        {
            "source_file": filename,
            "company_name": info.company_name,
            "taxpayer_id": info.taxpayer_id,
            "taxpayer_id_source": taxpayer_id_source,
            "report_period_start": info.report_period_start,
            "report_period_end": info.report_period_end,
            "report_type": info.report_type,
            "report_type_source": report_type_source,
            "accounting_standard": info.accounting_standard,
            "accounting_standard_source": accounting_standard_source,
            "report_date": info.report_date,
            "report_date_source": report_date_source,
        },
    )
    return info
