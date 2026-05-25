from __future__ import annotations

import re
from typing import Any

from ..normalizer import detect_unit
from ..schema import CompanyInfo


def _first(patterns: tuple[str, ...], text: str) -> str:
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip(" ：:，,")
    return ""


def _normalize_date(value: str) -> str:
    return str(value or "").replace("年", "-").replace("月", "-").replace("日", "").replace("/", "-").replace(".", "-")


def identify_financial_report(text: str, filename: str = "", metadata: dict[str, Any] | None = None) -> CompanyInfo:
    source = f"{filename}\n{text}"
    unit, _ = detect_unit(source)
    if "小企业会计准则" in source:
        standard = "small_enterprise_accounting_standard"
    elif "企业会计准则" in source:
        standard = "enterprise_accounting_standard"
    else:
        standard = "unknown"
    if "季报" in source or "季度" in source:
        report_type = "quarterly"
    elif "月报" in source or re.search(r"20\d{2}(?:0[1-9]|1[0-2])财务报表", source):
        report_type = "monthly"
    elif "年报" in source or "年度" in source:
        report_type = "annual"
    else:
        report_type = "unknown"
    report_date = _normalize_date(_first(
        (
            r"(?:报送日期|报告日期|申报日期)\s*[:：]?\s*((?:20\d{2})[-年/.]\d{1,2}[-月/.]\d{1,2}日?)",
        ),
        source,
    ))
    period_match = re.search(
        r"(?:税款所属期起止|税款所属期|税款所属时间)\s*[:：]?\s*"
        r"((?:20\d{2})[-年/.]\d{1,2}[-月/.]\d{1,2}日?)\s*(?:至|到|[-~—－])\s*"
        r"((?:20\d{2})[-年/.]\d{1,2}[-月/.]\d{1,2}日?)",
        source,
    )
    if period_match:
        start = _normalize_date(period_match.group(1))
        end = _normalize_date(period_match.group(2))
    else:
        start = ""
        end = ""
    year_match = re.search(r"(20\d{2})", filename or source)
    year = year_match.group(1) if year_match else (report_date[:4] if report_date else "")
    if report_type == "annual" and year and not start:
        start, end = f"{year}-01-01", f"{year}-12-31"
    return CompanyInfo(
        accounting_standard=standard,
        report_type=report_type,
        report_period_start=start,
        report_period_end=end,
        company_name=_first((r"(?:企业名称|公司名称|纳税人名称)\s*[:：]\s*([^\n\r]+)",), text),
        taxpayer_id=_first((r"(?:纳税人识别号|统一社会信用代码)\s*[:：]\s*([0-9A-Z]{15,20})",), text),
        report_date=report_date,
        currency="CNY",
        unit=unit,
    )
