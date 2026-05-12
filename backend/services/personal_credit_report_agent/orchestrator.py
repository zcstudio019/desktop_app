from __future__ import annotations

from typing import Any, Callable

from .extract_basic_info import extract_basic_info
from .extract_credit_card_accounts import extract_credit_card_accounts
from .extract_credit_summary import extract_credit_summary
from .extract_guarantees import extract_guarantees
from .extract_loan_accounts import extract_loan_accounts
from .extract_overdue_records import extract_overdue_records
from .extract_public_records import extract_public_records
from .extract_query_records import extract_query_records
from .markdown_renderer import render_personal_credit_markdown
from .normalizer import normalize_report_json
from .risk_analyzer import analyze_personal_credit_risk
from .schema import clone_default_report_json
from .segmenter import segment_report
from .validator import validate_report_json


def _safe_call(default: Any, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    try:
        value = func(*args, **kwargs)
        return default if value is None else value
    except Exception:
        return default


def _build_risk_flags(report: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    summary = report.get("credit_summary") or {}
    if summary.get("credit_card_overdue_count"):
        flags.append("存在信用卡逾期账户")
    if summary.get("credit_card_90d_overdue_count"):
        flags.append("存在90天以上信用卡逾期记录")
    if summary.get("housing_loan_overdue_count") or summary.get("other_loan_overdue_count"):
        flags.append("存在贷款逾期账户")
    if report.get("guarantees"):
        flags.append("存在担保信息")
    return flags


def _indicator_risk_flags(indicators: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    if indicators.get("has_current_overdue"):
        flags.append("存在当前逾期金额")
    if indicators.get("has_90d_overdue"):
        flags.append("存在90天以上逾期")
    if indicators.get("has_bad_debt_or_compensation"):
        flags.append("存在呆账/代偿/核销/强制执行记录")
    if indicators.get("high_frequency_query_flag"):
        flags.append("贷款审批查询频繁")
    if indicators.get("risk_level") == "medium" and "信用卡使用率过高" in (indicators.get("risk_reasons") or []):
        flags.append("信用卡使用率过高")
    return flags


def run_personal_credit_report_agent(text: str, source_file: str | None = None, debug: bool = False) -> dict[str, Any]:
    sections = _safe_call({"full_text": str(text or "")}, segment_report, text)
    report = clone_default_report_json()
    report["basic_info"] = _safe_call(report["basic_info"], extract_basic_info, sections, source_file)
    report["credit_summary"] = _safe_call(report["credit_summary"], extract_credit_summary, sections)
    report["loan_accounts"] = _safe_call([], extract_loan_accounts, sections)
    report["credit_card_accounts"] = _safe_call([], extract_credit_card_accounts, sections)
    report["guarantees"] = _safe_call([], extract_guarantees, sections)
    report["overdue_records"] = _safe_call([], extract_overdue_records, sections)
    report["public_records"] = _safe_call([], extract_public_records, sections)
    report["query_records"] = _safe_call([], extract_query_records, sections)
    report = normalize_report_json(report)
    indicators = _safe_call({}, analyze_personal_credit_risk, report)
    report["personal_credit_indicators"] = indicators if isinstance(indicators, dict) else {}
    indicator_warnings = report["personal_credit_indicators"].get("warnings") if isinstance(report["personal_credit_indicators"], dict) else []
    if isinstance(indicator_warnings, list):
        report["warnings"] = [*list(report.get("warnings") or []), *indicator_warnings]
    report["risk_flags"] = [*_build_risk_flags(report), *_indicator_risk_flags(report["personal_credit_indicators"])]
    report = normalize_report_json(report)
    warnings, missing_fields = validate_report_json(report)
    markdown = render_personal_credit_markdown(report)
    debug_payload: dict[str, Any] = {}
    if debug:
        debug_payload = {
            "section_keys": list(sections.keys()),
            "section_lengths": {key: len(value) for key, value in sections.items() if isinstance(value, str)},
        }
    return {
        "report_type": "personal_credit_report",
        "report_json": report,
        "report_markdown": markdown,
        "sections": sections,
        "debug": debug_payload,
        "warnings": warnings,
        "missing_fields": missing_fields,
    }
