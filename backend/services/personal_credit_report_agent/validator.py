from __future__ import annotations

import re
from typing import Any

ID_CARD_PATTERN = re.compile(r"^(?:[1-9]\d{5}(?:(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx]|\d{9}))$")


def _append_missing(missing: list[str], field: str, value: Any) -> None:
    if value in (None, ""):
        missing.append(field)


def _summary_count(summary: dict[str, Any], *keys: str) -> int:
    total = 0
    for key in keys:
        value = summary.get(key)
        if isinstance(value, int):
            total += value
        elif isinstance(value, str):
            match = re.search(r"\d+", value)
            if match:
                total += int(match.group(0))
    return total


def _first_summary_count(summary: dict[str, Any], *keys: str) -> int:
    for key in keys:
        count = _summary_count(summary, key)
        if count:
            return count
    return 0


def _contains_any(value: Any, keywords: tuple[str, ...]) -> bool:
    text = str(value or "")
    return any(keyword in text for keyword in keywords)


def _warn_once(warnings: list[str], warning: str) -> None:
    if warning not in warnings:
        warnings.append(warning)


def validate_report_json(report: dict[str, Any]) -> tuple[list[str], list[str]]:
    warnings = list(report.get("warnings") or [])
    missing_fields = list(report.get("missing_fields") or [])
    basic = report.get("basic_info") if isinstance(report.get("basic_info"), dict) else {}
    summary = report.get("credit_summary") if isinstance(report.get("credit_summary"), dict) else {}

    for field in ("report_number", "name", "id_number"):
        path = f"basic_info.{field}"
        if path not in missing_fields:
            _append_missing(missing_fields, path, basic.get(field))
    if not basic.get("id_number") or not ID_CARD_PATTERN.fullmatch(str(basic.get("id_number") or "")):
        _warn_once(warnings, "证件号码未识别或格式异常")
    if "报告时间" in str(basic.get("report_number") or ""):
        _warn_once(warnings, "报告编号中混入报告时间，已尝试自动拆分")

    basic_pollution_keywords = (
        "查询记录",
        "查询记录明细",
        "贷款账户明细",
        "贷记卡账户明细",
        "准贷记卡账户明细",
        "信贷记录概要",
        "机构",
        "中征码",
        "统一社会信用代码",
        "企业名称",
    )
    for field in ("name", "id_type", "id_number", "report_number", "report_time", "marital_status"):
        if _contains_any(basic.get(field), basic_pollution_keywords):
            _warn_once(warnings, f"basic_info_contaminated: {field}")

    if not isinstance(report.get("loan_accounts"), list):
        warnings.append("loan_accounts_not_array")
        report["loan_accounts"] = []
    if not isinstance(report.get("credit_card_accounts"), list):
        warnings.append("credit_card_accounts_not_array")
        report["credit_card_accounts"] = []
    if not isinstance(report.get("query_records"), list):
        warnings.append("query_records_not_array")
        report["query_records"] = []
    if not isinstance(report.get("related_repayment_responsibilities"), list):
        warnings.append("related_repayment_responsibilities_not_array")
        report["related_repayment_responsibilities"] = []
    if not isinstance(report.get("non_credit_transactions"), list):
        warnings.append("non_credit_transactions_not_array")
        report["non_credit_transactions"] = []

    expected_loans = _summary_count(
        summary,
        "outstanding_loan_account_count",
        "housing_loan_outstanding_count",
        "other_loan_outstanding_count",
    )
    actual_loans = len(report.get("loan_accounts") or [])
    if expected_loans and abs(expected_loans - actual_loans) >= 3:
        warnings.append(f"loan_account_count_mismatch: expected={expected_loans}, actual={actual_loans}")

    expected_cards = _first_summary_count(
        summary,
        "active_credit_card_account_count",
        "credit_card_active_count",
        "credit_card_account_count",
    )
    actual_cards = len(report.get("credit_card_accounts") or [])
    if expected_cards and abs(expected_cards - actual_cards) >= 3:
        warnings.append(f"credit_card_account_count_mismatch: expected={expected_cards}, actual={actual_cards}")
    if expected_cards >= 30:
        _warn_once(warnings, f"credit_card_account_count_unusually_large: {expected_cards}")

    for index, loan in enumerate(report.get("loan_accounts") or [], start=1):
        if not isinstance(loan, dict):
            continue
        joined = " ".join(str(loan.get(key) or "") for key in ("business_type", "evidence", "evidence_text", "institution"))
        if _contains_any(joined, ("贷记卡", "准贷记卡", "信用卡", "授信额度", "已用额度")):
            _warn_once(warnings, f"loan_account_contains_card_terms: account={index}")
        suspicious_text = str(loan.get("institution") or "")
        if re.search(r"(查询记录|报告基础信息|信贷记录概要)", suspicious_text):
            _warn_once(warnings, f"loan_account_section_title_mixed: account={index}")
        evidence = " ".join(str(loan.get(key) or "") for key in ("institution", "evidence", "evidence_text", "account_no"))
        if re.search(r"(相关还款责任|保证合同编号|保证人|共同借款人|查询记录明细|查询机构|贷款审批|信用卡审批|贷后管理)", evidence):
            _warn_once(warnings, f"loan_account_pollution_suspected: account={index}")

    for index, card in enumerate(report.get("credit_card_accounts") or [], start=1):
        if not isinstance(card, dict):
            continue
        joined = " ".join(str(card.get(key) or "") for key in ("account_status", "evidence", "evidence_text", "history_performance"))
        if "未销户" not in joined and re.search(r"(销户|已销户|注销|已注销|关闭|已关闭)", joined):
            _warn_once(warnings, f"closed_credit_card_account_still_present: account={index}")

    for item in report.get("related_repayment_responsibilities") or []:
        if not isinstance(item, dict):
            continue
        if _summary_count(item, "loan_balance") > 0:
            _warn_once(warnings, "存在相关还款责任余额，请关注个人对企业贷款承担的保证/共同借款责任。")
            break

    report["warnings"] = warnings
    report["missing_fields"] = missing_fields
    return warnings, missing_fields
