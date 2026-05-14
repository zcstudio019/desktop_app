from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any


def _default_indicators() -> dict[str, Any]:
    return {
        "total_loan_balance": "",
        "total_credit_card_limit": "",
        "total_credit_card_used": "",
        "credit_card_usage_rate": None,
        "has_current_overdue": False,
        "has_90d_overdue": False,
        "has_bad_debt_or_compensation": False,
        "loan_approval_queries_1m": 0,
        "loan_approval_queries_3m": 0,
        "loan_approval_queries_6m": 0,
        "credit_card_approval_queries_3m": 0,
        "institution_query_last_1_month": 0,
        "institution_query_last_3_months": 0,
        "institution_query_last_6_months": 0,
        "personal_query_last_1_month": 0,
        "personal_query_last_3_months": 0,
        "personal_query_last_6_months": 0,
        "related_repayment_responsibility_count": 0,
        "related_repayment_total_balance": "",
        "has_related_repayment_responsibility": False,
        "high_frequency_query_flag": False,
        "risk_level": "low",
        "risk_reasons": [],
        "warnings": [],
    }


def parse_money(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace(",", "").replace("，", "").replace("人民币", "").replace("元", "")
    multiplier = 10000.0 if "万" in text else 1.0
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0)) * multiplier
    except Exception:
        return None


def _summary_number(value: Any) -> int:
    match = re.search(r"\d+", str(value or ""))
    return int(match.group(0)) if match else 0


def _fmt_money(value: float) -> str:
    if not value:
        return ""
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    for pattern in (
        r"((?:19|20)\d{2})[-./年](\d{1,2})[-./月](\d{1,2})日?",
        r"((?:19|20)\d{2})(\d{2})(\d{2})",
    ):
        match = re.search(pattern, text)
        if not match:
            continue
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except Exception:
            return None
    return None


def _months_between(reference: date, target: date) -> int:
    months = (reference.year - target.year) * 12 + reference.month - target.month
    if reference.day < target.day:
        months -= 1
    return months


def _reference_date(report_json: dict[str, Any], warnings: list[str]) -> date:
    dates: list[date] = []
    basic = report_json.get("basic_info") if isinstance(report_json.get("basic_info"), dict) else {}
    report_date = _parse_date(basic.get("report_time"))
    if report_date:
        dates.append(report_date)
    for item in report_json.get("query_records") or []:
        if not isinstance(item, dict):
            continue
        parsed = _parse_date(item.get("query_date"))
        if parsed:
            dates.append(parsed)
        elif item.get("query_date"):
            warnings.append(f"query_date_parse_failed: {item.get('query_date')}")
    return max(dates) if dates else datetime.now().date()


def _joined_risk_text(report_json: dict[str, Any]) -> str:
    chunks: list[str] = []
    for key in ("loan_accounts", "credit_card_accounts", "related_repayment_responsibilities", "overdue_records", "public_records", "query_records", "risk_flags"):
        value = report_json.get(key)
        if isinstance(value, list):
            chunks.extend(str(item) for item in value)
        else:
            chunks.append(str(value or ""))
    return "\n".join(chunks)


def _joined_structured_risk_text(items: list[dict[str, Any]]) -> str:
    keys = (
        "account_status",
        "five_category",
        "overdue_amount",
        "overdue_months",
        "overdue_info",
        "history_performance",
        "record_type",
        "status",
        "content",
        "amount",
        "months",
    )
    return "\n".join(" ".join(str(item.get(key) or "") for key in keys) for item in items)


def _has_positive_money(items: list[dict[str, Any]], keys: tuple[str, ...]) -> bool:
    for item in items:
        for key in keys:
            parsed = parse_money(item.get(key))
            if parsed is not None and parsed > 0:
                return True
    return False


def analyze_personal_credit_risk(report_json: dict[str, Any]) -> dict[str, Any]:
    indicators = _default_indicators()
    warnings: list[str] = indicators["warnings"]
    try:
        loans = [item for item in report_json.get("loan_accounts") or [] if isinstance(item, dict)]
        cards = [item for item in report_json.get("credit_card_accounts") or [] if isinstance(item, dict)]
        queries = [item for item in report_json.get("query_records") or [] if isinstance(item, dict)]
        overdue_records = [item for item in report_json.get("overdue_records") or [] if isinstance(item, dict)]
        public_records = [item for item in report_json.get("public_records") or [] if isinstance(item, dict)]
        related = [item for item in report_json.get("related_repayment_responsibilities") or [] if isinstance(item, dict)]
        summary = report_json.get("credit_summary") if isinstance(report_json.get("credit_summary"), dict) else {}

        loan_balance = sum(value for item in loans if (value := parse_money(item.get("balance"))) is not None)
        card_limit = sum(value for item in cards if (value := parse_money(item.get("credit_limit"))) is not None)
        card_used = sum(value for item in cards if (value := parse_money(item.get("used_limit") or item.get("used_amount"))) is not None)
        indicators["total_loan_balance"] = _fmt_money(loan_balance)
        indicators["total_credit_card_limit"] = _fmt_money(card_limit)
        indicators["total_credit_card_used"] = _fmt_money(card_used)
        if card_limit > 0:
            indicators["credit_card_usage_rate"] = round(card_used / card_limit, 4)

        related_balance = sum(value for item in related if (value := parse_money(item.get("loan_balance"))) is not None)
        indicators["related_repayment_responsibility_count"] = len(related)
        indicators["related_repayment_total_balance"] = _fmt_money(related_balance)
        indicators["has_related_repayment_responsibility"] = len(related) > 0

        summary_current_overdue = (
            _summary_number(summary.get("credit_card_overdue_account_count") or summary.get("credit_card_overdue_count"))
            + _summary_number(summary.get("loan_overdue_account_count"))
        )
        summary_90d_overdue = (
            _summary_number(summary.get("credit_card_90d_overdue_account_count") or summary.get("credit_card_90d_overdue_count"))
            + _summary_number(summary.get("loan_90d_overdue_account_count"))
        )

        indicators["has_current_overdue"] = summary_current_overdue > 0 or _has_positive_money([*loans, *cards], ("overdue_amount",))
        if not indicators["has_current_overdue"]:
            for item in overdue_records:
                amount = parse_money(item.get("amount"))
                status_text = str(item.get("status") or item.get("record_type") or item.get("evidence_text") or "")
                if amount is not None and amount > 0 and "逾期" in status_text:
                    indicators["has_current_overdue"] = True
                    break

        structured_risk_text = _joined_structured_risk_text([*loans, *cards, *overdue_records, *public_records])
        risk_text = _joined_risk_text(report_json)
        indicators["has_90d_overdue"] = summary_90d_overdue > 0 or "90天以上逾期" in structured_risk_text or "90 天以上逾期" in structured_risk_text
        indicators["has_bad_debt_or_compensation"] = any(keyword in risk_text for keyword in ("呆账", "代偿", "核销", "强制执行"))

        query_statistics = report_json.get("query_statistics") if isinstance(report_json.get("query_statistics"), dict) else {}
        institution_stats = query_statistics.get("institution_query") if isinstance(query_statistics.get("institution_query"), dict) else {}
        personal_stats = query_statistics.get("personal_query") if isinstance(query_statistics.get("personal_query"), dict) else {}
        if institution_stats or personal_stats:
            indicators["institution_query_last_1_month"] = _summary_number(institution_stats.get("last_1_month"))
            indicators["institution_query_last_3_months"] = _summary_number(institution_stats.get("last_3_months"))
            indicators["institution_query_last_6_months"] = _summary_number(institution_stats.get("last_6_months"))
            indicators["personal_query_last_1_month"] = _summary_number(personal_stats.get("last_1_month"))
            indicators["personal_query_last_3_months"] = _summary_number(personal_stats.get("last_3_months"))
            indicators["personal_query_last_6_months"] = _summary_number(personal_stats.get("last_6_months"))
            indicators["loan_approval_queries_1m"] = indicators["institution_query_last_1_month"]
            indicators["loan_approval_queries_3m"] = indicators["institution_query_last_3_months"]
            indicators["loan_approval_queries_6m"] = indicators["institution_query_last_6_months"]
        else:
            reference = _reference_date(report_json, warnings)
            for item in queries:
                query_date = _parse_date(item.get("query_date"))
                if not query_date:
                    if item.get("query_date"):
                        warnings.append(f"query_date_parse_failed: {item.get('query_date')}")
                    continue
                months = _months_between(reference, query_date)
                if months < 0:
                    continue
                reason = str(item.get("query_reason") or item.get("evidence") or item.get("evidence_text") or "")
                if "贷款审批" in reason:
                    if months < 1:
                        indicators["loan_approval_queries_1m"] += 1
                    if months < 3:
                        indicators["loan_approval_queries_3m"] += 1
                    if months < 6:
                        indicators["loan_approval_queries_6m"] += 1
                if "信用卡审批" in reason and months < 3:
                    indicators["credit_card_approval_queries_3m"] += 1

        indicators["high_frequency_query_flag"] = indicators["institution_query_last_3_months"] >= 4 or indicators["institution_query_last_6_months"] >= 8 or indicators["loan_approval_queries_3m"] >= 4 or indicators["loan_approval_queries_6m"] >= 8
        usage_rate = indicators.get("credit_card_usage_rate")
        reasons: list[str] = []
        if indicators["has_current_overdue"]:
            reasons.append("存在当前逾期")
        if indicators["has_90d_overdue"]:
            reasons.append("存在90天以上逾期")
        if indicators["has_bad_debt_or_compensation"]:
            reasons.append("存在呆账/代偿/核销/强制执行记录")
        if indicators["high_frequency_query_flag"]:
            reasons.append("贷款审批查询频繁")
        if isinstance(usage_rate, (int, float)) and usage_rate >= 0.8:
            reasons.append("信用卡使用率过高")
        if _summary_number(summary.get("personal_related_repayment_responsibility_account_count")) > 0:
            reasons.append("存在为个人相关还款责任账户")
        if _summary_number(summary.get("enterprise_related_repayment_responsibility_account_count")) > 0:
            reasons.append("存在为企业相关还款责任账户")
        if indicators["has_related_repayment_responsibility"]:
            reasons.append("存在相关还款责任，可能影响银行对法人/实控人连带负债的判断")

        if indicators["has_current_overdue"] or indicators["has_90d_overdue"] or indicators["has_bad_debt_or_compensation"]:
            indicators["risk_level"] = "high"
        elif indicators["high_frequency_query_flag"] or (isinstance(usage_rate, (int, float)) and usage_rate >= 0.8):
            indicators["risk_level"] = "medium"
        indicators["risk_reasons"] = reasons
    except Exception as exc:
        warnings.append(f"risk_analyzer_failed: {exc}")
    return indicators
