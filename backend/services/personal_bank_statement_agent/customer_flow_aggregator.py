from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import logging
from typing import Any

from .deterministic_summary import build_deterministic_personal_flow_summary
from .normalizer import round2
from .skills.financing_judgement_skill import build_financing_judgement
from .skills.risk_signal_skill import detect_risk_signals

PERSONAL_FLOW_TYPES = {
    "personal_flow",
    "personal_bank_statement",
    "bank_statement_personal",
    "individual_bank_statement",
    "个人流水",
    "个人银行流水",
}

logger = logging.getLogger(__name__)


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _payload(extraction: dict[str, Any]) -> dict[str, Any]:
    data = _dict(extraction.get("extracted_data"))
    return _dict(data.get("extracted_json") or data.get("data") or data)


def _dedupe_extractions(extractions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    keys: list[str] = []
    for extraction in sorted(extractions, key=lambda item: str(item.get("created_at") or "")):
        payload = dict(_payload(extraction))
        if payload:
            payload.setdefault("source_file", extraction.get("file_name") or "")
            payload.setdefault("original_filename", extraction.get("file_name") or "")
        normalized = build_deterministic_personal_flow_summary(payload) if payload else {}
        extraction["_deterministic_payload"] = normalized
        file_name = str(
            extraction.get("file_name")
            or payload.get("source_file")
            or payload.get("file_name")
            or ""
        )
        file_hash = str(
            extraction.get("file_hash")
            or payload.get("file_hash")
            or _dict(extraction.get("document")).get("file_hash")
            or ""
        )
        file_size = str(
            extraction.get("file_size")
            or payload.get("file_size")
            or _dict(extraction.get("document")).get("file_size")
            or ""
        )
        fallback_signature = str(normalized.get("transaction_signature") or extraction.get("doc_id") or extraction.get("extraction_id") or "")
        identity = file_hash or file_size or fallback_signature
        key = f"{file_name}|{identity}" if file_name or identity else str(extraction.get("extraction_id") or len(keys))
        if key not in latest:
            keys.append(key)
        latest[key] = extraction
    return [latest[key] for key in keys if key in latest]


def _date(value: Any) -> str:
    text = str(value or "")
    return text[:10] if len(text) >= 10 else text


def _months(start: str, end: str, fallback: int) -> int:
    try:
        a = datetime.strptime(start, "%Y-%m-%d")
        b = datetime.strptime(end, "%Y-%m-%d")
        return max(1, (b.year - a.year) * 12 + b.month - a.month + 1)
    except Exception:
        return max(1, fallback or 1)


def _merge_top(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = defaultdict(lambda: {"name": "未知对手方", "account": "", "amount": 0.0, "count": 0})
    for item in items:
        item = _dict(item)
        key = f"{item.get('name') or '未知对手方'}|{item.get('account') or ''}"
        target = merged[key]
        target["name"] = item.get("name") or "未知对手方"
        target["account"] = item.get("account") or ""
        target["amount"] = round2(float(target.get("amount") or 0) + float(item.get("amount") or 0))
        target["count"] = int(target.get("count") or 0) + int(item.get("count") or 0)
    return sorted(merged.values(), key=lambda item: float(item.get("amount") or 0), reverse=True)[:10]


def aggregate_customer_personal_flows(extractions: list[dict[str, Any]]) -> dict[str, Any]:
    before_dedupe = len(extractions)
    extractions = _dedupe_extractions(extractions)
    logger.info("[PersonalFlow][DEDUP] before=%s after=%s", before_dedupe, len(extractions))
    source_files: list[dict[str, Any]] = []
    accounts: list[dict[str, Any]] = []
    transactions: list[dict[str, Any]] = []
    warnings: list[str] = []
    summary_warnings: list[dict[str, str]] = []
    failed_sources: list[dict[str, Any]] = []
    start_dates: list[str] = []
    end_dates: list[str] = []
    monthly: dict[str, dict[str, Any]] = defaultdict(lambda: {"month": "", "raw_income": 0.0, "raw_expense": 0.0, "salary_income": 0.0, "operating_income": 0.0, "stable_income": 0.0})
    top_income: list[dict[str, Any]] = []
    top_expense: list[dict[str, Any]] = []
    sums = defaultdict(float)
    salary_months: set[str] = set()
    salary_sources: dict[str, dict[str, Any]] = defaultdict(lambda: {"counterparty_name": "", "amount": 0.0, "count": 0, "months": set(), "salary_type": ""})
    salary_notes: list[str] = []
    salary_confidences: list[float] = []
    fast_matches: list[dict[str, Any]] = []
    base_info: dict[str, Any] = {}
    payroll_like_transactions: list[dict[str, Any]] = []
    recovered_counterparties: list[dict[str, Any]] = []
    ai_summary_sources: list[dict[str, Any]] = []

    for extraction in extractions:
        extraction_type = str(extraction.get("extraction_type") or extraction.get("document_type") or "")
        if extraction_type and extraction_type not in PERSONAL_FLOW_TYPES:
            continue
        payload = _dict(extraction.get("_deterministic_payload")) or build_deterministic_personal_flow_summary(_payload(extraction))
        if not payload:
            continue
        doc_id = str(extraction.get("doc_id") or extraction.get("document_id") or "")
        extraction_id = str(extraction.get("extraction_id") or "")
        file_name = str(extraction.get("file_name") or payload.get("source_file") or "")
        status = str(payload.get("extraction_status") or extraction.get("extraction_status") or "").lower()
        if status in {"failed", "partial_failed"}:
            failed_sources.append({"document_id": doc_id, "extraction_id": extraction_id, "file_name": file_name, "status": status})
            warnings.extend(str(item) for item in _list(payload.get("warnings")) if item)
            continue
        summary = _dict(payload.get("customer_level_summary"))
        detail_transactions = _list(payload.get("transactions"))
        detail_summary = _dict(payload.get("deterministic_summary"))
        ai_summary_raw = _dict(payload.get("ai_summary_raw"))
        payload_debug = _dict(payload.get("debug"))
        payroll_like_transactions.extend(_dict(item) for item in _list(payload_debug.get("payroll_like_transactions")))
        recovered_counterparties.extend(_dict(item) for item in _list(payload_debug.get("recovered_counterparties")))
        if ai_summary_raw:
            ai_summary_sources.append(ai_summary_raw)
        for mismatch_warning in _list(payload.get("summary_warnings")):
            mismatch_warning = _dict(mismatch_warning)
            if mismatch_warning:
                summary_warnings.append(mismatch_warning)
                warnings.append(f"{mismatch_warning.get('code')}: {mismatch_warning.get('evidence')}")
        effective_raw_income = detail_summary.get("total_income") if detail_summary else summary.get("raw_total_income")
        effective_raw_expense = detail_summary.get("total_expense") if detail_summary else summary.get("raw_total_expense")
        effective_net_cash_flow = detail_summary.get("net_cash_flow") if detail_summary else summary.get("net_cash_flow")
        logger.info("[PersonalFlow][AI_SUMMARY_RAW] income=%s expense=%s net=%s", ai_summary_raw.get("total_income") or 0, ai_summary_raw.get("total_expense") or 0, ai_summary_raw.get("net_cash_flow") or 0)
        logger.info(
            "[PersonalFlow][DETAIL_SUMMARY] income=%s expense=%s net=%s income_count=%s expense_count=%s",
            effective_raw_income or 0,
            effective_raw_expense or 0,
            effective_net_cash_flow or 0,
            detail_summary.get("income_count") or 0,
            detail_summary.get("expense_count") or 0,
        )
        logger.info("[PersonalFlow][TRANSACTIONS_COUNT] count=%s", len(detail_transactions))
        logger.info("[PersonalFlow][SUMMARY_SOURCE] deterministic_from_transactions")
        logger.info("[PersonalFlow][SUMMARY_MISMATCH] %s", bool(payload.get("summary_warnings")))
        source_accounts = [_dict(item) for item in _list(payload.get("accounts"))]
        if not base_info and any(payload.get(key) for key in ("bank_name", "account_name", "account_no")):
            base_info = {
                "bank_name": payload.get("bank_name") or "",
                "account_name": payload.get("account_name") or "",
                "account_no": payload.get("account_no") or "",
                "currency": payload.get("currency") or "人民币",
                "account_type": payload.get("account_type") or "",
                "statement_period": payload.get("statement_period") or {},
                "print_date": payload.get("print_date") or "",
            }
        source_files.append(
            {
                "document_id": doc_id,
                "extraction_id": extraction_id,
                "file_name": file_name,
                "created_at": extraction.get("created_at") or "",
                "source_file": payload.get("source_file") or file_name,
                "document_type": extraction_type or "personal_flow",
                "bank_name": payload.get("bank_name") or (source_accounts[0].get("bank_name") if source_accounts else ""),
                "account_no": payload.get("account_no") or (source_accounts[0].get("account_no") if source_accounts else ""),
                "period": payload.get("statement_period") or {"start_date": summary.get("period_start") or "", "end_date": summary.get("period_end") or ""},
                "flow_nature": payload.get("flow_nature") or {},
                "verified_income": (payload.get("income_verification") or {}).get("verified_income") if isinstance(payload.get("income_verification"), dict) else summary.get("verified_income") or 0,
                "confirmed_salary_income": (payload.get("income_verification") or {}).get("confirmed_salary_income") if isinstance(payload.get("income_verification"), dict) else summary.get("salary_income") or 0,
                "suspected_salary_income": (payload.get("income_verification") or {}).get("suspected_salary_income") if isinstance(payload.get("income_verification"), dict) else summary.get("suspected_salary_income") or 0,
                "low_confidence_suspected_salary_income": (payload.get("income_verification") or {}).get("low_confidence_suspected_salary_income") if isinstance(payload.get("income_verification"), dict) else summary.get("low_confidence_suspected_salary_income") or 0,
                "suspected_salary_income_low_confidence": (payload.get("income_verification") or {}).get("suspected_salary_income_low_confidence") if isinstance(payload.get("income_verification"), dict) else summary.get("suspected_salary_income_low_confidence") or 0,
                "unknown_inflow": (payload.get("income_verification") or {}).get("unknown_inflow") if isinstance(payload.get("income_verification"), dict) else summary.get("unknown_inflow") or 0,
                "loan_repayment_expense": (payload.get("expense_analysis") or {}).get("loan_repayment_expense") if isinstance(payload.get("expense_analysis"), dict) else summary.get("loan_repayment_expense") or 0,
                "risk_signals": payload.get("risk_signals") or [],
                "account_count": len(source_accounts),
                "raw_total_income": effective_raw_income or 0,
                "raw_total_expense": effective_raw_expense or 0,
                "deterministic_summary": detail_summary,
                "ai_summary_raw": ai_summary_raw,
                "warnings": payload.get("warnings") or [],
            }
        )
        income = _dict(payload.get("income_verification"))
        expense = _dict(payload.get("expense_analysis"))
        logger.info(
            "[PersonalFlow][SALARY] confirmed=%s suspected=%s low_confidence=%s sources=%s",
            income.get("confirmed_salary_income") or 0,
            income.get("suspected_salary_income") or 0,
            income.get("suspected_salary_income_low_confidence") or 0,
            income.get("salary_sources") or [],
        )
        sums["raw_total_income"] += float(effective_raw_income or income.get("raw_total_income") or summary.get("raw_total_income") or 0)
        sums["raw_total_expense"] += float(effective_raw_expense or expense.get("raw_total_expense") or summary.get("raw_total_expense") or 0)
        sums["salary_income"] += float(income.get("verified_salary_income") or summary.get("salary_income") or 0)
        sums["confirmed_salary_income"] += float(income.get("confirmed_salary_income") or income.get("verified_salary_income") or summary.get("salary_income") or 0)
        sums["suspected_salary_income"] += float(income.get("suspected_salary_income") or summary.get("suspected_salary_income") or 0)
        low_confidence_salary = float(income.get("low_confidence_suspected_salary_income") or income.get("suspected_salary_income_low_confidence") or summary.get("low_confidence_suspected_salary_income") or summary.get("suspected_salary_income_low_confidence") or 0)
        sums["low_confidence_suspected_salary_income"] += low_confidence_salary
        sums["suspected_salary_income_low_confidence"] += low_confidence_salary
        sums["operating_income"] += float(income.get("verified_operating_income") or summary.get("operating_income") or 0)
        sums["stable_income"] += float(income.get("stable_income") or summary.get("stable_income") or 0)
        sums["verified_income"] += float(income.get("verified_income") or summary.get("verified_income") or 0)
        sums["unknown_inflow"] += float(income.get("unknown_inflow") or summary.get("unknown_inflow") or 0)
        sums["loan_inflow"] += float(income.get("loan_inflow") or summary.get("loan_inflow") or 0)
        sums["internal_transfer_income"] += float(income.get("internal_transfer_income") or summary.get("internal_transfer_income") or 0)
        sums["self_transfer_income"] += float(income.get("self_transfer_income") or 0)
        sums["personal_transfer_income"] += float(income.get("personal_transfer_income") or 0)
        sums["related_party_income"] += float(income.get("related_party_income") or 0)
        sums["platform_collection_income"] += float(income.get("platform_collection_income") or 0)
        sums["company_business_inflow"] += float(income.get("company_business_inflow") or 0)
        sums["investment_income"] += float(income.get("investment_income") or 0)
        sums["refund_income"] += float(income.get("refund_income") or 0)
        sums["interest_income"] += float(income.get("interest_income") or 0)
        sums["loan_repayment_expense"] += float(expense.get("loan_repayment_expense") or summary.get("loan_repayment_expense") or 0)
        for key in (
            "credit_card_repayment_expense",
            "online_loan_repayment_expense",
            "quick_payment_expense",
            "living_expense",
            "investment_expense",
            "internal_transfer_expense",
            "related_party_transfer_expense",
            "business_or_company_outflow",
            "cash_withdrawal",
            "fee_expense",
        ):
            sums[key] += float(expense.get(key) or 0)
        sums["net_operating_cash_flow"] += float(summary.get("net_operating_cash_flow") or effective_net_cash_flow or 0)
        fast_matches.extend(_dict(item) for item in _list(_dict(payload.get("fast_in_fast_out_analysis")).get("matches")))
        if income.get("salary_confidence"):
            salary_confidences.append(float(income.get("salary_confidence") or 0))
        salary_notes.extend(str(item) for item in _list(income.get("salary_detection_notes")) if item)
        for source in _list(income.get("salary_sources")):
            source = _dict(source)
            name = str(source.get("counterparty_name") or "").strip()
            if not name:
                continue
            target = salary_sources[name]
            target["counterparty_name"] = name
            target["amount"] = round2(float(target.get("amount") or 0) + float(source.get("amount") or 0))
            target["count"] = int(target.get("count") or 0) + int(source.get("count") or 0)
            target["salary_type"] = source.get("salary_type") or target.get("salary_type") or ""
            for month in _list(source.get("months")):
                if month:
                    target["months"].add(str(month))
                    salary_months.add(str(month))
        for tx in detail_transactions:
            salary_type = (_dict(_dict(tx).get("salary_detection")).get("salary_type") or "")
            if salary_type in {"confirmed_salary", "suspected_salary", "low_confidence_suspected_salary"}:
                month = str(_dict(tx).get("transaction_date") or "")[:7]
                if month:
                    salary_months.add(month)
        if summary.get("period_start"):
            start_dates.append(_date(summary.get("period_start")))
        if summary.get("period_end"):
            end_dates.append(_date(summary.get("period_end")))
        for account in source_accounts:
            account = {**account, "source_document_id": doc_id, "source_extraction_id": extraction_id, "source_file": file_name}
            accounts.append(account)
            top_income.extend(_list(account.get("top_income_counterparties")))
            top_expense.extend(_list(account.get("top_expense_counterparties")))
            for month in _list(account.get("monthly_trend")):
                month = _dict(month)
                month_key = str(month.get("month") or "")
                if not month_key:
                    continue
                target = monthly[month_key]
                target["month"] = month_key
                for key in ("raw_income", "raw_expense", "salary_income", "operating_income", "stable_income"):
                    target[key] = round2(float(target.get(key) or 0) + float(month.get(key) or 0))
            if not detail_transactions:
                for tx in _list(account.get("transactions")):
                    transactions.append({**_dict(tx), "source_document_id": doc_id, "source_extraction_id": extraction_id, "source_file": file_name})
        if detail_transactions:
            for tx in detail_transactions:
                transactions.append({**_dict(tx), "source_document_id": doc_id, "source_extraction_id": extraction_id, "source_file": file_name})
        warnings.extend(str(item) for item in _list(payload.get("warnings")) if item)

    period_start = min(start_dates) if start_dates else ""
    period_end = max(end_dates) if end_dates else ""
    month_count = _months(period_start, period_end, len(monthly))
    summary = {
        "account_count": len(accounts),
        "period_start": period_start,
        "period_end": period_end,
        "raw_total_income": round2(sums["raw_total_income"]),
        "raw_total_expense": round2(sums["raw_total_expense"]),
        "salary_income": round2(sums["salary_income"]),
        "confirmed_salary_income": round2(sums["confirmed_salary_income"]),
        "suspected_salary_income": round2(sums["suspected_salary_income"]),
        "low_confidence_suspected_salary_income": round2(sums["low_confidence_suspected_salary_income"]),
        "suspected_salary_income_low_confidence": round2(sums["suspected_salary_income_low_confidence"]),
        "operating_income": round2(sums["operating_income"]),
        "stable_income": round2(sums["stable_income"]),
        "internal_transfer_income": round2(sums["internal_transfer_income"]),
        "self_transfer_income": round2(sums["self_transfer_income"]),
        "personal_transfer_income": round2(sums["personal_transfer_income"]),
        "related_party_income": round2(sums["related_party_income"]),
        "platform_collection_income": round2(sums["platform_collection_income"]),
        "company_business_inflow": round2(sums["company_business_inflow"]),
        "investment_income": round2(sums["investment_income"]),
        "refund_income": round2(sums["refund_income"]),
        "interest_income": round2(sums["interest_income"]),
        "loan_inflow": round2(sums["loan_inflow"]),
        "net_operating_cash_flow": round2(sums["net_operating_cash_flow"]),
        "avg_monthly_income": round2(sums["raw_total_income"] / month_count),
        "avg_monthly_stable_income": round2(sums["stable_income"] / month_count),
        "income_stability_score": 0.0,
        "repayment_capacity_score": 0.0,
        "verified_income": round2(sums["verified_income"]),
        "unknown_inflow": round2(sums["unknown_inflow"]),
        "loan_repayment_expense": round2(sums["loan_repayment_expense"]),
        "customer_raw_total_income": round2(sums["raw_total_income"]),
        "customer_verified_income": round2(sums["verified_income"]),
        "customer_unknown_inflow": round2(sums["unknown_inflow"]),
        "customer_loan_repayment_expense": round2(sums["loan_repayment_expense"]),
        "customer_avg_monthly_verified_income": round2(sums["verified_income"] / month_count),
        "customer_repayment_pressure": round(sums["loan_repayment_expense"] / sums["raw_total_income"], 6) if sums["raw_total_income"] else 0.0,
    }
    deterministic_summary = {
        "total_income": summary["raw_total_income"],
        "total_expense": summary["raw_total_expense"],
        "income_count": sum(1 for tx in transactions if float(tx.get("credit_amount") or 0) > 0),
        "expense_count": sum(1 for tx in transactions if float(tx.get("debit_amount") or 0) > 0),
        "net_cash_flow": round2(summary["raw_total_income"] - summary["raw_total_expense"]),
        "avg_monthly_income": summary["avg_monthly_income"],
        "avg_monthly_expense": round2(summary["raw_total_expense"] / month_count),
        "month_count": month_count,
    }
    income_verification = {
        "raw_total_income": summary["raw_total_income"],
        "confirmed_salary_income": summary["confirmed_salary_income"],
        "suspected_salary_income": summary["suspected_salary_income"],
        "low_confidence_suspected_salary_income": summary["low_confidence_suspected_salary_income"],
        "suspected_salary_income_low_confidence": summary["suspected_salary_income_low_confidence"],
        "verified_salary_income": summary["salary_income"],
        "verified_income": summary["verified_income"],
        "stable_income": summary["stable_income"],
        "unknown_inflow": summary["unknown_inflow"],
        "internal_transfer_income": summary["internal_transfer_income"],
        "self_transfer_income": summary["self_transfer_income"],
        "personal_transfer_income": summary["personal_transfer_income"],
        "related_party_income": summary["related_party_income"],
        "platform_collection_income": summary["platform_collection_income"],
        "company_business_inflow": summary["company_business_inflow"],
        "investment_income": summary["investment_income"],
        "refund_income": summary["refund_income"],
        "interest_income": summary["interest_income"],
        "salary_income_count": sum(1 for tx in transactions if (_dict(tx.get("salary_detection")).get("salary_type") == "confirmed_salary")),
        "suspected_salary_count": sum(1 for tx in transactions if (_dict(tx.get("salary_detection")).get("salary_type") == "suspected_salary")),
        "suspected_salary_count_low_confidence": sum(1 for tx in transactions if (_dict(tx.get("salary_detection")).get("salary_type") == "low_confidence_suspected_salary")),
        "salary_months": len(salary_months),
        "salary_avg_monthly_amount": round2((summary["confirmed_salary_income"] or summary["suspected_salary_income"] or summary["suspected_salary_income_low_confidence"]) / len(salary_months)) if salary_months else 0,
        "salary_confidence": round2(sum(salary_confidences) / len(salary_confidences)) if salary_confidences else 0,
        "salary_sources": [
            {
                **{key: value for key, value in source.items() if key != "months"},
                "months": sorted(str(month) for month in source.get("months") or []),
            }
            for source in sorted(salary_sources.values(), key=lambda item: float(item.get("amount") or 0), reverse=True)[:10]
        ],
        "salary_detection_notes": list(dict.fromkeys(salary_notes)),
    }
    income_analysis = {"income_volatility": 0, "monthly_income": list(monthly.values())}
    expense_analysis = {
        "raw_total_expense": summary["raw_total_expense"],
        "loan_repayment_expense": summary["loan_repayment_expense"],
        "credit_card_repayment_expense": round2(sums["credit_card_repayment_expense"]),
        "online_loan_repayment_expense": round2(sums["online_loan_repayment_expense"]),
        "quick_payment_expense": round2(sums["quick_payment_expense"]),
        "living_expense": round2(sums["living_expense"]),
        "investment_expense": round2(sums["investment_expense"]),
        "internal_transfer_expense": round2(sums["internal_transfer_expense"]),
        "related_party_transfer_expense": round2(sums["related_party_transfer_expense"]),
        "business_or_company_outflow": round2(sums["business_or_company_outflow"]),
        "cash_withdrawal": round2(sums["cash_withdrawal"]),
        "fee_expense": round2(sums["fee_expense"]),
        "loan_repayment_ratio": round(summary["loan_repayment_expense"] / summary["raw_total_expense"], 6) if summary["raw_total_expense"] else 0.0,
        "has_frequent_loan_or_credit_card_repayment": False,
        "has_abnormal_large_expense": any("abnormal_large_expense" in (tx.get("risk_tags") or []) for tx in transactions),
    }
    net_cash_flow = round2(summary["raw_total_income"] - summary["raw_total_expense"])
    retention_ratio = round(net_cash_flow / summary["raw_total_income"], 6) if summary["raw_total_income"] else 0.0
    cash_retention_analysis = {
        "net_cash_flow": net_cash_flow,
        "retention_ratio": retention_ratio,
        "income_expense_match_ratio": round(summary["raw_total_expense"] / summary["raw_total_income"], 6) if summary["raw_total_income"] else 0.0,
        "retention_level": "weak" if summary["raw_total_income"] and retention_ratio <= 0.05 else "medium" if summary["raw_total_income"] else "unknown",
        "message": "账户净流入占收入比例较低" if summary["raw_total_income"] and retention_ratio <= 0.05 else "",
    }
    matched_amount = round2(sum(float(item.get("expense_amount") or 0) for item in fast_matches))
    fast_in_fast_out_analysis = {
        "has_fast_in_fast_out": bool(fast_matches),
        "matched_count": len(fast_matches),
        "matched_amount": matched_amount,
        "matched_amount_ratio": round(matched_amount / summary["raw_total_income"], 6) if summary["raw_total_income"] else 0.0,
        "matches": fast_matches[:100],
    }
    repayment_flow_count = sum(1 for item in source_files if (_dict(item.get("flow_nature")).get("primary_type") == "repayment_account_flow"))
    salary_flow_count = sum(1 for item in source_files if (_dict(item.get("flow_nature")).get("primary_type") == "salary_flow"))
    operating_flow_count = sum(1 for item in source_files if (_dict(item.get("flow_nature")).get("primary_type") == "operating_flow"))
    customer_notes: list[str] = []
    if source_files and repayment_flow_count == len(source_files):
        customer_notes.append("当前资料主要为还款账户流水，缺少真实收入账户流水。")
    elif repayment_flow_count and (salary_flow_count or operating_flow_count):
        customer_notes.append("客户同时存在收入账户流水和还款账户流水，应区分收入证明与还款行为证明。")
    flow_nature = {
        "primary_type": "repayment_account_flow" if source_files and repayment_flow_count == len(source_files) else ("mixed_flow" if repayment_flow_count else "unknown"),
        "confidence": 0.8 if repayment_flow_count else 0.4,
        "reasons": customer_notes,
    }
    risk_signals = detect_risk_signals(
        summary,
        transactions,
        income_verification,
        expense_analysis,
        month_count,
        flow_nature=flow_nature,
    )
    risk_signals.extend(summary_warnings)
    judgement = build_financing_judgement(
        summary,
        risk_signals,
        month_count,
        income_verification=income_verification,
        expense_analysis=expense_analysis,
        flow_nature=flow_nature,
    )
    return {
        **base_info,
        "doc_type": "personal_flow",
        "document_type": "personal_flow",
        "normalized_document_type": "personal_bank_statement",
        "aggregation_scope": "customer",
        "document_count": len(source_files),
        "account_count": len(accounts),
        "source_document_count": len(source_files),
        "source_files": source_files,
        "documents": source_files,
        "failed_sources": failed_sources,
        "accounts": accounts,
        "transactions": transactions[:2000],
        "deterministic_summary": deterministic_summary,
        "raw_summary": deterministic_summary,
        "ai_summary_raw": {"source_files": [item.get("ai_summary_raw") for item in source_files if item.get("ai_summary_raw")]},
        "monthly_trend": [monthly[key] for key in sorted(monthly)],
        "top_income_counterparties": _merge_top(top_income),
        "top_expense_counterparties": _merge_top(top_expense),
        "customer_level_summary": summary,
        "income_verification": income_verification,
        "expense_analysis": expense_analysis,
        "cash_retention_analysis": cash_retention_analysis,
        "fast_in_fast_out_analysis": fast_in_fast_out_analysis,
        "flow_nature": flow_nature,
        "customer_level_notes": customer_notes,
        "risk_signals": risk_signals,
        "financing_judgement": judgement,
        "summary_warnings": summary_warnings,
        "warnings": list(dict.fromkeys(warnings)),
        "debug": {
            "summary_source": "deterministic_from_transactions",
            "transaction_count": len(transactions),
            "salary_candidate_count": sum(
                1 for item in payroll_like_transactions
                if item.get("salary_type") in {"confirmed_salary", "suspected_salary", "low_confidence_suspected_salary"}
            ),
            "payroll_like_transactions": payroll_like_transactions[:100],
            "recovered_counterparties": recovered_counterparties[:100],
            "ai_summary_raw": {"source_files": ai_summary_sources},
            "warnings": summary_warnings + [{"message": item} for item in list(dict.fromkeys(warnings))],
        },
    }
