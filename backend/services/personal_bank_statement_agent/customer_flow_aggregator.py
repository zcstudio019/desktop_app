from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

from .normalizer import round2
from .skills.financing_judgement_skill import build_financing_judgement
from .skills.risk_signal_skill import detect_risk_signals
from .summary_utils import (
    build_deterministic_summary,
    build_summary_mismatch_warning,
    collect_transaction_details,
    get_ai_summary_raw,
)

PERSONAL_FLOW_TYPES = {
    "personal_flow",
    "personal_bank_statement",
    "bank_statement_personal",
    "individual_bank_statement",
    "个人流水",
    "个人银行流水",
}


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _payload(extraction: dict[str, Any]) -> dict[str, Any]:
    data = _dict(extraction.get("extracted_data"))
    return _dict(data.get("extracted_json") or data.get("data") or data)


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

    for extraction in extractions:
        extraction_type = str(extraction.get("extraction_type") or extraction.get("document_type") or "")
        if extraction_type and extraction_type not in PERSONAL_FLOW_TYPES:
            continue
        payload = _payload(extraction)
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
        detail_transactions = collect_transaction_details(payload)
        detail_summary = build_deterministic_summary(detail_transactions) if detail_transactions else {}
        ai_summary_raw = get_ai_summary_raw(payload)
        mismatch_warning = build_summary_mismatch_warning(ai_summary_raw, detail_summary) if detail_summary else None
        if mismatch_warning:
            summary_warnings.append(mismatch_warning)
            warnings.append(f"{mismatch_warning['code']}: {mismatch_warning['evidence']}")
        effective_raw_income = detail_summary.get("total_income") if detail_summary else summary.get("raw_total_income")
        effective_raw_expense = detail_summary.get("total_expense") if detail_summary else summary.get("raw_total_expense")
        effective_net_cash_flow = detail_summary.get("net_cash_flow") if detail_summary else summary.get("net_cash_flow")
        source_accounts = [_dict(item) for item in _list(payload.get("accounts"))]
        source_files.append(
            {
                "document_id": doc_id,
                "extraction_id": extraction_id,
                "file_name": file_name,
                "source_file": payload.get("source_file") or file_name,
                "document_type": extraction_type or "personal_flow",
                "bank_name": payload.get("bank_name") or (source_accounts[0].get("bank_name") if source_accounts else ""),
                "account_no": payload.get("account_no") or (source_accounts[0].get("account_no") if source_accounts else ""),
                "period": payload.get("statement_period") or {"start_date": summary.get("period_start") or "", "end_date": summary.get("period_end") or ""},
                "flow_nature": payload.get("flow_nature") or {},
                "verified_income": (payload.get("income_verification") or {}).get("verified_income") if isinstance(payload.get("income_verification"), dict) else summary.get("verified_income") or 0,
                "confirmed_salary_income": (payload.get("income_verification") or {}).get("confirmed_salary_income") if isinstance(payload.get("income_verification"), dict) else summary.get("salary_income") or 0,
                "suspected_salary_income": (payload.get("income_verification") or {}).get("suspected_salary_income") if isinstance(payload.get("income_verification"), dict) else summary.get("suspected_salary_income") or 0,
                "unknown_inflow": (payload.get("income_verification") or {}).get("unknown_inflow") if isinstance(payload.get("income_verification"), dict) else summary.get("unknown_inflow") or 0,
                "loan_repayment_expense": (payload.get("expense_analysis") or {}).get("loan_repayment_expense") if isinstance(payload.get("expense_analysis"), dict) else summary.get("loan_repayment_expense") or 0,
                "risk_signals": payload.get("risk_signals") or [],
                "account_count": len(source_accounts),
                "raw_total_income": effective_raw_income or 0,
                "raw_total_expense": effective_raw_expense or 0,
                "deterministic_summary": detail_summary,
                "ai_summary_raw": ai_summary_raw,
            }
        )
        income = _dict(payload.get("income_verification"))
        expense = _dict(payload.get("expense_analysis"))
        sums["raw_total_income"] += float(effective_raw_income or income.get("raw_total_income") or summary.get("raw_total_income") or 0)
        sums["raw_total_expense"] += float(effective_raw_expense or expense.get("raw_total_expense") or summary.get("raw_total_expense") or 0)
        sums["salary_income"] += float(income.get("verified_salary_income") or summary.get("salary_income") or 0)
        sums["confirmed_salary_income"] += float(income.get("confirmed_salary_income") or income.get("verified_salary_income") or summary.get("salary_income") or 0)
        sums["suspected_salary_income"] += float(income.get("suspected_salary_income") or summary.get("suspected_salary_income") or 0)
        sums["operating_income"] += float(income.get("verified_operating_income") or summary.get("operating_income") or 0)
        sums["stable_income"] += float(income.get("stable_income") or summary.get("stable_income") or 0)
        sums["verified_income"] += float(income.get("verified_income") or summary.get("verified_income") or 0)
        sums["unknown_inflow"] += float(income.get("unknown_inflow") or summary.get("unknown_inflow") or 0)
        sums["loan_inflow"] += float(income.get("loan_inflow") or summary.get("loan_inflow") or 0)
        sums["internal_transfer_income"] += float(income.get("internal_transfer_income") or summary.get("internal_transfer_income") or 0)
        sums["loan_repayment_expense"] += float(expense.get("loan_repayment_expense") or summary.get("loan_repayment_expense") or 0)
        sums["net_operating_cash_flow"] += float(summary.get("net_operating_cash_flow") or effective_net_cash_flow or 0)
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
            for tx in _list(account.get("transactions")):
                transactions.append({**_dict(tx), "source_document_id": doc_id, "source_extraction_id": extraction_id, "source_file": file_name})
        if not source_accounts and detail_transactions:
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
        "operating_income": round2(sums["operating_income"]),
        "stable_income": round2(sums["stable_income"]),
        "internal_transfer_income": round2(sums["internal_transfer_income"]),
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
        "verified_salary_income": summary["salary_income"],
        "verified_income": summary["verified_income"],
        "stable_income": summary["stable_income"],
        "unknown_inflow": summary["unknown_inflow"],
        "salary_months": 0,
        "salary_avg_monthly_amount": 0,
        "salary_confidence": 0,
        "salary_sources": [],
        "salary_detection_notes": [],
    }
    income_analysis = {"income_volatility": 0, "monthly_income": list(monthly.values())}
    expense_analysis = {
        "raw_total_expense": summary["raw_total_expense"],
        "loan_repayment_expense": summary["loan_repayment_expense"],
        "loan_repayment_ratio": round(summary["loan_repayment_expense"] / summary["raw_total_expense"], 6) if summary["raw_total_expense"] else 0.0,
        "has_frequent_loan_or_credit_card_repayment": False,
        "has_abnormal_large_expense": any("abnormal_large_expense" in (tx.get("risk_tags") or []) for tx in transactions),
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
        "doc_type": "personal_flow",
        "document_type": "personal_flow",
        "normalized_document_type": "personal_bank_statement",
        "aggregation_scope": "customer",
        "source_document_count": len(source_files),
        "source_files": source_files,
        "failed_sources": failed_sources,
        "accounts": accounts,
        "transactions": transactions[:2000],
        "deterministic_summary": deterministic_summary,
        "ai_summary_raw": {"source_files": [item.get("ai_summary_raw") for item in source_files if item.get("ai_summary_raw")]},
        "monthly_trend": [monthly[key] for key in sorted(monthly)],
        "top_income_counterparties": _merge_top(top_income),
        "top_expense_counterparties": _merge_top(top_expense),
        "customer_level_summary": summary,
        "income_verification": income_verification,
        "expense_analysis": expense_analysis,
        "flow_nature": flow_nature,
        "customer_level_notes": customer_notes,
        "risk_signals": risk_signals,
        "financing_judgement": judgement,
        "summary_warnings": summary_warnings,
        "warnings": list(dict.fromkeys(warnings)),
    }
