from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

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
        source_accounts = [_dict(item) for item in _list(payload.get("accounts"))]
        source_files.append(
            {
                "document_id": doc_id,
                "extraction_id": extraction_id,
                "file_name": file_name,
                "document_type": extraction_type or "personal_flow",
                "account_count": len(source_accounts),
                "raw_total_income": summary.get("raw_total_income") or 0,
                "raw_total_expense": summary.get("raw_total_expense") or 0,
            }
        )
        for key in (
            "raw_total_income",
            "raw_total_expense",
            "salary_income",
            "operating_income",
            "stable_income",
            "internal_transfer_income",
            "loan_inflow",
            "net_operating_cash_flow",
        ):
            sums[key] += float(summary.get(key) or 0)
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
        "operating_income": round2(sums["operating_income"]),
        "stable_income": round2(sums["stable_income"]),
        "internal_transfer_income": round2(sums["internal_transfer_income"]),
        "loan_inflow": round2(sums["loan_inflow"]),
        "net_operating_cash_flow": round2(sums["net_operating_cash_flow"]),
        "avg_monthly_income": round2(sums["raw_total_income"] / month_count),
        "avg_monthly_stable_income": round2(sums["stable_income"] / month_count),
        "income_stability_score": 0.0,
        "repayment_capacity_score": 0.0,
    }
    income_analysis = {"income_volatility": 0, "monthly_income": list(monthly.values())}
    expense_analysis = {"has_frequent_loan_or_credit_card_repayment": False, "has_abnormal_large_expense": any("abnormal_large_expense" in (tx.get("risk_tags") or []) for tx in transactions)}
    risk_signals = detect_risk_signals(summary, transactions, income_analysis, expense_analysis, month_count)
    judgement = build_financing_judgement(summary, risk_signals, month_count)
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
        "monthly_trend": [monthly[key] for key in sorted(monthly)],
        "top_income_counterparties": _merge_top(top_income),
        "top_expense_counterparties": _merge_top(top_expense),
        "customer_level_summary": summary,
        "risk_signals": risk_signals,
        "financing_judgement": judgement,
        "warnings": list(dict.fromkeys(warnings)),
    }
