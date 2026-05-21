from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

ENTERPRISE_FLOW_TYPES = {
    "enterprise_flow",
    "enterprise_bank_statement",
    "bank_statement_enterprise",
    "company_bank_statement",
    "企业流水",
    "银行流水",
}


def _num(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return round(float(value), 2)
    except Exception:
        return 0.0


def _int(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(float(value))
    except Exception:
        return 0


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _extract_payload(extraction: dict[str, Any]) -> dict[str, Any]:
    data = _dict(extraction.get("extracted_data"))
    payload = data.get("extracted_json") or data.get("data") or data
    return _dict(payload)


def _date_key(value: Any) -> str:
    text = str(value or "")
    return text[:10] if len(text) >= 10 else text


def _month_count(months: list[dict[str, Any]], statement_period: dict[str, Any]) -> int:
    month_keys = {str(item.get("month") or "") for item in months if item.get("month")}
    if month_keys:
        return len(month_keys)
    months_count = _int(statement_period.get("months_count"))
    if months_count > 0:
        return months_count
    start = _date_key(statement_period.get("start_date"))
    end = _date_key(statement_period.get("end_date"))
    try:
        if start and end:
            start_dt = datetime.fromisoformat(start)
            end_dt = datetime.fromisoformat(end)
            return max(1, (end_dt.year - start_dt.year) * 12 + end_dt.month - start_dt.month + 1)
    except Exception:
        return 1
    return 1


def _account_key(account: dict[str, Any], source_file: str, source_doc_id: str) -> str:
    bank = str(account.get("bank_name") or "").strip()
    number = str(account.get("account_number") or "").strip()
    if number:
        return f"{bank}|{number}"
    return f"{bank}|{source_file}|{account.get('sheet_name') or source_doc_id}"


def _counterparty_key(item: dict[str, Any]) -> str:
    return f"{item.get('name') or item.get('counterparty_name') or '未知对手方'}|{item.get('counterparty_account') or ''}"


def _merge_counterparty(target: dict[str, dict[str, Any]], item: dict[str, Any]) -> None:
    key = _counterparty_key(item)
    stat = target.setdefault(
        key,
        {
            "name": item.get("name") or item.get("counterparty_name") or "未知对手方",
            "counterparty_account": item.get("counterparty_account") or "",
            "inflow": 0.0,
            "outflow": 0.0,
            "net": 0.0,
            "transaction_count": 0,
            "category_guess": item.get("category_guess") or item.get("category"),
            "is_related_party": False,
            "is_personal_counterparty": False,
            "risk_note": item.get("risk_note"),
        },
    )
    stat["inflow"] = round(_num(stat.get("inflow")) + _num(item.get("inflow")), 2)
    stat["outflow"] = round(_num(stat.get("outflow")) + _num(item.get("outflow")), 2)
    stat["net"] = round(stat["inflow"] - stat["outflow"], 2)
    stat["transaction_count"] = _int(stat.get("transaction_count")) + _int(item.get("transaction_count"))
    stat["is_related_party"] = bool(stat.get("is_related_party") or item.get("is_related_party"))
    stat["is_personal_counterparty"] = bool(stat.get("is_personal_counterparty") or item.get("is_personal_counterparty"))


def aggregate_customer_enterprise_flows(extractions: list[dict[str, Any]]) -> dict[str, Any]:
    accounts_by_key: dict[str, dict[str, Any]] = {}
    monthly_by_key: dict[str, dict[str, Any]] = {}
    counterparties: dict[str, dict[str, Any]] = {}
    transactions: list[dict[str, Any]] = []
    source_files: list[dict[str, Any]] = []
    warnings: list[str] = []
    start_dates: list[str] = []
    end_dates: list[str] = []

    for extraction in extractions:
        extraction_type = str(extraction.get("extraction_type") or extraction.get("document_type") or "")
        if extraction_type and extraction_type not in ENTERPRISE_FLOW_TYPES:
            continue
        payload = _extract_payload(extraction)
        if not payload:
            continue
        if str(payload.get("extraction_status") or extraction.get("extraction_status") or "").lower() in {"failed", "partial_failed"}:
            continue
        file_name = str(extraction.get("file_name") or payload.get("source_file") or "")
        doc_id = str(extraction.get("doc_id") or extraction.get("document_id") or "")
        extraction_id = str(extraction.get("extraction_id") or "")
        source_accounts = [_dict(item) for item in _list(payload.get("accounts"))]
        source_summary = _dict(payload.get("summary"))
        period = _dict(payload.get("statement_period"))
        if period.get("start_date"):
            start_dates.append(_date_key(period.get("start_date")))
        if period.get("end_date"):
            end_dates.append(_date_key(period.get("end_date")))

        source_files.append(
            {
                "document_id": doc_id,
                "extraction_id": extraction_id,
                "file_name": file_name,
                "uploaded_at": extraction.get("uploaded_at") or extraction.get("created_at") or "",
                "document_type": extraction_type or payload.get("document_type") or "enterprise_flow",
                "account_count": len(source_accounts),
                "total_inflow": _num(source_summary.get("total_inflow")),
                "total_outflow": _num(source_summary.get("total_outflow")),
                "transaction_count": _int(source_summary.get("transaction_count")),
            }
        )

        for account in source_accounts:
            key = _account_key(account, file_name, doc_id)
            target = accounts_by_key.setdefault(
                key,
                {
                    **account,
                    "source_file": file_name,
                    "source_document_id": doc_id,
                    "source_extraction_id": extraction_id,
                    "total_inflow": 0.0,
                    "total_outflow": 0.0,
                    "net_cashflow": 0.0,
                    "transaction_count": 0,
                    "inflow_count": 0,
                    "outflow_count": 0,
                },
            )
            target["total_inflow"] = round(_num(target.get("total_inflow")) + _num(account.get("total_inflow")), 2)
            target["total_outflow"] = round(_num(target.get("total_outflow")) + _num(account.get("total_outflow")), 2)
            target["net_cashflow"] = round(target["total_inflow"] - target["total_outflow"], 2)
            target["transaction_count"] = _int(target.get("transaction_count")) + _int(account.get("transaction_count"))
            target["inflow_count"] = _int(target.get("inflow_count")) + _int(account.get("inflow_count"))
            target["outflow_count"] = _int(target.get("outflow_count")) + _int(account.get("outflow_count"))
            target["ending_balance"] = account.get("ending_balance") if account.get("ending_balance") is not None else target.get("ending_balance")

        for month in _list(payload.get("monthly_summary") or payload.get("monthly_trends")):
            month = _dict(month)
            month_key = str(month.get("month") or "")
            if not month_key:
                continue
            target = monthly_by_key.setdefault(
                month_key,
                {"month": month_key, "inflow": 0.0, "outflow": 0.0, "net_cashflow": 0.0, "inflow_count": 0, "outflow_count": 0, "ending_balance": None},
            )
            target["inflow"] = round(_num(target.get("inflow")) + _num(month.get("inflow")), 2)
            target["outflow"] = round(_num(target.get("outflow")) + _num(month.get("outflow")), 2)
            target["net_cashflow"] = round(target["inflow"] - target["outflow"], 2)
            target["inflow_count"] = _int(target.get("inflow_count")) + _int(month.get("inflow_count"))
            target["outflow_count"] = _int(target.get("outflow_count")) + _int(month.get("outflow_count"))
            if month.get("ending_balance") is not None:
                target["ending_balance"] = month.get("ending_balance")

        for tx in _list(payload.get("transactions")):
            tx = _dict(tx)
            if len(transactions) < 2000:
                transactions.append({**tx, "source_file": tx.get("source_file") or file_name, "source_document_id": doc_id})

        cps = _dict(payload.get("counterparty_summary"))
        for item in _list(cps.get("top_inflow_counterparties")) + _list(cps.get("top_outflow_counterparties")):
            _merge_counterparty(counterparties, _dict(item))
        warnings.extend(str(item) for item in _list(payload.get("warnings")) if item)

    accounts = list(accounts_by_key.values())
    monthly_summary = [monthly_by_key[key] for key in sorted(monthly_by_key)]
    total_inflow = round(sum(_num(item.get("total_inflow")) for item in accounts), 2)
    total_outflow = round(sum(_num(item.get("total_outflow")) for item in accounts), 2)
    transaction_count = sum(_int(item.get("transaction_count")) for item in accounts)
    inflow_count = sum(_int(item.get("inflow_count")) for item in accounts)
    outflow_count = sum(_int(item.get("outflow_count")) for item in accounts)
    bank_names = {str(item.get("bank_name") or "").strip() for item in accounts if item.get("bank_name")}
    statement_period = {
        "start_date": min(start_dates) if start_dates else None,
        "end_date": max(end_dates) if end_dates else None,
        "months_count": None,
    }
    months = _month_count(monthly_summary, statement_period)
    statement_period["months_count"] = months
    estimated_operating_inflow = round(total_inflow, 2)
    summary = {
        "total_inflow": total_inflow,
        "total_outflow": total_outflow,
        "net_cashflow": round(total_inflow - total_outflow, 2),
        "transaction_count": transaction_count,
        "inflow_count": inflow_count,
        "outflow_count": outflow_count,
        "account_count": len(accounts),
        "bank_count": len(bank_names),
        "average_monthly_inflow": round(total_inflow / months, 2) if months else None,
        "average_monthly_outflow": round(total_outflow / months, 2) if months else None,
        "average_monthly_net_cashflow": round((total_inflow - total_outflow) / months, 2) if months else None,
        "estimated_operating_inflow": estimated_operating_inflow,
        "estimated_operating_outflow": total_outflow,
        "estimated_operating_net_cashflow": round(estimated_operating_inflow - total_outflow, 2),
        "excluded_internal_transfer_amount": 0.0,
        "excluded_related_party_inflow": 0.0,
        "excluded_personal_inflow": 0.0,
    }
    cp_items = list(counterparties.values())
    top_inflow = sorted(cp_items, key=lambda item: _num(item.get("inflow")), reverse=True)[:10]
    top_outflow = sorted(cp_items, key=lambda item: _num(item.get("outflow")), reverse=True)[:10]
    counterparty_summary = {
        "top_inflow_counterparties": top_inflow,
        "top_outflow_counterparties": top_outflow,
        "related_party_counterparties": [item for item in cp_items if item.get("is_related_party")],
        "personal_counterparties": [item for item in cp_items if item.get("is_personal_counterparty")],
        "customer_concentration_top5_ratio": round(sum(_num(item.get("inflow")) for item in top_inflow[:5]) / total_inflow, 4) if total_inflow else None,
        "supplier_concentration_top5_ratio": round(sum(_num(item.get("outflow")) for item in top_outflow[:5]) / total_outflow, 4) if total_outflow else None,
    }
    signals = []
    if total_inflow > 0 and abs(summary["net_cashflow"]) / total_inflow < 0.03:
        signals.append(
            {
                "code": "NET_CASHFLOW_TOO_LOW",
                "level": "medium",
                "title": "资金沉淀偏弱",
                "description": "客户级合并流水显示收入和支出高度接近，净流入占比较低。",
                "amount": summary["net_cashflow"],
                "ratio": round(abs(summary["net_cashflow"]) / total_inflow, 4),
                "evidence_refs": [],
                "suggestion": "补充经营合同、发票和账户余额证明，说明资金周转模式。",
            }
        )
    risk_analysis = {
        "overall_level": "medium" if signals else "low",
        "overall_score": 60 if signals else 80,
        "signals": signals,
        "strengths": [f"已合并 {len(source_files)} 份企业流水、{len(accounts)} 个账户。"] if accounts else [],
        "weaknesses": ["客户级净流入偏低。"] if signals else [],
    }
    financing_view = {
        "bank_recognizable_inflow": estimated_operating_inflow,
        "adjusted_operating_inflow": estimated_operating_inflow,
        "excluded_internal_transfer_amount": summary["excluded_internal_transfer_amount"],
        "excluded_related_party_inflow": summary["excluded_related_party_inflow"],
        "excluded_personal_inflow": summary["excluded_personal_inflow"],
        "suggested_credit_products": ["经营贷", "流动资金贷款"] if estimated_operating_inflow > 0 else ["补充流水资料后再评估"],
        "material_checklist": ["近 6-12 个月完整流水", "主要客户合同/订单", "发票或纳税申报材料", "多账户用途说明"],
        "bank_explanation": [
            f"本视图合并同一客户名下 {len(source_files)} 份企业流水资料。",
            f"客户级原始进账约 {total_inflow} 元，支出约 {total_outflow} 元，银行仍需结合交易背景确认经营性口径。",
        ],
        "conclusion": "客户级企业流水已合并展示，可作为授信初筛依据；仍建议结合合同、发票、纳税和账户用途说明核验经营真实性。",
    }
    result = {
        "document_type": "enterprise_flow",
        "normalized_document_type": "enterprise_bank_statement",
        "aggregation_scope": "customer",
        "source_document_count": len(source_files),
        "source_files": source_files,
        "statement_period": statement_period,
        "accounts": accounts,
        "transactions": transactions,
        "summary": summary,
        "monthly_summary": monthly_summary,
        "counterparty_summary": counterparty_summary,
        "risk_analysis": risk_analysis,
        "financing_view": financing_view,
        "evidence": [],
        "warnings": warnings,
    }
    logger.info(
        "[EnterpriseFlow][CustomerAggregate] source_docs=%s accounts=%s total_inflow=%s total_outflow=%s",
        len(source_files),
        len(accounts),
        total_inflow,
        total_outflow,
    )
    return result
