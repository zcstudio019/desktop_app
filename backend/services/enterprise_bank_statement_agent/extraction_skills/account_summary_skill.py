from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

logger = logging.getLogger(__name__)


def _round(value: float | None) -> float | None:
    return round(float(value), 2) if value is not None else None


def _has_positive_number(value: Any) -> bool:
    try:
        return value is not None and abs(float(value)) > 0
    except (TypeError, ValueError):
        return False


def _pick_amount(header_value: Any, transaction_value: float) -> float:
    if _has_positive_number(header_value):
        return float(header_value)
    return float(transaction_value or 0)


def _pick_count(header_value: Any, transaction_count: int) -> int:
    try:
        if header_value is not None and int(header_value) > 0:
            return int(header_value)
    except (TypeError, ValueError):
        pass
    return int(transaction_count or 0)


def build_account_summary(
    transactions: list[dict[str, Any]],
    accounts: list[dict[str, Any]],
    months_count: int | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    month_divisor = max(1, int(months_count or 1))
    by_account: dict[str, dict[str, Any]] = {item["account_id"]: dict(item) for item in accounts}
    tx_totals_by_account: dict[str, dict[str, Any]] = {}
    for tx in transactions:
        account_id = tx.get("account_id") or f"{tx.get('bank_name') or tx.get('sheet_name')}:{tx.get('account_number') or tx.get('sheet_name')}"
        account = by_account.setdefault(
            account_id,
            {
                "account_id": account_id,
                "bank_name": tx.get("bank_name"),
                "account_name": tx.get("account_name"),
                "account_number": tx.get("account_number"),
                "currency": tx.get("currency"),
                "sheet_name": tx.get("sheet_name"),
                "opening_balance": None,
                "ending_balance": None,
                "total_inflow": 0.0,
                "total_outflow": 0.0,
                "net_cashflow": 0.0,
                "transaction_count": 0,
            },
        )
        tx_total = tx_totals_by_account.setdefault(account_id, {"inflow": 0.0, "outflow": 0.0, "count": 0, "inflow_count": 0, "outflow_count": 0})
        credit_amount = float(tx.get("credit_amount") or 0)
        debit_amount = float(tx.get("debit_amount") or 0)
        tx_total["inflow"] += credit_amount
        tx_total["outflow"] += debit_amount
        tx_total["count"] += 1
        if credit_amount > 0:
            tx_total["inflow_count"] += 1
        if debit_amount > 0:
            tx_total["outflow_count"] += 1
        if tx.get("balance") is not None:
            if account.get("opening_balance") is None:
                account["opening_balance"] = tx.get("balance")
            account["ending_balance"] = tx.get("balance")
    normalized_accounts = []
    for account in by_account.values():
        tx_total = tx_totals_by_account.get(account["account_id"], {})
        tx_inflow = float(tx_total.get("inflow") or 0)
        tx_outflow = float(tx_total.get("outflow") or 0)
        tx_count = int(tx_total.get("count") or 0)
        summary_inflow = account.get("summary_inflow")
        summary_outflow = account.get("summary_outflow")
        if summary_inflow is not None or summary_outflow is not None:
            account_label = str(account.get("bank_name") or account.get("sheet_name") or "")
            if _has_positive_number(summary_inflow) and tx_inflow > 0 and abs(float(summary_inflow) - tx_inflow) > 1:
                warnings.append(f"{account.get('bank_name') or account.get('sheet_name')} 顶部总收入与明细收入合计不一致，收入采用顶部总收入。")
            if _has_positive_number(summary_outflow) and tx_outflow > 0 and abs(float(summary_outflow) - tx_outflow) > 1:
                warnings.append(f"{account.get('bank_name') or account.get('sheet_name')} 顶部总支出与明细支出合计不一致，支出采用顶部总支出。")
            if "泰隆" in account_label and not _has_positive_number(summary_outflow) and tx_outflow > 0:
                warnings.append(f"泰隆银行顶部总支出未识别，账户支出采用明细支出金额合计 {round(tx_outflow, 2)}。")
            if "泰隆" in account_label and _has_positive_number(summary_inflow) and tx_inflow > 0 and abs(float(summary_inflow) - tx_inflow) <= 1:
                warnings.append("泰隆银行收入采用顶部/明细一致结果。")

            account["total_inflow"] = _pick_amount(summary_inflow, tx_inflow)
            account["total_outflow"] = _pick_amount(summary_outflow, tx_outflow)
            account["inflow_count"] = _pick_count(account.get("summary_inflow_count"), int(tx_total.get("inflow_count") or 0))
            account["outflow_count"] = _pick_count(account.get("summary_outflow_count"), int(tx_total.get("outflow_count") or 0))
            account["transaction_count"] = int(account.get("inflow_count") or 0) + int(account.get("outflow_count") or 0)

            if "泰隆" in account_label and tx_outflow > 0 and float(account.get("total_outflow") or 0) == 0:
                account["total_outflow"] = tx_outflow
                warnings.append("泰隆银行顶部总支出未识别，账户支出采用明细支出金额合计。")
        elif tx_count > 0:
            account["total_inflow"] = tx_inflow
            account["total_outflow"] = tx_outflow
            account["transaction_count"] = tx_count
            account["inflow_count"] = int(tx_total.get("inflow_count") or 0)
            account["outflow_count"] = int(tx_total.get("outflow_count") or 0)
        elif (summary_inflow or summary_outflow) and account.get("transaction_count"):
            warnings.append(f"{account.get('bank_name') or account.get('sheet_name')} 交易明细未完整识别，账户汇总采用顶部累计发生额。")
        account["total_inflow"] = _round(account.get("total_inflow") or 0) or 0.0
        account["total_outflow"] = _round(account.get("total_outflow") or 0) or 0.0
        account["net_cashflow"] = _round(account["total_inflow"] - account["total_outflow"]) or 0.0
        normalized_accounts.append(account)

    total_inflow = sum(float(account.get("total_inflow") or 0) for account in normalized_accounts)
    total_outflow = sum(float(account.get("total_outflow") or 0) for account in normalized_accounts)
    inflow_count = sum(1 for tx in transactions if tx.get("direction") == "inflow")
    outflow_count = sum(1 for tx in transactions if tx.get("direction") == "outflow")
    for account in normalized_accounts:
        if account["account_id"] not in tx_totals_by_account:
            inflow_count += int(account.get("summary_inflow_count") or 0)
            outflow_count += int(account.get("summary_outflow_count") or 0)

    internal_transfer_inflow = sum(float(tx.get("credit_amount") or 0) for tx in transactions if tx.get("is_internal_transfer") and float(tx.get("credit_amount") or 0) > 0)
    internal_transfer_outflow = sum(float(tx.get("debit_amount") or 0) for tx in transactions if tx.get("is_internal_transfer") and float(tx.get("debit_amount") or 0) > 0)
    related_party_inflow = sum(float(tx.get("credit_amount") or 0) for tx in transactions if tx.get("is_related_party") and not tx.get("is_internal_transfer"))
    related_party_outflow = sum(float(tx.get("debit_amount") or 0) for tx in transactions if tx.get("is_related_party") and not tx.get("is_internal_transfer"))
    personal_transfer_inflow = sum(float(tx.get("credit_amount") or 0) for tx in transactions if tx.get("is_personal_counterparty") and not tx.get("is_internal_transfer"))
    personal_transfer_outflow = sum(float(tx.get("debit_amount") or 0) for tx in transactions if tx.get("is_personal_counterparty") and not tx.get("is_internal_transfer"))
    excluded_related_party_inflow = sum(float(tx.get("credit_amount") or 0) for tx in transactions if tx.get("is_related_party") and tx.get("exclude_from_operating") and not tx.get("is_internal_transfer"))
    excluded_related_party_outflow = sum(float(tx.get("debit_amount") or 0) for tx in transactions if tx.get("is_related_party") and tx.get("exclude_from_operating") and not tx.get("is_internal_transfer"))
    excluded_personal_inflow = sum(float(tx.get("credit_amount") or 0) for tx in transactions if tx.get("is_personal_counterparty") and tx.get("exclude_from_operating") and not tx.get("is_internal_transfer"))
    excluded_personal_outflow = sum(float(tx.get("debit_amount") or 0) for tx in transactions if tx.get("is_personal_counterparty") and tx.get("exclude_from_operating") and not tx.get("is_internal_transfer"))
    internal_amount = internal_transfer_inflow + internal_transfer_outflow
    operating_inflow_raw = total_inflow - internal_transfer_inflow - excluded_related_party_inflow - excluded_personal_inflow
    operating_outflow_raw = total_outflow - internal_transfer_outflow - excluded_related_party_outflow - excluded_personal_outflow
    operating_inflow = max(0.0, operating_inflow_raw)
    operating_outflow = max(0.0, operating_outflow_raw)
    if operating_inflow_raw < 0:
        logger.warning(
            "[EnterpriseFlow][OperatingSummaryWarning] operating_inflow_clamped reason=exclusions_exceed_raw raw_inflow=%s internal_inflow=%s related_excluded=%s personal_excluded=%s",
            round(total_inflow, 2),
            round(internal_transfer_inflow, 2),
            round(excluded_related_party_inflow, 2),
            round(excluded_personal_inflow, 2),
        )
    low_balance_threshold = 5000.0
    low_balance_count = sum(1 for tx in transactions if tx.get("balance") is not None and float(tx.get("balance") or 0) < low_balance_threshold)
    banks = {item.get("bank_name") for item in normalized_accounts if item.get("bank_name")}
    account_transaction_count = sum(int(account.get("transaction_count") or 0) for account in normalized_accounts)
    summary = {
        "raw_total_inflow": _round(total_inflow) or 0.0,
        "raw_total_outflow": _round(total_outflow) or 0.0,
        "raw_net_cashflow": _round(total_inflow - total_outflow) or 0.0,
        "total_inflow": _round(total_inflow) or 0.0,
        "total_outflow": _round(total_outflow) or 0.0,
        "net_cashflow": _round(total_inflow - total_outflow) or 0.0,
        "transaction_count": max(len(transactions), account_transaction_count),
        "inflow_count": inflow_count,
        "outflow_count": outflow_count,
        "account_count": len(normalized_accounts),
        "bank_count": len(banks),
        "average_monthly_inflow": _round(total_inflow / month_divisor),
        "average_monthly_outflow": _round(total_outflow / month_divisor),
        "average_monthly_net_cashflow": _round((total_inflow - total_outflow) / month_divisor),
        "max_single_inflow": _round(max((float(tx.get("credit_amount") or 0) for tx in transactions), default=0)),
        "max_single_outflow": _round(max((float(tx.get("debit_amount") or 0) for tx in transactions), default=0)),
        "low_balance_transaction_count": low_balance_count,
        "low_balance_threshold": low_balance_threshold,
        "estimated_operating_inflow": _round(operating_inflow),
        "estimated_operating_outflow": _round(operating_outflow),
        "estimated_operating_net_cashflow": _round(operating_inflow - operating_outflow),
        "excluded_internal_transfer_amount": _round(internal_amount),
        "excluded_related_party_inflow": _round(excluded_related_party_inflow),
        "excluded_personal_inflow": _round(excluded_personal_inflow),
        "internal_transfer_inflow": _round(internal_transfer_inflow) or 0.0,
        "internal_transfer_outflow": _round(internal_transfer_outflow) or 0.0,
        "internal_transfer_total": _round(internal_amount) or 0.0,
        "related_party_inflow": _round(related_party_inflow) or 0.0,
        "related_party_outflow": _round(related_party_outflow) or 0.0,
        "personal_transfer_inflow": _round(personal_transfer_inflow) or 0.0,
        "personal_transfer_outflow": _round(personal_transfer_outflow) or 0.0,
        "operating_inflow": _round(operating_inflow) or 0.0,
        "operating_outflow": _round(operating_outflow) or 0.0,
        "operating_net_cashflow": _round(operating_inflow - operating_outflow) or 0.0,
    }
    logger.info(
        "[EnterpriseFlow][InternalTransferSplit] inflow=%s outflow=%s total=%s count=%s",
        summary["internal_transfer_inflow"],
        summary["internal_transfer_outflow"],
        summary["internal_transfer_total"],
        sum(1 for tx in transactions if tx.get("is_internal_transfer")),
    )
    logger.info(
        "[EnterpriseFlow][OperatingSummary] raw_inflow=%s internal_inflow=%s related_inflow=%s personal_inflow=%s operating_inflow=%s",
        summary["raw_total_inflow"],
        summary["internal_transfer_inflow"],
        summary["excluded_related_party_inflow"],
        summary["excluded_personal_inflow"],
        summary["operating_inflow"],
    )
    logger.info(
        "[EnterpriseFlow][OperatingSummary] raw_outflow=%s internal_outflow=%s related_outflow=%s personal_outflow=%s operating_outflow=%s",
        summary["raw_total_outflow"],
        summary["internal_transfer_outflow"],
        round(excluded_related_party_outflow, 2),
        round(excluded_personal_outflow, 2),
        summary["operating_outflow"],
    )
    return summary, normalized_accounts, warnings
