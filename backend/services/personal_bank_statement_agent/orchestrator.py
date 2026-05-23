from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from .markdown_renderer import render_personal_bank_statement_markdown
from .normalizer import months_count, round2
from .schema import (
    Account,
    CleanSummary,
    CustomerLevelSummary,
    PersonalBankStatementExtraction,
    RawSummary,
    StatementPeriod,
    Transaction,
    to_plain_dict,
)
from .segmenter import read_personal_bank_statement_workbook
from .skills import (
    analyze_counterparties,
    analyze_expenses,
    analyze_income,
    build_financing_judgement,
    classify_transactions,
    detect_internal_transfers,
    detect_risk_signals,
    extract_account_info,
    extract_owner_info,
    extract_transactions,
)
from .validator import validate_personal_bank_statement_result

logger = logging.getLogger(__name__)

SUPPORTED_TYPES = {
    "personal_flow",
    "personal_bank_statement",
    "bank_statement_personal",
    "individual_bank_statement",
    "个人流水",
    "个人银行流水",
}


def _sum(transactions: list[dict[str, Any]], field: str, category: str | None = None) -> float:
    return round2(sum(float(tx.get(field) or 0) for tx in transactions if category is None or tx.get("category") == category))


def _build_monthly_trend(transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_month: dict[str, dict[str, float]] = defaultdict(lambda: {
        "raw_income": 0.0,
        "raw_expense": 0.0,
        "salary_income": 0.0,
        "operating_income": 0.0,
        "stable_income": 0.0,
        "loan_repayment": 0.0,
        "credit_card_repayment": 0.0,
    })
    for tx in transactions:
        date = str(tx.get("transaction_date") or "")
        month = date[:7] if len(date) >= 7 else "unknown"
        item = by_month[month]
        credit = float(tx.get("credit_amount") or 0)
        debit = float(tx.get("debit_amount") or 0)
        category = tx.get("category")
        item["raw_income"] += credit
        item["raw_expense"] += debit
        if category == "salary_income":
            item["salary_income"] += credit
            item["stable_income"] += credit
        elif category == "operating_income":
            item["operating_income"] += credit
            item["stable_income"] += credit
        elif category == "other_stable_income":
            item["stable_income"] += credit
        elif category == "loan_repayment_expense":
            item["loan_repayment"] += debit
        elif category == "credit_card_repayment_expense":
            item["credit_card_repayment"] += debit
    return [{"month": month, **{k: round2(v) for k, v in values.items()}} for month, values in sorted(by_month.items())]


def _build_clean_summary(transactions: list[dict[str, Any]]) -> dict[str, Any]:
    salary = _sum(transactions, "credit_amount", "salary_income")
    operating_income = _sum(transactions, "credit_amount", "operating_income")
    other_stable = _sum(transactions, "credit_amount", "other_stable_income")
    internal_income = _sum(transactions, "credit_amount", "internal_transfer")
    related_income = _sum(transactions, "credit_amount", "related_party_transfer")
    loan_inflow = _sum(transactions, "credit_amount", "loan_inflow")
    refund_income = _sum(transactions, "credit_amount", "refund")
    investment_income = _sum(transactions, "credit_amount", "investment_transfer")
    living = _sum(transactions, "debit_amount", "living_expense")
    operating_expense = _sum(transactions, "debit_amount", "operating_expense")
    loan_repayment = _sum(transactions, "debit_amount", "loan_repayment_expense")
    credit_card = _sum(transactions, "debit_amount", "credit_card_repayment_expense")
    internal_expense = _sum(transactions, "debit_amount", "internal_transfer")
    investment_expense = _sum(transactions, "debit_amount", "investment_transfer")
    stable = salary + operating_income + other_stable
    non_operating = round2(sum(float(tx.get("credit_amount") or 0) for tx in transactions if tx.get("direction") == "income" and tx.get("category") in {"other", "investment_transfer", "refund", "loan_inflow", "related_party_transfer"}))
    return {
        "salary_income": salary,
        "operating_income": operating_income,
        "other_stable_income": other_stable,
        "non_operating_income": non_operating,
        "internal_transfer_income": internal_income,
        "related_party_income": related_income,
        "loan_inflow": loan_inflow,
        "refund_income": refund_income,
        "investment_transfer_income": investment_income,
        "living_expense": living,
        "operating_expense": operating_expense,
        "loan_repayment_expense": loan_repayment,
        "credit_card_repayment_expense": credit_card,
        "internal_transfer_expense": internal_expense,
        "investment_expense": investment_expense,
        "net_operating_cash_flow": round2(stable - operating_expense - loan_repayment - credit_card),
    }


def _parse_date(value: Any) -> datetime | None:
    text = str(value or "")[:10]
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return None


def _build_raw_summary(transactions: list[dict[str, Any]]) -> dict[str, Any]:
    income = _sum(transactions, "credit_amount")
    expense = _sum(transactions, "debit_amount")
    return {
        "total_income": income,
        "total_expense": expense,
        "income_count": sum(1 for tx in transactions if float(tx.get("credit_amount") or 0) > 0),
        "expense_count": sum(1 for tx in transactions if float(tx.get("debit_amount") or 0) > 0),
        "net_cash_flow": round2(income - expense),
    }


def _build_cash_retention_analysis(raw_income: float, raw_expense: float) -> dict[str, Any]:
    net = round2(raw_income - raw_expense)
    retention_ratio = round(net / raw_income, 6) if raw_income else 0.0
    match_ratio = round(min(raw_income, raw_expense) / max(raw_income, raw_expense), 6) if raw_income and raw_expense else 0.0
    if not raw_income:
        level = "unknown"
        message = "未识别到收入，无法判断账户沉淀"
    elif retention_ratio <= 0.05 or match_ratio >= 0.95:
        level = "weak"
        message = "净流入占收入比例很低，收入和支出几乎对冲，账户沉淀弱"
    elif retention_ratio <= 0.2:
        level = "medium"
        message = "账户有一定沉淀，但净流入比例不高"
    else:
        level = "strong"
        message = "账户净流入比例较高"
    return {
        "net_cash_flow": net,
        "retention_ratio": retention_ratio,
        "income_expense_match_ratio": match_ratio,
        "retention_level": level,
        "message": message,
    }


def _eligible_fast_out(tx: dict[str, Any]) -> bool:
    return str(tx.get("category") or "") in {
        "loan_repayment_expense",
        "quick_payment_expense",
        "internal_transfer",
        "other_large_expense",
        "other_expense",
    } and float(tx.get("debit_amount") or 0) > 0


def _match_ratio(expense_amount: float, income_amount: float) -> float:
    if income_amount <= 0:
        return 0.0
    return round(expense_amount / income_amount, 6)


def _build_fast_in_fast_out_analysis(transactions: list[dict[str, Any]], raw_income: float) -> dict[str, Any]:
    incomes = [
        tx for tx in transactions
        if tx.get("direction") == "income" and float(tx.get("credit_amount") or 0) > 0
    ]
    expenses = [
        tx for tx in transactions
        if tx.get("direction") == "expense" and _eligible_fast_out(tx)
    ]
    used_expense_ids: set[str] = set()
    matches: list[dict[str, Any]] = []
    for income in incomes:
        income_date = _parse_date(income.get("transaction_date"))
        income_amount = float(income.get("credit_amount") or 0)
        if not income_date or income_amount <= 0:
            continue
        candidates: list[tuple[dict[str, Any], int]] = []
        for expense in expenses:
            if str(expense.get("transaction_id")) in used_expense_ids:
                continue
            expense_date = _parse_date(expense.get("transaction_date"))
            if not expense_date:
                continue
            days = (expense_date - income_date).days
            if 0 <= days <= 3:
                candidates.append((expense, days))
        candidates.sort(key=lambda item: (item[1], abs(float(item[0].get("debit_amount") or 0) - income_amount)))
        matched: list[dict[str, Any]] = []
        matched_days = 0
        for expense, days in candidates:
            amount = float(expense.get("debit_amount") or 0)
            ratio = _match_ratio(amount, income_amount)
            if 0.85 <= ratio <= 1.05:
                matched = [expense]
                matched_days = days
                break
        if not matched:
            running = 0.0
            combo: list[dict[str, Any]] = []
            combo_days = 0
            for expense, days in candidates[:8]:
                combo.append(expense)
                running += float(expense.get("debit_amount") or 0)
                combo_days = max(combo_days, days)
                ratio = _match_ratio(running, income_amount)
                if 0.85 <= ratio <= 1.05:
                    matched = combo
                    matched_days = combo_days
                    break
                if ratio > 1.05:
                    break
        if not matched:
            continue
        expense_amount = round2(sum(float(item.get("debit_amount") or 0) for item in matched))
        ratio = _match_ratio(expense_amount, income_amount)
        for item in matched + [income]:
            item["is_fast_in_fast_out_related"] = True
            tags = item.setdefault("risk_tags", [])
            if "fast_in_fast_out" not in tags:
                tags.append("fast_in_fast_out")
        for item in matched:
            used_expense_ids.add(str(item.get("transaction_id")))
        matches.append(
            {
                "income_transaction_id": income.get("transaction_id"),
                "expense_transaction_id": ",".join(str(item.get("transaction_id") or "") for item in matched),
                "income_date": income.get("transaction_date") or "",
                "expense_date": matched[-1].get("transaction_date") or "",
                "income_amount": round2(income_amount),
                "expense_amount": expense_amount,
                "days_between": matched_days,
                "match_ratio": ratio,
                "reason": "收入后 0-3 日内发生金额接近的贷款还款/快捷支付/转出支出",
            }
        )
    matched_amount = round2(sum(float(item.get("expense_amount") or 0) for item in matches))
    return {
        "has_fast_in_fast_out": bool(matches),
        "matched_count": len(matches),
        "matched_amount": matched_amount,
        "matched_amount_ratio": round(matched_amount / raw_income, 6) if raw_income else 0.0,
        "matches": matches,
    }


def _build_repayment_analysis(expense_analysis: dict[str, Any], fast_analysis: dict[str, Any], cash_analysis: dict[str, Any], income_verification: dict[str, Any], month_count: int) -> dict[str, Any]:
    raw_expense = float(expense_analysis.get("raw_total_expense") or 0)
    repayment_related = float(expense_analysis.get("loan_repayment_expense") or 0)
    ratio = repayment_related / raw_expense if raw_expense else 0.0
    evidence: list[str] = []
    if ratio >= 0.6:
        evidence.append("贷款相关支出占比较高")
    if float(cash_analysis.get("income_expense_match_ratio") or 0) >= 0.95:
        evidence.append("收入与支出高度接近")
    if float(cash_analysis.get("retention_ratio") or 0) <= 0.05 and float(income_verification.get("raw_total_income") or 0) > 0:
        evidence.append("账户净流入极低")
    if fast_analysis.get("matched_count"):
        evidence.append("多笔汇入后短期内发生贷款还款/快捷支付")
    raw_income = float(income_verification.get("raw_total_income") or 0)
    if raw_income and float(income_verification.get("unknown_inflow") or 0) / raw_income >= 0.5:
        evidence.append("收入主要为来源不明汇款汇入")
    if float(income_verification.get("verified_income") or 0) <= 0:
        evidence.append("可采信工资/经营收入很低或为 0")
    return {
        "is_repayment_account_flow": len(evidence) >= 3,
        "repayment_related_expense": round2(repayment_related),
        "repayment_related_expense_ratio": round(ratio, 6),
        "monthly_repayment_estimate": round2(repayment_related / max(1, month_count)),
        "repayment_frequency": int(expense_analysis.get("repayment_frequency") or 0),
        "evidence": evidence,
    }


def _build_flow_nature(income_verification: dict[str, Any], expense_analysis: dict[str, Any], repayment_analysis: dict[str, Any], cash_analysis: dict[str, Any], fast_analysis: dict[str, Any]) -> dict[str, Any]:
    reasons = list(repayment_analysis.get("evidence") or [])
    raw_income = float(income_verification.get("raw_total_income") or 0)
    salary = float(income_verification.get("verified_salary_income") or 0)
    operating = float(income_verification.get("verified_operating_income") or 0)
    if repayment_analysis.get("is_repayment_account_flow"):
        return {"primary_type": "repayment_account_flow", "confidence": min(0.95, 0.55 + len(reasons) * 0.08), "reasons": reasons}
    if raw_income and salary / raw_income >= 0.5:
        return {"primary_type": "salary_flow", "confidence": 0.8, "reasons": ["工资收入占原始收入比例较高"]}
    if raw_income and operating / raw_income >= 0.5:
        return {"primary_type": "operating_flow", "confidence": 0.8, "reasons": ["经营收入占原始收入比例较高"]}
    if salary > 0 or operating > 0:
        return {"primary_type": "mixed_flow", "confidence": 0.65, "reasons": ["同时存在部分可采信收入和其他流水"]}
    if fast_analysis.get("has_fast_in_fast_out") or float(expense_analysis.get("loan_repayment_ratio") or 0) > 0.35:
        reasons.append("存在快进快出或贷款还款支出占比较高")
        return {"primary_type": "mixed_flow", "confidence": 0.55, "reasons": reasons}
    return {"primary_type": "unknown", "confidence": 0.3, "reasons": ["未识别到明确工资、经营或还款账户主特征"]}


def _confidence(data: dict[str, Any]) -> float:
    score = 0.45
    if data.get("accounts"):
        score += 0.2
    if sum(len(item.get("transactions") or []) for item in data.get("accounts") or []) > 0:
        score += 0.25
    if data.get("risk_signals"):
        score += 0.02
    if data.get("warnings"):
        score -= min(0.2, len(data.get("warnings") or []) * 0.03)
    return round(max(0.0, min(score, 0.95)), 2)


def run_personal_bank_statement_agent(
    file_path: str | None = None,
    filename: str | None = None,
    text: str | None = None,
    raw_text: str | None = None,
    document_type: str = "personal_flow",
    customer_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = metadata or {}
    source_file = filename or (Path(file_path).name if file_path else "")
    warnings: list[str] = []
    try:
        workbook = read_personal_bank_statement_workbook(
            file_path=file_path,
            rows=metadata.get("rows") if isinstance(metadata.get("rows"), list) else None,
            filename=source_file,
        )
        warnings.extend(workbook.get("warnings") or [])
        owner = extract_owner_info(workbook, metadata)
        accounts_meta, period, account_warnings = extract_account_info(workbook, owner)
        warnings.extend(account_warnings)
        transactions, tx_warnings = extract_transactions(workbook, accounts_meta)
        warnings.extend(tx_warnings)
        transactions, internal_summary = detect_internal_transfers(transactions, accounts_meta)
        transactions = classify_transactions(transactions)
        counterparty = analyze_counterparties(transactions)
        raw_income = _sum(transactions, "credit_amount")
        raw_expense = _sum(transactions, "debit_amount")
        monthly = _build_monthly_trend(transactions)
        observed_months = len([item for item in monthly if item.get("month") != "unknown"])
        month_count = months_count(period.get("start_date"), period.get("end_date"), observed_months)
        income_analysis = analyze_income(transactions, month_count)
        expense_analysis = analyze_expenses(transactions, month_count)
        clean = _build_clean_summary(transactions)
        income_verification = {
            "raw_total_income": raw_income,
            "confirmed_salary_income": income_analysis.get("confirmed_salary_income") or 0,
            "suspected_salary_income": income_analysis.get("suspected_salary_income") or 0,
            "verified_salary_income": income_analysis.get("verified_salary_income") or 0,
            "verified_operating_income": income_analysis.get("verified_operating_income") or 0,
            "verified_other_stable_income": income_analysis.get("verified_other_stable_income") or 0,
            "unknown_inflow": income_analysis.get("unknown_inflow") or 0,
            "interest_income": income_analysis.get("interest_income") or 0,
            "loan_inflow": income_analysis.get("loan_inflow") or 0,
            "internal_transfer_income": income_analysis.get("internal_transfer_income") or 0,
            "related_party_income": income_analysis.get("related_party_income") or 0,
            "investment_redeem_income": income_analysis.get("investment_redeem_income") or 0,
            "refund_income": income_analysis.get("refund_income") or 0,
            "non_verified_income": income_analysis.get("non_verified_income") or 0,
            "verified_income": income_analysis.get("verified_income") or 0,
            "stable_income": income_analysis.get("stable_income") or 0,
            "avg_monthly_verified_income": income_analysis.get("avg_monthly_verified_income") or 0,
            "avg_monthly_stable_income": income_analysis.get("avg_monthly_stable_income") or 0,
            "salary_income_count": int(income_analysis.get("salary_income_count") or 0),
            "suspected_salary_count": int(income_analysis.get("suspected_salary_count") or 0),
            "salary_months": int(income_analysis.get("salary_months") or 0),
            "salary_avg_monthly_amount": income_analysis.get("salary_avg_monthly_amount") or 0,
            "salary_continuity_level": income_analysis.get("salary_continuity_level") or "none",
            "salary_confidence": income_analysis.get("salary_confidence") or 0,
            "salary_sources": income_analysis.get("salary_sources") or [],
            "salary_detection_notes": income_analysis.get("salary_detection_notes") or [],
            "conservative_verified_income": income_analysis.get("conservative_verified_income") or income_analysis.get("verified_income") or 0,
            "aggressive_estimated_income": income_analysis.get("aggressive_estimated_income") or 0,
            "verification_notes": income_analysis.get("verification_notes") or [],
        }
        expense_summary = {
            "raw_total_expense": raw_expense,
            "loan_repayment_expense": expense_analysis.get("loan_repayment_expense") or 0,
            "credit_card_repayment_expense": expense_analysis.get("credit_card_repayment_expense") or 0,
            "quick_payment_expense": expense_analysis.get("quick_payment_expense") or 0,
            "living_expense": expense_analysis.get("living_expense") or 0,
            "operating_expense": expense_analysis.get("operating_expense") or 0,
            "internal_transfer_expense": expense_analysis.get("internal_transfer_expense") or 0,
            "investment_expense": expense_analysis.get("investment_expense") or 0,
            "other_expense": expense_analysis.get("other_expense") or 0,
            "avg_monthly_loan_repayment": expense_analysis.get("avg_monthly_loan_repayment") or 0,
            "loan_repayment_ratio": expense_analysis.get("loan_repayment_ratio") or 0,
        }
        cash_retention = _build_cash_retention_analysis(raw_income, raw_expense)
        fast_in_fast_out = _build_fast_in_fast_out_analysis(transactions, raw_income)
        repayment_analysis = _build_repayment_analysis(expense_analysis, fast_in_fast_out, cash_retention, income_verification, month_count)
        flow_nature = _build_flow_nature(income_verification, expense_analysis, repayment_analysis, cash_retention, fast_in_fast_out)
        stable_income = round2(income_verification["stable_income"])
        customer_summary = {
            "account_count": len(accounts_meta),
            "period_start": period.get("start_date") or "",
            "period_end": period.get("end_date") or "",
            "raw_total_income": raw_income,
            "raw_total_expense": raw_expense,
            "salary_income": income_verification["verified_salary_income"],
            "suspected_salary_income": income_verification["suspected_salary_income"],
            "salary_months": income_verification["salary_months"],
            "salary_confidence": income_verification["salary_confidence"],
            "operating_income": income_verification["verified_operating_income"],
            "stable_income": stable_income,
            "internal_transfer_income": income_verification["internal_transfer_income"],
            "loan_inflow": income_verification["loan_inflow"],
            "net_operating_cash_flow": round2(stable_income - expense_summary["loan_repayment_expense"] - expense_summary["credit_card_repayment_expense"] - expense_summary["operating_expense"]),
            "avg_monthly_income": round2(raw_income / month_count),
            "avg_monthly_stable_income": income_verification["avg_monthly_stable_income"],
            "income_stability_score": round2(max(0, 100 - float(income_analysis.get("income_volatility") or 0) * 100)),
            "repayment_capacity_score": round2(min(100, (stable_income / max(1.0, expense_summary["loan_repayment_expense"] + expense_summary["credit_card_repayment_expense"])) * 50)) if stable_income else 0.0,
            "verified_income": income_verification["verified_income"],
            "unknown_inflow": income_verification["unknown_inflow"],
            "loan_repayment_expense": expense_summary["loan_repayment_expense"],
            "retention_ratio": cash_retention["retention_ratio"],
        }
        risk_signals = detect_risk_signals(
            customer_summary | clean,
            transactions,
            income_analysis,
            expense_analysis,
            month_count,
            cash_retention_analysis=cash_retention,
            repayment_analysis=repayment_analysis,
            fast_in_fast_out_analysis=fast_in_fast_out,
            flow_nature=flow_nature,
        )
        judgement = build_financing_judgement(
            customer_summary | clean,
            risk_signals,
            month_count,
            income_verification=income_verification,
            expense_analysis=expense_summary | {"repayment_frequency": expense_analysis.get("repayment_frequency") or 0},
            repayment_analysis=repayment_analysis,
            fast_in_fast_out_analysis=fast_in_fast_out,
            cash_retention_analysis=cash_retention,
            flow_nature=flow_nature,
        )
        by_account_no: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for tx in transactions:
            by_account_no[str(tx.get("account_no") or "")].append(tx)
        accounts = []
        for account in accounts_meta or [{"account_no": "", "bank_name": "", "account_name": owner.get("name") or "", "currency": "人民币", "statement_period": period}]:
            account_txs = by_account_no.get(str(account.get("account_no") or ""), transactions if len(accounts_meta) <= 1 else [])
            account_clean = _build_clean_summary(account_txs)
            account_raw_income = _sum(account_txs, "credit_amount")
            account_raw_expense = _sum(account_txs, "debit_amount")
            accounts.append(
                Account(
                    bank_name=account.get("bank_name") or "",
                    account_name=account.get("account_name") or owner.get("name") or "",
                    account_no=account.get("account_no") or "",
                    currency=account.get("currency") or "人民币",
                    statement_period=StatementPeriod(**(account.get("statement_period") or period)),
                    raw_summary=RawSummary(
                        total_income=account_raw_income,
                        total_expense=account_raw_expense,
                        income_count=sum(1 for tx in account_txs if float(tx.get("credit_amount") or 0) > 0),
                        expense_count=sum(1 for tx in account_txs if float(tx.get("debit_amount") or 0) > 0),
                        net_cash_flow=round2(account_raw_income - account_raw_expense),
                    ),
                    clean_summary=CleanSummary(**account_clean),
                    monthly_trend=_build_monthly_trend(account_txs),
                    top_income_counterparties=counterparty.get("top_income_counterparties") or [],
                    top_expense_counterparties=counterparty.get("top_expense_counterparties") or [],
                    transactions=[Transaction(**tx) for tx in account_txs],
                )
            )
        data = PersonalBankStatementExtraction(
            source_file=source_file,
            bank_name=(accounts_meta[0].get("bank_name") if accounts_meta else "") or "",
            account_name=(accounts_meta[0].get("account_name") if accounts_meta else owner.get("name")) or "",
            account_no=(accounts_meta[0].get("account_no") if accounts_meta else "") or "",
            currency=(accounts_meta[0].get("currency") if accounts_meta else "人民币") or "人民币",
            statement_period=StatementPeriod(**period),
            raw_summary=RawSummary(**_build_raw_summary(transactions)),
            income_verification=income_verification,
            expense_analysis=expense_summary,
            cash_retention_analysis=cash_retention,
            repayment_analysis=repayment_analysis,
            fast_in_fast_out_analysis=fast_in_fast_out,
            flow_nature=flow_nature,
            monthly_trend=monthly,
            top_income_counterparties=counterparty.get("top_income_counterparties") or [],
            top_expense_counterparties=counterparty.get("top_expense_counterparties") or [],
            transactions=[Transaction(**tx) for tx in transactions],
            owner=owner,
            accounts=accounts,
            customer_level_summary=CustomerLevelSummary(**customer_summary),
            risk_signals=risk_signals,
            financing_judgement=judgement,
            warnings=[],
        )
        extracted_json = to_plain_dict(data)
        extracted_json["internal_transfer_summary"] = internal_summary
        extracted_json["income_analysis_detail"] = income_analysis
        extracted_json["expense_analysis_detail"] = expense_analysis
        validation_warnings = validate_personal_bank_statement_result(extracted_json)
        warnings.extend(validation_warnings)
        if validation_warnings and not transactions:
            extracted_json["extraction_status"] = "failed"
        extracted_json["warnings"] = list(dict.fromkeys(str(item) for item in warnings if item))
        markdown = render_personal_bank_statement_markdown(extracted_json)
    except Exception as exc:
        logger.exception("[PersonalFlow] extraction failed file=%s", source_file)
        extracted_json = to_plain_dict(
            PersonalBankStatementExtraction(
                source_file=source_file,
                extraction_status="failed",
                warnings=[f"个人流水解析失败：{exc}"],
            )
        )
        markdown = render_personal_bank_statement_markdown(extracted_json)
    return {
        "title": "个人流水分析报告",
        "type": "personal_flow",
        "document_type": "personal_flow",
        "document_type_code": "personal_flow",
        "normalized_document_type": "personal_bank_statement",
        "schema_version": "personal_bank_statement.agent.v1",
        "skill_name": "personal_bank_statement_agent",
        "extracted_json": extracted_json,
        "markdown_summary": markdown,
        "markdown": markdown,
        "summary": markdown,
        "data": extracted_json,
        "confidence": _confidence(extracted_json),
        "warnings": extracted_json.get("warnings") or [],
        "raw_text_preview": str(text if text is not None else raw_text or "")[:3000],
    }
