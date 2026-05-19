from __future__ import annotations

from typing import Any


def empty_enterprise_bank_statement_result() -> dict[str, Any]:
    return {
        "document_type": "enterprise_bank_statement",
        "account_basic_info": {
            "company_name": "",
            "bank_name": "",
            "branch_name": "",
            "account_number": "",
            "currency": "人民币",
            "statement_period_start": "",
            "statement_period_end": "",
            "opening_balance": None,
            "closing_balance": None,
        },
        "statement_summary": {
            "total_debit_amount": None,
            "total_credit_amount": None,
            "total_debit_count": None,
            "total_credit_count": None,
            "total_transaction_count": None,
            "average_daily_balance": None,
            "monthly_average_credit": None,
            "monthly_average_debit": None,
        },
        "monthly_trends": [],
        "transactions": [],
        "counterparty_analysis": [],
        "large_transactions": [],
        "loan_related_transactions": [],
        "risk_signals": [],
        "financing_analysis": {
            "cash_flow_stability": "",
            "business_reality": "",
            "repayment_capacity": "",
            "abnormal_flow_risk": "",
            "suggested_credit_limit_reference": "",
            "summary": "",
        },
        "evidence": [],
        "warnings": [],
    }


def empty_transaction() -> dict[str, Any]:
    return {
        "transaction_date": "",
        "posting_date": "",
        "summary": "",
        "counterparty_name": "",
        "counterparty_account": "",
        "debit_amount": None,
        "credit_amount": None,
        "balance": None,
        "currency": "人民币",
        "transaction_type": "",
        "usage": "",
        "remark": "",
        "source_page": None,
        "source_text": "",
    }
