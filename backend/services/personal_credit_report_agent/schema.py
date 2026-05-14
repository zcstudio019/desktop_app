from __future__ import annotations

from copy import deepcopy
from typing import Any


SUMMARY_FIELDS = (
    "credit_card_account_count",
    "active_credit_card_account_count",
    "loan_account_count",
    "outstanding_loan_account_count",
    "credit_card_overdue_account_count",
    "credit_card_90d_overdue_account_count",
    "loan_overdue_account_count",
    "loan_90d_overdue_account_count",
    "personal_related_repayment_responsibility_account_count",
    "enterprise_related_repayment_responsibility_account_count",
)

BASIC_INFO_FIELDS = (
    "name",
    "id_type",
    "id_number",
    "report_number",
    "report_time",
    "marital_status",
    "source_file",
)

LOAN_ACCOUNT_FIELDS = (
    "account_no",
    "institution",
    "business_type",
    "open_date",
    "due_date",
    "amount",
    "issued_amount",
    "balance",
    "account_status",
    "five_category",
    "overdue_amount",
    "overdue_months",
    "latest_repayment_date",
    "latest_repayment_amount",
    "overdue_info",
    "last_repayment",
    "history_performance",
    "information_report_date",
    "evidence",
    "evidence_text",
)

CREDIT_CARD_ACCOUNT_FIELDS = (
    "account_no",
    "institution",
    "issuer",
    "card_type",
    "account_type",
    "currency",
    "account_status",
    "credit_limit",
    "used_limit",
    "used_amount",
    "overdue_amount",
    "overdue_months",
    "latest_repayment_date",
    "latest_repayment_amount",
    "last_repayment",
    "history_performance",
    "information_report_date",
    "evidence",
    "evidence_text",
)

QUERY_RECORD_FIELDS = (
    "query_date",
    "query_institution",
    "query_reason",
    "query_type",
    "evidence",
    "evidence_text",
)

GUARANTEE_FIELDS = (
    "guarantee_for",
    "guarantee_amount",
    "guarantee_balance",
    "guarantee_status",
    "evidence_text",
)

PUBLIC_RECORD_FIELDS = (
    "record_type",
    "record_date",
    "content",
    "amount",
    "authority",
    "evidence_text",
)

NON_CREDIT_TRANSACTION_FIELDS = (
    "record_type",
    "date",
    "institution",
    "amount",
    "content",
    "evidence",
)

OVERDUE_RECORD_FIELDS = (
    "record_type",
    "institution",
    "amount",
    "months",
    "status",
    "evidence_text",
)

RELATED_REPAYMENT_RESPONSIBILITY_FIELDS = (
    "start_date",
    "related_party",
    "responsibility_type",
    "institution",
    "responsibility_amount",
    "loan_balance",
    "contract_no",
    "as_of_date",
    "evidence",
)


def default_basic_info() -> dict[str, Any]:
    return {field: "" for field in BASIC_INFO_FIELDS}


def default_credit_summary() -> dict[str, Any]:
    return {field: None for field in SUMMARY_FIELDS}


def default_query_statistics() -> dict[str, dict[str, int]]:
    return {
        "institution_query": {
            "last_1_month": 0,
            "last_3_months": 0,
            "last_6_months": 0,
        },
        "personal_query": {
            "last_1_month": 0,
            "last_3_months": 0,
            "last_6_months": 0,
        },
    }


def default_report_json() -> dict[str, Any]:
    return {
        "schema_version": "personal_credit_report.agent.v1",
        "basic_info": default_basic_info(),
        "credit_summary": default_credit_summary(),
        "loan_accounts": [],
        "credit_card_accounts": [],
        "related_repayment_responsibilities": [],
        "guarantees": [],
        "non_credit_transactions": [],
        "overdue_records": [],
        "public_records": [],
        "query_records": [],
        "query_statistics": default_query_statistics(),
        "personal_credit_indicators": {},
        "risk_flags": [],
        "missing_fields": [],
        "warnings": [],
    }


def ensure_record_fields(record: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    normalized = {field: "" for field in fields}
    normalized.update({key: value for key, value in (record or {}).items() if value is not None})
    return normalized


def clone_default_report_json() -> dict[str, Any]:
    return deepcopy(default_report_json())
