from __future__ import annotations


DEFAULT_UPLOAD_LIMIT_MB = 50
LARGE_UPLOAD_LIMIT_MB = 200
CREDIT_REPORT_UPLOAD_LIMIT_MB = 100

LARGE_UPLOAD_DOCUMENT_TYPES = {
    "contract",
    "enterprise_flow",
    "personal_flow",
    "bank_statement",
    "bank_reconciliation_detail",
    "enterprise_bank_statement",
    "personal_bank_statement",
}

CREDIT_REPORT_DOCUMENT_TYPES = {
    "enterprise_credit",
    "personal_credit",
    "enterprise_credit_report",
    "personal_credit_report",
}


def get_upload_size_limit_mb(document_type: str | None, filename: str = "") -> int:
    normalized_type = str(document_type or "").strip().lower()
    normalized_filename = str(filename or "").strip().lower()
    if normalized_type in LARGE_UPLOAD_DOCUMENT_TYPES or "合同" in normalized_filename:
        return LARGE_UPLOAD_LIMIT_MB
    if normalized_type in CREDIT_REPORT_DOCUMENT_TYPES:
        return CREDIT_REPORT_UPLOAD_LIMIT_MB
    return DEFAULT_UPLOAD_LIMIT_MB
