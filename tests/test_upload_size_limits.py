from __future__ import annotations

from backend.upload_limits import get_upload_size_limit_mb


def test_large_document_upload_limits() -> None:
    assert get_upload_size_limit_mb("contract") == 200
    assert get_upload_size_limit_mb("enterprise_flow") == 200
    assert get_upload_size_limit_mb("personal_flow") == 200
    assert get_upload_size_limit_mb("bank_statement") == 200


def test_credit_and_lightweight_upload_limits() -> None:
    assert get_upload_size_limit_mb("enterprise_credit") == 100
    assert get_upload_size_limit_mb("personal_credit_report") == 100
    assert get_upload_size_limit_mb("id_card") == 50
    assert get_upload_size_limit_mb("business_license") == 50
    assert get_upload_size_limit_mb("") == 50


def test_contract_filename_gets_contract_limit_without_type_hint() -> None:
    filename = "合同001：张江创新药基地机电安装专业分包工程.pdf"
    assert get_upload_size_limit_mb(None, filename) == 200
