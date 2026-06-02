from backend.services.customer_financing_diagnostic_report_service import build_customer_financing_diagnostic_report
from backend.services.personal_credit_diagnostic_service import build_personal_credit_diagnostic_from_payload


def _payload(**overrides):
    payload = {
        "schema_version": "personal_credit_report.agent.v1",
        "credit_summary": {},
        "loan_accounts": [],
        "credit_card_accounts": [],
        "overdue_records": [],
        "query_statistics": {},
        "guarantees": [],
        "public_records": [],
    }
    payload.update(overrides)
    return payload


def test_no_personal_credit_report_returns_unknown():
    diagnostic = build_personal_credit_diagnostic_from_payload(None)

    assert diagnostic["has_personal_credit_report"] is False
    assert diagnostic["credit_status"] == "unknown"
    assert diagnostic["key_risks"]


def test_missing_personal_credit_enters_report_required_missing():
    personal_credit = build_personal_credit_diagnostic_from_payload(None)
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "测试客户"},
        {},
        {"required_missing": []},
        {"recommended_actions": []},
        {},
        personal_credit,
    )

    assert "法人/实际控制人个人征信报告" in report["material_checklist"]["required_missing"]


def test_loan_overdue_raises_attention_or_higher():
    diagnostic = build_personal_credit_diagnostic_from_payload(
        _payload(loan_accounts=[{"institution": "A银行", "balance": 100, "overdue_amount": 10, "overdue_months": 1}])
    )

    assert diagnostic["credit_status"] in {"attention", "risky"}
    assert diagnostic["overdue_summary"]["has_loan_overdue"] is True


def test_credit_card_overdue_raises_attention_or_higher():
    diagnostic = build_personal_credit_diagnostic_from_payload(
        _payload(credit_card_accounts=[{"issuer": "B银行", "used_amount": 1000, "overdue_amount": 50, "overdue_months": 1}])
    )

    assert diagnostic["credit_status"] in {"attention", "risky"}
    assert diagnostic["overdue_summary"]["has_credit_card_overdue"] is True


def test_serious_negative_is_risky():
    diagnostic = build_personal_credit_diagnostic_from_payload(
        _payload(public_records=[{"record_type": "强制执行", "amount": 10000, "status": "执行中"}])
    )

    assert diagnostic["credit_status"] == "risky"
    assert diagnostic["serious_negative_summary"]["has_serious_negative"] is True


def test_last_3_months_query_count_high():
    diagnostic = build_personal_credit_diagnostic_from_payload(
        _payload(query_statistics={"institution_query": {"last_3_months": 6, "last_6_months": 6}})
    )

    assert diagnostic["query_summary"]["query_risk_level"] == "high"


def test_last_6_months_query_count_high():
    diagnostic = build_personal_credit_diagnostic_from_payload(
        _payload(query_statistics={"institution_query": {"last_3_months": 2, "last_6_months": 10}})
    )

    assert diagnostic["query_summary"]["query_risk_level"] == "high"


def test_external_guarantee_creates_risk():
    diagnostic = build_personal_credit_diagnostic_from_payload(
        _payload(guarantees=[{"guarantee_balance": 88, "guarantee_status": "有效"}])
    )

    assert diagnostic["debt_summary"]["external_guarantee_balance"] == 88
    assert "个人征信存在对外担保，可能形成或有负债" in diagnostic["key_risks"]


def test_personal_credit_diagnostic_enters_financing_report():
    personal_credit = build_personal_credit_diagnostic_from_payload(
        _payload(loan_accounts=[{"institution": "A银行", "balance": 100}])
    )
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "测试客户"},
        {},
        {"required_missing": []},
        {"recommended_actions": []},
        {},
        personal_credit,
    )

    assert report["personal_credit_diagnostic"] == personal_credit


def test_report_markdown_contains_personal_credit_section():
    personal_credit = build_personal_credit_diagnostic_from_payload(None)
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "测试客户"},
        {},
        {},
        {},
        {},
        personal_credit,
    )

    assert "个人征信诊断" in report["report_markdown"]
