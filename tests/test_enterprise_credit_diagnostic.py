from backend.services.customer_financing_diagnostic_report_service import build_customer_financing_diagnostic_report
from backend.services.enterprise_credit_diagnostic_service import build_enterprise_credit_diagnostic_from_payload


def _payload(**overrides):
    payload = {
        "schema_version": "enterprise_credit.agent.v1",
        "credit_summary": {},
        "active_loans": [],
        "credit_facilities": [],
        "bank_guarantee_other_business": [],
    }
    payload.update(overrides)
    return payload


def test_no_enterprise_credit_report_returns_unknown():
    diagnostic = build_enterprise_credit_diagnostic_from_payload(None)

    assert diagnostic["has_enterprise_credit_report"] is False
    assert diagnostic["credit_status"] == "unknown"
    assert diagnostic["key_risks"]


def test_missing_enterprise_credit_enters_report_required_missing():
    enterprise_credit = build_enterprise_credit_diagnostic_from_payload(None)
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "测试客户"},
        {},
        {"required_missing": []},
        {"recommended_actions": []},
        enterprise_credit,
    )

    assert "企业征信报告" in report["material_checklist"]["required_missing"]


def test_active_loans_are_counted():
    diagnostic = build_enterprise_credit_diagnostic_from_payload(
        _payload(
            active_loans=[
                {"institution": "A银行", "balance": 100, "due_date": "2099-01-01"},
                {"institution": "B银行", "balance": 200, "due_date": "2099-02-01"},
            ]
        )
    )

    assert diagnostic["loan_summary"]["active_loan_count"] == 2
    assert diagnostic["debt_summary"]["total_unsettled_balance"] == 300


def test_overdue_loans_raise_attention_or_higher():
    diagnostic = build_enterprise_credit_diagnostic_from_payload(
        _payload(active_loans=[{"institution": "A银行", "balance": 100, "overdue_months": 1}])
    )

    assert diagnostic["credit_status"] in {"attention", "risky"}
    assert diagnostic["loan_summary"]["overdue_loans"]


def test_abnormal_classification_is_risky():
    diagnostic = build_enterprise_credit_diagnostic_from_payload(
        _payload(active_loans=[{"institution": "A银行", "balance": 100, "five_classification": "关注"}])
    )

    assert diagnostic["credit_status"] == "risky"
    assert diagnostic["loan_summary"]["abnormal_classification_loans"]


def test_external_guarantee_sets_flag():
    diagnostic = build_enterprise_credit_diagnostic_from_payload(
        _payload(credit_summary={"guarantee_balance": 50})
    )

    assert diagnostic["guarantee_summary"]["has_external_guarantee"] is True


def test_high_credit_usage_rate_creates_risk():
    diagnostic = build_enterprise_credit_diagnostic_from_payload(
        _payload(credit_facilities=[{"credit_amount": 100, "used_amount": 85}])
    )

    assert diagnostic["debt_summary"]["credit_usage_rate"] == 0.85
    assert "授信使用率较高，新增授信空间可能受限" in diagnostic["key_risks"]


def test_enterprise_credit_diagnostic_enters_financing_report():
    enterprise_credit = build_enterprise_credit_diagnostic_from_payload(
        _payload(active_loans=[{"institution": "A银行", "balance": 100}])
    )
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "测试客户"},
        {},
        {"required_missing": []},
        {"recommended_actions": []},
        enterprise_credit,
    )

    assert report["enterprise_credit_diagnostic"] == enterprise_credit


def test_report_markdown_contains_enterprise_credit_section():
    enterprise_credit = build_enterprise_credit_diagnostic_from_payload(None)
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "测试客户"},
        {},
        {},
        {},
        enterprise_credit,
    )

    assert "企业征信诊断" in report["report_markdown"]
