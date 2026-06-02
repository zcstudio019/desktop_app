from backend.services.customer_financing_diagnostic_report_service import build_customer_financing_diagnostic_report


def _build_report(completeness=None, diagnostic=None):
    return build_customer_financing_diagnostic_report(
        "customer-1",
        {
            "name": "示例客户",
            "customer_type": "enterprise",
            "phone": "13800000000",
            "intent_level": "A",
            "status": "跟进中",
        },
        {"customer_id": "customer-1", "documents": []},
        completeness or {},
        diagnostic or {},
    )


def test_empty_kyc_materials_still_generate_draft_report():
    report = _build_report()

    assert report["report_status"] == "draft"
    assert report["report_type"] == "customer_financing_diagnostic"
    assert report["financing_readiness"]["readiness_level"] == "not_ready"
    assert "暂无足够资料" in report["financing_readiness"]["summary"]


def test_kyc_diagnostic_result_enters_report():
    diagnostic = {
        "diagnostic_type": "kyc_financing_readiness",
        "readiness_level": "basic_ready",
        "material_completeness_score": 70,
        "usable_for_financing": True,
        "summary": "资料基本齐全",
        "recommended_actions": ["补充资产证明"],
    }
    report = _build_report(diagnostic=diagnostic)

    assert report["kyc_diagnostic"] == diagnostic


def test_required_missing_enters_material_checklist():
    report = _build_report(completeness={"required_missing": ["营业执照", "法人身份证"]})

    assert report["material_checklist"]["required_missing"] == ["营业执照", "法人身份证"]


def test_conflicts_warnings_and_key_risks_enter_risk_highlights():
    report = _build_report(
        completeness={"conflicts": ["企业名称与账户名称不一致"], "warnings": ["统一社会信用代码缺失"]},
        diagnostic={"key_risks": ["法定代表人与身份证姓名不一致"]},
    )

    assert report["risk_highlights"] == [
        "法定代表人与身份证姓名不一致",
        "企业名称与账户名称不一致",
        "统一社会信用代码缺失",
    ]


def test_financing_readiness_uses_diagnostic_result():
    report = _build_report(
        diagnostic={
            "material_completeness_score": 88,
            "usable_for_financing": True,
            "readiness_level": "ready",
            "summary": "已具备初步评估条件",
        }
    )

    assert report["financing_readiness"] == {
        "usable_for_financing": True,
        "readiness_level": "ready",
        "score": 88,
        "summary": "已具备初步评估条件",
    }


def test_report_markdown_contains_title():
    report = _build_report()

    assert "客户融资诊断报告" in report["report_markdown"]


def test_report_service_does_not_persist_or_add_table_contract():
    report = _build_report(completeness={"suggestions": ["补充开户信息"]})

    assert "id" not in report
    assert "table" not in report
    assert report["material_checklist"]["recommended_supplements"] == ["补充开户信息"]
