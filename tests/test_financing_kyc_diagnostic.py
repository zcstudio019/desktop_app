from __future__ import annotations

from backend.services.financing_kyc_diagnostic_service import build_financing_kyc_diagnostic


def complete_profile() -> dict:
    return {
        "customer_id": "customer-1",
        "enterprise_identity": {
            "company_name": "上海示例科技有限公司",
            "unified_social_credit_code": "91310115MA1K3ABCDE",
            "legal_representative": "李四",
            "source_document_id": "doc-business",
        },
        "person_identity": {
            "name": "李四",
            "id_number": "11010519491231002X",
            "source_document_id": "doc-id",
        },
        "bank_account": {
            "account_name": "上海示例科技有限公司",
            "account_number": "123456789012345678",
            "opening_bank": "中国工商银行上海分行营业部",
            "source_document_id": "doc-bank",
        },
        "assets": {"properties": [], "vehicles": []},
        "documents": [{"doc_id": "doc-business"}, {"doc_id": "doc-id"}, {"doc_id": "doc-bank"}],
    }


def completeness(score: int = 90, required_missing: list[str] | None = None, conflicts: list[str] | None = None) -> dict:
    return {
        "completeness_score": score,
        "required_missing": required_missing or [],
        "optional_missing": [],
        "warnings": [],
        "conflicts": conflicts or [],
        "suggestions": [],
    }


def test_no_kyc_profile_returns_not_ready() -> None:
    result = build_financing_kyc_diagnostic({"customer_id": "customer-1", "documents": []}, completeness(0))

    assert result["readiness_level"] == "not_ready"
    assert result["usable_for_financing"] is False
    assert "暂无足够KYC资料" in result["summary"]


def test_missing_business_license_returns_not_ready() -> None:
    result = build_financing_kyc_diagnostic(complete_profile(), completeness(required_missing=["营业执照"]))

    assert result["readiness_level"] == "not_ready"
    assert result["enterprise_status"] == "missing"
    assert "营业执照" in result["missing_materials"]


def test_missing_legal_person_id_returns_not_ready() -> None:
    result = build_financing_kyc_diagnostic(complete_profile(), completeness(required_missing=["法人身份证"]))

    assert result["readiness_level"] == "not_ready"
    assert result["identity_status"] == "missing"
    assert "法人身份证" in result["missing_materials"]


def test_missing_bank_account_returns_not_ready() -> None:
    result = build_financing_kyc_diagnostic(complete_profile(), completeness(required_missing=["开户许可证/基本存款账户信息"]))

    assert result["readiness_level"] == "not_ready"
    assert result["bank_account_status"] == "missing"
    assert "开户许可证/基本存款账户信息" in result["missing_materials"]


def test_complete_required_but_low_score_returns_basic_ready() -> None:
    result = build_financing_kyc_diagnostic(complete_profile(), completeness(score=75))

    assert result["readiness_level"] == "basic_ready"
    assert result["usable_for_financing"] is True


def test_complete_required_and_high_score_returns_ready() -> None:
    result = build_financing_kyc_diagnostic(complete_profile(), completeness(score=88))

    assert result["readiness_level"] == "ready"
    assert result["usable_for_financing"] is True


def test_company_name_conflict_returns_not_ready() -> None:
    result = build_financing_kyc_diagnostic(
        complete_profile(),
        completeness(conflicts=["企业名称与账户名称不一致：A / B"]),
    )

    assert result["readiness_level"] == "not_ready"
    assert result["usable_for_financing"] is False
    assert result["key_risks"]


def test_legal_representative_conflict_returns_not_ready() -> None:
    result = build_financing_kyc_diagnostic(
        complete_profile(),
        completeness(conflicts=["营业执照法定代表人与身份证姓名不一致：李四 / 张三"]),
    )

    assert result["readiness_level"] == "not_ready"
    assert result["usable_for_financing"] is False


def test_asset_status_for_property_and_vehicle() -> None:
    profile = complete_profile()
    profile["assets"] = {
        "properties": [{"owner": "王五", "certificate_number": "沪房权证字第1号", "property_address": "上海市"}],
        "vehicles": [{"plate_number": "沪A12345", "vehicle_owner": "上海示例科技有限公司", "vehicle_identification_number": "LSV123456789"}],
    }
    result = build_financing_kyc_diagnostic(profile, completeness())
    assert result["asset_status"] == "complete"

    profile["assets"]["vehicles"][0]["vehicle_owner"] = ""
    result = build_financing_kyc_diagnostic(profile, completeness())
    assert result["asset_status"] == "partial"
