from __future__ import annotations

import asyncio

from backend.services.kyc_completeness_service import evaluate_kyc_completeness
from backend.services.kyc_profile_sync_service import build_customer_kyc_profile


class FakeStorage:
    def __init__(self, extractions: list[dict] | None = None) -> None:
        self.extractions = extractions or []

    async def get_extractions_by_customer(self, customer_id: str) -> list[dict]:
        return self.extractions

    async def list_documents(self, customer_id: str) -> list[dict]:
        return []


def kyc_extraction(
    doc_type: str,
    fields: dict,
    doc_id: str = "doc-1",
    created_at: str = "2026-05-28T10:00:00",
    confirmed_fields: dict | None = None,
) -> dict:
    extraction = {
        "doc_id": doc_id,
        "created_at": created_at,
        "extraction_type": doc_type,
        "extraction_status": "success",
        "extracted_data": {
            "agent_type": "kyc_document_agent",
            "doc_type": doc_type,
            "doc_type_name": doc_type,
            "extraction_status": "success",
            "fields": fields,
        },
    }
    if confirmed_fields is not None:
        extraction["confirmed_data"] = {
            "confirmed_fields": confirmed_fields,
            "confirm_status": "partial",
        }
        extraction["confirm_status"] = "partial"
    return extraction


def build_profile(extractions: list[dict]) -> dict:
    return asyncio.run(build_customer_kyc_profile(FakeStorage(extractions), "customer-1"))


def test_empty_kyc_profile_without_kyc_extractions() -> None:
    profile = build_profile([])

    assert profile["customer_id"] == "customer-1"
    assert profile["documents"] == []
    assert profile["enterprise_identity"]["company_name"] == ""


def test_business_license_enters_enterprise_identity() -> None:
    profile = build_profile([
        kyc_extraction(
            "business_license",
            {
                "company_name": "上海示例科技有限公司",
                "unified_social_credit_code": "91310115MA1K3ABCDE",
                "legal_representative": "李四",
                "registered_capital": {"amount": 1000, "unit": "万元"},
                "registered_address": "上海市浦东新区世纪大道100号",
                "business_scope": "技术开发",
                "establishment_date": "2020-05-20",
            },
            "doc-business",
        )
    ])

    assert profile["enterprise_identity"]["company_name"] == "上海示例科技有限公司"
    assert profile["enterprise_identity"]["registered_capital"] == "1000万元"
    assert profile["enterprise_identity"]["source_document_id"] == "doc-business"


def test_business_license_registration_authority_prefers_confirmed_data() -> None:
    profile = build_profile([
        kyc_extraction(
            "business_license",
            {
                "company_name": "上海示例科技有限公司",
                "unified_social_credit_code": "91310115MA1K3ABCDE",
                "legal_representative": "李四",
                "registered_address": "上海市浦东新区世纪大道100号",
                "business_scope": "技术开发",
                "registration_authority": "",
            },
            "doc-business",
            confirmed_fields={"registration_authority": "上海市长宁区市场监督管理局"},
        )
    ])

    enterprise = profile["enterprise_identity"]
    assert enterprise["registration_authority"] == "上海市长宁区市场监督管理局"
    assert enterprise["field_sources"]["registration_authority"]["source"] == "confirmed_data"


def test_business_license_registration_authority_ignores_invalid_extracted_value() -> None:
    profile = build_profile([
        kyc_extraction(
            "business_license",
            {
                "company_name": "上海示例科技有限公司",
                "registration_authority": "未识别",
            },
            "doc-business",
        )
    ])

    assert profile["enterprise_identity"]["registration_authority"] == ""


def test_completeness_does_not_warn_registration_authority_when_confirmed() -> None:
    profile = build_profile([
        kyc_extraction(
            "business_license",
            {
                "company_name": "上海示例科技有限公司",
                "unified_social_credit_code": "91310115MA1K3ABCDE",
                "legal_representative": "李四",
                "registered_address": "上海市浦东新区世纪大道100号",
                "business_scope": "技术开发",
                "registration_authority": "",
            },
            "doc-business",
            confirmed_fields={"registration_authority": "上海市长宁区市场监督管理局"},
        )
    ])
    completeness = evaluate_kyc_completeness(profile)

    assert not any("登记机关" in item for item in completeness["warnings"])
    assert "营业执照" not in completeness["required_missing"]


def test_id_card_enters_person_identity() -> None:
    profile = build_profile([
        kyc_extraction(
            "id_card",
            {
                "name": "张三",
                "id_number": "11010519491231002X",
                "gender": "男",
                "birth_date": "1949-12-31",
                "address": "北京市朝阳区",
            },
            "doc-id",
        )
    ])

    assert profile["person_identity"]["name"] == "张三"
    assert profile["person_identity"]["id_number"] == "11010519491231002X"
    assert profile["person_identity"]["source_document_id"] == "doc-id"


def test_account_permit_enters_bank_account() -> None:
    profile = build_profile([
        kyc_extraction(
            "account_permit",
            {
                "bank_account_name": "上海示例科技有限公司",
                "bank_account_number": "123456789012345678",
                "opening_bank": "中国工商银行上海分行营业部",
                "account_type": "基本存款账户",
            },
            "doc-bank",
        )
    ])

    assert profile["bank_account"]["account_name"] == "上海示例科技有限公司"
    assert profile["bank_account"]["account_number"] == "123456789012345678"
    assert profile["bank_account"]["source_document_id"] == "doc-bank"


def test_property_cert_enters_assets_properties() -> None:
    profile = build_profile([
        kyc_extraction(
            "property_cert",
            {
                "owner": "王五",
                "certificate_number": "沪房权证浦字第123456号",
                "property_address": "上海市浦东新区花园路88号",
                "building_area": {"value": 89.5, "unit": "平方米"},
            },
            "doc-property",
        )
    ])

    assert profile["assets"]["properties"][0]["owner"] == "王五"
    assert profile["assets"]["properties"][0]["building_area"] == "89.5平方米"


def test_vehicle_license_enters_assets_vehicles() -> None:
    profile = build_profile([
        kyc_extraction(
            "vehicle_license",
            {
                "plate_number": "沪A12345",
                "vehicle_owner": "上海示例科技有限公司",
                "vehicle_identification_number": "LSVAC6187N2187654",
            },
            "doc-vehicle",
        )
    ])

    assert profile["assets"]["vehicles"][0]["plate_number"] == "沪A12345"
    assert profile["assets"]["vehicles"][0]["vehicle_owner"] == "上海示例科技有限公司"


def test_company_name_and_account_name_conflict() -> None:
    profile = build_profile([
        kyc_extraction("business_license", {"company_name": "上海示例科技有限公司", "legal_representative": "李四"}, "doc-business"),
        kyc_extraction("account_permit", {"bank_account_name": "上海另一个公司", "bank_account_number": "1234567890"}, "doc-bank"),
        kyc_extraction("id_card", {"name": "李四", "id_number": "11010519491231002X"}, "doc-id"),
    ])

    completeness = evaluate_kyc_completeness(profile)

    assert any("企业名称与账户名称不一致" in item for item in completeness["conflicts"])


def test_legal_representative_and_id_name_conflict() -> None:
    profile = build_profile([
        kyc_extraction("business_license", {"company_name": "上海示例科技有限公司", "legal_representative": "李四"}, "doc-business"),
        kyc_extraction("id_card", {"name": "张三", "id_number": "11010519491231002X"}, "doc-id"),
        kyc_extraction("account_permit", {"bank_account_name": "上海示例科技有限公司", "bank_account_number": "1234567890"}, "doc-bank"),
    ])

    completeness = evaluate_kyc_completeness(profile)

    assert any("营业执照法定代表人与身份证姓名不一致" in item for item in completeness["conflicts"])


def test_required_missing_for_core_kyc_documents() -> None:
    profile = build_profile([])
    completeness = evaluate_kyc_completeness(profile)

    assert "营业执照" in completeness["required_missing"]
    assert "法人身份证" in completeness["required_missing"]
    assert "开户许可证/基本存款账户信息" in completeness["required_missing"]
