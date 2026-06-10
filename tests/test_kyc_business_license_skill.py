from __future__ import annotations

import asyncio

from backend.services.kyc_document_agent import KycDocumentAgent
from backend.services.kyc_document_agent.classifier import classify
from backend.services.kyc_document_agent.renderer import render_markdown
from backend.services.kyc_profile_sync_service import build_customer_kyc_profile


FULL_OCR_TEXT = """
营业执照
统一社会信用代码 91320105S808481947
证照编号 05000000202309200049
名称 上海乐美兰电子商务有限公司
类型 有限责任公司（自然人投资或控股）
法定代表人 沃志方
注册资本 人民币500.0000万元整
成立日期 2011年09月28日
住所 南京市长宁区广顺路33号3幢6层672室
经营范围 许可项目：食品经营；一般项目：互联网销售、电子商务、日用百货销售。
登记机关 上海市长宁区市场监督管理局
2023年09月20日
"""


class FakeStorage:
    def __init__(self, extractions: list[dict]) -> None:
        self.extractions = extractions

    async def get_extractions_by_customer(self, customer_id: str) -> list[dict]:
        return self.extractions

    async def list_documents(self, customer_id: str) -> list[dict]:
        return []


def extract(text: str, declared_doc_type: str | None = None) -> dict:
    return KycDocumentAgent().extract({
        "text": text,
        "metadata": {"declared_doc_type": declared_doc_type} if declared_doc_type else {},
    })


def kyc_extraction(fields: dict, confirmed_fields: dict | None = None, doc_id: str = "doc-business", created_at: str = "2026-06-01T10:00:00") -> dict:
    return {
        "doc_id": doc_id,
        "created_at": created_at,
        "extraction_type": "business_license",
        "extraction_status": "success",
        "confirmed_data": {"confirmed_fields": confirmed_fields or {}},
        "extracted_data": {
            "agent_type": "kyc_document_agent",
            "doc_type": "business_license",
            "doc_type_name": "营业执照",
            "extraction_status": "success",
            "fields": fields,
        },
    }


def build_profile(extractions: list[dict]) -> dict:
    return asyncio.run(build_customer_kyc_profile(FakeStorage(extractions), "customer-1"))


def test_full_business_license_ocr_text_extracts_structured_fields() -> None:
    result = extract(FULL_OCR_TEXT)

    assert result["doc_type"] == "business_license"
    assert result["doc_type_name"] == "营业执照"
    assert result["owner_type"] == "enterprise"
    fields = result["fields"]
    assert fields["company_name"] == "上海乐美兰电子商务有限公司"
    assert fields["unified_social_credit_code"] == "91320105S808481947"
    assert fields["license_number"] == "05000000202309200049"
    assert fields["legal_representative"] == "沃志方"
    assert fields["registered_capital"] == "人民币500.0000万元整"
    assert fields["establishment_date"] == "2011-09-28"
    assert "南京市长宁区广顺路33号3幢6层672室" in fields["registered_address"]
    assert "许可项目" in fields["business_scope"]
    assert "市场监督管理局" in fields["registration_authority"]
    assert fields["issue_date"] == "2023-09-20"


def test_business_scope_can_span_multiple_lines_without_being_truncated() -> None:
    result = extract("""
营业执照
统一社会信用代码 91320105S808481947
名称 上海乐美兰电子商务有限公司
法定代表人 沃志方
住所 上海市长宁区广顺路33号
经营范围 许可项目：食品经营；
一般项目：互联网销售、日用百货销售、信息咨询服务。
登记机关 上海市长宁区市场监督管理局
2023年09月20日
""")

    scope = result["fields"]["business_scope"]
    assert "许可项目" in scope
    assert "一般项目" in scope
    assert "登记机关" not in scope


def test_registered_address_can_span_multiple_lines_and_stops_before_scope() -> None:
    result = extract("""
营业执照
统一社会信用代码 91320105S808481947
名称 上海乐美兰电子商务有限公司
法定代表人 沃志方
住所 上海市长宁区
广顺路33号3幢6层672室
经营范围 一般项目：电子商务。
登记机关 上海市长宁区市场监督管理局
""")

    address = result["fields"]["registered_address"]
    assert "上海市长宁区广顺路33号3幢6层672室" in address
    assert "经营范围" not in address
    assert "一般项目" not in address


def test_abnormal_credit_code_length_warns_but_does_not_force_failed() -> None:
    result = extract("""
营业执照
统一社会信用代码 91320105S80848194
名称 上海乐美兰电子商务有限公司
法定代表人 沃志方
住所 上海市长宁区广顺路33号
经营范围 一般项目：电子商务。
""")

    assert result["extraction_status"] in {"partial", "success"}
    assert "统一社会信用代码长度异常，请人工核对" in result["validation"]["warnings"]


def test_partial_core_fields_extracts_partial_status() -> None:
    result = extract("""
营业执照
名称 上海乐美兰电子商务有限公司
法定代表人 沃志方
""")

    assert result["extraction_status"] == "partial"


def test_classifier_recognizes_business_license() -> None:
    assert classify("营业执照\n统一社会信用代码 91320105S808481947\n法定代表人 沃志方") == "business_license"


def test_classifier_does_not_misclassify_food_business_license() -> None:
    text = "食品经营许可证\n统一社会信用代码 91320105S808481947\n法定代表人 沃志方\n经营项目 食品销售"
    assert classify(text) == "food_business_license"


def test_renderer_outputs_chinese_markdown_without_raw_json_or_english_keys() -> None:
    result = extract(FULL_OCR_TEXT)
    markdown = render_markdown(result)

    assert "```json" not in markdown
    assert "company_name" not in markdown
    assert "unified_social_credit_code" not in markdown
    assert "registration_authority" not in markdown
    assert "fields" not in markdown
    assert "{" not in markdown
    assert "}" not in markdown
    assert "### 企业基础信息" in markdown
    assert "- 名称：上海乐美兰电子商务有限公司" in markdown
    assert "登记机关：上海市长宁区市场监督管理局" in markdown


def test_registration_authority_with_explicit_label_extracts() -> None:
    result = extract("""
营业执照
统一社会信用代码 91320105S808481947
名称 上海乐美兰电子商务有限公司
法定代表人 沃志方
登记机关 上海市长宁区市场监督管理局
2023年09月20日
""")

    assert result["fields"]["registration_authority"] == "上海市长宁区市场监督管理局"
    assert result["fields"]["issue_date"] == "2023-09-20"


def test_registration_authority_minimal_same_line_extracts() -> None:
    result = extract("""
营业执照
登记机关 上海市长宁区市场监督管理局
2023年09月20日
""", declared_doc_type="business_license")

    assert result["fields"]["registration_authority"] == "上海市长宁区市场监督管理局"


def test_registration_authority_after_label_newline_extracts() -> None:
    result = extract("""
登记机关
上海市长宁区市场监督管理局
2023年09月20日
""", declared_doc_type="business_license")

    assert result["fields"]["registration_authority"] == "上海市长宁区市场监督管理局"


def test_registration_authority_from_seal_text_without_label_extracts() -> None:
    result = extract("""
营业执照
名称 上海乐美兰电子商务有限公司
法定代表人 沃志方
上海市长宁区市场监督管理局
2023年09月20日
""")

    assert result["fields"]["registration_authority"] == "上海市长宁区市场监督管理局"


def test_registration_authority_minimal_seal_text_without_label_extracts() -> None:
    result = extract("""
营业执照
上海市长宁区市场监督管理局
2023年09月20日
""", declared_doc_type="business_license")

    assert result["fields"]["registration_authority"] == "上海市长宁区市场监督管理局"


def test_registration_authority_from_split_seal_lines_extracts() -> None:
    result = extract("""
营业执照
名称 上海乐美兰电子商务有限公司
法定代表人 沃志方
上海市长宁区
市场监督管理局
2023年09月20日
""")

    assert result["fields"]["registration_authority"] == "上海市长宁区市场监督管理局"


def test_registration_authority_from_market_split_line_extracts() -> None:
    result = extract("""
营业执照
上海市长宁区市场
监督管理局
2023年09月20日
""", declared_doc_type="business_license")

    assert result["fields"]["registration_authority"] == "上海市长宁区市场监督管理局"


def test_registration_authority_does_not_use_date_when_name_missing() -> None:
    result = extract("""
登记机关
2023年09月20日
""", declared_doc_type="business_license")

    assert not result["fields"].get("registration_authority")
    assert "registration_authority" in result["missing_fields"]


def test_registration_authority_does_not_use_label_itself() -> None:
    result = extract("""
营业执照
登记机关
""", declared_doc_type="business_license")

    assert not result["fields"].get("registration_authority")


def test_business_license_fields_enter_enterprise_identity() -> None:
    profile = build_profile([
        kyc_extraction({
            "company_name": "上海乐美兰电子商务有限公司",
            "unified_social_credit_code": "91320105S808481947",
            "legal_representative": "沃志方",
            "registered_capital": "人民币500.0000万元整",
            "company_type": "有限责任公司（自然人投资或控股）",
            "establishment_date": "2011-09-28",
            "registered_address": "南京市长宁区广顺路33号3幢6层672室",
            "business_scope": "许可项目：食品经营",
            "registration_authority": "上海市长宁区市场监督管理局",
            "issue_date": "2023-09-20",
        })
    ])

    enterprise = profile["enterprise_identity"]
    assert enterprise["company_name"] == "上海乐美兰电子商务有限公司"
    assert enterprise["company_type"] == "有限责任公司（自然人投资或控股）"
    assert enterprise["registration_authority"] == "上海市长宁区市场监督管理局"
    assert enterprise["source_document_id"] == "doc-business"


def test_confirmed_data_takes_priority_over_extracted_data() -> None:
    profile = build_profile([
        kyc_extraction(
            {"company_name": "OCR公司", "legal_representative": "OCR姓名"},
            {"company_name": "人工确认公司", "legal_representative": "人工确认姓名"},
        )
    ])

    assert profile["enterprise_identity"]["company_name"] == "人工确认公司"
    assert profile["enterprise_identity"]["legal_representative"] == "人工确认姓名"
