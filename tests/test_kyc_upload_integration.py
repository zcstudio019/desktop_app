from __future__ import annotations

import json

from backend.services.document_extractor_service import KYC_DOC_TYPES, run_document_extraction


def test_business_license_text_dispatches_to_kyc_document_agent() -> None:
    result = run_document_extraction(
        text="""
营业执照
统一社会信用代码 91310115MA1K3ABCDE
名称 上海示例科技有限公司
法定代表人 李四
注册资本 人民币1000万元
成立日期 2020年05月20日
住所 上海市浦东新区世纪大道100号
""",
        pages=[],
        filename="business-license.pdf",
        customer_id="enterprise_上海示例科技有限公司",
        declared_doc_type=None,
    )

    assert result["agent_type"] == "kyc_document_agent"
    assert result["doc_type"] == "business_license"
    assert result["document_type_code"] == "business_license"
    assert result["fields"]["company_name"] == "上海示例科技有限公司"


def test_id_card_text_dispatches_to_kyc_document_agent() -> None:
    result = run_document_extraction(
        text="""
居民身份证
姓名 张三
性别 男
出生 1949年12月31日
公民身份号码 11010519491231002X
""",
        pages=[],
        filename="id-card.png",
        customer_id="personal_张三",
        declared_doc_type=None,
    )

    assert result["agent_type"] == "kyc_document_agent"
    assert result["doc_type"] == "id_card"
    assert result["fields"]["name"] == "张三"


def test_non_kyc_declared_text_does_not_dispatch_to_kyc_document_agent() -> None:
    result = run_document_extraction(
        text="借款合同\n甲方 上海示例科技有限公司\n乙方 某银行\n金额 100万元",
        pages=[],
        filename="contract.pdf",
        customer_id="enterprise_上海示例科技有限公司",
        declared_doc_type="other_document",
    )

    assert result.get("agent_type") != "kyc_document_agent"
    assert result.get("document_type_code") == "other_document"


def test_legacy_upload_type_maps_to_kyc_agent_type() -> None:
    result = run_document_extraction(
        text="""
开户许可证
存款人名称 上海示例科技有限公司
账号 123456789012345678
开户银行 中国工商银行上海分行营业部
核准号 J100000000001
""",
        pages=[],
        filename="account-license.pdf",
        customer_id="enterprise_上海示例科技有限公司",
        declared_doc_type="account_license",
    )

    assert result["agent_type"] == "kyc_document_agent"
    assert result["doc_type"] == "account_permit"
    assert result["document_type_code"] in KYC_DOC_TYPES


def test_kyc_extraction_result_is_structured_json_serializable() -> None:
    result = run_document_extraction(
        text="""
营业执照
统一社会信用代码 91310115MA1K3ABCDE
名称 上海示例科技有限公司
法定代表人 李四
""",
        pages=[],
        filename="business-license.pdf",
        customer_id="enterprise_上海示例科技有限公司",
        declared_doc_type="business_license",
    )

    extraction_record = {
        "agent_type": result["agent_type"],
        "doc_type": result["doc_type"],
        "doc_type_name": result["doc_type_name"],
        "owner_type": result["owner_type"],
        "extraction_status": result["extraction_status"],
        "fields": result["fields"],
        "validation": result["validation"],
        "confidence": result["confidence"],
        "evidence": result["evidence"],
        "missing_fields": result["missing_fields"],
        "markdown": result["markdown"],
    }
    encoded = json.dumps(extraction_record, ensure_ascii=False)
    decoded = json.loads(encoded)

    assert decoded["agent_type"] == "kyc_document_agent"
    assert decoded["fields"]["company_name"] == "上海示例科技有限公司"
