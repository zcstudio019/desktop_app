from __future__ import annotations

from backend.services.document_agents.registry import get_document_agent
from backend.services.document_extractor_service import KYC_DOC_TYPES, run_document_extraction
from backend.services.kyc_document_agent.orchestrator import KycDocumentAgent


def test_property_cert_routes_to_property_cert_agent() -> None:
    result = run_document_extraction(
        "不动产权证书\n权利人 沃志方\n坐落 上海市宝山区示例路1号\n建筑面积 88.88平方米",
        [],
        "房产正面.pdf",
        "customer-1",
        declared_doc_type="property_cert",
    )

    assert result["agent_type"] == "property_cert_agent"
    assert result["document_type_code"] == "property_cert"


def test_property_cert_agent_is_registered_for_real_estate_cert() -> None:
    assert get_document_agent("property_cert").agent_name == "property_cert_agent"
    assert get_document_agent("real_estate_cert").agent_name == "property_cert_agent"


def test_kyc_document_agent_no_longer_handles_property_cert() -> None:
    assert "property_cert" not in KYC_DOC_TYPES
    assert "real_estate_cert" not in KYC_DOC_TYPES

    result = KycDocumentAgent().extract(
        {
            "text": "不动产权证书\n权利人 沃志方\n不动产单元号 310000000000GB00001F00010001",
            "metadata": {"filename": "房产正面.pdf", "declared_doc_type": "property_cert"},
        }
    )
    assert result["doc_type"] == "unknown"
    assert "PropertyCertAgent" in result["classification_reason"]


def test_simple_kyc_documents_still_use_kyc_agent() -> None:
    result = run_document_extraction(
        "居民身份证\n姓名 张三\n公民身份号码 11010519491231002X",
        [],
        "身份证.pdf",
        "customer-1",
        declared_doc_type="id_card",
    )

    assert result["agent_type"] == "kyc_document_agent"
    assert result["document_type_code"] == "id_card"
