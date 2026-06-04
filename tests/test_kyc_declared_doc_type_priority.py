from __future__ import annotations

from backend.services.document_extractor_service import build_structured_extraction
from backend.services.kyc_document_agent.classifier import classify_with_reason
from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent


WEAK_PROPERTY_TEXT = """
根据《中华人民共和国物权法》等法律法规
为保护不动产权利人合法权益
登记机构
中华人民共和国国土资源部监制
编号 N-D31001337469
"""


def test_declared_property_cert_wins_over_weak_ocr_text():
    result = run_kyc_document_agent({
        "text": WEAK_PROPERTY_TEXT,
        "pages": [],
        "metadata": {
            "filename": "房产.pdf",
            "customer_id": "customer-1",
            "declared_doc_type": "property_cert",
        },
    })

    assert result["doc_type"] == "property_cert"
    assert result["doc_type_name"] == "房产证/房地产权证"
    assert result["owner_type"] == "asset"
    assert result["doc_type"] != "unknown"
    assert result["extraction_status"] in {"partial", "failed"}


def test_classifier_declared_property_cert_returns_property_reason():
    classification = classify_with_reason(
        "neaing onensumpy uoneA pue uoneunuex",
        filename="房产.pdf",
        declared_doc_type="property_cert",
    )

    assert classification["doc_type"] == "property_cert"
    assert classification["doc_type_name"] == "房产证/房地产权证"
    assert classification["owner_type"] == "asset"
    assert classification["reason"] == "declared_doc_type 指定为 property_cert"


def test_collateral_filename_property_maps_to_property_cert():
    result = build_structured_extraction(
        WEAK_PROPERTY_TEXT,
        "collateral",
        filename="房产.pdf",
        customer_id="customer-1",
        customer_name="测试客户",
    )

    assert result["agent_type"] == "kyc_document_agent"
    assert result["doc_type"] == "property_cert"
    assert result["document_type_code"] == "property_cert"
    assert result["metadata"]["declared_doc_type"] == "property_cert"
    assert result["metadata"]["original_document_type"] == "collateral"


def test_collateral_without_property_filename_does_not_force_kyc():
    classification = classify_with_reason(
        "无任何KYC关键词",
        filename="普通附件.pdf",
        declared_doc_type="collateral",
    )

    assert classification["doc_type"] == "unknown"
