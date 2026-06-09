from pathlib import Path

from backend.services.document_extractor_service import run_document_extraction


MARRIAGE_OCR_TEXT = """
结婚证
结婚证字号 J310112-2018-006527
姓名 孙峰 性别 男 国籍 中国
身份证件号 310107197805162416
姓名 程丽 性别 女 国籍 中国
身份证件号 341126198803100449
中华人民共和国民政部监制
"""


def test_run_document_extraction_routes_declared_marriage_certificate_to_kyc_agent():
    result = run_document_extraction(
        text=MARRIAGE_OCR_TEXT,
        pages=[{"page": 1, "text": MARRIAGE_OCR_TEXT, "source": "marriage_certificate_ocr_rotated"}],
        filename="林勇结婚证.pdf",
        customer_id="customer-1",
        declared_doc_type="marriage_certificate",
        metadata={"customer_name": "测试客户"},
    )

    assert result["agent_type"] == "kyc_document_agent"
    assert result["doc_type"] == "marriage_certificate"
    assert result["fields"]["certificate_no"] == "J310112-2018-006527"
    assert result["fields"]["holder_1"]["id_number"] == "310107197805162416"


def test_file_router_has_marriage_certificate_ocr_rotation_and_kyc_debug_logs():
    source = Path("backend/routers/file.py").read_text(encoding="utf-8")

    assert "_ocr_pdf_pages_with_marriage_rotation" in source
    assert "_score_marriage_certificate_ocr_text" in source
    assert "marriage_certificate_ocr_rotated" in source
    assert "[KYC_DEBUG]" in source
    assert "empty text before KycDocumentAgent" in source


def test_marriage_certificate_ocr_keywords_do_not_route_divorce_certificate():
    result = run_document_extraction(
        text="离婚证 离婚登记 姓名 张三 身份证件号 310101199001011234",
        pages=[],
        filename="离婚证.pdf",
        customer_id="customer-1",
        declared_doc_type=None,
        metadata={},
    )

    assert result.get("doc_type") != "marriage_certificate"
