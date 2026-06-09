import asyncio

from backend.services.kyc_document_agent.classifier import classify
from backend.services.kyc_document_agent.orchestrator import KycDocumentAgent
from backend.services.kyc_profile_sync_service import build_customer_kyc_profile


SAMPLE_ONE = """
结婚证
结婚证字号 J310112-2018-006527
姓名 孙峰 性别 男 国籍 中国
出生日期 1978年05月16日
身份证件号 310107197805162416
姓名 程丽 性别 女 国籍 中国
出生日期 1988年03月10日
身份证件号 341126198803100449
中华人民共和国民政部监制
"""


SAMPLE_TWO = """
结婚证字号 苏吴字第202208号
姓名 林勇
性别 男
国籍 中国
出生日期 1979年3月16日
身份证件号 320523197903162443
姓名 吕燕
性别 女
国籍 中国
出生日期 1979年11月8日
身份证件号 320523197911081922
符合《中华人民共和国婚姻法》关于结婚的规定，准予登记，发给此证
"""


class FakeStorage:
    def __init__(self, extractions):
        self.extractions = extractions

    async def get_extractions_by_customer(self, customer_id):
        return self.extractions

    async def list_documents(self, customer_id):
        return []


def test_sample_one_extracts_marriage_certificate_fields():
    result = KycDocumentAgent().extract({"text": SAMPLE_ONE, "metadata": {"filename": "结婚证.pdf"}})
    fields = result["fields"]

    assert result["doc_type"] == "marriage_certificate"
    assert fields["certificate_no"] == "J310112-2018-006527"
    assert fields["holder_1"]["name"] == "孙峰"
    assert fields["holder_1"]["gender"] == "男"
    assert fields["holder_1"]["birth_date"] == "1978-05-16"
    assert fields["holder_1"]["id_number"] == "310107197805162416"
    assert fields["holder_2"]["name"] == "程丽"
    assert fields["holder_2"]["gender"] == "女"
    assert fields["holder_2"]["birth_date"] == "1988-03-10"
    assert fields["holder_2"]["id_number"] == "341126198803100449"
    assert fields["marital_status"] == "已婚"


def test_sample_two_extracts_chinese_certificate_no_and_holders():
    result = KycDocumentAgent().extract({"text": SAMPLE_TWO, "metadata": {"filename": "结婚证.pdf"}})
    fields = result["fields"]

    assert result["doc_type"] == "marriage_certificate"
    assert fields["certificate_no"] == "苏吴字第202208号"
    assert fields["holder_1"]["name"] == "林勇"
    assert fields["holder_1"]["id_number"] == "320523197903162443"
    assert fields["holder_2"]["name"] == "吕燕"
    assert fields["holder_2"]["id_number"] == "320523197911081922"


def test_classifier_detects_marriage_certificate_and_excludes_divorce():
    assert classify("结婚证字号 J310112-2018-006527 姓名 孙峰 身份证件号 310107197805162416") == "marriage_certificate"
    assert classify("离婚证 离婚登记 姓名 张三") != "marriage_certificate"


def test_birth_mismatch_enters_warnings():
    text = SAMPLE_TWO.replace("1979年3月16日", "1980年3月16日")
    result = KycDocumentAgent().extract({"text": text, "metadata": {"filename": "结婚证.pdf"}})
    warnings = result["validation"]["warnings"]
    assert any("出生日期" in item for item in warnings)


def test_missing_certificate_no_is_partial():
    result = KycDocumentAgent().extract({"text": SAMPLE_TWO.replace("结婚证字号 苏吴字第202208号", ""), "metadata": {"filename": "结婚证.pdf"}})
    assert result["extraction_status"] == "partial"
    assert "结婚证字号" in result["missing_fields"]


def test_kyc_profile_aggregates_marriage_info():
    result = KycDocumentAgent().extract({"text": SAMPLE_TWO, "metadata": {"filename": "结婚证.pdf"}})
    extraction = {
        "doc_id": "doc-marriage",
        "extraction_type": "marriage_certificate",
        "created_at": "2026-01-01T00:00:00",
        "extracted_data": result,
    }
    profile = asyncio.run(build_customer_kyc_profile(FakeStorage([extraction]), "customer-1"))
    marriage = profile["marriage"]
    assert marriage["marital_status"] == "已婚"
    assert marriage["certificate_no"] == "苏吴字第202208号"
    assert marriage["holder_1_name"] == "林勇"
    assert marriage["holder_2_id_number"] == "320523197911081922"


def test_confirmed_data_has_priority_for_marriage_profile():
    result = KycDocumentAgent().extract({"text": SAMPLE_TWO, "metadata": {"filename": "结婚证.pdf"}})
    extraction = {
        "doc_id": "doc-marriage",
        "extraction_type": "marriage_certificate",
        "created_at": "2026-01-01T00:00:00",
        "extracted_data": result,
        "confirmed_data": {
            "confirmed_fields": {
                "certificate_no": "人工确认字号",
                "holder_1": {"name": "人工配偶一", "id_number": "111111111111111111"},
                "holder_2": {"name": "人工配偶二", "id_number": "222222222222222222"},
            }
        },
    }
    profile = asyncio.run(build_customer_kyc_profile(FakeStorage([extraction]), "customer-1"))
    assert profile["marriage"]["certificate_no"] == "人工确认字号"
    assert profile["marriage"]["holder_1_name"] == "人工配偶一"


def test_markdown_contains_marriage_sections():
    result = KycDocumentAgent().extract({"text": SAMPLE_ONE, "metadata": {"filename": "结婚证.pdf"}})
    markdown = result["markdown"]
    assert "## 结婚证" in markdown
    assert "### 配偶一" in markdown
    assert "### 配偶二" in markdown


def test_id_numbers_infer_birth_and_gender_when_ocr_missing_labels():
    text = """
    结婚证
    结婚证字号 12345678
    姓名 林勇
    身份证件号 110105194912310031
    姓 名 吕燕
    身份证件号 11010519491231002X
    """
    result = KycDocumentAgent().extract({"text": text, "metadata": {"filename": "林勇结婚证.pdf"}})
    fields = result["fields"]
    assert fields["holder_1"]["birth_date"] == "1949-12-31"
    assert fields["holder_1"]["gender"] == "男"
    assert fields["holder_2"]["birth_date"] == "1949-12-31"
    assert fields["holder_2"]["gender"] == "女"
    warnings = result["validation"]["warnings"]
    assert "配偶一出生日期由身份证号推断" in warnings
    assert "配偶一性别由身份证号推断" in warnings


def test_empty_marriage_certificate_text_is_not_success():
    result = KycDocumentAgent().extract({"text": "", "metadata": {"filename": "林勇结婚证.pdf", "declared_doc_type": "marriage_certificate"}})
    assert result["doc_type"] == "marriage_certificate"
    assert result["extraction_status"] == "failed"
    assert "未获取到有效 OCR 文本或字段识别失败" in result["validation"]["warnings"]


def test_marriage_markdown_is_chinese_and_not_raw_fields_json():
    result = KycDocumentAgent().extract({"text": SAMPLE_ONE, "metadata": {"filename": "结婚证.pdf"}})
    markdown = result["markdown"]
    assert "## 结婚证" in markdown
    assert "配偶一" in markdown
    assert "fields" not in markdown
    assert "holder_1" not in markdown
    assert "{'" not in markdown
    assert '{"' not in markdown
