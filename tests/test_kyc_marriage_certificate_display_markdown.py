import pytest

from backend.services.markdown_profile_service import _build_single_document_section


BUSINESS_MARKDOWN = """## 结婚证
- 资料类型：结婚证
- 来源文件：林勇结婚证.pdf
- 原件状态：可查看
- 提取状态：成功
- 婚姻状态：已婚
- 结婚证字号：政字第2002208号
- 登记机关：浙江省乐清市民政局
- 发证日期：2022-03-15
- 登记日期：2022-03-15

### 配偶一
- 姓名：林勇
- 性别：男
- 国籍：中国
- 出生日期：1979-03-16
- 身份证号：未识别
- 疑似身份证号：330323790316243

### 配偶二
- 姓名：黄晓回
- 性别：女
- 国籍：中国
- 出生日期：1979-11-08
- 身份证号：未识别
- 疑似身份证号：330323791108192"""


class DummyStorage:
    async def get_document(self, doc_id: str):
        return {
            "doc_id": doc_id,
            "file_name": "林勇结婚证.pdf",
            "file_path": "data/uploads/林勇结婚证.pdf",
        }


@pytest.mark.asyncio
async def test_marriage_certificate_profile_section_uses_business_markdown_only():
    extraction_result = {
        "extraction_id": "ext-1",
        "doc_id": "doc-1",
        "customer_id": "customer-1",
        "extraction_type": "marriage_certificate",
        "extracted_data": {
            "agent_type": "kyc_document_agent",
            "doc_type": "marriage_certificate",
            "doc_type_name": "结婚证",
            "owner_type": "person",
            "fields": {
                "certificate_no": "政字第2002208号",
                "holder_1": {"name": "林勇"},
                "holder_2": {"name": "黄晓回"},
            },
            "validation": {"is_valid": True, "warnings": ["身份证号疑似 OCR 缺位"]},
            "confidence": {"overall": 0.8},
            "evidence": {"certificate_no": {"value": "政字第2002208号"}},
            "missing_fields": ["配偶一身份证号"],
            "raw_text_preview": "政 字第 2002208 号",
            "metadata": {"filename": "林勇结婚证.pdf"},
            "markdown": BUSINESS_MARKDOWN,
        },
    }

    markdown, source = await _build_single_document_section(DummyStorage(), "customer-1", extraction_result)
    lower_markdown = markdown.lower()

    assert markdown == BUSINESS_MARKDOWN
    assert source["source_type"] == "marriage_certificate"
    assert "## 结婚证" in markdown
    assert "资料类型：结婚证" in markdown
    assert "来源文件：林勇结婚证.pdf" in markdown
    assert "提取状态：成功" in markdown
    assert "结婚证字号：政字第2002208号" in markdown
    assert "登记机关：浙江省乐清市民政局" in markdown
    assert "姓名：林勇" in markdown
    assert "姓名：黄晓回" in markdown
    for forbidden in (
        "fields",
        "validation",
        "confidence",
        "evidence",
        "raw text preview",
        "metadata",
        "agent type",
        "classification reason",
        "doc type",
        "owner type",
    ):
        assert forbidden not in lower_markdown
