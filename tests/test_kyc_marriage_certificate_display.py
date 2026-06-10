from pathlib import Path

from backend.services.kyc_document_agent.orchestrator import KycDocumentAgent


LINYONG_OCR_TEXT = """
--- 第 1 页 ---
人民共国 婚姻证件管理专用章 囍 中华人民共和国民政部监制 持证人林勇
--- 第 2 页 ---
政 字第 2002208 号
姓名 林勇
申请结婚，经审查符合
性别 男
《中华人民共和国婚姻法》
出生日期 1979年3月16日
关于结婚的规定，准予登记，
国籍 发给此证。
身份证件号 330323790316243
姓名 黄晓回
性别 女
出生日期 1979年11月8日
发证机关:
国籍
身份证件号 330323791108192
发证日期: 2022年3月15日
"""


FORBIDDEN_MARKDOWN_TOKENS = (
    "fields",
    "validation",
    "confidence",
    "evidence",
    "missing fields",
    "raw text preview",
    "metadata",
    "agent type",
    "classification reason",
    "customer name",
)


def test_linyong_marriage_certificate_markdown_is_business_only():
    result = KycDocumentAgent().extract({"text": LINYONG_OCR_TEXT, "metadata": {"filename": "林勇结婚证.pdf"}})
    markdown = result["markdown"]

    assert "## 结婚证" in markdown
    assert "资料类型：结婚证" in markdown
    assert "来源文件：林勇结婚证.pdf" in markdown
    assert "提取状态：成功" in markdown
    assert "结婚证字号：政字第2002208号" in markdown
    assert "登记机关：浙江省乐清市民政局" in markdown
    assert "姓名：林勇" in markdown
    assert "姓名：黄晓回" in markdown
    assert "疑似身份证号：330323790316243" in markdown
    assert "疑似身份证号：330323791108192" in markdown
    assert "### 校验提醒" not in markdown
    lower_markdown = markdown.lower()
    for token in FORBIDDEN_MARKDOWN_TOKENS:
        assert token not in lower_markdown


def test_frontend_marriage_result_view_uses_markdown_only_branch():
    source = Path("src/components/KycExtractionResult.tsx").read_text(encoding="utf-8")

    assert "normalizedResult.doc_type === 'marriage_certificate'" in source
    assert "normalizedResult.markdown" in source
    marriage_branch = source.split("normalizedResult.doc_type === 'marriage_certificate'", 1)[1].split("if (normalizedResult.doc_type === 'id_card')", 1)[0]
    assert "normalizedResult.confidence" not in marriage_branch
    assert "normalizedResult.evidence" not in marriage_branch
    assert "normalizedResult.missing_fields" not in marriage_branch
