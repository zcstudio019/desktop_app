from __future__ import annotations

from backend.services.kyc_document_agent.classifier import classify
from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent


FRONT_TEXT = """
中华人民共和国居民身份证
姓名 张三
性别 男 民族 汉
出生 1990年01月02日
住址 上海市浦东新区世纪大道100号
公民身份号码 310101199001021234
"""

BACK_TEXT = """
居民身份证
签发机关 上海市公安局浦东分局
有效期限 2010.01.01-2030.01.01
"""


def _extract(text: str) -> dict:
    return run_kyc_document_agent(
        {
            "text": text,
            "pages": [],
            "metadata": {"filename": "身份证.png"},
        }
    )


def test_id_card_front_extracts_core_fields():
    result = _extract(FRONT_TEXT)
    fields = result["fields"]

    assert result["doc_type"] == "id_card"
    assert fields["name"] == "张三"
    assert fields["gender"] == "男"
    assert fields["ethnicity"] == "汉"
    assert fields["birth_date"] == "1990-01-02"
    assert "上海市浦东新区世纪大道100号" in fields["address"]
    assert fields["id_number"] == "310101199001021234"
    assert result["extraction_status"] in {"partial", "success"}
    assert result["evidence"]["name"]["evidence_text"]
    assert result["evidence"]["id_number"]["evidence_text"]


def test_id_card_back_extracts_authority_and_valid_period():
    result = _extract(BACK_TEXT)
    fields = result["fields"]

    assert fields["issuing_authority"] == "上海市公安局浦东分局"
    assert fields["valid_from"] == "2010-01-01"
    assert fields["valid_to"] == "2030-01-01"
    assert result["extraction_status"] == "partial"


def test_id_card_front_and_back_merged_success_when_complete():
    result = _extract(FRONT_TEXT.replace("310101199001021234", "31010119900102123X") + "\n" + BACK_TEXT)

    assert result["fields"]["id_number"] == "31010119900102123X"
    assert result["fields"]["valid_to"] == "2030-01-01"
    assert result["extraction_status"] == "success"


def test_id_card_valid_to_long_term():
    result = _extract("签发机关 上海市公安局浦东分局\n有效期限 2010.01.01-长期")

    assert result["fields"]["valid_from"] == "2010-01-01"
    assert result["fields"]["valid_to"] == "长期"


def test_id_card_number_lowercase_x_is_uppercase():
    result = _extract("姓名 张三\n性别 男 民族 汉\n出生 1990年01月02日\n住址 上海\n公民身份号码 31010119900102123x")

    assert result["fields"]["id_number"] == "31010119900102123X"


def test_id_card_multiline_address_stops_before_id_number():
    result = _extract(
        """
姓名 张三
性别 男 民族 汉
出生 1990年01月02日
住址 上海市浦东新区
世纪大道100号 幢101室
公民身份号码 31010119900102123X
"""
    )

    assert result["fields"]["address"] == "上海市浦东新区世纪大道100号幢101室"
    assert "310101" not in result["fields"]["address"]


def test_id_card_birth_date_mismatch_adds_warning():
    result = _extract("姓名 张三\n性别 男 民族 汉\n出生 1991年01月02日\n住址 上海\n公民身份号码 31010119900102123X")

    assert any("出生日期" in item and "不一致" in item for item in result["validation"]["warnings"])


def test_id_card_invalid_check_digit_adds_warning():
    result = _extract("姓名 张三\n性别 男 民族 汉\n出生 1990年01月02日\n住址 上海\n公民身份号码 310101199001021231")

    assert any("身份证号码" in item and "校验" in item for item in result["validation"]["warnings"])


def test_classifier_recognizes_front_and_back_id_card():
    assert classify("姓名 张三 性别 男 民族 汉 出生 1990年01月02日 住址 上海 公民身份号码 31010119900102123X") == "id_card"
    assert classify("签发机关 上海市公安局浦东分局 有效期限 2010.01.01-2030.01.01") == "id_card"


def test_classifier_does_not_misclassify_household_register_or_marriage_cert():
    assert classify("居民户口簿 姓名 张三 公民身份号码 31010119900102123X") == "household_register"
    assert classify("结婚证 持证人 张三 身份证号码 31010119900102123X 婚姻登记员 李四") == "marriage_cert"


def test_id_card_markdown_uses_chinese_business_labels():
    result = _extract(FRONT_TEXT.replace("310101199001021234", "31010119900102123X") + "\n" + BACK_TEXT)
    markdown = result["markdown"]

    assert "## 居民身份证" in markdown
    assert "### 基础信息" in markdown
    assert "### 签发信息" in markdown
    assert "姓名" in markdown
    assert "身份证号码" in markdown
    assert "id_number" not in markdown
    assert "fields" not in markdown


def test_id_card_profile_fields_are_kept_for_downstream_sync():
    result = _extract(FRONT_TEXT.replace("310101199001021234", "31010119900102123X") + "\n" + BACK_TEXT)
    fields = result["fields"]

    assert result["owner_type"] == "person"
    assert fields["name"]
    assert fields["id_number"]
    assert fields["address"]
