from __future__ import annotations

from backend.services.kyc_document_agent.classifier import classify
from backend.services.kyc_document_agent.orchestrator import KycDocumentAgent
from backend.services.kyc_document_agent.renderer import render_markdown


SAMPLE_TEXT = """居民户口簿
户别 非农家庭户口
户号 05001716
户主姓名 林勇
住址 上海市奉贤区泽丰路88弄2号1101室
2025年02月26日 签发

常住人口登记卡
姓名 林勇
户主或与户主关系 户主
性别 男 民族 汉族
出生地 浙江省乐清市 籍贯 浙江省乐清市
出生日期 1979年03月16日
公民身份号码 330323197903162430
文化程度 高中
婚姻状况 有配偶
服务处所 不详
职业 不详

常住人口登记卡
姓名 黄晓闽
户主或与户主关系 妻
性别 女 民族 汉
出生地 浙江省乐清市 籍贯 浙江省乐清市
出生日期 1979年11月08日
公民身份号码 330323197911081921
文化程度 高中
婚姻状况 已婚
职业 不详
"""


def extract(text: str) -> dict:
    return KycDocumentAgent().extract({"text": text})


def test_classifier_recognizes_household_card() -> None:
    assert classify("常住人口登记卡\n姓名 林勇\n公民身份号码 330323197903162430") == "household_register"


def test_classifier_recognizes_household_header_combination() -> None:
    text = "户别 非农家庭户口\n户号 05001716\n户主姓名 林勇"
    assert classify(text) == "household_register"


def test_household_register_extracts_household_info_and_members() -> None:
    result = extract(SAMPLE_TEXT)
    fields = result["fields"]
    household_info = fields["household_info"]
    members = fields["members"]

    assert result["doc_type"] == "household_register"
    assert result["doc_type_name"] == "户口本"
    assert result["owner_type"] == "person"
    assert result["extraction_status"] in {"success", "partial"}
    assert household_info["household_type"] == "非农家庭户口"
    assert household_info["household_number"] == "05001716"
    assert household_info["household_head"] == "林勇"
    assert household_info["household_address"] == "上海市奉贤区泽丰路88弄2号1101室"
    assert household_info["issue_date"] == "2025-02-26"
    assert len(members) == 2
    assert members[0]["name"] == "林勇"
    assert members[0]["relationship_to_head"] == "户主"
    assert members[0]["gender"] == "男"
    assert members[0]["ethnicity"] == "汉族"
    assert members[0]["birth_date"] == "1979-03-16"
    assert members[0]["id_number"] == "330323197903162430"
    assert members[0]["marital_status"] == "已婚"
    assert members[1]["name"] == "黄晓闽"
    assert members[1]["relationship_to_head"] == "妻"
    assert members[1]["ethnicity"] == "汉族"
    assert members[1]["id_number"] == "330323197911081921"


def test_household_register_deduplicates_members_by_id_number() -> None:
    text = SAMPLE_TEXT + """
常住人口登记卡
姓名 林勇
户主或与户主关系 户主
出生日期 1979年03月16日
公民身份号码 330323197903162430
"""
    result = extract(text)
    id_numbers = [member.get("id_number") for member in result["fields"]["members"]]
    assert id_numbers.count("330323197903162430") == 1


def test_household_register_validator_warns_birth_mismatch() -> None:
    text = """常住人口登记卡
姓名 林勇
户主或与户主关系 户主
性别 男 民族 汉族
出生日期 1980年03月16日
公民身份号码 330323197903162430
"""
    result = extract(text)
    warnings = result["validation"]["warnings"]
    assert any("出生日期与户口本出生日期不一致" in item for item in warnings)


def test_household_register_missing_head_and_member_is_partial() -> None:
    result = extract("居民户口簿\n户别 家庭户\n户号 006038967")
    assert result["extraction_status"] == "partial"
    assert "household_head" in result["missing_fields"]
    assert "members" in result["missing_fields"]


def test_household_register_renderer_chinese_markdown_no_json() -> None:
    result = extract(SAMPLE_TEXT)
    markdown = render_markdown(result)
    assert "## 户口本" in markdown
    assert "### 户信息" in markdown
    assert "- 户主姓名：林勇" in markdown
    assert "### 家庭成员" in markdown
    assert "#### 成员 1：林勇" in markdown
    assert "- 公民身份号码：330323197903162430" in markdown
    assert "```json" not in markdown
    assert '"members"' not in markdown
    assert "household_info" not in markdown
