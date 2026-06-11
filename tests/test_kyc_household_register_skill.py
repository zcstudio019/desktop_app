from __future__ import annotations

from backend.services.kyc_document_agent.classifier import classify
from backend.services.kyc_document_agent.orchestrator import KycDocumentAgent
from backend.services.kyc_document_agent.renderer import render_markdown


PAGE_1_SHANGHAI_HOME = """居民户口簿
户别 非农家庭户口 户主姓名 林勇
户号 05001756 住址 上海市奉贤区泽丰路88弄2号1101室
No 096874416
上海市公安局奉贤分局金海派出所
省级公安机关户口专用章
承办人签章 施捷 2025年02月26日 签发
"""

PAGE_2_LINYONG_SHANGHAI = """住址变动登记
常住人口登记卡
姓名 林勇 户主或与户主关系 户主 性别 男 民族 汉族
出生地 浙江省乐清市 籍贯 浙江省乐清市
出生日期 1979年03月16日
公民身份号码 330323197903162430
婚姻状况 有配偶
何时由何地迁来本市（县） 2025年02月26日 从浙江省乐清市北白象镇小港村迁来
何时由何地迁来本址 2025年02月26日 从浙江省乐清市北白象镇小港村迁来
"""

PAGE_3_LINCHENKAI_SHANGHAI = """常住人口登记卡
姓名 林晨烺 户主或与户主关系 子 性别 男 民族 汉族
出生地 上海市奉贤区 籍贯 浙江省乐清市
出生日期 2010年08月31日
公民身份号码 330382201008311718
婚姻状况 未婚
"""

PAGE_4_ZHEJIANG_HOME = """居民户口簿
户别 家庭户
户号 006038967
户主姓名 林勇
住址 浙江省乐清市北白象镇小港村
乐清市公安局北白象派出所
承办人签章 倪王蕾
2022年12月13日 签发
"""

PAGE_5_LINYONG_ZHEJIANG = """常住人口登记卡
姓名 林勇 户主或与户主关系 户主 性别 男 民族 汉族
出生地 浙江省乐清市 籍贯 浙江省乐清市
出生日期 1979年03月16日
公民身份号码 330323197903162430
文化程度 高中 婚姻状况 已婚 兵役状况 未服兵役
身高 175cm 血型 不明 宗教信仰 无
服务处所 不详 职业 不详
登记日期 2022年12月13日
"""

PAGE_6_HUANG = """常住人口登记卡
姓名 黄晓回 户主或与户主关系 妻 性别 女 民族 汉
出生地 浙江省乐清市 籍贯 浙江省乐清市
出生日期 1979年11月08日
公民身份号码 330323197911081921
文化程度 高中 婚姻状况 已婚
身高 163cm 血型 不明 宗教信仰 无
服务处所 不详 职业 不详
登记日期 2022年12月13日
"""

PAGE_7_LINCHENKAI_ZHEJIANG = """常住人口登记卡
姓名 林晨恺关系子 性别 男 民族 汉族
出生地 上海市奉贤县 籍贯 浙江省乐清市
出生日期 2010年08月31日
公民身份号码 330382201008311718
婚姻状况 未婚 血型 不明
何时由何地迁来本市 2010年09月09日 首次申报
登记日期 2022年12月13日
"""

PAGE_8_LINCHENMU = """常住人口登记卡
姓名 林晨沐关 户主或与户主关系 女 性别 女 民族 汉族
曾用名 林沐
出生地 浙江省乐清市 籍贯 浙江省乐清市
出生日期 2002年11月01日
公民身份号码 330382200211010027
婚姻状况 未婚 血型 不明
何时由何地迁来本市 2002年12月11日 首次申报
何时由何地迁来本址 从浙江省乐清市北白象镇小港村迁来
登记日期 2022年12月13日
"""

PAGE_8_MIXED_CHANGE_AND_LINCHENMU = """登记事项变更和更正记载
变更项目 曾用名 变更后 林沐
常住人口登记卡
姓名 林晨沐关 户主或与户主关系 女 性别 女 民族 汉族
曾用名 林沐
出生地 浙江省乐清市 籍贯 浙江省乐清市
出生日期 2002年11月01日
公民身份号码 330382200211010027
婚姻状况 未婚 血型 不明
何时由何地迁来本市 2002.12.11 首次申报
何时由何地迁来本址 从浙江省乐清市北白象镇小港村迁来
登记日期 2022.12.13
"""

SAMPLE_PAGES = [
    PAGE_1_SHANGHAI_HOME,
    PAGE_2_LINYONG_SHANGHAI,
    PAGE_3_LINCHENKAI_SHANGHAI,
    PAGE_4_ZHEJIANG_HOME,
    PAGE_5_LINYONG_ZHEJIANG,
    PAGE_6_HUANG,
    PAGE_7_LINCHENKAI_ZHEJIANG,
    PAGE_8_LINCHENMU,
]


def extract(text: str = "", pages: list[str] | None = None) -> dict:
    payload = {"text": text or "\n".join(pages or []), "pages": pages or []}
    return KycDocumentAgent().extract(payload)


def members_by_name(result: dict) -> dict[str, dict]:
    return {member.get("name"): member for member in result["fields"]["members"]}


def test_classifier_recognizes_household_card() -> None:
    assert classify("常住人口登记卡\n姓名 林勇\n公民身份号码 330323197903162430") == "household_register"


def test_classifier_recognizes_household_header_combination() -> None:
    text = "户别 非农家庭户口\n户号 05001756\n户主姓名 林勇"
    assert classify(text) == "household_register"


def test_home_page_fields_do_not_shift() -> None:
    result = extract(PAGE_1_SHANGHAI_HOME)
    info = result["fields"]["household_info"]
    assert info["household_type"] == "非农家庭户口"
    assert info["household_number"] == "05001756"
    assert info["household_head"] == "林勇"
    assert info["household_address"] == "上海市奉贤区泽丰路88弄2号1101室"
    assert info["booklet_number"] == "096874416"
    assert info["undertaker"] == "施捷"
    assert info["issue_date"] == "2025-02-26"


def test_issuing_authority_does_not_use_notice_text() -> None:
    text = PAGE_1_SHANGHAI_HOME + "\n注意事项 进行户籍调查、核对的主要依据，具有法律效力，应妥善保管。"
    result = extract(text)
    authority = result["fields"]["household_info"].get("issuing_authority") or ""
    assert authority == "上海市公安局奉贤分局金海派出所"
    assert "进行户籍调查" not in authority
    assert len(authority) <= 35


def test_issuing_authority_rejects_report_noise() -> None:
    text = """居民户口簿
户别 非农家庭户口 户主姓名 林勇
户号 05001756 住址 上海市奉贤区泽丰路88弄2号1101室
须立即报告户口登记机关
2025年02月26日 签发
"""
    result = extract(text)
    assert result["fields"]["household_info"].get("issuing_authority") in {"", None}


def test_address_does_not_include_stamp_noise() -> None:
    text = """居民户口簿
户别 非农家庭户口 户主姓名 林勇
户号 05001756
住址 上海市奉贤区泽丰路88弄2号1101室 省级公安机关户口专用章 金海派出所
2025年02月26日 签发
"""
    result = extract(text)
    address = result["fields"]["household_info"]["household_address"]
    assert address == "上海市奉贤区泽丰路88弄2号1101室"
    assert "省级公安机关" not in address
    assert "户口专用章" not in address
    assert "金海派出所" not in address


def test_member_name_is_not_label_fragment_and_relation_is_extracted() -> None:
    result = extract(PAGE_2_LINYONG_SHANGHAI)
    member = result["fields"]["members"][0]
    assert member["name"] == "林勇"
    assert member["relationship_to_head"] == "户主"
    assert member["gender"] == "男"
    assert member["ethnicity"] == "汉族"
    assert "或与" not in member["name"]
    assert "关系" not in member["name"]


def test_member_relationships_and_sample_name_corrections() -> None:
    result = extract(pages=SAMPLE_PAGES)
    by_name = members_by_name(result)
    assert by_name["林勇"]["relationship_to_head"] == "户主"
    assert by_name["黄晓回"]["relationship_to_head"] == "妻"
    assert "黄晓闽" not in by_name
    assert by_name["林晨恺"]["relationship_to_head"] == "子"
    assert by_name["林晨沐"]["relationship_to_head"] == "女"


def test_member_name_preserves_huang_xiaohui_visible_value() -> None:
    text = """常住人口登记卡
姓名 黄晓回 户主或与户主关系 妻 性别 女 民族 汉族
出生地 浙江省乐清市 籍贯 浙江省乐清市
出生日期 1979年11月08日
公民身份号码 330323197911081921
文化程度 高中 婚姻状况 已婚
身高 163cm 血型 不明 服务处所 不详 职业 不详
登记日期 2022年12月13日
"""
    result = extract(text)
    member = result["fields"]["members"][0]
    assert member["name"] == "黄晓回"
    assert member["relationship_to_head"] == "妻"
    assert member["id_number"] == "330323197911081921"
    assert "黄晓闽" not in render_markdown(result)


def test_member_name_backfilled_from_id_number_fallback() -> None:
    text = """常住人口登记卡
姓名 或与林勇关系 户主或与户主关系 户主 性别 男 民族 汉族
出生日期 1979年03月16日
公民身份号码 330323197903162430
"""
    result = extract(text)
    member = result["fields"]["members"][0]
    assert member["name"] == "林勇"
    assert result["fields"]["household_info"]["household_head"] == "林勇"


def test_mixed_change_record_page_still_extracts_l_chenmu() -> None:
    result = extract(PAGE_8_MIXED_CHANGE_AND_LINCHENMU)
    by_name = members_by_name(result)
    assert "林晨沐" in by_name
    member = by_name["林晨沐"]
    assert member["former_name"] == "林沐"
    assert member["relationship_to_head"] == "女"
    assert member["id_number"] == "330382200211010027"
    assert member["migration_to_city"] == "2002-12-11 首次申报"
    assert member["migration_to_address"] == "从浙江省乐清市北白象镇小港村迁来"
    assert member["registration_date"] == "2022-12-13"


def test_member_fields_do_not_shift() -> None:
    result = extract(pages=SAMPLE_PAGES)
    by_name = members_by_name(result)
    lin_yong = by_name["林勇"]
    assert lin_yong["education_level"] == "高中"
    assert lin_yong["marital_status"] == "已婚"
    assert "派出所" not in lin_yong.get("military_status", "")
    assert "330323" not in lin_yong.get("religion", "")
    assert "何时由何地" not in lin_yong.get("service_place", "")
    assert "迁来" not in lin_yong.get("occupation", "")
    assert lin_yong["height"] == "175cm"
    assert lin_yong["blood_type"] == "不明"
    for member in result["fields"]["members"]:
        assert "派出所" not in member.get("service_place", "")
        assert "何时由何地" not in member.get("occupation", "")
        assert "首次申报" not in member.get("occupation", "")
        assert "身份号码" not in member.get("religion", "")
        assert member.get("marital_status") != "三"
        assert member.get("education_level") not in {"已婚", "未婚", "有配偶"}


def test_multi_page_dedup_merges_source_pages() -> None:
    result = extract(pages=SAMPLE_PAGES)
    by_name = members_by_name(result)
    lin_yong = by_name["林勇"]
    assert len([member for member in result["fields"]["members"] if member.get("name") == "林勇"]) == 1
    assert lin_yong["id_number"] == "330323197903162430"
    assert set(lin_yong["source_pages"]) == {2, 5}
    assert lin_yong["height"] == "175cm"


def test_household_records_keep_two_home_pages_and_latest_is_primary() -> None:
    result = extract(pages=SAMPLE_PAGES)
    fields = result["fields"]
    assert len(fields["household_records"]) == 2
    assert fields["household_info"]["household_number"] == "05001756"
    assert fields["household_info"]["household_address"] == "上海市奉贤区泽丰路88弄2号1101室"
    assert fields["household_info"]["issue_date"] == "2025-02-26"
    assert fields["household_records"][1]["household_number"] == "006038967"
    assert fields["household_records"][1]["household_address"] == "浙江省乐清市北白象镇小港村"


def test_address_change_records_do_not_come_from_notice() -> None:
    text = """住址变动登记
注意事项 五、全户迁出户口管辖区的，应向户口登记机关缴销居民户口簿。
"""
    result = extract(text)
    records = result["fields"]["household_info"].get("address_change_records") or []
    assert not records


def test_no_head_warning_when_head_member_exists() -> None:
    result = extract(pages=SAMPLE_PAGES)
    warnings = result["validation"]["warnings"]
    assert not any("未识别到户主成员" in item for item in warnings)


def test_full_sample_members_are_four_people() -> None:
    result = extract(pages=SAMPLE_PAGES)
    names = [member.get("name") for member in result["fields"]["members"]]
    assert len(names) == 4
    assert names == ["林勇", "黄晓回", "林晨恺", "林晨沐"]


def test_household_register_renderer_chinese_markdown_no_json() -> None:
    result = extract(pages=SAMPLE_PAGES)
    markdown = render_markdown(result)
    assert "## 户口本" in markdown
    assert "### 当前户信息" in markdown
    assert "- 户号：05001756" in markdown
    assert "### 户信息记录" in markdown
    assert markdown.count("- 户号：05001756") == 1
    assert "- 户号：006038967" in markdown
    assert "#### 成员 1：林勇" in markdown
    assert "#### 成员 2：黄晓回" in markdown
    assert "#### 成员 4：林晨沐" in markdown
    assert "- 与户主关系：户主" in markdown
    assert "- 公民身份号码：330323197903162430" in markdown
    assert "未识别到户主成员" not in markdown
    assert "```json" not in markdown
    assert "members:" not in markdown
    assert "household_info:" not in markdown
    assert "须立即报告户口登记机关" not in markdown
    assert "黄晓闽" not in markdown
    assert "{" not in markdown
    assert "}" not in markdown
