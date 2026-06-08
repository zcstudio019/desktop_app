from __future__ import annotations

from backend.services.property_cert_agent.merger import merge_pages
from backend.services.property_cert_agent.normalizer import normalize_property_cert_fields
from backend.services.property_cert_agent.renderer import render_markdown
from backend.services.property_cert_agent.skills.new_real_estate_cert_skill import extract as extract_new_real_estate


RAW_TEXT_WITH_COVER_AND_DETAIL = """
为保护不动产权利人合法权益，对不动产权利人申请登记，经审查核实，准予登记，颁发此证。
权利人
智先生数字科技（上海）有限公司
共有情况
单独所有
沪（2022）宝字
不动产权第011468
号
坐落
殷高西路101号
不动产单元号
310113015003GB00011F00020088
权利类型
国有建设用地使用权/房屋所有权
权利性质
出让
土地用途：其它商服用地/房屋用途：办公
房屋状况：
室号部位：306；
类型：办公楼；
总层数：17；
竣工日期：2007年。
"""


def test_new_real_estate_skill_extracts_owner_and_co_owner_from_detail_table() -> None:
    fields = extract_new_real_estate({"text": RAW_TEXT_WITH_COVER_AND_DETAIL})["fields"]

    assert fields["权利人"] == "智先生数字科技（上海）有限公司"
    assert fields["共有情况"] == "单独所有"
    assert fields["权利类型"] == "国有建设用地使用权/房屋所有权"


def test_normalizer_removes_cover_instruction_owner_and_recovers_detail_owner() -> None:
    normalized = normalize_property_cert_fields(
        {"权利人": "合法权益，对", "共有情况": ""},
        raw_text=RAW_TEXT_WITH_COVER_AND_DETAIL,
        page_role="new_real_estate_detail_page",
    )

    assert normalized["权利人"] == "智先生数字科技（上海）有限公司"
    assert normalized["共有情况"] == "单独所有"
    assert "合法权益，对" not in normalized.values()


def test_merger_does_not_allow_cover_owner_to_override_detail_owner() -> None:
    merged = merge_pages(
        [
            {"page_role": "cover_page", "fields": {"权利人": "合法权益，对", "共有情况": "错误"}},
            {
                "page_role": "new_real_estate_detail_page",
                "fields": {"权利人": "智先生数字科技（上海）有限公司", "共有情况": "单独所有", "权证编号": "沪(2022)宝字不动产权第011468号"},
            },
        ]
    )

    assert merged["fields"]["权利人"] == "智先生数字科技（上海）有限公司"
    assert merged["fields"]["共有情况"] == "单独所有"


def test_renderer_markdown_does_not_show_invalid_owner() -> None:
    markdown = render_markdown(
        {
            "_raw_text": RAW_TEXT_WITH_COVER_AND_DETAIL,
            "page_roles": ["new_real_estate_detail_page"],
            "fields": {"权利人": "合法权益，对"},
            "metadata": {"filename": "产权证-306(1).pdf"},
            "validation": {},
        }
    )

    assert "权利人: 智先生数字科技（上海）有限公司" in markdown
    assert "共有情况: 单独所有" in markdown
    assert "合法权益，对" not in markdown
