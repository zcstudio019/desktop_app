from __future__ import annotations

from backend.services.markdown_profile_service import (
    PROPERTY_DOCUMENT_TYPES,
    _build_kyc_property_profile_section_lines,
)


def _new_real_estate_extracted_data() -> dict:
    return {
        "agent_type": "kyc_document_agent",
        "doc_type": "property_cert",
        "doc_type_name": "房产证/房地产权证",
        "fields": {
            "权利人": "沃志方",
            "共有情况": "单独所有",
            "权证编号": "沪(2018)徐字不动产权第015979号",
            "坐落": "华发路406弄10号",
            "不动产单元号": "310104019001GB00045F00430086",
            "权利类型": "国有建设用地使用权/房屋所有权",
            "权利性质": "土地权利性质：出让",
            "土地用途": "住宅",
            "房屋用途": "居住",
            "宗地面积": "135460.00 平方米",
            "建筑面积": "62.40 平方米",
            "使用期限": "2015年10月16日起2076年12月28日止",
            "地号": "徐汇区华泾镇448街坊2/3丘",
            "室号或部位": "1705",
            "建筑类型": "公寓",
            "总层数": "29",
            "竣工日期": "2011年",
        },
    }


def test_profile_markdown_property_types_include_kyc_codes():
    assert "property_cert" in PROPERTY_DOCUMENT_TYPES
    assert "real_estate_cert" in PROPERTY_DOCUMENT_TYPES


def test_profile_markdown_renders_best_kyc_property_fields_without_empty_placeholder():
    lines = _build_kyc_property_profile_section_lines(
        ["房产正面.pdf"],
        True,
        _new_real_estate_extracted_data(),
        ["房产.pdf"],
    )
    markdown = "\n".join(lines)

    assert "暂无可展示字段" not in markdown
    assert "来源文件：房产正面.pdf" in markdown
    assert "补充文件：房产.pdf" in markdown
    assert "共有情况：单独所有" in markdown
    assert "不动产单元号：310104019001GB00045F00430086" in markdown
    assert "权利类型：国有建设用地使用权/房屋所有权" in markdown
    assert "使用期限：2015年10月16日起2076年12月28日止" in markdown


def test_profile_markdown_empty_property_fields_show_actionable_warning():
    lines = _build_kyc_property_profile_section_lines(
        ["房产.pdf"],
        True,
        {"agent_type": "kyc_document_agent", "doc_type": "property_cert", "fields": {}},
        [],
    )
    markdown = "\n".join(lines)

    assert "暂无可展示字段" not in markdown
    assert "字段页 OCR 未能提取关键字段" in markdown
