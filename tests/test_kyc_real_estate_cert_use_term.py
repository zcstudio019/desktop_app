from __future__ import annotations

from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent
from backend.services.kyc_document_agent.renderer import get_display_fields
from backend.services.kyc_document_agent.skills.property_cert_skill import (
    extract_real_estate_use_term,
    extract_use_term_from_property_cert_text,
)


def _extract(text: str) -> dict:
    return run_kyc_document_agent(
        {
            "text": text,
            "pages": [],
            "metadata": {"filename": "房产正面.pdf", "declared_doc_type": "property_cert"},
        }
    )


def test_real_estate_cert_extracts_complete_use_term():
    result = _extract(
        "不动产权证书 权利人 沃志方 不动产单元号 310104019001GB00045F00430086 "
        "使用期限 国有建设用地使用权使用期限：2015年10月16日起2076年12月28日止 "
        "建筑面积：62.40平方米 竣工日期：2011年"
    )

    assert result["fields"]["使用期限"] == "2015年10月16日起2076年12月28日止"
    assert result["fields"]["土地使用期限"] == "2015年10月16日起2076年12月28日止"
    assert result["fields"]["使用期限"] != "2015年10月16日起2076"


def test_real_estate_cert_extracts_cross_line_use_term():
    result = _extract(
        "不动产权证书 权利人 沃志方 不动产单元号 310104019001GB00045F00430086\n"
        "使用期限：2015年10月16日起2076\n年12月28日止\n"
        "建筑面积：62.40平方米\n竣工日期：2011年"
    )

    assert result["fields"]["使用期限"] == "2015年10月16日起2076年12月28日止"


def test_real_estate_cert_normalizes_term_without_stop_when_ocr_misses_stop():
    result = _extract(
        "不动产权证书 权利人 沃志方 不动产单元号 310104019001GB00045F00430086 "
        "使用期限：2015年10月16日起2076年12月28日 "
        "建筑面积：62.40平方米"
    )

    assert result["fields"]["使用期限"] == "2015年10月16日起2076年12月28日止"


def test_extract_use_term_from_property_cert_text_uses_label_line_and_next_lines():
    text = (
        "用途：土地用途：住宅/房屋用途：居住\n"
        "使用期限\n"
        "国有建设用地使用权使用期限：2015年10月16日起2076\n"
        "年12月28日止\n"
        "房屋状况：室号部位：1705"
    )

    assert extract_use_term_from_property_cert_text(text) == "2015年10月16日起2076年12月28日止"


def test_extract_real_estate_use_term_uses_2015_2076_neighbor_lines():
    text = (
        "面积：宗地面积：135460.00平方米/建筑面积：62.40平方米\n"
        "国有建设用地使用权\n"
        "2015年10月16日起2076\n"
        "年12月28日止\n"
        "房屋状况：室号部位：1705；类型：公寓"
    )

    assert extract_real_estate_use_term(text) == "2015年10月16日起2076年12月28日止"


def test_extract_real_estate_use_term_from_appended_use_term_region_ocr():
    text = (
        "不动产权证书 权利人 沃志方 不动产单元号 310104019001GB00045F00430086\n"
        "--- Property Certificate Field OCR page=1 region=use_term_region_15_70_38_55 variant=contrast_x2 ---\n"
        "国有建设用地使用权使用期限：2015年10月16日起2076年12月28日止"
    )

    assert extract_real_estate_use_term(text) == "2015年10月16日起2076年12月28日止"


def test_use_term_aliases_render_as_use_term_for_new_real_estate_cert():
    display = get_display_fields(
        {
            "doc_type": "property_cert",
            "fields": {
                "权证编号": "沪(2018)徐字不动产权第015979号",
                "不动产单元号": "310104019001GB00045F00430086",
                "宗地面积": "135460.00 平方米",
                "land_use_term": "2015年10月16日起2076年12月28日止",
                "室号或部位": "1705",
            },
        }
    )
    keys = list(display)

    assert display["使用期限"] == "2015年10月16日起2076年12月28日止"
    assert "土地使用期限" not in display
    assert keys.index("宗地面积") < keys.index("使用期限") < keys.index("室号或部位")


def test_use_term_wins_over_truncated_land_use_term_alias():
    display = get_display_fields(
        {
            "doc_type": "property_cert",
            "fields": {
                "权证编号": "沪(2018)徐字不动产权第015979号",
                "不动产单元号": "310104019001GB00045F00430086",
                "使用期限": "2015年10月16日起2076",
                "land_use_term": "2015年10月16日起2076年12月28日止",
            },
        }
    )

    assert display["使用期限"] == "2015年10月16日起2076年12月28日止"
    assert "土地使用期限" not in display
