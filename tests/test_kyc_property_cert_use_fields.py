from __future__ import annotations

from backend.services.kyc_document_agent.renderer import render_markdown
from backend.services.kyc_document_agent.skills.property_cert_skill import extract


def _extract(text: str) -> dict:
    return extract({
        "text": text,
        "pages": [],
        "metadata": {"filename": "林勇产证.pdf", "customer_id": "customer-1", "source": "unit_test"},
    })


def test_land_and_house_use_are_extracted_from_separate_sections():
    result = _extract(
        "上海市 房地产权证\n"
        "土地状况\n"
        "权属性质 国有建设用地使用权\n"
        "用途 住宅用地\n"
        "宗地号 奉贤区光明镇2街坊1/5丘\n"
        "房屋状况\n"
        "建筑类型 公寓\n"
        "用途 居住\n"
        "总层数 14\n"
        "竣工日期 2011年\n"
    )

    assert result["fields"]["土地用途"] == "住宅用地"
    assert result["fields"]["land_use"] == "住宅用地"
    assert result["fields"]["房屋用途"] == "居住"
    assert result["fields"]["house_use"] == "居住"
    assert result["fields"]["use_type"] == "居住"
    assert result["fields"]["building_use"] == "居住"


def test_land_use_and_house_use_do_not_cross_pollute():
    result = _extract(
        "上海市 房地产权证\n"
        "土地状况\n"
        "用途 住宅用地\n"
        "宗地号 奉贤区光明镇2街坊1/5丘\n"
        "房屋状况\n"
        "建筑类型 公寓\n"
        "用途 居住\n"
        "总层数 14\n"
    )

    assert result["fields"]["土地用途"] == "住宅用地"
    assert result["fields"]["土地用途"] != "居住"
    assert result["fields"]["房屋用途"] == "居住"
    assert result["fields"]["房屋用途"] != "住宅用地"


def test_property_cert_markdown_displays_both_use_fields_without_generic_usage():
    markdown = render_markdown({
        "doc_type": "property_cert",
        "doc_type_name": "房产证/房地产权证",
        "fields": {
            "权利人": "林勇、黄晓囡",
            "权证编号": "沪房地奉字(2014)第004478号",
            "土地用途": "住宅用地",
            "宗地号": "奉贤区光明镇2街坊1/5丘",
            "建筑类型": "公寓",
            "房屋用途": "居住",
            "竣工日期": "2011年",
        },
    })

    assert "土地用途: 住宅用地" in markdown
    assert "房屋用途: 居住" in markdown
    assert "\n- 用途: 住宅用地" not in markdown
    assert "\n- 用途: 居住" not in markdown
    assert markdown.index("土地用途: 住宅用地") < markdown.index("宗地号: 奉贤区光明镇2街坊1/5丘")
    assert markdown.index("建筑类型: 公寓") < markdown.index("房屋用途: 居住")
