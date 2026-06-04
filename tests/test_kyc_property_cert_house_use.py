from __future__ import annotations

from backend.services.kyc_document_agent.renderer import render_markdown
from backend.services.kyc_document_agent.skills.property_cert_skill import extract


def _extract(text: str) -> dict:
    return extract({
        "text": text,
        "pages": [],
        "metadata": {"filename": "林勇产证.pdf", "customer_id": "customer-1", "source": "unit_test"},
    })


def test_house_use_extracts_from_normal_usage_label():
    result = _extract("上海市 房地产权证\n土地状况\n用途 住宅用地\n房屋状况\n建筑类型 公寓\n用途 居住\n总层数 14\n竣工日期 2011年")

    assert result["fields"]["土地用途"] == "住宅用地"
    assert result["fields"]["房屋用途"] == "居住"
    assert result["fields"]["house_use"] == "居住"
    assert result["fields"]["building_use"] == "居住"
    assert result["fields"]["use_type"] == "居住"


def test_house_use_extracts_from_spaced_usage_label():
    result = _extract("上海市 房地产权证\n土地状况\n用 途 住宅用地\n房屋状况\n建筑类型 公寓\n用 途 居住\n总层数 14")

    assert result["fields"]["土地用途"] == "住宅用地"
    assert result["fields"]["房屋用途"] == "居住"


def test_house_use_extracts_from_broken_usage_lines():
    result = _extract("上海市 房地产权证\n土地状况\n用\n途\n住宅用地\n房屋状况\n建筑类型 公寓\n用\n途\n居住\n总层数 14")

    assert result["fields"]["土地用途"] == "住宅用地"
    assert result["fields"]["房屋用途"] == "居住"


def test_house_use_fallback_from_building_section_context():
    result = _extract("上海市 房地产权证\n土地状况\n用途 住宅用地\n房屋状况\n室号或部位 1101\n建筑面积 148.08 平方米\n建筑类型 公寓\n居住\n总层数 14\n竣工日期 2011年")

    assert result["fields"]["房屋用途"] == "居住"
    assert result["fields"]["土地用途"] == "住宅用地"


def test_property_cert_display_contains_house_and_land_use_without_generic_usage():
    result = _extract("上海市 房地产权证\n土地状况\n用途 住宅用地\n宗地号 奉贤区光明镇2街坊1/5丘\n房屋状况\n建筑类型 公寓\n用途 居住\n总层数 14\n竣工日期 2011年")
    markdown = render_markdown(result)

    assert "土地用途: 住宅用地" in markdown
    assert "房屋用途: 居住" in markdown
    assert "\n- 用途: 居住" not in markdown
    assert "\n- 用途: 住宅用地" not in markdown
    assert "房屋用途: 住宅用地" not in markdown
    assert "土地用途: 居住" not in markdown
    assert markdown.index("建筑类型: 公寓") < markdown.index("房屋用途: 居住") < markdown.index("总层数: 14")
