from __future__ import annotations

from backend.services.kyc_document_agent.renderer import render_markdown
from backend.services.kyc_document_agent.skills.property_cert_skill import extract


def _extract(text: str) -> dict:
    return extract({
        "text": text,
        "pages": [],
        "metadata": {"filename": "林勇产证.pdf", "customer_id": "customer-1", "source": "unit_test"},
    })


def test_land_use_extracts_from_normal_usage_label():
    result = _extract("上海市 房地产权证\n土地状况\n权属性质 国有建设用地使用权\n用途 住宅用地\n宗地号 奉贤区光明镇2街坊1/5丘\n房屋状况")

    assert result["fields"]["用途"] == "住宅用地"
    assert result["fields"]["land_use"] == "住宅用地"


def test_land_use_extracts_from_spaced_usage_label():
    result = _extract("上海市 房地产权证\n土地状况\n权属性质 国有建设用地使用权\n用 途 住宅用地\n宗地号 奉贤区光明镇2街坊1/5丘\n房屋状况")

    assert result["fields"]["用途"] == "住宅用地"
    assert result["fields"]["land_use"] == "住宅用地"


def test_land_use_extracts_from_broken_usage_lines():
    result = _extract("上海市 房地产权证\n土地状况\n权属性质 国有建设用地使用权\n用\n途\n住宅用地\n宗地号 奉贤区光明镇2街坊1/5丘\n房屋状况")

    assert result["fields"]["用途"] == "住宅用地"


def test_completion_date_extracts_from_normal_label():
    result = _extract("上海市 房地产权证\n房屋状况\n建筑类型 公寓\n竣工日期 2011年\n填证单位 奉贤区")

    assert result["fields"]["竣工日期"] == "2011年"
    assert result["fields"]["completion_date"] == "2011年"


def test_completion_date_extracts_from_spaced_label():
    result = _extract("上海市 房地产权证\n房屋状况\n建筑类型 公寓\n竣 工 日 期 2011年\n填证单位 奉贤区")

    assert result["fields"]["竣工日期"] == "2011年"


def test_completion_date_extracts_from_next_line():
    result = _extract("上海市 房地产权证\n房屋状况\n建筑类型 公寓\n竣工日期\n2011年\n填证单位 奉贤区")

    assert result["fields"]["竣工日期"] == "2011年"


def test_completion_date_rejects_non_date_values():
    result = _extract("上海市 房地产权证\n房屋状况\n使用权面积 独用\n建筑类型 公寓\n用途 居住\n竣工日期 独用\n填证单位 奉贤区")

    assert "竣工日期" not in result["fields"]
    assert "completion_date" not in result["fields"]


def test_property_cert_markdown_displays_usage_and_completion_date():
    result = _extract(
        "上海市 房地产权证\n"
        "土地状况\n"
        "权属性质 国有建设用地使用权\n"
        "用途 住宅用地\n"
        "宗地号 奉贤区光明镇2街坊1/5丘\n"
        "房屋状况\n"
        "建筑类型 公寓\n"
        "用途 居住\n"
        "竣工日期 2011年\n"
    )
    markdown = render_markdown(result)

    assert "用途: 住宅用地" in markdown
    assert "房屋用途: 居住" in markdown
    assert "竣工日期: 2011年" in markdown
    assert "土地用途" not in markdown
    assert "竣工日期: 独用" not in markdown
