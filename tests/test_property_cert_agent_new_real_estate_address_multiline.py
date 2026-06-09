from backend.services.property_cert_agent.renderer import render_markdown
from backend.services.property_cert_agent.skills.new_real_estate_cert_skill import extract


MULTILINE_ADDRESS_TEXT = """
权利人
北京咏旺物业管理有限公司
坐落
惠南镇东门大街200号1-6层，惠南镇东门大街200号14
幢1-2层、4层东2间
不动产单元号
详见附记
权利类型
国有建设用地使用权/房屋所有权
"""


EXPECTED_ADDRESS = "惠南镇东门大街200号1-6层，惠南镇东门大街200号14幢1-2层、4层东2间"


def test_new_real_estate_skill_extracts_multiline_address_until_next_label() -> None:
    fields = extract({"text": MULTILINE_ADDRESS_TEXT})["fields"]
    assert fields["坐落"] == EXPECTED_ADDRESS


def test_multiline_address_renders_without_truncation() -> None:
    fields = extract({"text": MULTILINE_ADDRESS_TEXT})["fields"]
    markdown = render_markdown(
        {
            "fields": fields,
            "page_roles": ["new_real_estate_detail_page"],
            "metadata": {"filename": "咏旺不动产权证.pdf"},
        }
    )

    assert f"坐落: {EXPECTED_ADDRESS}" in markdown
    assert "坐落: 惠南镇东门大街200号1-6层，惠南镇东门大街200号14\n" not in markdown
