from backend.services.property_cert_agent.renderer import render_markdown
from backend.services.property_cert_agent.skills.attachment_page_skill import extract_attachment_house_details


COLUMN_TEXT = """
附记
房屋状况
室号或部位
200号1层
200号2层
200号3层
200号4层
200号5层
200号6层
14幢1-2层、4层东2间
建筑面积
2783.21
2638.43
2924.16
1756.08
1115.72
590.40
371.00
用途
商业
类型
商场
总层数
6
4
竣工日期
1990年
1979年
"""


def test_attachment_column_summary_extracts_house_details_without_unit_number() -> None:
    details = extract_attachment_house_details(COLUMN_TEXT)

    assert details["不动产单元号列表"] == []
    assert details["房屋用途列表"] == ["商业"]
    assert details["建筑类型列表"] == ["商场"]
    assert details["总层数列表"] == ["6", "4"]
    assert details["竣工日期列表"] == ["1990年", "1979年"]
    assert "200号6层" in details["室号或部位列表"]
    assert "14幢1-2层、4层东2间" in details["室号或部位列表"]


def test_renderer_displays_attachment_summary_when_rows_are_unavailable() -> None:
    markdown = render_markdown(
        {
            "fields": {"权利人": "北京咏旺物业管理有限公司", "建筑类型": "商场"},
            "risk_sections": {
                "附记": [
                    {
                        "室号或部位列表": ["200号1层", "200号2层"],
                        "房屋用途列表": ["商业"],
                        "建筑类型列表": ["商场"],
                        "总层数列表": ["6", "4"],
                        "竣工日期列表": ["1990年", "1979年"],
                    }
                ]
            },
            "page_roles": ["new_real_estate_detail_page", "attachment_page"],
            "metadata": {"filename": "咏旺不动产权证.pdf"},
        }
    )

    assert "### 附记明细" in markdown
    assert "- 室号或部位: 200号1层、200号2层" in markdown
    assert "- 房屋用途: 商业" in markdown
    assert "- 建筑类型: 商场" in markdown
