from backend.services.property_cert_agent.agent import run_property_cert_agent
from backend.services.property_cert_agent.merger import merge_pages
from backend.services.property_cert_agent.page_role import detect_page_role
from backend.services.property_cert_agent.skills.attachment_page_skill import extract as extract_attachment_page


DETAIL_TEXT = """
权利人
北京咏旺物业管理有限公司
共有情况
单独所有
权证编号
沪(2023)浦字不动产权第077133号
封面编号
D31004264460
坐落
惠南镇东门大街200号1-6层，惠南镇东门大街200号14幢1-2层、4层东2间
不动产单元号
详见附记
权利类型
国有建设用地使用权/房屋所有权
权利性质
出让
土地用途
商业用地
房屋用途
详见附记
地号
惠南镇6街坊33/2丘
宗地面积
5402.00平方米
建筑面积
12379.00平方米
使用期限
2023年08月09日起2042年08月21日止
建筑类型
国有
登记机构
不动产登记专用章
"""

ATTACHMENT_TEXT = """
不动产单元号 使用权面积 房屋状况 室号或部位 建筑面积 类型 用途 总层数 竣工日期
3101
15030001GB00325F00020006 使用权 200号1层 2783.21平方米 商业 商场 总层数6 1990年
3101
15030001GB00325F00020005 使用权 200号2层 2838.43平方米 商业 商场 总层数6 1990年
3101
15030001GB00325F00020004 使用权 200号3层 2924.16平方米 商业 商场 总层数6 1990年
3101
15030001GB00325F00020003 使用权 200号4层 1756.08平方米 商业 商场 总层数6 1990年
3101
15030001GB00325F00020001 使用权 200号5层 1115.72平方米 商业 商场 总层数6 1990年
3101
15030001GB00325P00020002 使用权 200号6层 590.40平方米 商业 商场 总层数6 1990年
3101
15030001GB00325F00030001 使用权 14 1.2层、4层东2间 371.00平方米 商业 商场 总层数4 1979年
使用权 4层东2间 371.00平方米 商业 商场 总层数4 1979年
合计
"""

EXPECTED_UNITS = [
    "310115030001GB00325F00020006",
    "310115030001GB00325F00020005",
    "310115030001GB00325F00020004",
    "310115030001GB00325F00020003",
    "310115030001GB00325F00020001",
    "310115030001GB00325F00020002",
    "310115030001GB00325F00030001",
]


def test_yongwang_page_5_is_attachment_page_without_attachment_title() -> None:
    assert detect_page_role(ATTACHMENT_TEXT, {"page_no": 5}) == "attachment_page"


def test_attachment_page_skill_extracts_yongwang_lists() -> None:
    fields = extract_attachment_page({"text": ATTACHMENT_TEXT, "metadata": {"page_no": 5}})["fields"]

    assert fields["房屋用途列表"] == ["商业"]
    assert fields["建筑类型列表"] == ["商场"]
    assert fields["总层数列表"] == ["6", "4"]
    assert fields["竣工日期列表"] == ["1990年", "1979年"]
    assert "200号1层" in fields["室号或部位列表"]
    assert "14幢1-2层、4层东2间" in fields["室号或部位列表"]
    assert fields["不动产单元号列表"] == EXPECTED_UNITS


def test_merger_backfills_yongwang_attachment_fields() -> None:
    attachment_fields = extract_attachment_page({"text": ATTACHMENT_TEXT, "metadata": {"page_no": 5}})["fields"]
    merged = merge_pages(
        [
            {"page_role": "new_real_estate_detail_page", "fields": {"房屋用途": "详见附记", "建筑类型": "国有"}},
            {"page_role": "attachment_page", "fields": attachment_fields},
        ]
    )

    assert merged["fields"]["房屋用途"] == "商业"
    assert merged["fields"]["建筑类型"] == "商场"
    assert merged["fields"]["总层数"] == "6、4"
    assert merged["fields"]["竣工日期"] == "1990年、1979年"
    assert merged["fields"]["不动产单元号"] == "、".join(EXPECTED_UNITS)


def test_yongwang_attachment_backfills_final_markdown() -> None:
    result = run_property_cert_agent(
        {
            "metadata": {
                "filename": "咏旺不动产权证.pdf",
                "raw_pages": [
                    {"page": 1, "text": DETAIL_TEXT, "metadata": {"page_no": 1}},
                    {"page": 2, "text": ATTACHMENT_TEXT, "metadata": {"page_no": 2}},
                ],
            }
        }
    )
    markdown = result["markdown"]

    assert "attachment_page" in result["page_roles"]
    assert "不动产单元号: 详见附记" not in markdown
    assert "房屋用途: 详见附记" not in markdown
    assert "详见附记" not in markdown
    assert "不动产单元号: 使用权" not in markdown
    assert "建筑类型: 国有" not in markdown
    assert f"不动产单元号: {'、'.join(EXPECTED_UNITS)}" in markdown
    assert "权利人: 北京咏旺物业管理有限公司" in markdown
    assert "房屋用途: 商业" in markdown
    assert "建筑类型: 商场" in markdown
    assert "室号或部位: 200号1层、200号2层、200号3层、200号4层、200号5层、200号6层、14幢1-2层、4层东2间" in markdown
    assert "总层数: 6、4" in markdown
    assert "竣工日期: 1990年、1979年" in markdown
    assert "建筑面积: 12379.00 平方米" in markdown
    assert "### 附记明细" in markdown
    assert "| 不动产单元号 | 室号或部位 | 建筑面积 | 房屋用途 | 建筑类型 | 总层数 | 竣工日期 |" in markdown
    assert "| 310115030001GB00325F00020006 | 200号1层 | 2783.21平方米 | 商业 | 商场 | 6 | 1990年 |" in markdown
