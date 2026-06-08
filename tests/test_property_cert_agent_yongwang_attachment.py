from backend.services.property_cert_agent.agent import run_property_cert_agent


DETAIL_TEXT = """
权利人
上海咏旺企业管理有限公司
权证编号
沪(2021)浦字不动产权第000001号
坐落
惠南镇东门大街200号
不动产单元号
详见附记
权利类型
国有建设用地使用权/房屋所有权
权利性质
出让
土地用途
其它商服用地
房屋用途
详见附记
宗地面积
12000.00平方米
建筑面积
5421.64平方米
使用期限
2010年1月1日起2040年1月1日止
"""

ATTACHMENT_TEXT = """
附记
不动产单元号 房屋用途 类型 建筑面积 总层数 竣工日期
310115012345GB00012F00010001 商业 商场 2783.21平方米 总层数6 1990年
310115012345GB00012F00010002 商业 商场 2638.43平方米 总层数6 1990年
合计
"""


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

    assert "不动产单元号: 详见附记" not in markdown
    assert "房屋用途: 详见附记" not in markdown
    assert "详见附记" not in markdown
    assert "不动产单元号: 310115012345GB00012F00010001、310115012345GB00012F00010002" in markdown
    assert "房屋用途: 商业" in markdown
    assert "建筑类型: 商场" in markdown
