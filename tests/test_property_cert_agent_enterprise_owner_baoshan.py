from __future__ import annotations

from backend.services.property_cert_agent import run_property_cert_agent


BAOSHAN_ENTERPRISE_TEXT = """
沪（2022）宝字
不动产权第011468
号
权利人
智先生数字科技（上海）有限公司
共有情况
单独所有
坐落
殷高西路101号
不动产单元号
310113015003GB00011F00020088
权利类型
国有建设用地使用权/房屋所有权
权利性质
出让
土地用途：其它商服用地/房屋用途：办公；宗地面积：8615.00平方米
地号
宝山区高境镇9街坊73/7丘；
使用权面积：相应的土地面积；
独用面积：；
分摊面积：。
房屋状况：
室号部位：306；
类型：办公楼；
总层数：17；
竣工日期：2007年。
建筑面积
800.35平方米
使用期限
2018年8月8日起2046年8月10日止
封面编号
D31003610514
"""


def test_enterprise_owner_baoshan_markdown_is_standardized() -> None:
    result = run_property_cert_agent(
        {
            "text": BAOSHAN_ENTERPRISE_TEXT,
            "metadata": {"filename": "企业房产证.pdf", "declared_doc_type": "property_cert"},
        }
    )
    fields = result["fields"]
    markdown = result["markdown"]

    assert fields["权利人"] == "智先生数字科技（上海）有限公司"
    assert fields["权证编号"] == "沪(2022)宝字不动产权第011468号"
    assert fields["封面编号"] == "D31003610514"
    assert fields["土地用途"] == "其它商服用地"
    assert fields["房屋用途"] == "办公"
    assert fields["地号"] == "宝山区高境镇9街坊73/7丘"
    assert fields["宗地面积"] == "8615.00 平方米"
    assert fields["建筑面积"] == "800.35 平方米"
    assert fields["建筑类型"] == "办公楼"

    assert "权证编号: D31003610514" not in markdown
    assert "权证编号: 沪(2022)宝字不动产权第011468号" in markdown
    assert "土地用途: 其它商服用地" in markdown
    assert "房屋用途: 办公" in markdown
    assert "地号: 宝山区高境镇9街坊73/7丘" in markdown
    assert "建筑类型: 办公楼" in markdown
    assert "\\n" not in markdown
    assert "使用权面积" not in markdown
    assert "独用面积" not in markdown
    assert "分摊面积" not in markdown
